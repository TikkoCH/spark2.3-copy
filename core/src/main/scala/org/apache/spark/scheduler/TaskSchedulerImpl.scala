/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.{Locale, Timer, TimerTask}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.Set
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, ThreadUtils, Utils}

/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a `LocalSchedulerBackend` and setting
 * isLocal to true. It handles common logic, like determining a scheduling order across jobs, waking
 * up to launch speculative tasks, etc.
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * runTasks method.
 *
 * THREADING: [[SchedulerBackend]]s and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * [[SchedulerBackend]]s synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
 */
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int, // 最大失败次数
    isLocal: Boolean = false) // 是否本地
  extends TaskScheduler with Logging {

  import TaskSchedulerImpl._

  def this(sc: SparkContext) = {
    this(sc, sc.conf.get(config.MAX_TASK_FAILURES))
  }

  // Lazily initializing blackListTrackOpt to avoid getting empty ExecutorAllocationClient,
  // because ExecutorAllocationClient is created after this TaskSchedulerImpl.
  private[scheduler] lazy val blacklistTrackerOpt = maybeCreateBlacklistTracker(sc)
  // SparkConf
  val conf = sc.conf

  // How often to check for speculative tasks
  /** 任务推断执行的时间间隔,可通过spark.speculation.interval配置,默认100ms*/
  val SPECULATION_INTERVAL_MS = conf.getTimeAsMs("spark.speculation.interval", "100ms")

  // Duplicate copies of a task will only be launched if the original copy has been running for
  // at least this amount of time. This is to avoid the overhead of launching speculative copies
  // of tasks that are very short.
  /** 用于保证原始任务至少运行的时间,原始任务只有超过此时间限制,才允许启动副本任务*/
  val MIN_TIME_TO_SPECULATION = 100
  /** 对任务进行推断执行的ScheduledThreadPoolExecutor*/
  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")

  // Threshold above which we warn user initial TaskSet may be starved
  /** 判断TaskSet饥饿的阈值.*/
  val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // CPUs to request per task
  /** 每个task需要分配CPU核心数*/
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
  /** StageId,Attempt,TaskSetManager的二级缓存*/
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  // Protected by `this`
  /** task和TaskSetManager之间的映射关系*/
  private[scheduler] val taskIdToTaskSetManager = new HashMap[Long, TaskSetManager]
  /** Task与执行Task的Executor之间的映射缓存*/
  val taskIdToExecutorId = new HashMap[Long, String]
  /** 标记TaskShcedulerImpl是否已经接收到Task*/
  @volatile private var hasReceivedTask = false
  /** 标记TaskShcedulerImpl接受的Task是否已经运行.*/
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  /** 用于生成新提交Task的id*/
  val nextTaskId = new AtomicLong(0)

  // IDs of the tasks running on each executor
  /** 缓存Executor与运行在此Executor上task之间的映射关系.*/
  private val executorIdToRunningTaskIds = new HashMap[String, HashSet[Long]]

  def runningTasksByExecutors: Map[String, Int] = synchronized {
    executorIdToRunningTaskIds.toMap.mapValues(_.size)
  }

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  /** executor和host之间的映射缓存*/
  protected val hostToExecutors = new HashMap[String, HashSet[String]]

  /** host和机架之间的映射*/
  protected val hostsByRack = new HashMap[String, HashSet[String]]
  /** executorId和host之间的映射*/
  protected val executorIdToHost = new HashMap[String, String]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null
  /** 调度后端接口*/
  var backend: SchedulerBackend = null
  /** mapOutTrackerMaster*/
  val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
  /** 调度池构建器*/
  private var schedulableBuilder: SchedulableBuilder = null
  // default scheduler is FIFO
  private val schedulingModeConf = conf.get(SCHEDULER_MODE_PROPERTY, SchedulingMode.FIFO.toString)
  /** 调度模式*/
  val schedulingMode: SchedulingMode =
    try {
      SchedulingMode.withName(schedulingModeConf.toUpperCase(Locale.ROOT))
    } catch {
      case e: java.util.NoSuchElementException =>
        throw new SparkException(s"Unrecognized $SCHEDULER_MODE_PROPERTY: $schedulingModeConf")
    }
  /** 根调度池*/
  val rootPool: Pool = new Pool("", schedulingMode, 0, 0)

  // This is a var so that we can reset it for testing purposes.
  /** 作用是通过线程池对Slave发送Task的执行结果进行处理*/
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }
  /** 本类初始化的必经方法*/
  def initialize(backend: SchedulerBackend) {
    // 设置backend
    this.backend = backend
    schedulableBuilder = {
      // 根据调度模式创建相应的调度池构建器
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported $SCHEDULER_MODE_PROPERTY: " +
          s"$schedulingMode")
      }
    }
    // 创建builder之后直接创建一个调度池.
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()
  /** 启动TaskSchedulerImpl*/
  override def start() {
    // 先启动后端
    backend.start()

    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      // 固定间隔的调度方法,实际执行了checkSpeculatableTasks
      speculationScheduler.scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
          checkSpeculatableTasks()
        }
      }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }
  /**
    * TaskSchedulerImpl对任务资源的运行依赖于SchedulerBackend,为了避免
    * TaskSchedulerImpl在SchedulerBackend准备就绪之前就将Task交给SchedulerBackend处理,
    * 因此该方法用于等待SchedulerBackend准备就绪
    * */
  override def postStartHook() {
    waitBackendReady()
  }
  /** Dag将Stage中各个分区的Task封装成TaskSet,然后本类对象提交,这个方法就是提交task*/
  override def submitTasks(taskSet: TaskSet) {
    // 获取所有task
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      // 创建TaskSetManager
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      // 获取stageId
      val stage = taskSet.stageId
      // 从缓存中获取或设置TaskSet关联的Stage,Stage尝试及刚创建的TaskSetManager之间的关系.
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
      stageTaskSets(taskSet.stageAttemptId) = manager
      // 是否有冲突的TaskSet,taskSetsByStageIdAndAttempt中不应存在stage相同,task不相同的情况
      val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
        ts.taskSet != taskSet && !ts.isZombie
      }
      // 如果冲突,抛异常
      if (conflictingTaskSet) {
        throw new IllegalStateException(s"more than one active taskSet for stage $stage:" +
          s" ${stageTaskSets.toSeq.map{_._2.taskSet.id}.mkString(",")}")
      }
      // 将TaskSetManager天交到调度池builder中的调度池中.
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
      // 如果不是本地模式并且没有收到task
      if (!isLocal && !hasReceivedTask) {
        // 定期检查饥饿状态
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      // 改成已接收task状态
      hasReceivedTask = true
    }
    // 该方法给Task分配资源并运行Task
    backend.reviveOffers()
  }

  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  // private[scheduler]标签用于允许测试环境切换不同的taskSetmanager
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager = {
    // 创建一个TaskSetManager
    new TaskSetManager(this, taskSet, maxTaskFailures, blacklistTrackerOpt)
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        // There are two possible cases here:
        // 1. The task set manager has been created and some tasks have been scheduled.
        //    In this case, send a kill signal to the executors to kill the task and then abort
        //    the stage.
        // 2. The task set manager has been created but no tasks has been scheduled. In this case,
        //    simply abort the stage.
        tsm.runningTasksSet.foreach { tid =>
            taskIdToExecutorId.get(tid).foreach(execId =>
              backend.killTask(tid, execId, interruptThread, reason = "Stage cancelled"))
        }
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  override def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    logInfo(s"Killing task $taskId: $reason")
    val execId = taskIdToExecutorId.get(taskId)
    if (execId.isDefined) {
      backend.killTask(taskId, execId.get, interruptThread, reason)
      true
    } else {
      logWarning(s"Could not kill task $taskId because no task with that ID was found.")
      false
    }
  }

  /**
   * 调用以指示与给定TaskSetManager关联的所有任务尝试（包括推测任务）已完成，
    * 因此应清除与TaskSetManager关联的状态。
    * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      taskSetsForStage -= manager.taskSet.stageAttemptId
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    manager.parent.removeSchedulable(manager)
    logInfo(s"Removed TaskSet ${manager.taskSet.id}, whose tasks have all completed, from pool" +
      s" ${manager.parent.name}")
  }
  /** 给单个TaskSet中的Task提供资源*/
  private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      tasks: IndexedSeq[ArrayBuffer[TaskDescription]]) : Boolean = {
    // 是否启动了任务
    var launchedTask = false
    // nodes and executors that are blacklisted for the entire application have already been
    // filtered out by this point
    for (i <- 0 until shuffledOffers.size) {
      // 获取ExecutorId
      val execId = shuffledOffers(i).executorId
      // 获取主机
      val host = shuffledOffers(i).host
      // 如果可用cpu数大于CPUS_PER_TASK
      if (availableCpus(i) >= CPUS_PER_TASK) {
        try {
          // 给符合条件的Task创建TaskDescription
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            // 添加到tasks数组
            tasks(i) += task
            // taskid
            val tid = task.taskId
            // 更新缓存
            taskIdToTaskSetManager(tid) = taskSet
            taskIdToExecutorId(tid) = execId
            executorIdToRunningTaskIds(execId).add(tid)
            // 减去核心数
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            // 改为启动了task
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    return launchedTask // 返回是否给taskSet中的某个task分配到了资源.
  }

  /**
   * 集群manager调用来提供slave上的资源,我们通过按优先顺序询问我们的活动taskset来执行任务。以循环方式
    * 为每个节点填充任务,以便整个集群任务的负载均衡.
    * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(offers: IndexedSeq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // 标记活动的slave并记住其主机名,如果新executor添加进来对其追踪
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
    var newExecAvail = false
    for (o <- offers) {
      // 如果不包含offer的host
      if (!hostToExecutors.contains(o.host)) {
        // 创建一个新映射
        hostToExecutors(o.host) = new HashSet[String]()
      }
      // 如果缓存中不包含offer的executorId
      if (!executorIdToRunningTaskIds.contains(o.executorId)) {
        // 更新缓存
        hostToExecutors(o.host) += o.executorId
        // 投递ExecutorAdded事件.
        executorAdded(o.executorId, o.host)
        executorIdToHost(o.executorId) = o.host
        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
        // 标记添加了新的Executor
        newExecAvail = true
      }
      // 遍历机架,更新haost与机架之间的关系
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }
    // 在发出任何offer之前，删除黑名单已过期的黑名单中的所有节点。
    // 这样做是为了避免单独的线程并增加同步开销，并且还因为更新黑名单仅在进行任务提供时才相关。
    // Before making any offers, remove any nodes from the blacklist whose blacklist has expired. Do
    // this here to avoid a separate thread and added synchronization overhead, and also because
    // updating the blacklist is only relevant when task offers are being made.
    blacklistTrackerOpt.foreach(_.applyBlacklistTimeout())

    val filteredOffers = blacklistTrackerOpt.map { blacklistTracker =>
      offers.filter { offer =>
        !blacklistTracker.isNodeBlacklisted(offer.host) &&
          !blacklistTracker.isExecutorBlacklisted(offer.executorId)
      }
    }.getOrElse(offers)
    //  对所有orkerOffer进行随机洗牌,避免将任务总是分配给同样一组worker
    val shuffledOffers = shuffleOffers(filteredOffers)
    // Build a list of tasks to assign to each worker.
    // 根据每个Workeroffer的可用CPu核心数,创建同等大小的任务描述数组
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores / CPUS_PER_TASK))
    // 将每个WorkerOffer可用的cpu放入数组
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    // 对rootPool中所有TaskSetManager按照调度算法排序
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
        // 每个taskSet从新计算本地性.
        taskSet.executorAdded()
      }
    }
    // 按照我们的调度顺序获取每个TaskSet，然后按地点级别的递增顺序为每个节点提供它，
    // 以便它有机会在所有节点上启动本地任务。 注意：preferredLocality顺序：
    // PROCESS_LOCAL，NODE_LOCAL，NO_PREF，RACK_LOCAL，ANY
    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    for (taskSet <- sortedTaskSets) {
      var launchedAnyTask = false
      var launchedTaskAtCurrentMaxLocality = false
      for (currentMaxLocality <- taskSet.myLocalityLevels) {
        // 按照本地性大小顺序
        do {
          // 给单个TaskSet中的Task提供资源
          launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(
            taskSet, currentMaxLocality, shuffledOffers, availableCpus, tasks)
          launchedAnyTask |= launchedTaskAtCurrentMaxLocality
        } while (launchedTaskAtCurrentMaxLocality)
      }
      if (!launchedAnyTask) {
        // 如果TaskSet中没有任何任务获得了资源,放弃黑名单中的Task
        taskSet.abortIfCompletelyBlacklisted(hostToExecutors)
      }
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks // 返回已经获得资源的任务列表
  }

  /**
   * Shuffle offers around to avoid always placing tasks on the same workers.  Exposed to allow
   * overriding in tests, so it can be deterministic.
   */
  protected def shuffleOffers(offers: IndexedSeq[WorkerOffer]): IndexedSeq[WorkerOffer] = {
    Random.shuffle(offers)
  }
  /** 用于更新task状态*/
  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var failedExecutor: Option[String] = None
    var reason: Option[ExecutorLossReason] = None
    synchronized {
      try {
        // 从缓存中获取task对应的TaskSetManager
        taskIdToTaskSetManager.get(tid) match {
          case Some(taskSet) =>
            if (state == TaskState.LOST) {
              // 如果task状态是LOST
              // TaskState.LOST is only used by the deprecated Mesos fine-grained scheduling mode,
              // where each executor corresponds to a single task, so mark the executor as failed.
              val execId = taskIdToExecutorId.getOrElse(tid, throw new IllegalStateException(
                "taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)"))
              if (executorIdToRunningTaskIds.contains(execId)) {
                // 如果executor上正在运行task
                reason = Some(
                  SlaveLost(s"Task $tid was lost, so marking the executor as lost as well."))
                // 删除Executor
                removeExecutor(execId, reason.get)
                failedExecutor = Some(execId)
              }
            }
            if (TaskState.isFinished(state)) {
              // 如果task状态是完成状态.FINISHED, FAILED, KILLED, LOST
              // 清除task在taskIdToTaskSetManager,taskIdToExecutorId等缓存中的数据
              cleanupTaskState(tid)
              // 减少正在运行任务的数量
              taskSet.removeRunningTask(tid)
              if (state == TaskState.FINISHED) {
                // 如果是finished,处理成功的taskresult
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                // 如果是FAILED,KILLED,LOST,对失败tsakresult处理
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates) or its " +
                "executor has been marked as failed.")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    // 安置丢失的Executor上正在运行的Task.
    if (failedExecutor.isDefined) {
      assert(reason.isDefined)
      // 发送ExecutorLost消息
      dagScheduler.executorLost(failedExecutor.get, reason.get)
      // 给Task分配资源并运行task
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  override def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = synchronized {
      accumUpdates.flatMap { case (id, updates) =>
        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
        taskIdToTaskSetManager.get(id).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
        }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }
  /** 获取taskResult之后处理*/
  def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskFailedReason): Unit = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && !taskSetManager.someAttemptSucceeded(tid)) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw new SparkException(s"Exiting due to error from cluster scheduler: $message")
      }
    }
  }

  override def stop() {
    speculationScheduler.shutdown()
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    starvationTimer.cancel()
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  /** 检查活动job中的所有推断任务*/
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks(MIN_TIME_TO_SPECULATION)
    }
    if (shouldRevive) {
      // 如果有可以推断执行的任务,以localSchedulerBackend为例,
      // 关于向localEndpoint发送ReviveOffers消息,LocalEndpoint接收到
      // ReviveOffers调用其reviveOffers方法分配资源运行Task.
      backend.reviveOffers()
    }
  }
  /** 处理丢失Executor*/
  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      // 如果executorIdToRunningTaskIds中包含ExecutorId
      // 说明Executor上有task正在运行
      if (executorIdToRunningTaskIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        // 删除Executor
        removeExecutor(executorId, reason)
        // 将failedExecutor赋值为此executor
        failedExecutor = Some(executorId)
      } else {
        // 不包含ExecutorId,说明没有task正在运行
        // 获取host
        executorIdToHost.get(executorId) match {
          case Some(hostPort) =>
            // 如果仍然存在host映射,说明我们不知道executor的失联原因.
            // 当最终知道原因时,调用removeExecutor来更新运行在该executor上的task
            // If the host mapping still exists, it means we don't know the loss reason for the
            // executor. So call removeExecutor() to update tasks running on that executor when
            // the real loss reason is finally known.
            logExecutorLoss(executorId, hostPort, reason)
            removeExecutor(executorId, reason)

          case None =>
            // 我们可能因不同的失联原因多次调用executorLost
            // We may get multiple executorLost() calls with different loss reasons. For example,
            // one may be triggered by a dropped connection from the slave while another may be a
            // report of executor termination from Mesos. We produce log messages for both so we
            // eventually report the termination reason.
            logError(s"Lost an executor $executorId (already removed): $reason")
        }
      }
    }
    // 不持锁调用dagScheduler.executorLost,防止死锁
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get, reason)
      // 给task分配资源并运行task
      backend.reviveOffers()
    }
  }

  override def workerRemoved(workerId: String, host: String, message: String): Unit = {
    logInfo(s"Handle removed worker $workerId: $message")
    dagScheduler.workerRemoved(workerId, host, message)
  }

  private def logExecutorLoss(
      executorId: String,
      hostPort: String,
      reason: ExecutorLossReason): Unit = reason match {
    case LossReasonPending =>
      logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
    case ExecutorKilled =>
      logInfo(s"Executor $executorId on $hostPort killed by driver.")
    case _ =>
      logError(s"Lost executor $executorId on $hostPort: $reason")
  }

  /**
   * 清除TaskScheduler的状态以跟踪给定任务。
    * Cleans up the TaskScheduler's state for tracking the given task.
   */
  private def cleanupTaskState(tid: Long): Unit = {
    taskIdToTaskSetManager.remove(tid)
    taskIdToExecutorId.remove(tid).foreach { executorId =>
      executorIdToRunningTaskIds.get(executorId).foreach { _.remove(tid) }
    }
  }

  /**
   * 移除Executor及其对应的host和机架等缓存信息.
    * 从我们的所有数据结构中删除executor并将其标记为丢失。 如果executor的丢失原因尚不清楚，
    * 请不要删除其与主机的关联，也不要更新任何正在运行的任务的状态，因为丢失原因定义了我们是否会使这些任务失败。
    * Remove an executor from all our data structures and mark it as lost. If the executor's loss
   * reason is not yet known, do not yet remove its association with its host nor update the status
   * of any running tasks, since the loss reason defines whether we'll fail those tasks.
   */
  private def removeExecutor(executorId: String, reason: ExecutorLossReason) {
    // The tasks on the lost executor may not send any more status updates (because the executor
    // has been lost), so they should be cleaned up here.
    //
    executorIdToRunningTaskIds.remove(executorId).foreach { taskIds =>
      logDebug("Cleaning up TaskScheduler state for tasks " +
        s"${taskIds.mkString("[", ",", "]")} on failed executor $executorId")
      // We do not notify the TaskSetManager of the task failures because that will
      // happen below in the rootPool.executorLost() call.
      // 清除Executor上正在运行的task在TaskIdToTaskSetManager,taskIdToExecutorId缓存
      taskIds.foreach(cleanupTaskState)
    }

    val host = executorIdToHost(executorId)
    val execs = hostToExecutors.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      // 删除executor对应的host
      hostToExecutors -= host
      // 从机架中删除hosts中的host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          // 如果hosts为空了,删除机架
          hostsByRack -= rack
        }
      }
    }

    if (reason != LossReasonPending) {
      // executorIdToHost缓存中删除executor
      executorIdToHost -= executorId
      // 此Executor上正在运行的Task作为失败task处理,最后重新提交这些task
      rootPool.executorLost(executorId, host, reason)
    }
    // 黑名单中移除executor
    blacklistTrackerOpt.foreach(_.handleRemovedExecutor(executorId))
  }

  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    hostToExecutors.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    hostToExecutors.contains(host)
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.contains(execId)
  }

  def isExecutorBusy(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.get(execId).exists(_.nonEmpty)
  }

  /**
   * Get a snapshot of the currently blacklisted nodes for the entire application.  This is
   * thread-safe -- it can be called without a lock on the TaskScheduler.
   */
  def nodeBlacklist(): scala.collection.immutable.Set[String] = {
    blacklistTrackerOpt.map(_.nodeBlacklist()).getOrElse(scala.collection.immutable.Set())
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None
  /** 等待Backend就绪*/
  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    // 一直等待backend开始
    while (!backend.isReady) {
      // Might take a while for backend to be ready if it is waiting on resources.
      if (sc.stopped.get) {
        // For example: the master removes the application for some reason
        throw new IllegalStateException("Spark context stopped while waiting for backend")
      }
      synchronized {
        this.wait(100)
      }
    }
  }

  override def applicationId(): String = backend.applicationId()

  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  private[scheduler] def taskSetManagerForAttempt(
      stageId: Int,
      stageAttemptId: Int): Option[TaskSetManager] = {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId)
    } yield {
      manager
    }
  }

}


private[spark] object TaskSchedulerImpl {

  val SCHEDULER_MODE_PROPERTY = "spark.scheduler.mode"

  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used. The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given {@literal <h1, [o1, o2, o3]>}, {@literal <h2, [o4]>} and
   * {@literal <h3, [o5, o6]>}, returns {@literal [o1, o5, o4, o2, o6, o3]}.
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.getOrElse(key, null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size) {
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

  private def maybeCreateBlacklistTracker(sc: SparkContext): Option[BlacklistTracker] = {
    if (BlacklistTracker.isBlacklistEnabled(sc.conf)) {
      val executorAllocClient: Option[ExecutorAllocationClient] = sc.schedulerBackend match {
        case b: ExecutorAllocationClient => Some(b)
        case _ => None
      }
      Some(new BlacklistTracker(sc, executorAllocClient))
    } else {
      None
    }
  }

}

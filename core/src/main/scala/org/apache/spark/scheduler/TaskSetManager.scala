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

import java.io.NotSerializableException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.math.max
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.scheduler.SchedulingMode._
import org.apache.spark.util.{AccumulatorV2, Clock, SystemClock, Utils}
import org.apache.spark.util.collection.MedianHeap

/**
 * Schedules the tasks within a single TaskSet in the TaskSchedulerImpl. This class keeps track of
 * each task, retries tasks if they fail (up to a limited number of times), and
 * handles locality-aware scheduling for this TaskSet via delay scheduling. The main interfaces
 * to it are resourceOffer, which asks the TaskSet whether it wants to run a task on one node,
 * and statusUpdate, which tells it that one of its tasks changed state (e.g. finished).
 *
 * THREADING: This class is designed to only be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
 *
 * @param sched           the TaskSchedulerImpl associated with the TaskSetManager
 * @param taskSet         the TaskSet to manage scheduling for
 * @param maxTaskFailures if any particular task fails this number of times, the entire
 *                        task set will be aborted
 */
private[spark] class TaskSetManager(
    sched: TaskSchedulerImpl,   // TaskSetManager所属TaskSchedulerImpl
    val taskSet: TaskSet,       // 管理的TaskSet
    val maxTaskFailures: Int,   // 最大task失败次数
    blacklistTracker: Option[BlacklistTracker] = None,  // 黑名单跟踪器
    // 时间                                   //
    clock: Clock = new SystemClock()) extends Schedulable with Logging {
  // SparkConf
  private val conf = sched.sc.conf

  // SPARK-21563 make a copy of the jars/files so they are consistent across the TaskSet
  private val addedJars = HashMap[String, Long](sched.sc.addedJars.toSeq: _*)
  private val addedFiles = HashMap[String, Long](sched.sc.addedFiles.toSeq: _*)
  // 开始推断执行的任务分数
  // Quantile of tasks at which to start speculation
  val SPECULATION_QUANTILE = conf.getDouble("spark.speculation.quantile", 0.75)
  // 推断的乘数
  val SPECULATION_MULTIPLIER = conf.getDouble("spark.speculation.multiplier", 1.5)
  // 结果总大小的字节限制,默认1g,可通过spark.driver.MaxResultSize配置
  // Limit of bytes for total size of results (default is 1GB)
  val maxResultSize = Utils.getMaxResultSize(conf)
  // 是否开启推断
  val speculationEnabled = conf.getBoolean("spark.speculation", false)
  // SparkEnv
  // Serializer for closures and tasks.
  val env = SparkEnv.get
  // SparkEnv的closureSerializer的实例.
  val ser = env.closureSerializer.newInstance()
  // Task数组
  val tasks = taskSet.tasks
  // TaskSet的Task数量
  val numTasks = tasks.length
  // 对每个Task的赋值运行数进行记录的数组.按照索引与task数组的同一索引位置的task对应,记录task复制运行数量.
  val copiesRunning = new Array[Int](numTasks)
  // 对每个Task是否执行成功进行记录的数组.下标与task数组对应
  // For each task, tracks whether a copy of the task has succeeded. A task will also be
  // marked as "succeeded" if it failed with a fetch failure, in which case it should not
  // be re-run because the missing map data needs to be regenerated first.
  val successful = new Array[Boolean](numTasks)
  // 每个task的执行失败次数记录的数组.下标与task数组对应
  private val numFailures = new Array[Int](numTasks)
  // 当task被其他尝试task杀死时，设置boolean的相应索引，`spark.speculation`设置为true时生效。
  // 其他人结束的task不应该在executor丢失时重新提交。
  // Set the coresponding index of Boolean var when the task killed by other attempt tasks,
  // this happened while we set the `spark.speculation` to true. The task killed by others
  // should not resubmit while executor lost.
  private val killedByOtherAttempt: Array[Boolean] = new Array[Boolean](numTasks)
  // 每个task的所有执行尝试信息的数组,下标对应task数组
  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  // 执行成功的task数量
  private[scheduler] var tasksSuccessful = 0
  // 公平调度算法的权重
  val weight = 1
  // 用于FAIR算法的参考值
  val minShare = 0
  // 调度的优先级
  var priority = taskSet.priority
  // 调度池所属Stage的id
  var stageId = taskSet.stageId
  // TaskSetManager名称,可能会参与FAIR算法.
  val name = "TaskSet_" + taskSet.id
  // TaskSetManager的父pool
  var parent: Pool = null
  // 所有Task执行结果总大小
  private var totalResultSize = 0L
  // 计算过的Task数量
  private var calculatedTasks = 0

  // TaskSetManager所管理的TaskSet的Executor或节点的黑名单
  private[scheduler] val taskSetBlacklistHelperOpt: Option[TaskSetBlacklist] = {
    blacklistTracker.map { _ =>
      new TaskSetBlacklist(conf, stageId, clock)
    }
  }
  // 正在运行的Task集合
  private[scheduler] val runningTasksSet = new HashSet[Long]
  // 正在运行task数量
  override def runningTasks: Int = runningTasksSet.size

  def someAttemptSucceeded(tid: Long): Boolean = {
    successful(taskInfos(tid).index)
  }

  // True once no more tasks should be launched for this task set manager. TaskSetManagers enter
  // the zombie state once at least one attempt of each task has completed successfully, or if the
  // task set is aborted (for example, because it was killed).  TaskSetManagers remain in the zombie
  // state until all tasks have finished running; we keep TaskSetManagers that are in the zombie
  // state in order to continue to track and account for the running tasks.
  // TODO: We should kill any running task attempts when the task set manager becomes a zombie.
  /**
    * 当TaskSetManager锁管理的TaskSet中所有的Task都执行成功了,不在有更多的Task尝试被启动时,就处于僵尸状态.
    * 例如,每个Task至少有一次尝试成功,或者TaskSet都被abort了,TaskSetManager会进入僵尸状态.
    * 知道所有的Task都运行成功位置,TaskSetManager将一直处在僵尸状态.僵尸状态下继续跟踪,记录正在运行的task.
    * */
  private[scheduler] var isZombie = false

  // Set of pending tasks for each executor. These collections are actually
  // treated as stacks, in which new tasks are added to the end of the
  // ArrayBuffer and removed from the end. This makes it faster to detect
  // tasks that repeatedly fail because whenever a task failed, it is put
  // back at the head of the stack. These collections may contain duplicates
  // for two reasons:
  // (1): Tasks are only removed lazily; when a task is launched, it remains
  // in all the pending lists except the one that it was launched from.
  // (2): Tasks may be re-added to these lists multiple times as a result
  // of failures.
  // Duplicates are handled in dequeueTaskFromList, which ensures that a
  // task hasn't already started running before launching it.
  /**
    * 每个Executor上待处理的Task集合,即Executorid与待处理Taskid的集合之间的映射关系.
    * 集合中存在重复元素有两种原因:
    * (1)task删除延迟,当task启动了,启动task时，它将保留在除启动任务之外的所有待处理列表中。
    * (2)task可能被重新添加到这些列表中多次.
    * 重复由dequeueTaskFromList处理,其确保了task在启动之前没有开始运行
    * */
  private val pendingTasksForExecutor = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each host. Similar to pendingTasksForExecutor,
  // but at host level.
  /**
    * 每个Host上待处理的Task集合,Host与待处理TaskId集合之间的映射
    * */
  private val pendingTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each rack -- similar to the above.
  /** 每个机架上待处理的Task集合,机架与待处理Taskid集合之间的映射*/
  private val pendingTasksForRack = new HashMap[String, ArrayBuffer[Int]]

  // Set containing pending tasks with no locality preferences.
  /** 没有任何本地偏好的待处理Taskid集合*/
  private[scheduler] var pendingTasksWithNoPrefs = new ArrayBuffer[Int]

  // Set containing all pending tasks (also used as a stack, as above).
  /** 所有待处理的Taskid的集合*/
  private val allPendingTasks = new ArrayBuffer[Int]

  // Tasks that can be speculated. Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet.
  /** 能够进行推断执行的Taskid的集合.这类Task占所有Task的比例很小.推断执行是指Task的一次尝试运行非常缓慢,
    * 根据推断,如果此时可以另起一次尝试运行,后来的尝试运行也比原先的尝试运行要快.
    * */
  private[scheduler] val speculatableTasks = new HashSet[Int]

  // Task index, start and finish time for each task attempt (indexed by task ID)
  /** Task的身份标识与Task尝试的信息(启动时间,完成时间等)之间的映射*/
  private val taskInfos = new HashMap[Long, TaskInfo]

  // Use a MedianHeap to record durations of successful tasks so we know when to launch
  // speculative tasks. This is only used when speculation is enabled, to avoid the overhead
  // of inserting into the heap when the heap won't be used.
  /**使用MedianHeap记录成功任务的持续时间，以便我们知道何时启动推测任务。 这仅在启用推测时使用，
    * 以避免在不使用堆时插入堆的开销。
    * */
  val successfulTaskDurations = new MedianHeap()
  // 异常打印到日志的时间间隔
  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL =
    conf.getLong("spark.logging.exceptionPrintInterval", 10000)

  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
  /**
    * 用于缓存异常信息,异常次数及最后发生磁异常的时间之间的映射关系.配合上面那个属性使用.
    * */
  private val recentExceptions = HashMap[String, (Int, Long)]()

  // Figure out the current map output tracker epoch and set it on all tasks
  /** MapOutputTracker的epoch,用于故障转移*/
  val epoch = sched.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  for (i <- (0 until numTasks).reverse) {
    addPendingTask(i)
  }

  /**
   * 跟踪在给定task位置首选项和当前可用executor集合的情况下有效的位置级别集合.这是在添加和删除executor时更新的。
    * 这允许性能优化，跳过不相关的级别（例如，如果没有task可以为当前executor集合运行PROCESS_LOCAL，
    * 则跳过PROCESS_LOCAL）。
    * Track the set of locality levels which are valid given the tasks locality preferences and
   * the set of currently available executors.  This is updated as executors are added and removed.
   * This allows a performance optimization, of skipping levels that aren't relevant (eg., skip
   * PROCESS_LOCAL if no tasks could be run PROCESS_LOCAL for the current set of executors).
   */
  private[scheduler] var myLocalityLevels = computeValidLocalityLevels()

  // Time to wait at each level
  /** 与myLocalityLevels的每个本地级别相对应,标识对应本地级别的等待时间*/
  private[scheduler] var localityWaits = myLocalityLevels.map(getLocalityWait)

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last launched a task at that level, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task.
  private var currentLocalityIndex = 0 // Index of our current locality level in validLocalityLevels
  /** 当前的本地性级别上运行Task的时间*/
  private var lastLaunchTime = clock.getTimeMillis()  // Time we last launched a task at this level

  override def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = null

  override def schedulingMode: SchedulingMode = SchedulingMode.NONE
  /** 当前发现序列化后的task大小超过了100k,次数想将被设置为true,并且答应警告级别的日志*/
  private[scheduler] var emittedTaskSizeWarning = false

  /** Add a task to all the pending-task lists that it should be on. */
  private[spark] def addPendingTask(index: Int) {
    for (loc <- tasks(index).preferredLocations) {
      loc match {
        case e: ExecutorCacheTaskLocation =>
          pendingTasksForExecutor.getOrElseUpdate(e.executorId, new ArrayBuffer) += index
        case e: HDFSCacheTaskLocation =>
          val exe = sched.getExecutorsAliveOnHost(loc.host)
          exe match {
            case Some(set) =>
              for (e <- set) {
                pendingTasksForExecutor.getOrElseUpdate(e, new ArrayBuffer) += index
              }
              logInfo(s"Pending task $index has a cached location at ${e.host} " +
                ", where there are executors " + set.mkString(","))
            case None => logDebug(s"Pending task $index has a cached location at ${e.host} " +
                ", but there are no executors alive there.")
          }
        case _ =>
      }
      pendingTasksForHost.getOrElseUpdate(loc.host, new ArrayBuffer) += index
      for (rack <- sched.getRackForHost(loc.host)) {
        pendingTasksForRack.getOrElseUpdate(rack, new ArrayBuffer) += index
      }
    }

    if (tasks(index).preferredLocations == Nil) {
      pendingTasksWithNoPrefs += index
    }

    allPendingTasks += index  // No point scanning this whole list to find the old task there
  }

  /**
   * Return the pending tasks list for a given executor ID, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForExecutor(executorId: String): ArrayBuffer[Int] = {
    pendingTasksForExecutor.getOrElse(executorId, ArrayBuffer())
  }

  /**
   * Return the pending tasks list for a given host, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
    pendingTasksForHost.getOrElse(host, ArrayBuffer())
  }

  /**
   * Return the pending rack-local task list for a given rack, or an empty list if
   * there is no map entry for that rack
   */
  private def getPendingTasksForRack(rack: String): ArrayBuffer[Int] = {
    pendingTasksForRack.getOrElse(rack, ArrayBuffer())
  }

  /**
   * Dequeue a pending task from the given list and return its index.
   * Return None if the list is empty.
   * This method also cleans up any tasks in the list that have already
   * been launched, since we want that to happen lazily.
   */
  private def dequeueTaskFromList(
      execId: String,
      host: String,
      list: ArrayBuffer[Int]): Option[Int] = {
    var indexOffset = list.size
    while (indexOffset > 0) {
      indexOffset -= 1
      val index = list(indexOffset)
      if (!isTaskBlacklistedOnExecOrNode(index, execId, host)) {
        // This should almost always be list.trimEnd(1) to remove tail
        list.remove(indexOffset)
        if (copiesRunning(index) == 0 && !successful(index)) {
          return Some(index)
        }
      }
    }
    None
  }

  /** Check whether a task is currently running an attempt on a given host */
  private def hasAttemptOnHost(taskIndex: Int, host: String): Boolean = {
    taskAttempts(taskIndex).exists(_.host == host)
  }

  private def isTaskBlacklistedOnExecOrNode(index: Int, execId: String, host: String): Boolean = {
    taskSetBlacklistHelperOpt.exists { blacklist =>
      blacklist.isNodeBlacklistedForTask(host, index) ||
        blacklist.isExecutorBlacklistedForTask(execId, index)
    }
  }

  /**
   * 根据指定的host,executor和本地性级别,从可推断的task中找出可推断的task在taskset中的索引和相应的本地性级别.
    * Return a speculative task for a given executor if any are available. The task should not have
   * an attempt running on this host, in case the host is slow. In addition, the task should meet
   * the given locality constraint.
   */
  // Labeled as protected to allow tests to override providing speculative tasks if necessary
  protected def dequeueSpeculativeTask(execId: String, host: String, locality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value)] =
  {
    // 移除已经完成的task
    speculatableTasks.retain(index => !successful(index)) // Remove finished tasks from set
    // 是否可以在host上运行的函数
    def canRunOnHost(index: Int): Boolean = {
      !hasAttemptOnHost(index, host) &&
        !isTaskBlacklistedOnExecOrNode(index, execId, host)
    }
    // 如果推测任务不为空
    if (!speculatableTasks.isEmpty) {
      // 检查本地处理的任务; 请注意，当我们复制缓存的block时，任务在多个节点上可以是本地处理的，如Spark Streaming
      // Check for process-local tasks; note that tasks can be process-local
      // on multiple nodes when we replicate cached blocks, as in Spark Streaming
      // 对于speculatableTasks中可以在host上运行的task
      for (index <- speculatableTasks if canRunOnHost(index)) {
        // 获取偏好位置
        val prefs = tasks(index).preferredLocations
        // 获取偏好executor
        val executors = prefs.flatMap(_ match {
          case e: ExecutorCacheTaskLocation => Some(e.executorId)
          case _ => None
        });
        if (executors.contains(execId)) {
          // 如果偏好executor包含指定的executor,将task从推断task缓存中移除,并返回task索引和PROCESS_LOCAL
          speculatableTasks -= index
          return Some((index, TaskLocality.PROCESS_LOCAL))
        }
      }

      // Check for node-local tasks
      // 如果指定的本地性级别<=Node_Local
      if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          // 获取task偏好的host
          val locations = tasks(index).preferredLocations.map(_.host)
          if (locations.contains(host)) {
            // 如果偏好host包含指定host,将task的索引从推测任务中移除
            speculatableTasks -= index
            // 返回下标和NODE_LOCAL元组
            return Some((index, TaskLocality.NODE_LOCAL))
          }
        }
      }

      // Check for no-preference tasks
      // 如果本地级别<=NO_PREF,和上面的if都很相似
      if (TaskLocality.isAllowed(locality, TaskLocality.NO_PREF)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          val locations = tasks(index).preferredLocations
          if (locations.size == 0) {
            speculatableTasks -= index
            return Some((index, TaskLocality.PROCESS_LOCAL))
          }
        }
      }

      // Check for rack-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
        for (rack <- sched.getRackForHost(host)) {
          for (index <- speculatableTasks if canRunOnHost(index)) {
            val racks = tasks(index).preferredLocations.map(_.host).flatMap(sched.getRackForHost)
            if (racks.contains(rack)) {
              speculatableTasks -= index
              return Some((index, TaskLocality.RACK_LOCAL))
            }
          }
        }
      }

      // Check for non-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.ANY))
        }
      }
    }

    None
  }

  /**
   * Dequeue a pending task for a given node and return its index and locality level.
   * Only search for tasks matching the given locality constraint.
   *
   * @return An option containing (task index within the task set, locality, is speculative?)
   */
  private def dequeueTask(execId: String, host: String, maxLocality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value, Boolean)] =
  {
    for (index <- dequeueTaskFromList(execId, host, getPendingTasksForExecutor(execId))) {
      return Some((index, TaskLocality.PROCESS_LOCAL, false))
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      for (index <- dequeueTaskFromList(execId, host, getPendingTasksForHost(host))) {
        return Some((index, TaskLocality.NODE_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
      for (index <- dequeueTaskFromList(execId, host, pendingTasksWithNoPrefs)) {
        return Some((index, TaskLocality.PROCESS_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- dequeueTaskFromList(execId, host, getPendingTasksForRack(rack))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
      for (index <- dequeueTaskFromList(execId, host, allPendingTasks)) {
        return Some((index, TaskLocality.ANY, false))
      }
    }

    // find a speculative task if all others tasks have been scheduled
    dequeueSpeculativeTask(execId, host, maxLocality).map {
      case (taskIndex, allowedLocality) => (taskIndex, allowedLocality, true)}
  }

  /**
   * Respond to an offer of a single executor from the scheduler by finding a task
   *
   * NOTE: this function is either called with a maxLocality which
   * would be adjusted by delay scheduling algorithm or it will be with a special
   * NO_PREF locality which will be not modified
   *
   * @param execId the executor Id of the offered resource
   * @param host  the host Id of the offered resource
   * @param maxLocality the maximum locality we want to schedule the tasks at
   */
  @throws[TaskNotSerializableException]
  def resourceOffer(
      execId: String,
      host: String,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    val offerBlacklisted = taskSetBlacklistHelperOpt.exists { blacklist =>
      blacklist.isNodeBlacklistedForTaskSet(host) ||
        blacklist.isExecutorBlacklistedForTaskSet(execId)
    }
    if (!isZombie && !offerBlacklisted) {
      val curTime = clock.getTimeMillis()

      var allowedLocality = maxLocality

      if (maxLocality != TaskLocality.NO_PREF) {
        allowedLocality = getAllowedLocalityLevel(curTime)
        if (allowedLocality > maxLocality) {
          // We're not allowed to search for farther-away tasks
          allowedLocality = maxLocality
        }
      }

      dequeueTask(execId, host, allowedLocality).map { case ((index, taskLocality, speculative)) =>
        // Found a task; do some bookkeeping and return a task description
        val task = tasks(index)
        val taskId = sched.newTaskId()
        // Do various bookkeeping
        copiesRunning(index) += 1
        val attemptNum = taskAttempts(index).size
        val info = new TaskInfo(taskId, index, attemptNum, curTime,
          execId, host, taskLocality, speculative)
        taskInfos(taskId) = info
        taskAttempts(index) = info :: taskAttempts(index)
        // Update our locality level for delay scheduling
        // NO_PREF will not affect the variables related to delay scheduling
        if (maxLocality != TaskLocality.NO_PREF) {
          currentLocalityIndex = getLocalityIndex(taskLocality)
          lastLaunchTime = curTime
        }
        // Serialize and return the task
        val serializedTask: ByteBuffer = try {
          ser.serialize(task)
        } catch {
          // If the task cannot be serialized, then there's no point to re-attempt the task,
          // as it will always fail. So just abort the whole task-set.
          case NonFatal(e) =>
            val msg = s"Failed to serialize task $taskId, not attempting to retry it."
            logError(msg, e)
            abort(s"$msg Exception during serialization: $e")
            throw new TaskNotSerializableException(e)
        }
        if (serializedTask.limit() > TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024 &&
          !emittedTaskSizeWarning) {
          emittedTaskSizeWarning = true
          logWarning(s"Stage ${task.stageId} contains a task of very large size " +
            s"(${serializedTask.limit() / 1024} KB). The maximum recommended task size is " +
            s"${TaskSetManager.TASK_SIZE_TO_WARN_KB} KB.")
        }
        addRunningTask(taskId)

        // We used to log the time it takes to serialize the task, but task size is already
        // a good proxy to task serialization time.
        // val timeTaken = clock.getTime() - startTime
        val taskName = s"task ${info.id} in stage ${taskSet.id}"
        logInfo(s"Starting $taskName (TID $taskId, $host, executor ${info.executorId}, " +
          s"partition ${task.partitionId}, $taskLocality, ${serializedTask.limit()} bytes)")

        sched.dagScheduler.taskStarted(task, info)
        new TaskDescription(
          taskId,
          attemptNum,
          execId,
          taskName,
          index,
          addedFiles,
          addedJars,
          task.localProperties,
          serializedTask)
      }
    } else {
      None
    }
  }

  private def maybeFinishTaskSet() {
    if (isZombie && runningTasks == 0) {
      sched.taskSetFinished(this)
      if (tasksSuccessful == numTasks) {
        blacklistTracker.foreach(_.updateBlacklistForSuccessfulTaskSet(
          taskSet.stageId,
          taskSet.stageAttemptId,
          taskSetBlacklistHelperOpt.get.execToFailures))
      }
    }
  }

  /**
   * 获取允许的本地性级别
    * Get the level we can launch tasks according to delay scheduling, based on current wait time.
   */
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    // Remove the scheduled or finished tasks lazily
    // 延迟删除计划中的或完成的task
    def tasksNeedToBeScheduledFrom(pendingTaskIds: ArrayBuffer[Int]): Boolean = {
      // 获取待执行的task
      var indexOffset = pendingTaskIds.size
      while (indexOffset > 0) {
        indexOffset -= 1
        val index = pendingTaskIds(indexOffset)
        // 如果该task下标复制运行数量==0并且该task没有成功
        if (copiesRunning(index) == 0 && !successful(index)) {
          // 返回true
          return true
        } else {
          // 否则从待执行的task中删除
          pendingTaskIds.remove(indexOffset)
        }
      }
      false
    }
    // 浏览可在每个位置安排执行的任务列表,如果仍有任何需要安排的任务，则返回true.延迟清理已安排的任务。
    // Walk through the list of tasks that can be scheduled at each location and returns true
    // if there are any tasks that still need to be scheduled. Lazily cleans up tasks that have
    // already been scheduled.
    def moreTasksToRunIn(pendingTasks: HashMap[String, ArrayBuffer[Int]]): Boolean = {
      // 遍历pendingTask,如果存在要执行的任务就返回true,否则,删除所有非待执行任务
      val emptyKeys = new ArrayBuffer[String]
      val hasTasks = pendingTasks.exists {
        case (id: String, tasks: ArrayBuffer[Int]) =>
          // 如果task列表中存在要执行的任务
          if (tasksNeedToBeScheduledFrom(tasks)) {
            true
          } else {
            // 否则空键集合加上本id
            emptyKeys += id
            false
          }
      }
      // 删除所有emptyKey
      // The key could be executorId, host or rackId
      emptyKeys.foreach(id => pendingTasks.remove(id))
      hasTasks
    }

    while (currentLocalityIndex < myLocalityLevels.length - 1) {
      val moreTasks = myLocalityLevels(currentLocalityIndex) match {
        case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasksForExecutor)
        case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasksForHost)
        case TaskLocality.NO_PREF => pendingTasksWithNoPrefs.nonEmpty
        case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasksForRack)
      }
      if (!moreTasks) {
        // This is a performance optimization: if there are no more tasks that can
        // be scheduled at a particular locality level, there is no point in waiting
        // for the locality wait timeout (SPARK-4939).
        lastLaunchTime = curTime
        logDebug(s"No tasks for locality level ${myLocalityLevels(currentLocalityIndex)}, " +
          s"so moving to locality level ${myLocalityLevels(currentLocalityIndex + 1)}")
        currentLocalityIndex += 1
      } else if (curTime - lastLaunchTime >= localityWaits(currentLocalityIndex)) {
        // Jump to the next locality level, and reset lastLaunchTime so that the next locality
        // wait timer doesn't immediately expire
        lastLaunchTime += localityWaits(currentLocalityIndex)
        logDebug(s"Moving to ${myLocalityLevels(currentLocalityIndex + 1)} after waiting for " +
          s"${localityWaits(currentLocalityIndex)}ms")
        currentLocalityIndex += 1
      } else {
        return myLocalityLevels(currentLocalityIndex)
      }
    }
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * 在myLocalityLevels中查找给定位置的索引。 这也是为了通过返回我们拥有的下一个最大级别来处理
    * 不在myLocalityLevels中的位置（如果我们以某种方式得到那些）。
    * 使用myLocalityLevels中的最后一个值为ANY。
    * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = {
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    index
  }

  /**
   * Check whether the given task set has been blacklisted to the point that it can't run anywhere.
   *
   * It is possible that this taskset has become impossible to schedule *anywhere* due to the
   * blacklist.  The most common scenario would be if there are fewer executors than
   * spark.task.maxFailures. We need to detect this so we can fail the task set, otherwise the job
   * will hang.
   *
   * There's a tradeoff here: we could make sure all tasks in the task set are schedulable, but that
   * would add extra time to each iteration of the scheduling loop. Here, we take the approach of
   * making sure at least one of the unscheduled tasks is schedulable. This means we may not detect
   * the hang as quickly as we could have, but we'll always detect the hang eventually, and the
   * method is faster in the typical case. In the worst case, this method can take
   * O(maxTaskFailures + numTasks) time, but it will be faster when there haven't been any task
   * failures (this is because the method picks one unscheduled task, and then iterates through each
   * executor until it finds one that the task isn't blacklisted on).
   */
  private[scheduler] def abortIfCompletelyBlacklisted(
      hostToExecutors: HashMap[String, HashSet[String]]): Unit = {
    taskSetBlacklistHelperOpt.foreach { taskSetBlacklist =>
      val appBlacklist = blacklistTracker.get
      // Only look for unschedulable tasks when at least one executor has registered. Otherwise,
      // task sets will be (unnecessarily) aborted in cases when no executors have registered yet.
      if (hostToExecutors.nonEmpty) {
        // find any task that needs to be scheduled
        val pendingTask: Option[Int] = {
          // usually this will just take the last pending task, but because of the lazy removal
          // from each list, we may need to go deeper in the list.  We poll from the end because
          // failed tasks are put back at the end of allPendingTasks, so we're more likely to find
          // an unschedulable task this way.
          val indexOffset = allPendingTasks.lastIndexWhere { indexInTaskSet =>
            copiesRunning(indexInTaskSet) == 0 && !successful(indexInTaskSet)
          }
          if (indexOffset == -1) {
            None
          } else {
            Some(allPendingTasks(indexOffset))
          }
        }

        pendingTask.foreach { indexInTaskSet =>
          // try to find some executor this task can run on.  Its possible that some *other*
          // task isn't schedulable anywhere, but we will discover that in some later call,
          // when that unschedulable task is the last task remaining.
          val blacklistedEverywhere = hostToExecutors.forall { case (host, execsOnHost) =>
            // Check if the task can run on the node
            val nodeBlacklisted =
              appBlacklist.isNodeBlacklisted(host) ||
                taskSetBlacklist.isNodeBlacklistedForTaskSet(host) ||
                taskSetBlacklist.isNodeBlacklistedForTask(host, indexInTaskSet)
            if (nodeBlacklisted) {
              true
            } else {
              // Check if the task can run on any of the executors
              execsOnHost.forall { exec =>
                appBlacklist.isExecutorBlacklisted(exec) ||
                  taskSetBlacklist.isExecutorBlacklistedForTaskSet(exec) ||
                  taskSetBlacklist.isExecutorBlacklistedForTask(exec, indexInTaskSet)
              }
            }
          }
          if (blacklistedEverywhere) {
            val partition = tasks(indexInTaskSet).partitionId
            abort(s"""
              |Aborting $taskSet because task $indexInTaskSet (partition $partition)
              |cannot run anywhere due to node and executor blacklist.
              |Most recent failure:
              |${taskSetBlacklist.getLatestFailureReason}
              |
              |Blacklisting behavior can be configured via spark.blacklist.*.
              |""".stripMargin)
          }
        }
      }
    }
  }

  /**
   * Marks the task as getting result and notifies the DAG Scheduler
   */
  def handleTaskGettingResult(tid: Long): Unit = {
    val info = taskInfos(tid)
    info.markGettingResult(clock.getTimeMillis())
    sched.dagScheduler.taskGettingResult(info)
  }

  /**
   * Check whether has enough quota to fetch the result with `size` bytes
   */
  def canFetchMoreResults(size: Long): Boolean = sched.synchronized {
    totalResultSize += size
    calculatedTasks += 1
    if (maxResultSize > 0 && totalResultSize > maxResultSize) {
      val msg = s"Total size of serialized results of ${calculatedTasks} tasks " +
        s"(${Utils.bytesToString(totalResultSize)}) is bigger than spark.driver.maxResultSize " +
        s"(${Utils.bytesToString(maxResultSize)})"
      logError(msg)
      abort(msg)
      false
    } else {
      true
    }
  }

  /**
   * Marks a task as successful and notifies the DAGScheduler that the task has ended.
   */
  def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]): Unit = {
    val info = taskInfos(tid)
    val index = info.index
    info.markFinished(TaskState.FINISHED, clock.getTimeMillis())
    if (speculationEnabled) {
      successfulTaskDurations.insert(info.duration)
    }
    removeRunningTask(tid)

    // Kill any other attempts for the same task (since those are unnecessary now that one
    // attempt completed successfully).
    for (attemptInfo <- taskAttempts(index) if attemptInfo.running) {
      logInfo(s"Killing attempt ${attemptInfo.attemptNumber} for task ${attemptInfo.id} " +
        s"in stage ${taskSet.id} (TID ${attemptInfo.taskId}) on ${attemptInfo.host} " +
        s"as the attempt ${info.attemptNumber} succeeded on ${info.host}")
      killedByOtherAttempt(index) = true
      sched.backend.killTask(
        attemptInfo.taskId,
        attemptInfo.executorId,
        interruptThread = true,
        reason = "another attempt succeeded")
    }
    if (!successful(index)) {
      tasksSuccessful += 1
      logInfo(s"Finished task ${info.id} in stage ${taskSet.id} (TID ${info.taskId}) in" +
        s" ${info.duration} ms on ${info.host} (executor ${info.executorId})" +
        s" ($tasksSuccessful/$numTasks)")
      // Mark successful and stop if all the tasks have succeeded.
      successful(index) = true
      if (tasksSuccessful == numTasks) {
        isZombie = true
      }
    } else {
      logInfo("Ignoring task-finished event for " + info.id + " in stage " + taskSet.id +
        " because task " + index + " has already completed successfully")
    }
    // This method is called by "TaskSchedulerImpl.handleSuccessfulTask" which holds the
    // "TaskSchedulerImpl" lock until exiting. To avoid the SPARK-7655 issue, we should not
    // "deserialize" the value when holding a lock to avoid blocking other threads. So we call
    // "result.value()" in "TaskResultGetter.enqueueSuccessfulTask" before reaching here.
    // Note: "result.value()" only deserializes the value when it's called at the first time, so
    // here "result.value()" just returns the value and won't block other threads.
    sched.dagScheduler.taskEnded(tasks(index), Success, result.value(), result.accumUpdates, info)
    maybeFinishTaskSet()
  }

  /**
   * Marks the task as failed, re-adds it to the list of pending tasks, and notifies the
   * DAG Scheduler.
   */
  def handleFailedTask(tid: Long, state: TaskState, reason: TaskFailedReason) {
    val info = taskInfos(tid)
    if (info.failed || info.killed) {
      return
    }
    removeRunningTask(tid)
    info.markFinished(state, clock.getTimeMillis())
    val index = info.index
    copiesRunning(index) -= 1
    var accumUpdates: Seq[AccumulatorV2[_, _]] = Seq.empty
    val failureReason = s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid, ${info.host}," +
      s" executor ${info.executorId}): ${reason.toErrorString}"
    val failureException: Option[Throwable] = reason match {
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        if (!successful(index)) {
          successful(index) = true
          tasksSuccessful += 1
        }
        isZombie = true

        if (fetchFailed.bmAddress != null) {
          blacklistTracker.foreach(_.updateBlacklistForFetchFailure(
            fetchFailed.bmAddress.host, fetchFailed.bmAddress.executorId))
        }

        None

      case ef: ExceptionFailure =>
        // ExceptionFailure's might have accumulator updates
        accumUpdates = ef.accums
        if (ef.className == classOf[NotSerializableException].getName) {
          // If the task result wasn't serializable, there's no point in trying to re-execute it.
          logError("Task %s in stage %s (TID %d) had a not serializable result: %s; not retrying"
            .format(info.id, taskSet.id, tid, ef.description))
          abort("Task %s in stage %s (TID %d) had a not serializable result: %s".format(
            info.id, taskSet.id, tid, ef.description))
          return
        }
        val key = ef.description
        val now = clock.getTimeMillis()
        val (printFull, dupCount) = {
          if (recentExceptions.contains(key)) {
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
              recentExceptions(key) = (0, now)
              (true, 0)
            } else {
              recentExceptions(key) = (dupCount + 1, printTime)
              (false, dupCount + 1)
            }
          } else {
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }
        if (printFull) {
          logWarning(failureReason)
        } else {
          logInfo(
            s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid) on ${info.host}, executor" +
              s" ${info.executorId}: ${ef.className} (${ef.description}) [duplicate $dupCount]")
        }
        ef.exception

      case e: ExecutorLostFailure if !e.exitCausedByApp =>
        logInfo(s"Task $tid failed because while it was being computed, its executor " +
          "exited for a reason unrelated to the task. Not counting this failure towards the " +
          "maximum number of failures for the task.")
        None

      case e: TaskFailedReason =>  // TaskResultLost, TaskKilled, and others
        logWarning(failureReason)
        None
    }

    sched.dagScheduler.taskEnded(tasks(index), reason, null, accumUpdates, info)

    if (!isZombie && reason.countTowardsTaskFailures) {
      assert (null != failureReason)
      taskSetBlacklistHelperOpt.foreach(_.updateBlacklistForFailedTask(
        info.host, info.executorId, index, failureReason))
      numFailures(index) += 1
      if (numFailures(index) >= maxTaskFailures) {
        logError("Task %d in stage %s failed %d times; aborting job".format(
          index, taskSet.id, maxTaskFailures))
        abort("Task %d in stage %s failed %d times, most recent failure: %s\nDriver stacktrace:"
          .format(index, taskSet.id, maxTaskFailures, failureReason), failureException)
        return
      }
    }

    if (successful(index)) {
      logInfo(s"Task ${info.id} in stage ${taskSet.id} (TID $tid) failed, but the task will not" +
        s" be re-executed (either because the task failed with a shuffle data fetch failure," +
        s" so the previous stage needs to be re-run, or because a different copy of the task" +
        s" has already succeeded).")
    } else {
      addPendingTask(index)
    }

    maybeFinishTaskSet()
  }

  def abort(message: String, exception: Option[Throwable] = None): Unit = sched.synchronized {
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    sched.dagScheduler.taskSetFailed(taskSet, message, exception)
    isZombie = true
    maybeFinishTaskSet()
  }

  /** If the given task ID is not in the set of running tasks, adds it.
   *
   * Used to keep track of the number of running tasks, for enforcing scheduling policies.
   */
  def addRunningTask(tid: Long) {
    if (runningTasksSet.add(tid) && parent != null) {
      parent.increaseRunningTasks(1)
    }
  }

  /** If the given task ID is in the set of running tasks, removes it. */
  def removeRunningTask(tid: Long) {
    if (runningTasksSet.remove(tid) && parent != null) {
      parent.decreaseRunningTasks(1)
    }
  }

  override def getSchedulableByName(name: String): Schedulable = {
    null
  }

  override def addSchedulable(schedulable: Schedulable) {}

  override def removeSchedulable(schedulable: Schedulable) {}

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
    sortedTaskSetQueue += this
    sortedTaskSetQueue
  }

  /** Called by TaskScheduler when an executor is lost so we can re-enqueue our tasks */
  override def executorLost(execId: String, host: String, reason: ExecutorLossReason) {
    // Re-enqueue any tasks that ran on the failed executor if this is a shuffle map stage,
    // and we are not using an external shuffle server which could serve the shuffle outputs.
    // The reason is the next stage wouldn't be able to fetch the data from this dead executor
    // so we would need to rerun these tasks on other executors.
    if (tasks(0).isInstanceOf[ShuffleMapTask] && !env.blockManager.externalShuffleServiceEnabled
        && !isZombie) {
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = taskInfos(tid).index
        if (successful(index) && !killedByOtherAttempt(index)) {
          successful(index) = false
          copiesRunning(index) -= 1
          tasksSuccessful -= 1
          addPendingTask(index)
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          sched.dagScheduler.taskEnded(
            tasks(index), Resubmitted, null, Seq.empty, info)
        }
      }
    }
    for ((tid, info) <- taskInfos if info.running && info.executorId == execId) {
      val exitCausedByApp: Boolean = reason match {
        case exited: ExecutorExited => exited.exitCausedByApp
        case ExecutorKilled => false
        case _ => true
      }
      handleFailedTask(tid, TaskState.FAILED, ExecutorLostFailure(info.executorId, exitCausedByApp,
        Some(reason.toString)))
    }
    // recalculate valid locality levels and waits when executor is lost
    recomputeLocality()
  }
  /**
    * 推测任务有两类,一类是可推断任务的监测和缓存;另一类是从缓存中找到可推断任务进行推断执行
    * */
  /**
   * 检查当前TaskSetManager中是否村在有需要推断的任务.
    * Check for tasks to be speculated and return true if there are any. This is called periodically
   * by the TaskScheduler.
   *
   */
  override def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean = {
    // 如果我们只有一个task或者task集合是僵尸状态
    // Can't speculate if we only have one task, and no need to speculate if the task set is a
    // zombie.
    if (isZombie || numTasks == 1) {
      return false  // 无可推断任务
    }
    var foundTasks = false
    // 计算进行推断的最小完成任务数量
    val minFinishedForSpeculation = (SPECULATION_QUANTILE * numTasks).floor.toInt
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)
    // 如果执行成功task数量大于等于minFinishedForSpeculation并且执行成功任务数量大于0
    if (tasksSuccessful >= minFinishedForSpeculation && tasksSuccessful > 0) {
      val time = clock.getTimeMillis()
      // 找出taskinfo中执行成功的任务尝试信息中执行时间处于中间时间的medianDuration
      val medianDuration = successfulTaskDurations.median
      // SPECULATION_MULTIPLIER * medianDuration和minTimeToSpeculation中的最大值
      val threshold = max(SPECULATION_MULTIPLIER * medianDuration, minTimeToSpeculation)
      // TODO: 阈值还应该考虑任务持续时间的标准偏差并且具有较低的值
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)
      // 遍历运行中任务
      for (tid <- runningTasksSet) {
        // 获取其taskInfo
        val info = taskInfos(tid)
        val index = info.index
        // 不是已成功task&&复制运行数量==1&&运行时间>阈值&&推测task缓存不包含该task
        if (!successful(index) && copiesRunning(index) == 1 && info.timeRunning(time) > threshold &&
          !speculatableTasks.contains(index)) {
          logInfo(
            "Marking task %d in stage %s (on %s) as speculatable because it ran more than %.0f ms"
              .format(index, taskSet.id, info.host, threshold))
          // 符合上面if的放入speculatableTasks缓存,并且提交一个推测任务
          speculatableTasks += index
          sched.dagScheduler.speculativeTaskSubmitted(tasks(index))
          foundTasks = true
        }
      }
    }
    foundTasks
  }
  /**
    * 获取某个本地性级别的等待时间.
    * */
  private def getLocalityWait(level: TaskLocality.TaskLocality): Long = {
    // 获取默认的等待时间.
    val defaultWait = conf.get(config.LOCALITY_WAIT)
    // 根据本地性级别匹配到对应的配置属性.
    val localityWaitKey = level match {
      case TaskLocality.PROCESS_LOCAL => "spark.locality.wait.process"
      case TaskLocality.NODE_LOCAL => "spark.locality.wait.node"
      case TaskLocality.RACK_LOCAL => "spark.locality.wait.rack"
      case _ => null
    }

    if (localityWaitKey != null) {
      // 如果localityWaitKey不为null,获取本地性级别对应的等待时间
      conf.getTimeAsMs(localityWaitKey, defaultWait.toString)
    } else {
      // 否则返回null
      0L
    }
  }

  /**
   * 计算有效的本地性级别,这样可以将task按照本地性级别由高到低分配给Executor
    * Compute the locality levels used in this TaskSet. Assumes that all tasks have already been
   * added to queues using addPendingTask.
   *
   */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    // 如果存在Executor上待处理的Task集合且存在executor在sched是活动的
    if (!pendingTasksForExecutor.isEmpty &&
        pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))) {
      levels += PROCESS_LOCAL // 允许本地性级别里包括PROCESS_LOCAL
    }
    // 如果host存在待处理的任务并且Host上存在已被激活的Executor
    if (!pendingTasksForHost.isEmpty &&
        pendingTasksForHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
      levels += NODE_LOCAL   // 允许本地性级别里包括NODE_LOCAL
    }
    // 如果存在没有任何本地行偏好待处理的Task
    if (!pendingTasksWithNoPrefs.isEmpty) {
      levels += NO_PREF // 允许本地性级别里包括NO_PREF
    }
    // 如果存在机架上待处理的Task集合并且存在已经激活的Executor
    if (!pendingTasksForRack.isEmpty &&
        pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
      levels += RACK_LOCAL // 允许本地性级别里包括RACK_LOCAL
    }
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }

  def recomputeLocality() {
    val previousLocalityLevel = myLocalityLevels(currentLocalityIndex)
    myLocalityLevels = computeValidLocalityLevels()
    localityWaits = myLocalityLevels.map(getLocalityWait)
    currentLocalityIndex = getLocalityIndex(previousLocalityLevel)
  }

  def executorAdded() {
    recomputeLocality()
  }
}

private[spark] object TaskSetManager {
  // The user will be warned if any stages contain a task that has a serialized size greater than
  // this.
  val TASK_SIZE_TO_WARN_KB = 100
}

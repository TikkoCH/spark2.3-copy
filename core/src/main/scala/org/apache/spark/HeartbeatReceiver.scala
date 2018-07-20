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

package org.apache.spark

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable
import scala.concurrent.Future

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util._

/**
 * A heartbeat from executors to the driver. This is a shared message used by several internal
 * components to convey liveness or execution information for in-progress tasks. It will also
 * expire the hosts that have not heartbeated for more than spark.network.timeout.
 * spark.executor.heartbeatInterval should be significantly less than spark.network.timeout.
 */
private[spark] case class Heartbeat(
    executorId: String,
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])], // taskId -> accumulator updates
    blockManagerId: BlockManagerId)

/**
 * 通知HeartbeatReceiverSparkContext的_taskScheduler属性已经持有了TaskScheduler引用.
  * An event that SparkContext uses to notify HeartbeatReceiver that SparkContext.taskScheduler is
 * created.
 */
private[spark] case object TaskSchedulerIsSet

private[spark] case object ExpireDeadHosts

private case class ExecutorRegistered(executorId: String)

private case class ExecutorRemoved(executorId: String)
// executor会向Driver上的HeartbeatReceiver发送HeartBeat消息,
// HeartbeatReceiver接收之后会回复heartbeatresponse消息
// reregisterBlockManager标识是否要求Executor重新向BlockManagerMaster注册BlockManager
private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
 * 运行在Dirver上,用以接受各个Executor的心跳(上面的样例类)消息,对各个Executor的生命周期进行监控
  * Lives in the driver to receive heartbeats from executors..
 */
private[spark] class HeartbeatReceiver(sc: SparkContext, clock: Clock)
  extends SparkListener with ThreadSafeRpcEndpoint with Logging {
  // sc是SparkContext,clock是封装的system.currentTimeMills
  def this(sc: SparkContext) {
    this(sc, new SystemClock)
  }

  sc.listenerBus.addToManagementQueue(this)
  // sparkEnv的组件,RpcEnv
  override val rpcEnv: RpcEnv = sc.env.rpcEnv
  // TaskSchedulerImpl
  private[spark] var scheduler: TaskScheduler = null

  // executor ID -> timestamp of when the last heartbeat from this executor was received
  /** 用于维护Executor的身份标识月HeartbeatReceiver最后一次收到Executor的心跳消息和时间戳之间的映射关系*/
  private val executorLastSeen = new mutable.HashMap[String, Long]

  // "spark.network.timeout" uses "seconds", while `spark.storage.blockManagerSlaveTimeoutMs` uses
  // "milliseconds"
  /** Executor节点上的BlockManager的超时时间,可通过spark.storage.blockManagerSlaveTimeoutMs配置*/
  private val slaveTimeoutMs =
    sc.conf.getTimeAsMs("spark.storage.blockManagerSlaveTimeoutMs", "120s")
  /** Executor的超时时间,可通过spark.network.timeout配置*/
  private val executorTimeoutMs =
    sc.conf.getTimeAsSeconds("spark.network.timeout", s"${slaveTimeoutMs}ms") * 1000

  // "spark.network.timeoutInterval" uses "seconds", while
  // "spark.storage.blockManagerTimeoutIntervalMs" uses "milliseconds"
  /** 超时的间隔.可通过spark.storage.blockManagerTimeoutIntervalMs配置,默认60秒*/
  private val timeoutIntervalMs =
    sc.conf.getTimeAsMs("spark.storage.blockManagerTimeoutIntervalMs", "60s")
  /** 检查超时的间隔,可通过spark.network.timeoutInterval配置*/
  private val checkTimeoutIntervalMs =
    sc.conf.getTimeAsSeconds("spark.network.timeoutInterval", s"${timeoutIntervalMs}ms") * 1000
  /** 向eventLoopThread提交执行超时检查的定时任务后返回的SchedulerFuture*/
  private var timeoutCheckingTask: ScheduledFuture[_] = null

  // "eventLoopThread" is used to run some pretty fast actions. The actions running in it should not
  // block the thread for a long time.
  /** 用于执行心跳接收器的超时检查任务,只包含一个线程*/
  private val eventLoopThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartbeat-receiver-event-loop-thread")
  /** 用于杀掉Executor,单线程*/
  private val killExecutorThread = ThreadUtils.newDaemonSingleThreadExecutor("kill-executor-thread")
  // 向Dispatcher注册HeartbeatReceiver时,HeartbeatReceiver对应的Inbox将向自身的message列表放入Onstart消息
  // 之后DIspatcher将处理Onstart消息,并调用HeartbeatReceiver的onStart方法
  override def onStart(): Unit = {
    // 创建定时调度的timeoutCheckingTask,定时向自身发送ExpireDeadHosts消息
    timeoutCheckingTask = eventLoopThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        Option(self).foreach(_.ask[Boolean](ExpireDeadHosts))
      }
    }, 0, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    // Messages sent and received locally
    case ExecutorRegistered(executorId) =>
      // 更新executorLastSeen缓存
      executorLastSeen(executorId) = clock.getTimeMillis()
      // 向自己回复true
      context.reply(true)
    case ExecutorRemoved(executorId) =>
      // 缓存中删除executor,并向自己回复true.
      executorLastSeen.remove(executorId)
      context.reply(true)
    case TaskSchedulerIsSet =>
      // 缓存中获取scheduler
      scheduler = sc.taskScheduler
      // 向SparkContext回复true
      context.reply(true)
    case ExpireDeadHosts =>
      expireDeadHosts()
      context.reply(true)

    // Messages received from executors
    case heartbeat @ Heartbeat(executorId, accumUpdates, blockManagerId) =>
      if (scheduler != null) {
        if (executorLastSeen.contains(executorId)) {
          // 更新executor的最后一次通信时间
          executorLastSeen(executorId) = clock.getTimeMillis()
          eventLoopThread.submit(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              // 更新正在处理的Task的度量信息,并让BlockManagerMaster知道BlockManager仍然活着
              val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, accumUpdates, blockManagerId)
              val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
              // 向Executor回复HeartbeatResponse消息.
              context.reply(response)
            }
          })
        } else {
          // This may happen if we get an executor's in-flight heartbeat immediately
          // after we just removed it. It's not really an error condition so we should
          // not log warning here. Otherwise there may be a lot of noise especially if
          // we explicitly remove executors (SPARK-4134).
          // 如果executorLastSeen中不包含executorId,那么想Executor发送HeartbeatResponse消息,
          // 此消息会要求BlockManager重新出则.这种情况发生在刚从executorLastSeen中移除Executor
          // 却收到了Executor在被移除之前发送的heartBeat消息
          logDebug(s"Received heartbeat from unknown executor $executorId")
          context.reply(HeartbeatResponse(reregisterBlockManager = true))
        }
      } else {
        // Because Executor will sleep several seconds before sending the first "Heartbeat", this
        // case rarely happens. However, if it really happens, log it and ask the executor to
        // register itself again.
        // 因为Executor会在发送第一个“Heartbeat”之前几秒钟睡眠，这种情况很少发生。
        // 但是，如果确实发生了，请记录它并要求执行者再次注册。
        logWarning(s"Dropping $heartbeat because TaskScheduler is not ready yet")
        context.reply(HeartbeatResponse(reregisterBlockManager = true))
      }
  }

  /**
   * 向eventloop发送ExecutorRegistered消息来添加新的executor.只用于测试
    * Send ExecutorRegistered to the event loop to add a new executor. Only for test.
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
   */
  def addExecutor(executorId: String): Option[Future[Boolean]] = {
    // 向HeartBeatReceiver自己发送了ExecutorRegistered消息.
    Option(self).map(_.ask[Boolean](ExecutorRegistered(executorId)))
  }

  /**
   * 如果心跳接收器未停止，通知executor注册。
    * If the heartbeat receiver is not stopped, notify it of executor registrations.
   */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    addExecutor(executorAdded.executorId)
  }

  /**
   * 向eventloop中发送ExecutorRemoved消息来删除executor,只用于测试.
    * Send ExecutorRemoved to the event loop to remove an executor. Only for test.
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
   */
  def removeExecutor(executorId: String): Option[Future[Boolean]] = {
    // 向HeartBeatReceiver自己发送ExecutorRemoved消息.
    Option(self).map(_.ask[Boolean](ExecutorRemoved(executorId)))
  }

  /**
   * 如果心跳接收器未停止，通知executor删除，以便它不记录多余的错误。
    * 我们必须在实际删除executor之后执行此操作以防止以下竞争条件：
    * 如果我们过早地从数据结构中删除执行程序的元数据，可能会在执行程序
    * 实际删除之前从执行程序获得正在进行的心跳， 在这种情况下，我们仍然会
    * 在稍后将executor标记为死主机，并使用大量错误消息使其过期。
    * If the heartbeat receiver is not stopped, notify it of executor removals so it doesn't
   * log superfluous errors.
   *
   * Note that we must do this after the executor is actually removed to guard against the
   * following race condition: if we remove an executor's metadata from our data structure
   * prematurely, we may get an in-flight heartbeat from the executor before the executor is
   * actually removed, in which case we will still mark the executor as a dead host later
   * and expire it with loud error messages.
   */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    removeExecutor(executorRemoved.executorId)
  }
  /** 检查超时的executor*/
  private def expireDeadHosts(): Unit = {
    logTrace("Checking for hosts with no recent heartbeats in HeartbeatReceiver.")
    // 获取当前时间
    val now = clock.getTimeMillis()
    // 遍历executorLastSeen
    for ((executorId, lastSeenMs) <- executorLastSeen) {
      // 如果对应executor的上次通信时间至今大于executorLastSeen,说明executor长时间没有发送心跳了
      if (now - lastSeenMs > executorTimeoutMs) {
        logWarning(s"Removing executor $executorId with no recent heartbeats: " +
          s"${now - lastSeenMs} ms exceeds timeout $executorTimeoutMs ms")
        // 调用executorLost处理丢失的Executor.移除丢失的Executor,
        // 并对Executor上运行的Task冲ixnfenpeiziyuan进行调度
        scheduler.executorLost(executorId, SlaveLost("Executor heartbeat " +
          s"timed out after ${now - lastSeenMs} ms"))
          // Asynchronously kill the executor to avoid blocking the current thread
        // 提交kill Executor的任务.
        killExecutorThread.submit(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            // Note: we want to get an executor back after expiring this one,
            // so do not simply call `sc.killExecutor` here (SPARK-8119)
            sc.killAndReplaceExecutor(executorId)
          }
        })
        // 从executorLastSeen中移除executor
        executorLastSeen.remove(executorId)
      }
    }
  }

  override def onStop(): Unit = {
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel(true)
    }
    eventLoopThread.shutdownNow()
    killExecutorThread.shutdownNow()
  }
}


private[spark] object HeartbeatReceiver {
  val ENDPOINT_NAME = "HeartbeatReceiver"
}

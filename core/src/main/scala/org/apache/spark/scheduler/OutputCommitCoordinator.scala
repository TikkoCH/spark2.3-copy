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
// scalastyle:off
import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.{RpcUtils, ThreadUtils}

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage
private case class AskPermissionToCommitOutput(stage: Int, partition: Int, attemptNumber: Int)

/**
 *
  * 确定任务是否可以把输出提到到HFDS的管理者。 使用先提交者胜的策略。
  * 在driver 端和执行器端都要初始化OutputCommitCoordinator。
  * 在执行器端，有一个指向driver 端OutputCommitCoordinatorEndpoing对象的引用，
  * 所以提交输出的请求到被转发到driver端的OutputCommitCoordinator.
  * Authority that decides whether tasks can commit output to HDFS. Uses a "first committer wins"
 * policy.
 *
 * OutputCommitCoordinator is instantiated in both the drivers and executors. On executors, it is
 * configured with a reference to the driver's OutputCommitCoordinatorEndpoint, so requests to
 * commit output will be forwarded to the driver's OutputCommitCoordinator.
 *
 * This class was introduced in SPARK-4879; see that JIRA issue (and the associated pull requests)
 * for an extensive design discussion.
 */
private[spark] class OutputCommitCoordinator(conf: SparkConf, isDriver: Boolean) extends Logging {

  // Initialized by SparkEnv 由SparkEnv初始化
  var coordinatorRef: Option[RpcEndpointRef] = None

  private type StageId = Int
  private type PartitionId = Int
  private type TaskAttemptNumber = Int
  private val NO_AUTHORIZED_COMMITTER: TaskAttemptNumber = -1
  private case class StageState(numPartitions: Int) {
    val authorizedCommitters = Array.fill[TaskAttemptNumber](numPartitions)(NO_AUTHORIZED_COMMITTER)
    val failures = mutable.Map[PartitionId, mutable.Set[TaskAttemptNumber]]()
  }

  /**
   * Map from active stages's id => authorized task attempts for each partition id, which hold an
   * exclusive lock on committing task output for that partition, as well as any known failed
   * attempts in the stage.
   *
   * Entries are added to the top-level map when stages start and are removed they finish
   * (either successfully or unsuccessfully).
   *
   * Access to this map should be guarded by synchronizing on the OutputCommitCoordinator instance.
   */
  private val stageStates = mutable.Map[StageId, StageState]()

  /**
   * 返回OutputCoordinator的内部数据结构是否为空.
    * Returns whether the OutputCommitCoordinator's internal data structures are all empty.
   */
  def isEmpty: Boolean = {
    stageStates.isEmpty
  }

  /**
   * task调用该方法询问他们是否有权提交输出到HDFS.如果task的尝试已经被认定可提交,
    * 那么后续其他尝试提交相同任务的请求会被拒绝.如果验证task尝试失败(如,由于executor失联)
    * 那么后续task尝试提交任务会被验证通过.
    * Called by tasks to ask whether they can commit their output to HDFS.
   *
   * If a task attempt has been authorized to commit, then all other attempts to commit the same
   * task will be denied.  If the authorized task attempt fails (e.g. due to its executor being
   * lost), then a subsequent task attempt may be authorized to commit its output.
   *
   * @param stage the stage number
   * @param partition the partition number
   * @param attemptNumber how many times this task has been attempted
   *                      (see [[TaskContext.attemptNumber()]])
   * @return true if this task is authorized to commit, false otherwise
   */
  def canCommit(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, partition, attemptNumber)
    coordinatorRef match {
      case Some(endpointRef) =>
        ThreadUtils.awaitResult(endpointRef.ask[Boolean](msg),
          RpcUtils.askRpcTimeout(conf).duration) // 询问是否有权限提交到HDFS
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  /**
   * DAGScheduler会当Stage开始时调用该方法.该方法用于启动给定Stage的输出提交到HDFS的协调机制.
    * 本质上为创建给定StageState样例类,该样例类包含三个属性:
    * numPartitions;authorizedCommitters;failures,该样例类就在上面可以看一下.
    * Called by the DAGScheduler when a stage starts.
   *
   * @param stage the stage id.
   * @param maxPartitionId the maximum partition id that could appear in this stage's tasks (i.e.
   *                       the maximum possible value of `context.partitionId`).
   */
  private[scheduler] def stageStart(stage: StageId, maxPartitionId: Int): Unit = synchronized {
    stageStates(stage) = new StageState(maxPartitionId + 1)
  }

  // Called by DAGScheduler 根据StageId删除stage
  private[scheduler] def stageEnd(stage: StageId): Unit = synchronized {
    stageStates.remove(stage)
  }
  // 当指定Stage的指定分区的任务执行完成会调用taskCompeted方法.
  // Called by DAGScheduler
  private[scheduler] def taskCompleted(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber,
      reason: TaskEndReason): Unit = synchronized {
    val stageState = stageStates.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
        // 执行成功,啥都没发生..
      case Success =>
      // The task output has been committed successfully
        //执行拒绝,log
      case denied: TaskCommitDenied =>
        logInfo(s"Task was denied committing, stage: $stage, partition: $partition, " +
          s"attempt: $attemptNumber")
      case otherReason =>
        // 将尝试标记为未来的提交协议黑名单
        // Mark the attempt as failed to blacklist from future commit protocol
        stageState.failures.getOrElseUpdate(partition, mutable.Set()) += attemptNumber
        if (stageState.authorizedCommitters(partition) == attemptNumber) {
          logDebug(s"Authorized committer (attemptNumber=$attemptNumber, stage=$stage, " +
            s"partition=$partition) failed; clearing lock")
          // 将其值修改为NO_AUTHORIZED_COMMITTER,这样后续的尝试有权限提交.
          stageState.authorizedCommitters(partition) = NO_AUTHORIZED_COMMITTER
        }
    }
  }
  // 发送StopCoordinator用于停止OutputCommitCoordinatorEndpoint
  def stop(): Unit = synchronized {
    if (isDriver) {
      coordinatorRef.foreach(_ send StopCoordinator)
      coordinatorRef = None
      // 清空stageStates
      stageStates.clear()
    }
  }
  // 标记为private[scheduler],测试可用.
  // Marked private[scheduler] instead of private so this can be mocked in tests
  // 用于判断给定的任务尝试是否有权限将给定stage的指定分区数据提交到hdfs
  private[scheduler] def handleAskPermissionToCommit(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber): Boolean = synchronized {
    // 从缓存中找到指定stage的制定分区的TaskAttemptNumber
    stageStates.get(stage) match {
      case Some(state) if attemptFailed(state, partition, attemptNumber) =>
        logInfo(s"Denying attemptNumber=$attemptNumber to commit for stage=$stage," +
          s" partition=$partition as task attempt $attemptNumber has already failed.")
        false
      case Some(state) =>
        state.authorizedCommitters(partition) match {
            // 如果是NO_AUTHORIZED_COMMITTER,说明是首次提交指定Stage的指定分区的输出.
            // 按照第一提交这胜利策略,给定TaskAttempNumber有权将给定Stage的指定分区提交到HDFS.
          case NO_AUTHORIZED_COMMITTER =>
            logDebug(s"Authorizing attemptNumber=$attemptNumber to commit for stage=$stage, " +
              s"partition=$partition")
            // 需要告诉后面任务已经有人提交,将attemptNumber缓存
            state.authorizedCommitters(partition) = attemptNumber
            true
          case existingCommitter =>
            // Coordinator在接受AskPermissionToCommit请求时应该是幂等的.
            // Coordinator should be idempotent when receiving AskPermissionToCommit.
            if (existingCommitter == attemptNumber) {
              logWarning(s"Authorizing duplicate request to commit for " +
                s"attemptNumber=$attemptNumber to commit for stage=$stage," +
                s" partition=$partition; existingCommitter = $existingCommitter." +
                s" This can indicate dropped network traffic.")
              true
            } else {
              // 对于不等于attemptNumber的已存在的指定stage的指定分区的输出就无权提交了.
              logDebug(s"Denying attemptNumber=$attemptNumber to commit for stage=$stage, " +
                s"partition=$partition; existingCommitter = $existingCommitter")
              false
            }
        }
      case None =>
        logDebug(s"Stage $stage has completed, so not allowing" +
          s" attempt number $attemptNumber of partition $partition to commit")
        false
    }
  }

  private def attemptFailed(
      stageState: StageState,
      partition: PartitionId,
      attempt: TaskAttemptNumber): Boolean = synchronized {
    stageState.failures.get(partition).exists(_.contains(attempt))
  }
}

private[spark] object OutputCommitCoordinator {

  // This endpoint is used only for RPC
  private[spark] class OutputCommitCoordinatorEndpoint(
      override val rpcEnv: RpcEnv, outputCommitCoordinator: OutputCommitCoordinator)
    extends RpcEndpoint with Logging {

    logDebug("init") // force eager creation of logger

    override def receive: PartialFunction[Any, Unit] = {
      // 该消息用于停止OutputCommitCoordinatorEndpoint
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        stop()
    }
    // handleAskPermissionToCommit处理消息,确认客户端是否有权限将输出提交到HDFS
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case AskPermissionToCommitOutput(stage, partition, attemptNumber) =>
        context.reply(
          outputCommitCoordinator.handleAskPermissionToCommit(stage, partition, attemptNumber))
    }
  }
}

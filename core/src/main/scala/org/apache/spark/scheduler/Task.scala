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
import java.util.Properties

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.APP_CALLER_CONTEXT
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util._

/**
 * A unit of execution. We have two kinds of Task's in Spark:
 *
 *  - [[org.apache.spark.scheduler.ShuffleMapTask]]
 *  - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param partitionId index of the number in the RDD
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 */
private[spark] abstract class Task[T](
    val stageId: Int,
    val stageAttemptId: Int,  // Stage的尝试id
    val partitionId: Int,     // 分区Id
    @transient var localProperties: Properties = new Properties,    // Task执行属性信息
    // The default value is only used in tests.
    serializedTaskMetrics: Array[Byte] =
      SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array(),
    val jobId: Option[Int] = None,
    val appId: Option[String] = None,
    val appAttemptId: Option[String] = None) extends Serializable {

  @transient lazy val metrics: TaskMetrics =
    SparkEnv.get.closureSerializer.newInstance().deserialize(ByteBuffer.wrap(serializedTaskMetrics))

  /**
   * Called by [[org.apache.spark.executor.Executor]] to run this task.
   *
   * @param taskAttemptId an identifier for this task attempt that is unique within a SparkContext.
   * @param attemptNumber how many times this task has been attempted (0 for the first attempt)
   * @return the result of the task along with updates of Accumulators.
   */
  final def run(
      taskAttemptId: Long,
      attemptNumber: Int,
      metricsSystem: MetricsSystem): T = {
    SparkEnv.get.blockManager.registerTask(taskAttemptId) // 任务尝试注册到BlockInfoManager
    // 创建任务尝试上下文
    context = new TaskContextImpl(
      stageId,
      stageAttemptId, // stageAttemptId and stageAttemptNumber are semantically equal
      partitionId,
      taskAttemptId,
      attemptNumber,
      taskMemoryManager,
      localProperties,
      metricsSystem,
      metrics)
    // 将任务尝试上下文保存到ThreadLocal中
    TaskContext.setTaskContext(context)
    taskThread = Thread.currentThread()
    // 如果任务尝试已经被kill,调用kill方法
    if (_reasonIfKilled != null) {
      kill(interruptThread = false, _reasonIfKilled)
    }
    // 创建调用者上下文
    new CallerContext(
      "TASK",
      SparkEnv.get.conf.get(APP_CALLER_CONTEXT),
      appId,
      appAttemptId,
      jobId,
      Option(stageId),
      Option(stageAttemptId),
      Option(taskAttemptId),
      Option(attemptNumber)).setCurrentContext()

    try {
      // 调用子类实现的runTask方法运行任务尝试
      runTask(context)
    } catch {
      case e: Throwable =>
        // Catch all errors; run task failure callbacks, and rethrow the exception.
        try {
          context.markTaskFailed(e)
        } catch {
          case t: Throwable =>
            e.addSuppressed(t)
        }
        context.markTaskCompleted(Some(e))
        throw e
    } finally {
      try {
        // Call the task completion callbacks. If "markTaskCompleted" is called twice, the second
        // one is no-op.
        // 调用markTaskCompleted,如果调用两次没关系,第二次不操作
        context.markTaskCompleted(None)
      } finally {
        try {
          Utils.tryLogNonFatalError {
            // 释放内存
            // Release memory used by this thread for unrolling blocks
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP)
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(
              MemoryMode.OFF_HEAP)
            // Notify any tasks waiting for execution memory to be freed to wake up and try to
            // acquire memory again. This makes impossible the scenario where a task sleeps forever
            // because there are no other tasks left to notify it. Since this is safe to do but may
            // not be strictly necessary, we should revisit whether we can remove this in the
            // future.
            val memoryManager = SparkEnv.get.memoryManager
            memoryManager.synchronized { memoryManager.notifyAll() }
          }
        } finally {
          // Though we unset the ThreadLocal here, the context member variable itself is still
          // queried directly in the TaskRunner to check for FetchFailedExceptions.
          TaskContext.unset() // 移除ThreadLocal中的TaskContextImpl
        }
      }
    }
  }

  private var taskMemoryManager: TaskMemoryManager = _

  def setTaskMemoryManager(taskMemoryManager: TaskMemoryManager): Unit = {
    this.taskMemoryManager = taskMemoryManager
  }

  def runTask(context: TaskContext): T

  def preferredLocations: Seq[TaskLocation] = Nil

  // Map output tracker epoch. Will be set by TaskSetManager.
  // MapOutputTracker的epoch,由TaskSetManager设置
  var epoch: Long = -1

  // Task context, to be initialized in run().
  /** Task执行的上下文信息,TaskContextImpl.将被set到ThreadLocal中,保证线程安全*/
  @transient var context: TaskContextImpl = _

  // The actual Thread on which the task is running, if any. Initialized in run().
  /** 运行任务尝试的线程*/
  @volatile @transient private var taskThread: Thread = _

  // If non-null, this task has been killed and the reason is as specified. This is used in case
  // context is not yet initialized when kill() is invoked.
  /** kill的原因*/
  @volatile @transient private var _reasonIfKilled: String = null
  /** 对RDD进行反序列化所花费的时间*/
  protected var _executorDeserializeTime: Long = 0
  /** 对RDD进行反序列化所花费的cpu时间*/
  protected var _executorDeserializeCpuTime: Long = 0

  /**
   * _reasonIfKilled的get方法
    * If defined, this task has been killed and this option contains the reason.
   */
  def reasonIfKilled: Option[String] = Option(_reasonIfKilled)

  /**
   * Returns the amount of time spent deserializing the RDD and function to be run.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime

  /**
   * Collect the latest values of accumulators used in this task. If the task failed,
   * filter out the accumulators whose values should not be included on failures.
   */
  def collectAccumulatorUpdates(taskFailed: Boolean = false): Seq[AccumulatorV2[_, _]] = {
    if (context != null) {
      // Note: internal accumulators representing task metrics always count failed values
      context.taskMetrics.nonZeroInternalAccums() ++
        // zero value external accumulators may still be useful, e.g. SQLMetrics, we should not
        // filter them out.
        context.taskMetrics.externalAccums.filter(a => !taskFailed || a.countFailedValues)
    } else {
      Seq.empty
    }
  }

  /**
   * 如果interruptThread为false,那么只会讲Task和TaskContextImpl标记为kull,如果为true,会利用java线程的
    * 中断机制中断线程.
    * Kills a task by setting the interrupted flag to true. This relies on the upper level Spark
   * code and user code to properly handle the flag. This function should be idempotent so it can
   * be called multiple times.
   * If interruptThread is true, we will also call Thread.interrupt() on the Task's executor thread.
   */
  def kill(interruptThread: Boolean, reason: String) {
    require(reason != null)
    _reasonIfKilled = reason
    if (context != null) {
      context.markInterrupted(reason)
    }
    if (interruptThread && taskThread != null) {
      taskThread.interrupt()
    }
  }
}

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
import java.util.concurrent.{ExecutorService, RejectedExecutionException}

import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{LongAccumulator, ThreadUtils, Utils}

/**
 * 运行一个线程池用于反序列化和远程获取task结果.
  * Runs a thread pool that deserializes and remotely fetches (if necessary) task results.
 */
private[spark] class TaskResultGetter(sparkEnv: SparkEnv, scheduler: TaskSchedulerImpl)
  extends Logging {
  /** 获取Task执行结果的线程数.可通过spark.resultGetter.threads属性配置,默认是4*/
  private val THREADS = sparkEnv.conf.getInt("spark.resultGetter.threads", 4)

  // Exposed for testing.公开用于测试
  /** 使用Executor的newFixedThreadPool方法创建ThreadPoolExecutor,用于提交获取Task执行结果的线程.
    * 线程池由THREADS决定
    * */
  protected val getTaskResultExecutor: ExecutorService =
    ThreadUtils.newDaemonFixedThreadPool(THREADS, "task-result-getter")

  // Exposed for testing.公开用于测试
  /**
    * 通过使用本地线程缓存,保证在使用SerializerInstance时是线程安全的.
    * */
  protected val serializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.closureSerializer.newInstance()
    }
  }
  /**
    * 都是线程变量,保证使用SerializerInstance对Task的执行结果进行反序列化是线程安全的.
    * */
  protected val taskResultSerializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.serializer.newInstance()
    }
  }
  /** 处理执行成功的Task执行结果.*/
  def enqueueSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      serializedData: ByteBuffer): Unit = {
    getTaskResultExecutor.execute(new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions {
        try { // 对Task的执行结果反序列化
          val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData) match {
              // 对于ResultTask来说DirectTaskResult的value是执行结果,
              // 对于ShuffleMapTask来说,结果是任务的状态.
            case directResult: DirectTaskResult[_] =>
              // 如果Task结果类型为DirectTaskResult,说明Task的执行结果保存在DirectTaskResult

              if (!taskSetManager.canFetchMoreResults(serializedData.limit())) {
                return
              }
              // 不持锁反序列化value，以便它不会阻止其他线程。 我们应该在这里调用它，
              // 这样当它在“TaskSetManager.handleSuccessfulTask”中再次调用时，它不需要反序列化该值。
              // deserialize "value" without holding any lock so that it won't block other threads.
              // We should call it here, so that when it's called again in
              // "TaskSetManager.handleSuccessfulTask", it does not need to deserialize the value.
              // 只需对DirectTaskResult保存的数据进行反序列化
              directResult.value(taskResultSerializer.get())
              // result,size元组
              (directResult, serializedData.limit())
            case IndirectTaskResult(blockId, size) =>
              // 如果结果类型为IndirectTaskResult,说明Task结果没有保存在IndirectTaskResult中
              if (!taskSetManager.canFetchMoreResults(size)) {
                // dropped by executor if size is larger than maxResultSize
                // 如果尺寸过大,让executor删除
                sparkEnv.blockManager.master.removeBlock(blockId)
                return
              }
              logDebug("Fetching indirect task result for TID %s".format(tid))
              // 需要向DAGSchedulerEventProcessLoop投递GettingResultEvent事件,
              // 一直点就能看到发送GettingResultEvent事件.
              scheduler.handleTaskGettingResult(taskSetManager, tid)
              // 从运行Task的节点上下载Block,
              val serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(blockId)
              if (!serializedTaskResult.isDefined) {
                // 如果在task结束之前和我们尝试获取结果之间时机器运行该任务失败,
                // 或者如果blockManager必须将结果刷出,我们不可能获取task结果,
                /* We won't be able to get the task result if the machine that ran the task failed
                 * between when the task ended and when we tried to fetch the result, or if the
                 * block manager had to flush the result. */
                scheduler.handleFailedTask(
                  taskSetManager, tid, TaskState.FINISHED, TaskResultLost)
                return
              }
              // 对获取到的结果反序列化
              val deserializedResult = serializer.get().deserialize[DirectTaskResult[_]](
                serializedTaskResult.get.toByteBuffer)
              // force deserialization of referenced value
              // 强制引用值的发序列化
              deserializedResult.value(taskResultSerializer.get())
              // 删除block
              sparkEnv.blockManager.master.removeBlock(blockId)
              (deserializedResult, size)
          }
          // 在从执行程序接收的累加器更新中设置任务结果大小。 我们需要在驱动程序上执行此操作，
          // 因为如果我们在执行程序上执行此操作，那么在更新大小后我们将不得不再次序列化结果。
          // Set the task result size in the accumulator updates received from the executors.
          // We need to do this here on the driver because if we did this on the executors then
          // we would have to serialize the result again after updating the size.
          result.accumUpdates = result.accumUpdates.map { a =>
            // 如果累加器的名称==internal.metrics.resultSize
            if (a.name == Some(InternalAccumulator.RESULT_SIZE)) {
              // 转换成LongAccumulator
              val acc = a.asInstanceOf[LongAccumulator]
              assert(acc.sum == 0L, "task result size should not have been set on the executors")
              // 更新累加器
              acc.setValue(size.toLong)
              acc
            } else {
              a
            }
          }
          // 标记一个task已经成功并且通知DAGScheduler任务已经结束.
          scheduler.handleSuccessfulTask(taskSetManager, tid, result)
        } catch {
          case cnf: ClassNotFoundException =>
            val loader = Thread.currentThread.getContextClassLoader
            taskSetManager.abort("ClassNotFound with classloader: " + loader)
          // Matching NonFatal so we don't catch the ControlThrowable from the "return" above.
          case NonFatal(ex) =>
            logError("Exception while getting task result", ex)
            taskSetManager.abort("Exception while getting task result: %s".format(ex))
        }
      }
    })
  }
  /** 处理失败的Task*/
  def enqueueFailedTask(taskSetManager: TaskSetManager, tid: Long, taskState: TaskState,
    serializedData: ByteBuffer) {
    var reason : TaskFailedReason = UnknownReason
    try {
      getTaskResultExecutor.execute(new Runnable {
        override def run(): Unit = Utils.logUncaughtExceptions {
          // 获取当前线程classloader或Spark的classloader
          val loader = Utils.getContextOrSparkClassLoader
          try {
            if (serializedData != null && serializedData.limit() > 0) {
              reason = serializer.get().deserialize[TaskFailedReason](
                serializedData, loader)// 对结果进行反序列化,得到失败元婴
            }
          } catch {
            case cnd: ClassNotFoundException =>
              // Log an error but keep going here -- the task failed, so not catastrophic
              // if we can't deserialize the reason.
              logError(
                "Could not deserialize TaskEndReason: ClassNotFound with classloader " + loader)
            case ex: Exception => // No-op
          } finally {
            // 如果反序列化是遇到错误,这个线程会结束,仍然通知scheduler任务失败了,避免scheduler
            // 认为任务还在运行导致挂起.
            // If there's an error while deserializing the TaskEndReason, this Runnable
            // will die. Still tell the scheduler about the task failure, to avoid a hang
            // where the scheduler thinks the task is still running.
            scheduler.handleFailedTask(taskSetManager, tid, taskState, reason)
          }
        }
      })
    } catch {
      case e: RejectedExecutionException if sparkEnv.isStopped =>
        // ignore it
    }
  }

  def stop() {
    getTaskResultExecutor.shutdownNow()
  }
}

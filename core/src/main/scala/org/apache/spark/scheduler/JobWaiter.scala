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

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Future, Promise}

import org.apache.spark.internal.Logging

/**
 * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
 * results to the given handler function.
 */
private[spark] class JobWaiter[T](
    dagScheduler: DAGScheduler, // JobWaiter等待执行完成的Job的调度者
    val jobId: Int,
    totalTasks: Int,  // 等待完成的Job包含的Task数量
    resultHandler: (Int, T) => Unit) // 执行结果的处理器
  extends JobListener with Logging {
  // 等待完成的Job中已经完成的Task数量
  private val finishedTasks = new AtomicInteger(0)
  // 如果job已经结束,这就是job的结果,对于0task的job结果是JobSucceeded.
  // If the job is finished, this will be its result. In the case of 0 task jobs (e.g. zero
  // partition RDDs), we set the jobResult directly to JobSucceeded.
  private val jobPromise: Promise[Unit] =
    if (totalTasks == 0) Promise.successful(()) else Promise()
  // job是否已完成
  def jobFinished: Boolean = jobPromise.isCompleted
  // JobPromise的future
  def completionFuture: Future[Unit] = jobPromise.future

  /**
   * 取消对job的执行,向DAGScheduler发送一个信号来取消job,在低级别调度程序取消属于此job的所有task之后，
    * 它将使用SparkException使此job失败.
    * Sends a signal to the DAGScheduler to cancel the job. The cancellation itself is handled
   * asynchronously. After the low level scheduler cancels all the tasks belonging to this job, it
   * will fail this job with a SparkException.
   */
  def cancel() {
    dagScheduler.cancelJob(jobId, None)
  }

  override def taskSucceeded(index: Int, result: Any): Unit = {
    // resultHandler call must be synchronized in case resultHandler itself is not thread safe.
    synchronized {
      // resultHandler处理job中每个task的执行结果.
      resultHandler(index, result.asInstanceOf[T])
    }
    // 增加task数量,判断是否等于总task数目
    if (finishedTasks.incrementAndGet() == totalTasks) {
      // 如果都完成了那么设置为success.
      jobPromise.success(())
    }
  }
  // 失败把jobPromise设置为失败
  override def jobFailed(exception: Exception): Unit = {
    if (!jobPromise.tryFailure(exception)) {
      logWarning("Ignore failure", exception)
    }
  }

}

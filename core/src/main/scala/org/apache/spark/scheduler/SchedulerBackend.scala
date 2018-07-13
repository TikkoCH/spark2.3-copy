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

/**
  * 用于调度系统的后端接口，允许在TaskSchedulerImpl下插入不同的接口.
  * 我们想象一个这个接口的实现,如类似Mesos的模型.
  * 当集群中有可用的机器时应用可以从Mesos资源管理器中获得资源并且在机器上启动任务
  * A backend interface for scheduling systems that allows plugging in different ones under
  * TaskSchedulerImpl. We assume a Mesos-like model where the application gets resource offers as
  * machines become available and can launch tasks on them.
  */
private[spark] trait SchedulerBackend {
  /**
    * appId是spark-application前缀加当前时间
    */
  private val appId = "spark-application-" + System.currentTimeMillis

  //四个方法,开始,结束,重新提供,默认并行数量
  def start(): Unit

  def stop(): Unit
  /** 给task分配资源并运行task*/
  def reviveOffers(): Unit

  def defaultParallelism(): Int

  /**
    * 请求executor杀死一个运行的任务
    * Requests that an executor kills a running task.
    *
    * @param taskId          任务的Id .
    *                        Id of the task.
    * @param executorId      运行任务执行器的Id
    *                        Id of the executor the task is running on.
    * @param interruptThread 是否需要中断任务线程
    *                        Whether the executor should interrupt the task thread.
    * @param reason          终止任务的原因
    *                        The reason for the task kill.
    */
  def killTask(
                taskId: Long,
                executorId: String,
                interruptThread: Boolean,
                reason: String): Unit =
    throw new UnsupportedOperationException

  /**
    * 是否就绪
    *
    * @return
    */
  def isReady(): Boolean = true

  /**
    * 获取job相关的应用程序Id
    * Get an application ID associated with the job.
    *
    * @return An application ID
    */
  def applicationId(): String = appId

  /**
    *如果集群管理器支持多次尝试，则获取此运行的尝试Id。
    * 在客户端模式下运行的应用程序不会有尝试ID。
    * Get the attempt ID for this run, if the cluster manager supports multiple
    * attempts. Applications run in client mode will not have attempt IDs.
    *
    * @return The application attempt id, if available.
    */
  def applicationAttemptId(): Option[String] = None

  /**
    * 获取驱动程序日志的URL。
    * 用来在web UI中的Executor标签中显示
    * Get the URLs for the driver logs. These URLs are used to display the links in the UI
    * Executors tab for the driver.
    *
    * @return Map containing the log names and their respective URLs
    */
  def getDriverLogUrls: Option[Map[String, String]] = None

}

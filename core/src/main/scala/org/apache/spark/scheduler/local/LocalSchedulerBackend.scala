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

package org.apache.spark.scheduler.local

import java.io.File
import java.net.URL
import java.nio.ByteBuffer

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskState}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo

private case class ReviveOffers()

private case class StatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private case class KillTask(taskId: Long, interruptThread: Boolean, reason: String)

private case class StopExecutor()

/**
 * Calls to [[LocalSchedulerBackend]] are all serialized through LocalEndpoint. Using an
 * RpcEndpoint makes the calls on [[LocalSchedulerBackend]] asynchronous, which is necessary
 * to prevent deadlock between [[LocalSchedulerBackend]] and the [[TaskSchedulerImpl]].
 */
private[spark] class LocalEndpoint(
    override val rpcEnv: RpcEnv,
    userClassPath: Seq[URL],      // 用户指定的classpath
    scheduler: TaskSchedulerImpl, // Driver中的TaskSchedulerImpl
    executorBackend: LocalSchedulerBackend, // 与LocalEndpoint相关联的LocalSchedulerBackend
    private val totalCores: Int)  // cpu核心数,local模式默认为1
  extends ThreadSafeRpcEndpoint with Logging {
  // 空闲CPU内核数.提交Task运行之前,freeCores==totalCores
  private var freeCores = totalCores
  /** local模式下,Driver处于同一JVM进程的Executor的身份标识,由于LocalEndpoint只在local模式下使用,
    * localExecutorId固定为driver
    * */
  val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  /**
    * Driver处于同一JVM进程的Executor的host,固定为localhost
    * */
  val localExecutorHostname = "localhost"
  /**
    * 与Driver处于同一JVM进程的Executor.由于LocalEndpoint的totalcores为1,所以本地只有一个Executor,
    * 并且直接实例化.
    * */
  private val executor = new Executor(
    localExecutorId, localExecutorHostname, SparkEnv.get, userClassPath, isLocal = true)
  // 重写rpc中的receive方法
  override def receive: PartialFunction[Any, Unit] = {
    case ReviveOffers =>
      reviveOffers()

    case StatusUpdate(taskId, state, serializedData) =>
      scheduler.statusUpdate(taskId, state, serializedData)
      if (TaskState.isFinished(state)) {
        freeCores += scheduler.CPUS_PER_TASK
        reviveOffers()
      }

    case KillTask(taskId, interruptThread, reason) =>
      executor.killTask(taskId, interruptThread, reason)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StopExecutor =>
      executor.stop()
      context.reply(true)
  }

  def reviveOffers() {
    // 创建一个workerOffer
    val offers = IndexedSeq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
    // resourceOffers给Task分配资源
    for (task <- scheduler.resourceOffers(offers).flatten) {
      // freeCores-1
      freeCores -= scheduler.CPUS_PER_TASK
      // 启动task
      executor.launchTask(executorBackend, task)
    }
  }
}

/**
 * local模式中调度后端接口.在local模式下,Executor,LocalSchedulerBackend,Driver都运行在一个JVM进程中.
  * Used when running a local version of Spark where the executor, backend, and master all run in
 * the same JVM. It sits behind a [[TaskSchedulerImpl]] and handles launching tasks on a single
 * Executor (created by the [[LocalSchedulerBackend]]) running locally.
 */
private[spark] class LocalSchedulerBackend(
    conf: SparkConf,
    scheduler: TaskSchedulerImpl,
    val totalCores: Int)    // CPU内核个数,固定为1
  extends SchedulerBackend with ExecutorBackend with Logging {
  // 当前应用程序标识.
  private val appId = "local-" + System.currentTimeMillis
  // LocalEndpoint的NettyRpcEndpoint
  private var localEndpoint: RpcEndpointRef = null
  // 用户指定classpath
  private val userClassPath = getUserClasspath(conf)
  // SparkContext中创建的LiveListenerBus
  private val listenerBus = scheduler.sc.listenerBus
  // LauncherBackend匿名实现类
  private val launcherBackend = new LauncherBackend() {
    override def conf: SparkConf = LocalSchedulerBackend.this.conf
    // 重写onStopRequest方法
    override def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  /**
   * Returns a list of URLs representing the user classpath.
   *
   * @param conf Spark configuration.
   */
  def getUserClasspath(conf: SparkConf): Seq[URL] = {
    val userClassPathStr = conf.getOption("spark.executor.extraClassPath")
    userClassPathStr.map(_.split(File.pathSeparator)).toSeq.flatten.map(new File(_).toURI.toURL)
  }
  // 构造对象时进行连接
  launcherBackend.connect()
  /** 启动LocalSchedulerBackend*/
  override def start() {
    val rpcEnv = SparkEnv.get.rpcEnv
    // 创建LocalEndpoint,持有rpcEnv=NettyRpcEndpointRef
    val executorEndpoint = new LocalEndpoint(rpcEnv, userClassPath, scheduler, this, totalCores)
    localEndpoint = rpcEnv.setupEndpoint("LocalSchedulerBackendEndpoint", executorEndpoint)
    // 事件总线中投递SparkListenerExecutorAdded消息
    listenerBus.post(SparkListenerExecutorAdded(
      System.currentTimeMillis,
      executorEndpoint.localExecutorId,
      new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty)))
    // 向LauncherServer中发送appid
    launcherBackend.setAppId(appId)
    // 向LauncherServer中stage
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop() {
    stop(SparkAppHandle.State.FINISHED)
  }
  /** 对Task进行资源分配后运行Task*/
  override def reviveOffers() {
    // LocalEndpoint接收ReviveOffers消息后,会调用reviveOffers方法给下一个要调度的Task分配资源并运行Task
    localEndpoint.send(ReviveOffers)
  }

  override def defaultParallelism(): Int =
    scheduler.conf.getInt("spark.default.parallelism", totalCores)

  override def killTask(
      taskId: Long, executorId: String, interruptThread: Boolean, reason: String) {
    localEndpoint.send(KillTask(taskId, interruptThread, reason))
  }
  /** Task状态更新.*/
  override def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer) {
    // 发送了一个StatusUpdate消息,localEndpoint接收到StatusUpdate消息后,会首先调用TaskScheduler的
    // statusUpdate方法更新Task状态.
    localEndpoint.send(StatusUpdate(taskId, state, serializedData))
  }

  override def applicationId(): String = appId
  /** 停止backend*/
  private def stop(finalState: SparkAppHandle.State): Unit = {
    // 发送一个StopExecutor消息
    localEndpoint.ask(StopExecutor)
    try {
      // 设置状态finalState
      launcherBackend.setState(finalState)
    } finally {
      // 关闭
      launcherBackend.close()
    }
  }

}

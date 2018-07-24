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

package org.apache.spark.scheduler.cluster

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.Future

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.deploy.client.{StandaloneAppClient, StandaloneAppClientListener}
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

/**
 * spark独立集群模manager的SchedulerBackend实现
  * A [[SchedulerBackend]] implementation for Spark's standalone cluster manager.
 */
private[spark] class StandaloneSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    masters: Array[String])
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with StandaloneAppClientListener
  with Logging {
  /** 独立模式app客户端*/
  private var client: StandaloneAppClient = null
  /** 是否停止*/
  private val stopping = new AtomicBoolean(false)
  /** 启动程序后端接口*/
  private val launcherBackend = new LauncherBackend() {
    override protected def conf: SparkConf = sc.conf
    override protected def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }
  /** 关闭的回调*/
  @volatile var shutdownCallback: StandaloneSchedulerBackend => Unit = _
  /** appid*/
  @volatile private var appId: String = _
  /** Semaphore实现的注册栅栏,等app向master注册完成后,将app当前状态告知LauncherServer*/
  private val registrationBarrier = new Semaphore(0)
  /** app可申请的最大内核数*/
  private val maxCores = conf.getOption("spark.cores.max").map(_.toInt)
  /** app期望获得的内核数*/
  private val totalExpectedCores = maxCores.getOrElse(0)

  override def start() {
    // 调用父类的start方法,读取sparkConf,创建DrvierEndpoint
    super.start()

    // SPARK-21159. The scheduler backend should only try to connect to the launcher when in client
    // mode. In cluster mode, the code that submits the application to the Master needs to connect
    // to the launcher instead.
    if (sc.deployMode == "client") {
      // 与LauncherServer建立连接
      launcherBackend.connect()
    }

    // The endpoint for executors to talk to us
    // 生成driverUrl
    val driverUrl = RpcEndpointAddress(
      sc.conf.get("spark.driver.host"),
      sc.conf.get("spark.driver.port").toInt,
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
    // 获取参数列表
    val args = Seq(
      "--driver-url", driverUrl,
      "--executor-id", "{{EXECUTOR_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--worker-url", "{{WORKER_URL}}")
    // 获取额外java参数
    val extraJavaOpts = sc.conf.getOption("spark.executor.extraJavaOptions")
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    // 额外类路径
    val classPathEntries = sc.conf.getOption("spark.executor.extraClassPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    // 额外库路径
    val libraryPathEntries = sc.conf.getOption("spark.executor.extraLibraryPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    val testingClassPath =
      if (sys.props.contains("spark.testing")) {
        sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
      } else {
        Nil
      }

    // Start executors with a few necessary configs for registering with the scheduler
    // 获取传递给Executor用于启动的配置sparkjavaOpts
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    // 创建command对象.Command样例类定义了执行Executor的命令
    val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    // 获取SparkUI的地址
    val webUrl = sc.ui.map(_.webUrl).getOrElse("")
    // 每个executor的核心数
    val coresPerExecutor = conf.getOption("spark.executor.cores").map(_.toInt)
    // If we're using dynamic allocation, set our initial executor limit to 0 for now.
    // ExecutorAllocationManager will send the real initial limit to the Master later.
    // Executor的初始限制,如果开启动态分配则是0,否则None.
    // 之后ExecutorAllocationManager会将真实的初始限制传给master
    val initialExecutorLimit =
      if (Utils.isDynamicAllocationEnabled(conf)) {
        Some(0)
      } else {
        None
      }
    // 创建应用描述信息
    val appDesc = ApplicationDescription(sc.appName, maxCores, sc.executorMemory, command,
      webUrl, sc.eventLogDir, sc.eventLogCodec, coresPerExecutor, initialExecutorLimit)
    // 创建StandaloneAppClient
    client = new StandaloneAppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
    // 启动
    client.start()
    // 设置state
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
    // 等待注册app完成
    waitForRegistration()
    // 设置状态
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop(): Unit = {
    stop(SparkAppHandle.State.FINISHED)
  }

  override def connected(appId: String) {
    logInfo("Connected to Spark cluster with app ID " + appId)
    // 设置appid
    this.appId = appId
    // 释放信号量
    notifyContext()
    // 向launchServer发送appId
    launcherBackend.setAppId(appId)
  }

  override def disconnected() {
    notifyContext()
    if (!stopping.get) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead(reason: String) {
    notifyContext()
    if (!stopping.get) {
      launcherBackend.setState(SparkAppHandle.State.KILLED)
      logError("Application has been killed. Reason: " + reason)
      try {
        scheduler.error(reason)
      } finally {
        // Ensure the application terminates, as we can no longer run jobs.
        sc.stopInNewThread()
      }
    }
  }

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int,
    memory: Int) {
    logInfo("Granted executor ID %s on hostPort %s with %d core(s), %s RAM".format(
      fullId, hostPort, cores, Utils.megabytesToString(memory)))
  }

  override def executorRemoved(
      fullId: String, message: String, exitStatus: Option[Int], workerLost: Boolean) {
    val reason: ExecutorLossReason = exitStatus match {
      case Some(code) => ExecutorExited(code, exitCausedByApp = true, message)
      case None => SlaveLost(message, workerLost = workerLost)
    }
    logInfo("Executor %s removed: %s".format(fullId, message))
    removeExecutor(fullId.split("/")(1), reason)
  }

  override def workerRemoved(workerId: String, host: String, message: String): Unit = {
    logInfo("Worker %s removed: %s".format(workerId, message))
    removeWorker(workerId, host, message)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalCoreCount.get() >= totalExpectedCores * minRegisteredRatio
  }

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  /**
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
   *
   * @return whether the request is acknowledged.
   */
  protected override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    Option(client) match {
      case Some(c) => c.requestTotalExecutors(requestedTotal)
      case None =>
        logWarning("Attempted to request executors before driver fully initialized.")
        Future.successful(false)
    }
  }

  /**
   * Kill the given list of executors through the Master.
   * @return whether the kill request is acknowledged.
   */
  protected override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    Option(client) match {
      case Some(c) => c.killExecutors(executorIds)
      case None =>
        logWarning("Attempted to kill executors before driver fully initialized.")
        Future.successful(false)
    }
  }

  private def waitForRegistration() = {
    registrationBarrier.acquire()
  }

  private def notifyContext() = {
    // 释放信号量...JUC中的Semaphore
    registrationBarrier.release()
  }
  /** 停止StandaloneSchedulerBackend*/
  private def stop(finalState: SparkAppHandle.State): Unit = {
    if (stopping.compareAndSet(false, true)) {
      // 如果设置成功
      try {
        // CoarseGrainedSchedulerBackend的stop方法
        super.stop()
        // 将client停掉
        client.stop()

        val callback = shutdownCallback
        if (callback != null) {
          callback(this)
        }
      } finally {
        launcherBackend.setState(finalState)
        launcherBackend.close()
      }
    }
  }

}

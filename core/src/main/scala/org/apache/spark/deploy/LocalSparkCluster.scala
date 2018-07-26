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

package org.apache.spark.deploy

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.deploy.master.Master
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.Utils

/**
 * Testing class that creates a Spark standalone process in-cluster (that is, running the
 * spark.deploy.master.Master and spark.deploy.worker.Workers in the same JVMs). Executors launched
 * by the Workers still run in separate JVMs. This can be used to test distributed operation and
 * fault recovery without spinning up a lot of processes.
 */
private[spark]
class LocalSparkCluster(
    numWorkers: Int,      //worker数量
    coresPerWorker: Int,  // 每个worker核心数
    memoryPerWorker: Int, // 每个worker内存数
    conf: SparkConf)      // sparkConf
  extends Logging {
  // 本地主机名
  private val localHostname = Utils.localHostName()
  // master的RpcEnv
  private val masterRpcEnvs = ArrayBuffer[RpcEnv]()
  // worker的RpcEnv
  private val workerRpcEnvs = ArrayBuffer[RpcEnv]()
  // exposed for testing
  // master的webUI端口
  var masterWebUIPort = -1

  def start(): Array[String] = {
    logInfo("Starting a local Spark cluster with " + numWorkers + " workers.")

    // Disable REST server on Master in this mode unless otherwise specified
    // 设置rest服务不可用
    val _conf = conf.clone()
      .setIfMissing("spark.master.rest.enabled", "false")
      .set("spark.shuffle.service.enabled", "false")

    /* Start the Master */
    // 启动Master
    val (rpcEnv, webUiPort, _) = Master.startRpcEnvAndEndpoint(localHostname, 0, 0, _conf)
    //将MasterWebUI端口保存到masterWebUIPort
    masterWebUIPort = webUiPort
    // RpcEnv放入masterRpcEnvs
    masterRpcEnvs += rpcEnv
    val masterUrl = "spark://" + Utils.localHostNameForURI() + ":" + rpcEnv.address.port
    val masters = Array(masterUrl)

    /* Start the Workers */
    for (workerNum <- 1 to numWorkers) {
      // 启动worker
      val workerEnv = Worker.startRpcEnvAndEndpoint(localHostname, 0, 0, coresPerWorker,
        memoryPerWorker, masters, null, Some(workerNum), _conf)
      // 放入workerRpcEnv缓存
      workerRpcEnvs += workerEnv
    }
    // 返回Master的url
    masters
  }

  def stop() {
    logInfo("Shutting down local Spark cluster.")
    // Stop the workers before the master so they don't get upset that it disconnected
    workerRpcEnvs.foreach(_.shutdown())
    masterRpcEnvs.foreach(_.shutdown())
    workerRpcEnvs.foreach(_.awaitTermination())
    masterRpcEnvs.foreach(_.awaitTermination())
    masterRpcEnvs.clear()
    workerRpcEnvs.clear()
  }
}

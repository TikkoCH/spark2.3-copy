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

package org.apache.spark.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.util.RpcUtils

/**
 *
  * rpc通信端点RpcEndpoint的引用.rpcEndPointRef是线程安全的.
  * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
 */
private[spark] abstract class RpcEndpointRef(conf: SparkConf)
  extends Serializable with Logging {
  // 最大尝试次数.可通过spark.rpc.numRetries配置,默认3次.
  private[this] val maxRetries = RpcUtils.numRetries(conf)
  // 重连等待时间.可通过spark.rpc.retry.wait配置,默认3秒
  private[this] val retryWaitMs = RpcUtils.retryWaitMs(conf)
  // rpc的ask操作默认超时时间.
  // 可通过spark.rpc.askTimeout(优先级高)或者spark.network.timeout配置.默认120秒
  private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

  /**
   * 返回对应RpcEndpoint的Rpc地址.
    * return the address for the [[RpcEndpointRef]]
   */
  def address: RpcAddress
 // 返回对应RpcEndpoint的名称
  def name: String

  /**
   * 发送单向异步消息.单向意味着不记录状态,也不期待得到客户端回复.对应actor模型中的at-most-once规则.
    * 类似Akka中Actor的tell方法.<br>
    * Sends a one-way asynchronous message. Fire-and-forget semantics.
   */
  def send(message: Any): Unit

  /**
   * 发送消息到RpcEndpoint中的receiveAndReply方法中并且返回一个Future来在指定时间内接收回复.
    * 该方法只会发送一次并且不会重试.<br>
    * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within the specified timeout.
   *
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  /**
   * 发送消息到RpcEndpoint中的receiveAndReply方法中并且返回一个Future来在指定时间内接收回复.
    *  该方法只会发送一次并且不会重试.<br>
    * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within a default timeout.
   *
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * default timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
   * loop of [[RpcEndpoint]].

   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * specified timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    val future = ask[T](message, timeout)
    timeout.awaitResult(future)
  }

}

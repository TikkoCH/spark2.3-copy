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

import org.apache.spark.SparkException

/**
 * 用来创建RpcEnv的工厂类.必须有一个空构造器,这样才能通过反射创建.
  * A factory class to create the [[RpcEnv]]. It must have an empty constructor so that it can be
 * created using Reflection.
 */
private[spark] trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv
}

/**
 * RPC的端点，定义了给定消息时要触发的函数.该对象保证了onStart,receive和onStop顺序调用.
  * 生命周期constructor -> onStart -> receive* -> onStop.<br>
  * An end point for the RPC that defines what functions to trigger given a message.
 *
 * It is guaranteed that `onStart`, `receive` and `onStop` will be called in sequence.
 *
 * The life-cycle of an endpoint is:
 *
 * {@code constructor -> onStart -> receive* -> onStop}
 *
 * Note: `receive` can be called concurrently. If you want `receive` to be thread-safe, please use
 * [[ThreadSafeRpcEndpoint]]
 *
 * If any error is thrown from one of [[RpcEndpoint]] methods except `onError`, `onError` will be
 * invoked with the cause. If `onError` throws an error, [[RpcEnv]] will ignore it.
 */
private[spark] trait RpcEndpoint {

  /**
   * 当前RpcEndpoint所属的RpcEnv
    * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   */
  val rpcEnv: RpcEnv

  /**
   * 当前RpcEndPoint对象相关联的RpcEndPointRef.当onStart调用之后self属性才有效.
    * 所以别在onStart执行之前调用self.当onStop调用之后,self变为null.<br>
    * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
   * called. And `self` will become `null` when `onStop` is called.
   *
   * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
   * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
   */
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  /**
   * 接收消息并处理,但不用给客户端回复.
    * Process messages from `RpcEndpointRef.send` or `RpcCallContext.reply`. If receiving a
   * unmatched message, `SparkException` will be thrown and sent to `onError`.
   */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

  /**
   * 收到消息处理并回复.通过RpcCallContext实现回复.
    * Process messages from `RpcEndpointRef.ask`. If receiving a unmatched message,
   * `SparkException` will be thrown and sent to `onError`.
   */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }

  /**
   * 处理message产生异常时调用
    * Invoked when any exception is thrown during handling messages.
   */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  /**
   * 客户端与当前节点连接上之后调用,可以针对连接进行一些处理.
    * Invoked when `remoteAddress` is connected to the current node.
   */
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * 断开连接是可以进行一些处理
    * Invoked when `remoteAddress` is lost.
   */
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * 连接发生网络错误调用.
    * Invoked when some network error happens in the connection between the current node and
   * `remoteAddress`.
   */
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * RpcEndPoint开始处理消息之前调用,可以执行些准备工作
    * Invoked before [[RpcEndpoint]] starts to handle any message.
   */
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
   * 停止当前RpcEndpoint.可以做一些收尾工作
    * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
   * use it to send or ask messages.
   */
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
   * 用于停止当前RpcEndpoint.
    * A convenient method to stop [[RpcEndpoint]].
   */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }
}

/**
 * 该特质要求RpcEnv在发送消息给他时是线程安全的.线程安全意味着,对于同一个线程安全的rpcEndPoint,
  * 处理一个消息先于处理下一个消息.换言之,在处理一条消息时对内部成员变量的修改,对于下一条消息是可见的.
  * 并且线程安全的RpcEnvPoint中的字段是瞬时的或恒等的.但是，
  * 不能保证相同的线程将针对不同的消息执行相同的线程安全RpcEndpoint.<br>
  * A trait that requires RpcEnv thread-safely sending messages to it.
 *
 * Thread-safety means processing of one message happens before processing of the next message by
 * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
 * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
 * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
 *
 * However, there is no guarantee that the same thread will be executing the same
 * [[ThreadSafeRpcEndpoint]] for different messages.
 */
private[spark] trait ThreadSafeRpcEndpoint extends RpcEndpoint

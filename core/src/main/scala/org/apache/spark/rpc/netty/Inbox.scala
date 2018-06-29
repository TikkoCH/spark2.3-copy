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

package org.apache.spark.rpc.netty
// scalastyle:off
import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}

// sealed class必须和subclass 在一个文件中
private[netty] sealed trait InboxMessage
// RpcEndpoint处理此类型的消息后不需要想客户端回复消息
private[netty] case class OneWayMessage(
    senderAddress: RpcAddress,
    content: Any) extends InboxMessage
// Rpc消息,RpcEndpoint处理完消息需要回复
private[netty] case class RpcMessage(
    senderAddress: RpcAddress,
    content: Any,
    context: NettyRpcCallContext) extends InboxMessage
// Inbox实例化之后,通知于此Inbox相关联的RpcEndpoin启动
private[netty] case object OnStart extends InboxMessage
// Inbox停止之后,通知于此Inbox相关联的RpcEndpoin停止
private[netty] case object OnStop extends InboxMessage

/**
  * 此消息用于告诉所有的RpcEndpoin,有远端进程已经与当前Rpc服务建立了连接
  * A message to tell all endpoints that a remote process has connected. */
private[netty] case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage

/**
  * 此消息用于告诉所有的RpcEndpoin,有远端进程已经与当前Rpc服务断开了连接
  * A message to tell all endpoints that a remote process has disconnected. */
private[netty] case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage

/**
  * 此消息用于告诉所有的RpcEndpoin,与远端某个地址之间的连接发生了错误
  * A message to tell all endpoints that a network error has happened. */
private[netty] case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage

/**
 *
  * 端点内的消息盒子.每个RpcEndpoint都有一个对应的消息盒子.该box存储InboxMessage消息的列表.
  * 所有的消息将缓存在message列表中,并由RpcEndpoint异步处理这些消息.发送消息是线程安全的.
  * An inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
 */
private[netty] class Inbox(
    val endpointRef: NettyRpcEndpointRef,
    val endpoint: RpcEndpoint)
  extends Logging {
  // 这是scala的语法,自身类型,inbox等同于this
  inbox =>  // Give this an alias so we can use it more clearly in closures.
  // 消息列表.用于缓存需要有对应RpcEndPoint处理的消息.
  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  /**
    * 停止即true
    * True if the inbox (and its associated endpoint) is stopped. */
  @GuardedBy("this")
  private var stopped = false

  /**
    * 是否允许多线程处理同时消息.默认否
    * Allow multiple threads to process messages at the same time. */
  @GuardedBy("this")
  private var enableConcurrent = false

  /**
    * 激活线程数量.正在处理messaages中消息的线程数量.
    * The number of threads processing messages for this inbox. */
  @GuardedBy("this")
  private var numActiveThreads = 0

  // OnStart should be the first message to process
  /**
    * Dispatcher注册RpcEndpoint时,会新创建EndPointData对象Dispatcher中放入RpcEndpoint名称和
    * EndpointData的map缓存中.并会将EndpointData放入dispatcher对象中的receivers阻塞队列.
    * 新创建EndPoint时,会创建其成员属性inboxreceivers中的消息会被MessageLoop线程不断消费,
    * 消费方法就是调用EndpointData.inbox.process.所以,在创建inbox时,应该在消息中添加OnStart消息.
    * 这样,当Dispatcher注册RpcEndpoint成功会开始处理inbox中的OnStart消息,
    * process方法还会调用RpcEndpoint中的onStart方法.
    */
  inbox.synchronized {
    // 向自身的messages列表中放入OnStart消息.
    messages.add(OnStart)
  }

  /**
   * 处理存储的消息
    * Process stored messages.
   */
  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    inbox.synchronized {
      // 不允许多线程并且活动线程数不等于0就返回
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }
      // 获取消息且不为空 活动线程数+1
      message = messages.poll()
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }
    while (true) {
      safelyCall(endpoint) {
        // 根据消息类型进行匹配,safelyCall的函数签名safelyCall(endpoint: RpcEndpoint)(action: => Unit)
        // 花括号中的内容就是action函数
        message match {
            // 根据消息的类型调用远端相应的方法
          case RpcMessage(_sender, content, context) =>
            try {
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unsupported message $message from ${_sender}")
              })
            } catch {
              case NonFatal(e) =>
                context.sendFailure(e)
                // Throw the exception -- this exception will be caught by the safelyCall function.
                // The endpoint's onError function will be called.
                throw e
            }

          case OneWayMessage(_sender, content) =>
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new SparkException(s"Unsupported message $message from ${_sender}")
            })

          case OnStart =>
            endpoint.onStart()
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true
                }
              }
            }

          case OnStop =>
            // 判断几个活动线程
            val activeThreads = inbox.synchronized { inbox.numActiveThreads }
            assert(activeThreads == 1,
              s"There should be only a single active thread but found $activeThreads threads.")
            // 删除endpoint的ref
            dispatcher.removeRpcEndpointRef(endpoint)
            // 调用endpoint的onStop方法.
            endpoint.onStop()
            assert(isEmpty, "OnStop should be the last message")

          case RemoteProcessConnected(remoteAddress) =>
            endpoint.onConnected(remoteAddress)

          case RemoteProcessDisconnected(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)

          case RemoteProcessConnectionError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }

      inbox.synchronized {
        // 允许并发会在调用onStop方法之后设为false,所以我们应该检查
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        if (!enableConcurrent && numActiveThreads != 1) {
          // If we are not the only one worker, exit
          // 如果不允许多个线程并且活动线程不等于一,那需要退出当前线程并将活动线程-1
          numActiveThreads -= 1
          return
        }
        message = messages.poll()
        // 如果没有消息处理了,那也需要活动线程-1
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    }
  }

  /**
    * 将消息添加到Inbox中的消息列表
    * 如果Inbox已经停止了,将拒收后续消息.否则就添加到消息列表中.
    * @param message
    */
  def post(message: InboxMessage): Unit = inbox.synchronized {
    if (stopped) {
      // We already put "OnStop" into "messages", so we should drop further messages
      onDrop(message)
    } else {
      messages.add(message)
      false
    }
  }

  def stop(): Unit = inbox.synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
    // message
    // 同步块确保了OnStop消息是最后的消息
    if (!stopped) {
      // 需要禁止多线程属性.这样在RpcEndpoint.onStop被调用时,只有一个线程来处理消息.
      // 所以RpcEndpoint.onStop可以安全地释放资源
      // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
      // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
      // safely.
      enableConcurrent = false
      stopped = true
      messages.add(OnStop)
      // Note: The concurrent events in messages will be processed one by one.
    }
  }

  def isEmpty: Boolean = inbox.synchronized { messages.isEmpty }

  /**
   * Called when we are dropping a message. Test cases override this to test message dropping.
   * Exposed for testing.
   */
  protected def onDrop(message: InboxMessage): Unit = {
    logWarning(s"Drop $message because $endpointRef is stopped")
  }

  /**
   * 闭包调用action函数,并且在异常情况下调用endpoint的onError函数.
    * 这里的action函数也可能出现异常,所以进行了try catch.
    * Calls action closure, and calls the endpoint's onError function in the case of exceptions.
   */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try action catch {
      case NonFatal(e) =>
        try endpoint.onError(e) catch {
          case NonFatal(ee) =>
            if (stopped) {
              logDebug("Ignoring error", ee)
            } else {
              logError("Ignoring error", ee)
            }
        }
    }
  }

}

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
import java.nio.ByteBuffer
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.rpc.{RpcAddress, RpcEnvStoppedException}

/**
  * 用于rpc客户端,向外发送消息.
  */
private[netty] sealed trait OutboxMessage {

  def sendWith(client: TransportClient): Unit

  def onFailure(e: Throwable): Unit

}

/**
  * 单向消息发送邮箱.
  * @param content
  */
private[netty] case class OneWayOutboxMessage(content: ByteBuffer) extends OutboxMessage
  with Logging {

  override def sendWith(client: TransportClient): Unit = {
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    e match {
      case e1: RpcEnvStoppedException => logDebug(e1.getMessage)
      case e1: Throwable => logWarning(s"Failed to send one-way RPC.", e1)
    }
  }

}
// content:内容
// _onFailure: 失败函数
// _onSuccess: 成功函数
private[netty] case class RpcOutboxMessage(
    content: ByteBuffer,
    _onFailure: (Throwable) => Unit,
    _onSuccess: (TransportClient, ByteBuffer) => Unit)
  extends OutboxMessage with RpcResponseCallback with Logging {
  // 客户端对象
  private var client: TransportClient = _
  // 请求id
  private var requestId: Long = _
  // sendWith方法为两个字段复制,并调用了client的sendRpc方法.
  override def sendWith(client: TransportClient): Unit = {
    this.client = client
    this.requestId = client.sendRpc(content, this)
  }
  // 超时如果client不为空,就删除请求.
  def onTimeout(): Unit = {
    if (client != null) {
      client.removeRpcRequest(requestId)
    } else {
      logError("Ask timeout before connecting successfully")
    }
  }
  // 实现RpcResponseCallback中的两个方法
  override def onFailure(e: Throwable): Unit = {
    _onFailure(e)
  }

  override def onSuccess(response: ByteBuffer): Unit = {
    _onSuccess(client, response)
  }

}
// nettyEnv:当前Outbox所在节点上的NettyRpcEnv
// address:Outbox所对应的远端NettyRpcEnv地址.
private[netty] class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {
  // 相当于this
  outbox => // Give this an alias so we can use it more clearly in closures.
  // 向其他远端NettyRpcEnv上的RpcEndpoint发送的消息列表
  @GuardedBy("this")
  private val messages = new java.util.LinkedList[OutboxMessage]
  // 当前Outbox内的TransportClient.用于消息发送
  @GuardedBy("this")
  private var client: TransportClient = null

  /**
   * 当前Outbox内连接任务的Future引用.如果没有连接则connectFuture为null.
    * connectFuture points to the connect task. If there is no connect task, connectFuture will be
   * null.
   */
  @GuardedBy("this")
  private var connectFuture: java.util.concurrent.Future[Unit] = null
  // 当前outbox是否是停止状态.
  @GuardedBy("this")
  private var stopped = false

  /**
   * 是否有线程正在处理messages列表中消息
    * If there is any thread draining the message queue
   */
  @GuardedBy("this")
  private var draining = false

  /**
   * 发送消息.如果没有活动的连接,缓存消息并重新启动一个连接.
    * 如果outbox已经停止,发送者会收到异常通知.
    * Send a message. If there is no active connection, cache it and launch a new connection. If
   * [[Outbox]] is stopped, the sender will be notified with a [[SparkException]].
   */
  def send(message: OutboxMessage): Unit = {
    val dropped = synchronized {
      if (stopped) {
        true
      } else {
        messages.add(message)
        false
      }
    }
    // 判断是否已经停止,如果停止发送异常.否则将OutboxMessage添加到messages列表,并处理消息.
    if (dropped) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
    } else {
      drainOutbox()
    }
  }

  /**
   * Drain the message queue. If there is other draining thread, just exit. If the connection has
   * not been established, launch a task in the `nettyEnv.clientConnectionExecutor` to setup the
   * connection.
   */
  private def drainOutbox(): Unit = {
    var message: OutboxMessage = null
    synchronized {
      // stopped,connectFuture!=null,draining,message == null这四种情况直接返回
      if (stopped) {
        return
      }
      if (connectFuture != null) {
        // 正在尝试连接远端,所以返回
        // We are connecting to the remote address, so just exit
        return
      }
      if (client == null) {
        // 没有连接任务并且client是null,所以尝试启动连接任务
        // There is no connect task but client is null, so we need to launch the connect task.
        launchConnectTask()
        return
      }
      if (draining) {
        // 如果正有线程在处理messages列表中的消息,返回
        // There is some thread draining, so just exit
        return
      }
      message = messages.poll()
      // 如果消息为空则返回,否则将draning设置为true.表示正在处理
      if (message == null) {
        return
      }
      draining = true
    }
    while (true) {
      try {
        val _client = synchronized { client }
        if (_client != null) {
          // 如果client不为null,发送消息
          message.sendWith(_client)
        } else {
          assert(stopped == true)
        }
      } catch {
        case NonFatal(e) =>
          handleNetworkFailure(e)
          return
      }
      synchronized {
        if (stopped) {
          return
        }
        message = messages.poll()
        // 发送完消息,再取消息,一直重复处理,直到消息为null,设置draning为false,返回
        if (message == null) {
          draining = false
          return
        }
      }
    }
  }

  private def launchConnectTask(): Unit = {
    // NettyEnv提交Callable内部类.返回值是一个Future,赋值给connectFuture.
    connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {
      // 构造匿名内部类
      // 重写call方法
      override def call(): Unit = {
        try {
          // 创建TransportClient
          val _client = nettyEnv.createClient(address)
          outbox.synchronized {
            client = _client
            if (stopped) {
              closeClient()
            }
          }
        } catch {
          case ie: InterruptedException =>
            // exit
            return
          case NonFatal(e) =>
            outbox.synchronized { connectFuture = null }
            handleNetworkFailure(e)
            return
        }
        outbox.synchronized { connectFuture = null }
        // 有可能没有线程正在处理消息.如果我们此处不进行处理,有可能会等到下个消息到来时消息仍未处理.
        // It's possible that no thread is draining now. If we don't drain here, we cannot send the
        // messages until the next message arrives.
        // 处理消息
        drainOutbox()
      }
    })
  }

  /**
   * Stop [[Inbox]] and notify the waiting messages with the cause.
   */
  private def handleNetworkFailure(e: Throwable): Unit = {
    synchronized {
      assert(connectFuture == null)
      if (stopped) {
        return
      }
      stopped = true
      closeClient()
    }
    // Remove this Outbox from nettyEnv so that the further messages will create a new Outbox along
    // with a new connection
    nettyEnv.removeOutbox(address)

    // Notify the connection failure for the remaining messages
    //
    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(e)
      message = messages.poll()
    }
    assert(messages.isEmpty)
  }

  private def closeClient(): Unit = synchronized {
    // Just set client to null. Don't close it in order to reuse the connection.
    client = null
  }

  /**
   * 停止Outbox.剩余的消息会通过SparkException进行通知.
    * Stop [[Outbox]]. The remaining messages in the [[Outbox]] will be notified with a
   * [[SparkException]].
   */
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      // 将stopped状态设为true,如果connectFuture不为null,取消
      stopped = true
      if (connectFuture != null) {
        connectFuture.cancel(true)
      }
      // 关闭outbox中的transportclient.
      closeClient()
    }
    // 清空messages列表中的消息
    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
      message = messages.poll()
    }
  }
}

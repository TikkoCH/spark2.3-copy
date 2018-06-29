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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

/**
  * 消息调度器,负责将RPC消息路由到应当对此消息处理的Rpc端点(RpcEndPoint).
  * 分配给进程的CPU内核的数量，用于调整线程池的大小。如果为0，则将考虑主机上可用的CPU。
  * A message dispatcher, responsible for routing RPC messages to the appropriate endpoint(s).
  *
  * @param numUsableCores Number of CPU cores allocated to the process, for sizing the thread pool.
  *                       If 0, will consider the available CPUs on the host.
  */
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv, numUsableCores: Int) extends Logging {

  /**
    * RPC端点数据,包括RpcEndPoint,NettyRpcEndpointRef和Inbox,属于同一个端点示例.
    * RpcEndPoint,NettyRpcEndpointRef和Inbox这三个对象通过EndpointData关联起来.
    */
  private class EndpointData(
                              val name: String,
                              val endpoint: RpcEndpoint,
                              val ref: NettyRpcEndpointRef) {
    val inbox = new Inbox(ref, endpoint)
  }

  // 端点示例RpcEndpoint名称和EndpointData的map
  private val endpoints: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String, EndpointData]
  // RpcEndpoint和RpcEndpointRef的映射map.有了该缓存,就可以使用endpoint实例从中快速获取
  // 或者删除endpointRef
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
  new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]
  // 存储端点数据EndpointData的阻塞队列.只有Inbox中有消息的EndpointData才会放入其中
  // Track the receivers whose inboxes may contain messages.
  private val receivers = new LinkedBlockingQueue[EndpointData]

  /**
    * 该路由是否停止.一旦停止,所有发出的消息会立即驳回.
    * True if the dispatcher has been stopped. Once stopped, all messages posted will be bounced
    * immediately.
    */
  @GuardedBy("this")
  private var stopped = false

  // 注册RpcEndpoint,并会将EndpointData放入receivers中
  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    // 根据当前RpcEndpoint所在NettyRpcEnv的地址和RpcEndpoint的名称创建RpcEndpointAddress对象.
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    // 创建NettyRpcEndpoint的引用对象
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      // 创建EndpointData,并放入endpoints缓存
      if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }
      val data = endpoints.get(name)
      // 添加到endpoint和endpointref的map缓存中.
      endpointRefs.put(data.endpoint, data.ref)
      // 把endpointData放入阻塞队列receivers的队尾.MessageLoop线程异步获取EndpointData,
      // 并处理其Inbox中刚放入的OnStart消息,最终调用RpcEndpoint的Onstart方法
      receivers.offer(data) // for the OnStart message
    }
    endpointRef // 最后返回了NettyRpcEndpointRef
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  // Should be idempotent
  private def unregisterRpcEndpoint(name: String): Unit = {
    val data = endpoints.remove(name) // 缓存中移除EndpointData
    if (data != null) {
      data.inbox.stop() // 调用EndpointData中Inbox的stop方法停止Inbox
      receivers.offer(data) // 放入队列是为了触发处理OnStop消息.for the OnStop message
    }
    // Don't clean `endpointRefs` here because it's possible that some messages are being processed
    // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
    // `removeRpcEndpointRef`.
  }

  // 判断Dispatcher是否停止,如果未停止,则调用unregisterRpcEndpoint
  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      if (stopped) {
        // This endpoint will be stopped by Dispatcher.stop() method.
        return
      }
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  /**
    * 向本对象中所有已注册的RpcEndpoint发送消息.
    * Send a message to all registered [[RpcEndpoint]]s in this process.
    *
    * This can be used to make network events known to all end points (e.g. "a new node connected").
    */
  def postToAll(message: InboxMessage): Unit = {
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next
      postMessage(name, message, (e) => {
        e match {
          case e: RpcEnvStoppedException => logDebug(s"Message $message dropped. ${e.getMessage}")
          case e: Throwable => logWarning(s"Message $message dropped. ${e.getMessage}")
        }
      }
      )
    }
  }

  /**
    * 提交由远程端点发送的消息
    * Posts a message sent by a remote endpoint. */
  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  /**
    * 提交由本地端点发送的消息
    * Posts a message sent by a local endpoint. */
  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  /**
    * 提交一个单向消息.转换成OneWayMessage放入Inbox
    * Posts a one-way message. */
  def postOneWayMessage(message: RequestMessage): Unit = {
    postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
      (e) => throw e)
  }

  /**
    * 将消息提交给指定RpcEndpoint.
    * Posts a message to a specific endpoint.
    *
    * @param endpointName      端点名称.name of the endpoint.
    * @param message           消息.the message to post
    * @param callbackIfStopped 如果停止的回调函数.callback function if the endpoint is stopped.
    */
  private def postMessage(
                           endpointName: String,
                           message: InboxMessage,
                           callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      // 通过名称从缓存中获取EndpointData
      val data = endpoints.get(endpointName)
      if (stopped) {
        Some(new RpcEnvStoppedException())
      } else if (data == null) {
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        // 如果缓存中有EndpointData并且Dispatcher没有停止,
        // 调用Inbox中的post方法把消息假如Inbox的消息列表中,然后将EndpointData放入receivers
        // 等待MessageLoop处理
        data.inbox.post(message)
        receivers.offer(data)
        None
      }
    }
    // We don't need to call `onStop` in the `synchronized` block
    error.foreach(callbackIfStopped)
  }

  /** 用于停止Dispatcher.四个事情:将stopped设置为false;注销endpoints,
    * 其中每个EndpointData的Inbox中放入OnStop消息;
    * 将receivers队尾加入终止状态PoisonPill;关闭线程池.
    */
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
    }
    // Stop all endpoints. This will queue all endpoints for processing by the message loops.
    endpoints.keySet().asScala.foreach(unregisterRpcEndpoint)
    // Enqueue a message that tells the message loops to stop.
    receivers.offer(PoisonPill)
    threadpool.shutdown()
  }

  def awaitTermination(): Unit = {
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  /**
    * Return if the endpoint exists
    */
  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }

  /**
    * 用来调度消息的线程池.
    * Thread pool used for dispatching messages. */
  private val threadpool: ThreadPoolExecutor = {
    // 获取线程池的大小.默认为2与当前系统可用处理器之间的最大值.
    val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
    val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",
      math.max(2, availableCores))
    // 创建固定大小线程池,名称为dispatcher-event-loop
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    // 启动多个运行MessageLoop任务的线程.数量与线程池大小相同.
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool // 返回线程池的引用
  }

  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        // 不断处理新消息
        while (true) {
          try {
            // 获取EndpointData.因为receovers是阻塞队列,只有有值才能拿到EndpointData
            val data = receivers.take()
            // 如果data等于PoisonPill,就退出
            if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.\
              // 再将其放回,保证其他MessageLoop也拿到PoisonPill.
              receivers.offer(PoisonPill)
              return
            }
            // 不是PoisonPill就可以处理消息了.
            data.inbox.process(Dispatcher.this)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  private val PoisonPill = new EndpointData(null, null, null)
}

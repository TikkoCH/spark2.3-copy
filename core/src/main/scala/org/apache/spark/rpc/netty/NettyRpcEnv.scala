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
import java.io._
import java.net.{InetSocketAddress, URI}
import java.nio.ByteBuffer
import java.nio.channels.{Pipe, ReadableByteChannel, WritableByteChannel}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.Nullable

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.{DynamicVariable, Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client._
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server._
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, JavaSerializerInstance, SerializationStream}
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, ThreadUtils, Utils}

private[netty] class NettyRpcEnv(
    val conf: SparkConf,
    javaSerializerInstance: JavaSerializerInstance,
    host: String,
    securityManager: SecurityManager,
    numUsableCores: Int) extends RpcEnv(conf) with Logging {
  // 三个参数sparkConf,模块名,可用核心
  private[netty] val transportConf = SparkTransportConf.fromSparkConf(
    conf.clone.set("spark.rpc.io.numConnectionsPerPeer", "1"),
    "rpc",
    conf.getInt("spark.rpc.io.threads", 0))
  // 消息调度器,负责将RPC消息路由到该对此消息处理的Rpc端点(RpcEndPoint).
  private val dispatcher: Dispatcher = new Dispatcher(this, numUsableCores)

  private val streamManager = new NettyStreamManager(this)

  private val transportContext = new TransportContext(transportConf,
    new NettyRpcHandler(dispatcher, this, streamManager))

  private def createClientBootstraps(): java.util.List[TransportClientBootstrap] = {
    if (securityManager.isAuthenticationEnabled()) {
      // 如果该对象的securityManager开启了验证,就创建一个包含AuthClientBootstrap的List返回
      java.util.Arrays.asList(new AuthClientBootstrap(transportConf,
        securityManager.getSaslUser(), securityManager))
    } else {
      // 否则返回空
      java.util.Collections.emptyList[TransportClientBootstrap]
    }
  }
  // 用于常规发送请求和响应
  private val clientFactory = transportContext.createClientFactory(createClientBootstraps())

  /**
    * 用于文件下载的独立客户端工厂。 这样可以避免使用与主RPCContext相同的RPChandler，
    * 以便由这些客户端引起的event与主要RPC通信保持隔离。 它还允许某些属性的不同配置，例如每个对等端的连接数。
    * A separate client factory for file downloads. This avoids using the same RPC handler as
   * the main RPC context, so that events caused by these clients are kept isolated from the
   * main RPC traffic.
   *
   * It also allows for different configuration of certain properties, such as the number of
   * connections per peer.
   */
  @volatile private var fileDownloadFactory: TransportClientFactory = _
  /**
    * 用于处理请求超时的调度器.类型是ScheduledExecutorService.
    */
  val timeoutScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("netty-rpc-env-timeout")

  // 因为TransportClientFactory.createClient方法是阻塞的,我们需要线程池来异步调用.
  // 默认大小64,可通过spark.rpc.connect.threads配置
  // Because TransportClientFactory.createClient is blocking, we need to run it in this thread pool
  // to implement non-blocking send/ask.
  // TODO: a non-blocking TransportClientFactory.createClient in future
  private[netty] val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection",
    conf.getInt("spark.rpc.connect.threads", 64))

  @volatile private var server: TransportServer = _

  private val stopped = new AtomicBoolean(false)

  /**
   * RpcAddress和Outbox的映射map缓存.每次向远端发送请求时,此消息首先放入该缓存,然后使用线程异步发送.
    * A map for [[RpcAddress]] and [[Outbox]]. When we are connecting to a remote [[RpcAddress]],
   * we just put messages to its [[Outbox]] to implement a non-blocking `send` method.
   */
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  /**
   * Remove the address's Outbox and stop it.
   */
  private[netty] def removeOutbox(address: RpcAddress): Unit = {
    val outbox = outboxes.remove(address)
    if (outbox != null) {
      outbox.stop()
    }
  }

  /**
    * 根据绑定地址和端口号启动Rpc服务.
    * @param bindAddress
    * @param port
    */
  def startServer(bindAddress: String, port: Int): Unit = {
    // 创建一个Bootstrap列表
    val bootstraps: java.util.List[TransportServerBootstrap] =
    // 如果通过安全验证
      if (securityManager.isAuthenticationEnabled()) {
        // 根据transport配置和安全管理生成一个包含一个AuthServerBootstrap元素的列表
        java.util.Arrays.asList(new AuthServerBootstrap(transportConf, securityManager))
      } else {
        java.util.Collections.emptyList() // 没通过验证则返回空
      }
    // 根据端口号和地址和引导器创建服务端
    server = transportContext.createServer(bindAddress, port, bootstraps)
    // 注册rpc服务端点.NAME=endpoint-verifier
    dispatcher.registerRpcEndpoint(
      RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher))
  }

  @Nullable
  override lazy val address: RpcAddress = {
    if (server != null) RpcAddress(host, server.getPort()) else null
  }

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)
    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
      if (find) {
        Future.successful(endpointRef)
      } else {
        Future.failed(new RpcEndpointNotFoundException(uri))
      }
    }(ThreadUtils.sameThread)
  }

  override def stop(endpointRef: RpcEndpointRef): Unit = {
    require(endpointRef.isInstanceOf[NettyRpcEndpointRef])
    dispatcher.stop(endpointRef)
  }

  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    if (receiver.client != null) {
      message.sendWith(receiver.client)
    } else {
      require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
      val targetOutbox = {
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }
      if (stopped.get) {
        // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
        outboxes.remove(receiver.address)
        targetOutbox.stop()
      } else {
        targetOutbox.send(message)
      }
    }
  }

  private[netty] def send(message: RequestMessage): Unit = {
    val remoteAddr = message.receiver.address
    if (remoteAddr == address) {
      // Message to a local RPC endpoint.
      try {
        dispatcher.postOneWayMessage(message)
      } catch {
        case e: RpcEnvStoppedException => logDebug(e.getMessage)
      }
    } else {
      // Message to a remote RPC endpoint.
      postToOutbox(message.receiver, OneWayOutboxMessage(message.serialize(this)))
    }
  }

  private[netty] def createClient(address: RpcAddress): TransportClient = {
    clientFactory.createClient(address.host, address.port)
  }

  private[netty] def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
    val promise = Promise[Any]()
    val remoteAddr = message.receiver.address

    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        e match {
          case e : RpcEnvStoppedException => logDebug (s"Ignored failure: $e")
          case _ => logWarning(s"Ignored failure: $e")
        }
      }
    }

    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply =>
        if (!promise.trySuccess(rpcReply)) {
          logWarning(s"Ignored message: $reply")
        }
    }

    try {
      if (remoteAddr == address) {
        val p = Promise[Any]()
        p.future.onComplete {
          case Success(response) => onSuccess(response)
          case Failure(e) => onFailure(e)
        }(ThreadUtils.sameThread)
        dispatcher.postLocalMessage(message, p)
      } else {
        val rpcMessage = RpcOutboxMessage(message.serialize(this),
          onFailure,
          (client, response) => onSuccess(deserialize[Any](client, response)))
        postToOutbox(message.receiver, rpcMessage)
        promise.future.failed.foreach {
          case _: TimeoutException => rpcMessage.onTimeout()
          case _ =>
        }(ThreadUtils.sameThread)
      }

      val timeoutCancelable = timeoutScheduler.schedule(new Runnable {
        override def run(): Unit = {
          onFailure(new TimeoutException(s"Cannot receive any reply from ${remoteAddr} " +
            s"in ${timeout.duration}"))
        }
      }, timeout.duration.toNanos, TimeUnit.NANOSECONDS)
      promise.future.onComplete { v =>
        timeoutCancelable.cancel(true)
      }(ThreadUtils.sameThread)
    } catch {
      case NonFatal(e) =>
        onFailure(e)
    }
    promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
  }

  private[netty] def serialize(content: Any): ByteBuffer = {
    javaSerializerInstance.serialize(content)
  }

  /**
   * Returns [[SerializationStream]] that forwards the serialized bytes to `out`.
   */
  private[netty] def serializeStream(out: OutputStream): SerializationStream = {
    javaSerializerInstance.serializeStream(out)
  }

  private[netty] def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize { () =>
        javaSerializerInstance.deserialize[T](bytes)
      }
    }
  }

  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpoint)
  }

  override def shutdown(): Unit = {
    cleanup()
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  private def cleanup(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      return
    }

    val iter = outboxes.values().iterator()
    while (iter.hasNext()) {
      val outbox = iter.next()
      outboxes.remove(outbox.address)
      outbox.stop()
    }
    if (timeoutScheduler != null) {
      timeoutScheduler.shutdownNow()
    }
    if (dispatcher != null) {
      dispatcher.stop()
    }
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
    if (clientConnectionExecutor != null) {
      clientConnectionExecutor.shutdownNow()
    }
    if (fileDownloadFactory != null) {
      fileDownloadFactory.close()
    }
  }

  override def deserialize[T](deserializationAction: () => T): T = {
    NettyRpcEnv.currentEnv.withValue(this) {
      deserializationAction()
    }
  }

  override def fileServer: RpcEnvFileServer = streamManager

  override def openChannel(uri: String): ReadableByteChannel = {
    val parsedUri = new URI(uri)
    require(parsedUri.getHost() != null, "Host name must be defined.")
    require(parsedUri.getPort() > 0, "Port must be defined.")
    require(parsedUri.getPath() != null && parsedUri.getPath().nonEmpty, "Path must be defined.")

    val pipe = Pipe.open()
    val source = new FileDownloadChannel(pipe.source())
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      val client = downloadClient(parsedUri.getHost(), parsedUri.getPort())
      val callback = new FileDownloadCallback(pipe.sink(), source, client)
      client.stream(parsedUri.getPath(), callback)
    })(catchBlock = {
      pipe.sink().close()
      source.close()
    })

    source
  }
  /** 用于下载的客户端*/
  private def downloadClient(host: String, port: Int): TransportClient = {
    // 双重检查锁防止对象逃逸,访问到未构造完成的对象
    if (fileDownloadFactory == null) synchronized {
      if (fileDownloadFactory == null) {
        // 所属模块为files
        val module = "files"
        val prefix = "spark.rpc.io."
        val clone = conf.clone()
        // 添加配置文件,将spark.rpc.io改成spark.files.io.名称.
        // Copy any RPC configuration that is not overridden in the spark.files namespace.
        conf.getAll.foreach { case (key, value) =>
          if (key.startsWith(prefix)) {
            val opt = key.substring(prefix.length())
            clone.setIfMissing(s"spark.$module.io.$opt", value)
          }
        }

        val ioThreads = clone.getInt("spark.files.io.threads", 1)
        val downloadConf = SparkTransportConf.fromSparkConf(clone, module, ioThreads)
        val downloadContext = new TransportContext(downloadConf, new NoOpRpcHandler(), true)
        fileDownloadFactory = downloadContext.createClientFactory(createClientBootstraps())
      }
    }
    // 如果fileDownloadFactory!=null直接创建客户端对象
    fileDownloadFactory.createClient(host, port)
  }

  private class FileDownloadChannel(source: Pipe.SourceChannel) extends ReadableByteChannel {

    @volatile private var error: Throwable = _

    def setError(e: Throwable): Unit = {
      // This setError callback is invoked by internal RPC threads in order to propagate remote
      // exceptions to application-level threads which are reading from this channel. When an
      // RPC error occurs, the RPC system will call setError() and then will close the
      // Pipe.SinkChannel corresponding to the other end of the `source` pipe. Closing of the pipe
      // sink will cause `source.read()` operations to return EOF, unblocking the application-level
      // reading thread. Thus there is no need to actually call `source.close()` here in the
      // onError() callback and, in fact, calling it here would be dangerous because the close()
      // would be asynchronous with respect to the read() call and could trigger race-conditions
      // that lead to data corruption. See the PR for SPARK-22982 for more details on this topic.
      error = e
    }

    override def read(dst: ByteBuffer): Int = {
      Try(source.read(dst)) match {
        // See the documentation above in setError(): if an RPC error has occurred then setError()
        // will be called to propagate the RPC error and then `source`'s corresponding
        // Pipe.SinkChannel will be closed, unblocking this read. In that case, we want to propagate
        // the remote RPC exception (and not any exceptions triggered by the pipe close, such as
        // ChannelClosedException), hence this `error != null` check:
        case _ if error != null => throw error
        case Success(bytesRead) => bytesRead
        case Failure(readErr) => throw readErr
      }
    }

    override def close(): Unit = source.close()

    override def isOpen(): Boolean = source.isOpen()

  }

  private class FileDownloadCallback(
      sink: WritableByteChannel,
      source: FileDownloadChannel,
      client: TransportClient) extends StreamCallback {

    override def onData(streamId: String, buf: ByteBuffer): Unit = {
      while (buf.remaining() > 0) {
        sink.write(buf)
      }
    }

    override def onComplete(streamId: String): Unit = {
      sink.close()
    }

    override def onFailure(streamId: String, cause: Throwable): Unit = {
      logDebug(s"Error downloading stream $streamId.", cause)
      source.setError(cause)
      sink.close()
    }

  }
}

private[netty] object NettyRpcEnv extends Logging {
  /**
   * When deserializing the [[NettyRpcEndpointRef]], it needs a reference to [[NettyRpcEnv]].
   * Use `currentEnv` to wrap the deserialization codes. E.g.,
   *
   * {{{
   *   NettyRpcEnv.currentEnv.withValue(this) {
   *     your deserialization codes
   *   }
   * }}}
   */
  private[netty] val currentEnv = new DynamicVariable[NettyRpcEnv](null)

  /**
   * Similar to `currentEnv`, this variable references the client instance associated with an
   * RPC, in case it's needed to find out the remote address during deserialization.
   */
  private[netty] val currentClient = new DynamicVariable[TransportClient](null)

}

private[rpc] class NettyRpcEnvFactory extends RpcEnvFactory with Logging {
  /**
    *
    * @param config
    * @return
    */
  def create(config: RpcEnvConfig): RpcEnv = {
    val sparkConf = config.conf
    // 使用Java序列化实例是线程安全的,如果以后计划支持Kryo序列化,就需要使用ThreadLocal来存储序列化实例.
    // 该实例用于RPC传输对象的序列化
    // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
    // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
    val javaSerializerInstance =
      new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
    // 创建NettyRpcEnv.对内部各个子组件实例化
    val nettyEnv =
      new NettyRpcEnv(sparkConf, javaSerializerInstance, config.advertiseAddress,
        config.securityManager, config.numUsableCores)
    if (!config.clientMode) {
      // 启动NettyRpcEnv.返回NettypRpcEnv和端口号.
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        nettyEnv.startServer(config.bindAddress, actualPort)
        (nettyEnv, nettyEnv.address.port)
      }
      try {
        //
        Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    nettyEnv
  }
}

/**
 * NettyRpcEnv版本的RpcEndpointRef.该类的行为取决于创建它的位置.
  *  在拥有RpcEndpoint的节点上,该对象是一个简单的包装了RpcEndpointAddress实例的对象.
  * 在其他接收引用序列化版本的机器上,表现就不同了.实例将跟踪发送引用的TransportClient，
  * 以便通过客户端连接发送消息到endpoint，而不需要打开新的连接.
  * 该引用的RpcAddress可以为null,意味着只有通过客户端连接引用才能使用,
  * 因为持有endpoint的进程不是实时监听到来的连接.这些引用应该与第三方共享,
  * 因为他们不会有向endpoint发送消息的能力. <br>
  * The NettyRpcEnv version of RpcEndpointRef.
 *
 * This class behaves differently depending on where it's created. On the node that "owns" the
 * RpcEndpoint, it's a simple wrapper around the RpcEndpointAddress instance.
 *
 * On other machines that receive a serialized version of the reference, the behavior changes. The
 * instance will keep track of the TransportClient that sent the reference, so that messages
 * to the endpoint are sent over the client connection, instead of needing a new connection to
 * be opened.
 *
 * The RpcAddress of this ref can be null; what that means is that the ref can only be used through
 * a client connection, since the process hosting the endpoint is not listening for incoming
 * connections. These refs should not be shared with 3rd parties, since they will not be able to
 * send messages to the endpoint.
 *
 * @param conf Spark configuration.
 * @param endpointAddress The address where the endpoint is listening.
 * @param nettyEnv The RpcEnv associated with this ref.
 */
private[netty] class NettyRpcEndpointRef(
    @transient private val conf: SparkConf,
    private val endpointAddress: RpcEndpointAddress,
    @transient @volatile private var nettyEnv: NettyRpcEnv) extends RpcEndpointRef(conf) {
  // TransportClient,用此向RpcEndpoint发送消息
  @transient @volatile var client: TransportClient = _
  // RpcEndpoint的地址
  override def address: RpcAddress =
    if (endpointAddress.rpcAddress != null) endpointAddress.rpcAddress else null
  //
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }
  // 对应RpcEndpoint的名称
  override def name: String = endpointAddress.name
  // 封装message为RequestMessage,然后调用NettyRpcEnv的ask方法
  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    nettyEnv.ask(new RequestMessage(nettyEnv.address, this, message), timeout)
  }
  // 封装message为RequestMessage,然后调用NettyRpcEnv的send方法
  override def send(message: Any): Unit = {
    require(message != null, "Message is null")
    nettyEnv.send(new RequestMessage(nettyEnv.address, this, message))
  }

  override def toString: String = s"NettyRpcEndpointRef(${endpointAddress})"

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => endpointAddress == other.endpointAddress
    case _ => false
  }

  final override def hashCode(): Int =
    if (endpointAddress == null) 0 else endpointAddress.hashCode()
}

/**
 * The message that is sent from the sender to the receiver.
 *
 * @param senderAddress the sender address. It's `null` if this message is from a client
 *                      `NettyRpcEnv`.
 * @param receiver the receiver of this message.
 * @param content the message content.
 */
private[netty] class RequestMessage(
    val senderAddress: RpcAddress,
    val receiver: NettyRpcEndpointRef,
    val content: Any) {

  /** Manually serialize [[RequestMessage]] to minimize the size. */
  def serialize(nettyEnv: NettyRpcEnv): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = new DataOutputStream(bos)
    try {
      writeRpcAddress(out, senderAddress)
      writeRpcAddress(out, receiver.address)
      out.writeUTF(receiver.name)
      val s = nettyEnv.serializeStream(out)
      try {
        s.writeObject(content)
      } finally {
        s.close()
      }
    } finally {
      out.close()
    }
    bos.toByteBuffer
  }

  private def writeRpcAddress(out: DataOutputStream, rpcAddress: RpcAddress): Unit = {
    if (rpcAddress == null) {
      out.writeBoolean(false)
    } else {
      out.writeBoolean(true)
      out.writeUTF(rpcAddress.host)
      out.writeInt(rpcAddress.port)
    }
  }

  override def toString: String = s"RequestMessage($senderAddress, $receiver, $content)"
}

private[netty] object RequestMessage {
  /**
    * 首先介绍一下DataInput和DataOutput.
    * 该参数中的DataInputStream中的数据是通过DataOutput写出的.
    * DataOutput写出顺序应该是 writeBoolean,WriteUtf,writeInt.
    * 在该方法中的InputStream中数据的顺序是|bool|UTF|Int|,分别对应了是否可读,地址和端口.
    * 所以下面的读取顺序能够读取到对应的信息,
    * readBoolean读取是否有地址,然后读取地址,再读取端口号,构造RpcAddress对象.
    */
  private def readRpcAddress(in: DataInputStream): RpcAddress = {
    // 该流是否可读
    val hasRpcAddress = in.readBoolean()
    if (hasRpcAddress) {
      // 读取地址和端口号
      RpcAddress(in.readUTF(), in.readInt())
    } else {
      null
    }
  }

  def apply(nettyEnv: NettyRpcEnv, client: TransportClient, bytes: ByteBuffer): RequestMessage = {
    // 根据bytes参数创建字节缓存流
    val bis = new ByteBufferInputStream(bytes)
    val in = new DataInputStream(bis)
    try {
      // 创建RpcAddress对象
      val senderAddress = readRpcAddress(in)
      // 创建端点的地址对象
      val endpointAddress = RpcEndpointAddress(readRpcAddress(in), in.readUTF())
      // 然后根据env的配置,RpcEndpointAddress和nettyEnv创建远端引用
      val ref = new NettyRpcEndpointRef(nettyEnv.conf, endpointAddress, nettyEnv)
      // 为ref设置TransportClient
      ref.client = client
      new RequestMessage(
        senderAddress,
        ref,
        // The remaining bytes in `bytes` are the message content.
        // 在读取地址等信息之后,剩下的就是消息内容了,反序列化得到对象
        nettyEnv.deserialize(client, bytes))
    } finally {
      in.close()
    }
  }
}

/**
 * A response that indicates some failure happens in the receiver side.
 */
private[netty] case class RpcFailure(e: Throwable)

/**
 * Dispatches incoming RPCs to registered endpoints.
 *
 * The handler keeps track of all client instances that communicate with it, so that the RpcEnv
 * knows which `TransportClient` instance to use when sending RPCs to a client endpoint (i.e.,
 * one that is not listening for incoming connections, but rather needs to be contacted via the
 * client socket).
 *
 * Events are sent on a per-connection basis, so if a client opens multiple connections to the
 * RpcEnv, multiple connection / disconnection events will be created for that client (albeit
 * with different `RpcAddress` information).
 */
private[netty] class NettyRpcHandler(
    dispatcher: Dispatcher,
    nettyEnv: NettyRpcEnv,
    streamManager: StreamManager) extends RpcHandler with Logging {

  // A variable to track the remote RpcEnv addresses of all clients
  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    // 通过internalReceive方法获取requestMessage对象
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }

  override def receive(
      client: TransportClient,
      message: ByteBuffer): Unit = {
    val messageToDispatch = internalReceive(client, message)
    // 单向消息
    dispatcher.postOneWayMessage(messageToDispatch)
  }

  private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage = {
    // 通过client获取地址
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    // 创建RequestMessage
    val requestMessage = RequestMessage(nettyEnv, client, message)
    if (requestMessage.senderAddress == null) {
      // Create a new message with the socket address of the client as the sender.
      new RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
    } else {
      // The remote RpcEnv listens to some port, we should also fire a RemoteProcessConnected for
      // the listening address
      val remoteEnvAddress = requestMessage.senderAddress
      if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
        // 最后调用postToALL方法向endpoints缓存中所有的EndpointData的Inbox中放入RemoteProcessConnected消息.
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
      }
      requestMessage
    }
  }

  override def getStreamManager: StreamManager = streamManager
  //想Inbox中投递RemoteProcessConnectionError消息
  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))
      // If the remove RpcEnv listens to some address, we should also fire a
      // RemoteProcessConnectionError for the remote RpcEnv listening address
      val remoteEnvAddress = remoteAddresses.get(clientAddr)
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessConnectionError(cause, remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null.
      // See java.net.Socket.getRemoteSocketAddress
      // Because we cannot get a RpcAddress, just log it
      logError("Exception before connecting to the client", cause)
    }
  }

  override def channelActive(client: TransportClient): Unit = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    dispatcher.postToAll(RemoteProcessConnected(clientAddr))
  }
  // 向Inbox中投递RemoteProcessDisconnected消息
  override def channelInactive(client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      nettyEnv.removeOutbox(clientAddr)
      dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))
      val remoteEnvAddress = remoteAddresses.remove(clientAddr)
      // If the remove RpcEnv listens to some address, we should also  fire a
      // RemoteProcessDisconnected for the remote RpcEnv listening address
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessDisconnected(remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null. In this case,
      // we can ignore it since we don't fire "Associated".
      // See java.net.Socket.getRemoteSocketAddress
    }
  }
}

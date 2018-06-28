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

import java.io.File
import java.nio.channels.ReadableByteChannel

import scala.concurrent.Future

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.apache.spark.util.RpcUtils


/**
  * 一个RpcEnv的实现类必须有一个RpcEnvFactory的空构造实现才能通过反射进行创建
 * A RpcEnv implementation must have a [[RpcEnvFactory]] implementation with an empty constructor
 * so that it can be created via Reflection.
 */
private[spark] object RpcEnv {

  def create(
      name: String,
      host: String,
      port: Int,
      conf: SparkConf,
      securityManager: SecurityManager,
      clientMode: Boolean = false): RpcEnv = {
    create(name, host, host, port, conf, securityManager, 0, clientMode)
  }

  def create(
      name: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Int,
      conf: SparkConf,
      securityManager: SecurityManager,
      numUsableCores: Int,
      clientMode: Boolean): RpcEnv = {
    val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager,
      numUsableCores, clientMode)
    // 返回了RpcEnv子类NettyRpcEnv对象
    new NettyRpcEnvFactory().create(config)
  }
}


/**
  * RpcEndpoint需要用一个名字注册到RpcEnv来接收消息.
  * 然后RpcEnv会处理从RpcEndpointRef或远程节点发送的消息，并将它们传递给相应的RpcEndpoint.
  * 对于RpcEnv捕获的未捕获异常,会使用RpcCallContext.sendFailure方法将异常传回发送者,
  * 在没有发送者或`otSerializableException`情况下会使用日志记录下来.
  * RpcEnv也会提供一些方法来检索给定名称或uri的RpcEndpointRef
 * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
 * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
 * nodes, and deliver them to corresponding [[RpcEndpoint]]s. For uncaught exceptions caught by
 * [[RpcEnv]], [[RpcEnv]] will use [[RpcCallContext.sendFailure]] to send exceptions back to the
 * sender, or logging them if no such sender or `NotSerializableException`.
 *
 * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.
 */
private[spark] abstract class RpcEnv(conf: SparkConf) {

  private[spark] val defaultLookupTimeout = RpcUtils.lookupRpcTimeout(conf)

  /**
    * 返回注册了RpcEndpoint的RpcEndpointRef.会用于实现RpcEndpoint.self
    * 如果对应的RpcEndpointRef不存在,返回null
   * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
   * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
   */
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
    * 返回RpcEnv监听的RpcEnv
   * Return the address that [[RpcEnv]] is listening to.
   */
  def address: RpcAddress

  /**
    * 根据name注册一个RpcEndpoint,并返回它的RpcEndpointRef.
    * RpcEnv不能保证线程安全
   * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
   * guarantee thread-safety.
   */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
    *异步根据`uri`检索RpcEndpointRef
   * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
   */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**
    * 同步根据uri检索RpcEndpointRef
   * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
   */
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }

  /**
    * 根据address和endpointName检索RpcEndpointRef.阻塞方法
   * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName`.
   * This is a blocking action.
   */
  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
  }

  /**
    * 根据指定的endpoint停止RpcEndpoint
   * Stop [[RpcEndpoint]] specified by `endpoint`.
   */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
    *异步终止RpcEnv.如果确定RpcEnv存在成功,在shutdown()之后直接调用awaitTermination()
   * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
   * call [[awaitTermination()]] straight after [[shutdown()]].
   */
  def shutdown(): Unit

  /**
    * 等到RpcEnv存在
   * Wait until [[RpcEnv]] exits.
   *
   * TODO do we need a timeout parameter?
   */
  def awaitTermination(): Unit

  /**
    * RpcEndpointRef不能在没有RpcEnv的情况下反序列化.
    * 所以当反序列化任何包含RpcEndpointRef的对象时,反序列化代码应该包装在这个方法中.
   * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
   * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
   */
  def deserialize[T](deserializationAction: () => T): T

  /**
    * 返回用于文件服务的实例.如果RpcEnv不正在执行server模式,可能返回null
   * Return the instance of the file server used to serve files. This may be `null` if the
   * RpcEnv is not operating in server mode.
   */
  def fileServer: RpcEnvFileServer

  /**
    *
    * 开启一个channel从指定URI下载文件.如果通过RpcEnvFileServer返回的URI使用的是spark格式
    * 这个方法会被Utils类调用来检索文件.
   * Open a channel to download a file from the given URI. If the URIs returned by the
   * RpcEnvFileServer use the "spark" scheme, this method will be called by the Utils class to
   * retrieve the files.
   *
   * @param uri URI with location of the file.
   */
  def openChannel(uri: String): ReadableByteChannel
}

/**
  *一个RpcEnv使用的服务来对应用拥有的其他进程提供文件服务
  * 这个文件服务器可以返回公共库处理的URI,或返回被RpcEnv#fetchFile处理的"spark"格式URI.
 * A server used by the RpcEnv to server files to other processes owned by the application.
 *
 * The file server can return URIs handled by common libraries (such as "http" or "hdfs"), or
 * it can return "spark" URIs which will be handled by `RpcEnv#fetchFile`.
 */
private[spark] trait RpcEnvFileServer {

  /**
    * 通过这个RpcEnv向服务器添加文件.
    * 当excutors被存储在driver本地文件系统中时,这用来为driver和excutor提供文件服务
   * Adds a file to be served by this RpcEnv. This is used to serve files from the driver
   * to executors when they're stored on the driver's local file system.
   *
   * @param file Local file to serve.
   * @return A URI for the location of the file.
   */
  def addFile(file: File): String

  /**
    *添加一个由此RpcEnv服务的jar.
    * 和addFile类似但是jar是通过SparkContext.addJar添加的
   * Adds a jar to be served by this RpcEnv. Similar to `addFile` but for jars added using
   * `SparkContext.addJar`.
   *
   * @param file Local file to serve.
   * @return A URI for the location of the file.
   */
  def addJar(file: File): String

  /**
    * 通过本文件服务添加一个本地目录
   * Adds a local directory to be served via this file server.
   *
   * @param baseUri 一个基础URI路径,可以以此作为相对路径来检索.可以是文件而不能是jar包
    *                Leading URI path (files can be retrieved by appending their relative
   *                path to this base URI). This cannot be "files" nor "jars".
   * @param path  本地的目录路径
    *              Path to the local directory.
   * @return file服务器中的目录根目录的URI
    *         URI for the root of the directory in the file server.
   */
  def addDirectory(baseUri: String, path: File): String

  /**
    * 验证并标准化目录的基本URI
    * Validates and normalizes the base URI for directories. */
  protected def validateDirectoryUri(baseUri: String): String = {
    val fixedBaseUri = "/" + baseUri.stripPrefix("/").stripSuffix("/")
    require(fixedBaseUri != "/files" && fixedBaseUri != "/jars",
      "Directory URI cannot be /files nor /jars.")
    fixedBaseUri
  }

}

/**
  * 样例类 RpcEnvConfig RpcEnv配置
  * @param conf SparkConf
  * @param name 名称
  * @param bindAddress 绑定地址
  * @param advertiseAddress 广告地址
  * @param port 端口
  * @param securityManager 安全管理器
  * @param numUsableCores 核心使用数量
  * @param clientMode 客户端模式
  */
private[spark] case class RpcEnvConfig(
    conf: SparkConf,
    name: String,
    bindAddress: String,
    advertiseAddress: String,
    port: Int,
    securityManager: SecurityManager,
    numUsableCores: Int,
    clientMode: Boolean)

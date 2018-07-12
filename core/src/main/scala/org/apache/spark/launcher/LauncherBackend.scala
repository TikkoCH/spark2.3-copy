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

package org.apache.spark.launcher

import java.net.{InetAddress, Socket}

import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.launcher.LauncherProtocol._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * 可用于与启动器服务器通信的类。 用户应扩展此类以提供抽象方法的实现。
  * A class that can be used to talk to a launcher server. Users should extend this class to
 * provide implementation for the abstract methods.
 *
 * See `LauncherServer` for an explanation of how launcher communication works.
 */
private[spark] abstract class LauncherBackend {
  /** 读取与LauncherServer建立的Socket连接上的消息的线程*/
  private var clientThread: Thread = _
  /** BackendConnection对象*/
  private var connection: BackendConnection = _
  /** LauncherBackend最后一次状态.*/
  private var lastState: SparkAppHandle.State = _
  @volatile private var _isConnected = false

  protected def conf: SparkConf
  /** 用于和LauncherServer建立连接*/
  def connect(): Unit = {
    // 端口
    val port = conf.getOption(LauncherProtocol.CONF_LAUNCHER_PORT)
      .orElse(sys.env.get(LauncherProtocol.ENV_LAUNCHER_PORT))
      .map(_.toInt)
    // 密钥
    val secret = conf.getOption(LauncherProtocol.CONF_LAUNCHER_SECRET)
      .orElse(sys.env.get(LauncherProtocol.ENV_LAUNCHER_SECRET))
    if (port != None && secret != None) {
      // 创建于LauncherServer建立连接的socket
      val s = new Socket(InetAddress.getLoopbackAddress(), port.get)
      // 创建BackendConnection
      connection = new BackendConnection(s)
      // 发送Hello.....
      connection.send(new Hello(secret.get, SPARK_VERSION))
      // 创建线程
      clientThread = LauncherBackend.threadFactory.newThread(connection)
      // 执行线程
      clientThread.start()
      // 设置已连接状态
      _isConnected = true
    }
  }

  def close(): Unit = {
    if (connection != null) {
      try {
        connection.close()
      } finally {
        if (clientThread != null) {
          clientThread.join()
        }
      }
    }
  }
  /** 向LauncherServer发送SetAppId消息,携带APPId*/
  def setAppId(appId: String): Unit = {
    if (connection != null) {
      connection.send(new SetAppId(appId))
    }
  }
  /** 向LauncherServer发送setStage消息,setState携带LauncherBackend最后一次状态*/
  def setState(state: SparkAppHandle.State): Unit = {
    if (connection != null && lastState != state) {
      connection.send(new SetState(state))
      lastState = state
    }
  }

  /**
    * 返回_isConnected状态
    * Return whether the launcher handle is still connected to this backend. */
  def isConnected(): Boolean = _isConnected

  /**
   * 实现应该提供该方法,尝试停止应用的方法.
    * Implementations should provide this method, which should try to stop the application
   * as gracefully as possible.
   */
  protected def onStopRequest(): Unit

  /**
   * 空方法.当launcher处理从该后端断开连接是时的回调,什么都没做.
    * Callback for when the launcher handle disconnects from this backend.
   */
  protected def onDisconnected() : Unit = { }
  /** 用于启动一个调用OnStopRequest方法的线程*/
  private def fireStopRequest(): Unit = {
    val thread = LauncherBackend.threadFactory.newThread(new Runnable() {
      override def run(): Unit = Utils.tryLogNonFatalError {
        onStopRequest()
      }
    })
    thread.start()
  }
  /** 用于保持LauncherServer的Socket链接,并通过此Socket链接发送消息.*/
  private class BackendConnection(s: Socket) extends LauncherConnection(s) {
    /** 处理LauncherServer发送的消息*/
    override protected def handle(m: Message): Unit = m match {
        // 只接受stop这一种消息.
      case _: Stop =>
        fireStopRequest()

      case _ =>
        throw new IllegalArgumentException(s"Unexpected message type: ${m.getClass().getName()}")
    }

    override def close(): Unit = {
      try {
        super.close()
      } finally {
        // 这个方法是空方法
        onDisconnected()
        _isConnected = false
      }
    }

  }

}

private object LauncherBackend {

  val threadFactory = ThreadUtils.namedThreadFactory("LauncherBackend")

}

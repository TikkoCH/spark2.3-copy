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

package org.apache.spark.launcher;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.launcher.LauncherProtocol.*;

/**
 * Encapsulates a connection between a launcher server and client. This takes care of the
 * communication (sending and receiving messages), while processing of messages is left for
 * the implementations.
 */
abstract class LauncherConnection implements Closeable, Runnable {

  private static final Logger LOG = Logger.getLogger(LauncherConnection.class.getName());
  /** 与LauncherServer的Socket服务端建立连接的Socket客户端*/
  private final Socket socket;
  /** 建立在Socket输出流上的ObjectOutputStream,用于向服务端发送消息*/
  private final ObjectOutputStream out;
  /** Socket客户端与LauncherServer的Socket服务端建立的连接是否已经关闭*/
  private volatile boolean closed;

  LauncherConnection(Socket socket) throws IOException {
    this.socket = socket;
    this.out = new ObjectOutputStream(socket.getOutputStream());
    this.closed = false;
  }

  protected abstract void handle(Message msg) throws IOException;
  /** 用于从Socket客户端的输入流中读取LauncherServer发送的消息,并调用handler方法进行处理*/
  @Override
  public void run() {
    try {
      // 获取输入流
      FilteredObjectInputStream in = new FilteredObjectInputStream(socket.getInputStream());
      while (isOpen()) {
        // 转换成message
        Message msg = (Message) in.readObject();
        // 处理
        handle(msg);
      }
    } catch (EOFException eof) {
      // Remote side has closed the connection, just cleanup.
      try {
        close();
      } catch (Exception unused) {
        // no-op.
      }
    } catch (Exception e) {
      if (!closed) {
        LOG.log(Level.WARNING, "Error in inbound message handling.", e);
        try {
          close();
        } catch (Exception unused) {
          // no-op.
        }
      }
    }
  }
  /** 通过socket客户端与LaunchServer建立的连接向LauncherServer发送消息*/
  protected synchronized void send(Message msg) throws IOException {
    try {
      // 检查是否关闭
      CommandBuilderUtils.checkState(!closed, "Disconnected.");
      // 写数据
      out.writeObject(msg);
      out.flush();
    } catch (IOException ioe) {
      if (!closed) {
        LOG.log(Level.WARNING, "Error when sending message.", ioe);
        try {
          // 关闭连接
          close();
        } catch (Exception unused) {
          // no-op.
        }
      }
      throw ioe;
    }
  }
  /** 关闭Socket与LauncherServer的连接*/
  @Override
  public synchronized void close() throws IOException {
    if (isOpen()) {
      // 关闭状态设置为true
      closed = true;
      // 关闭socket
      socket.close();
    }
  }

  boolean isOpen() {
    return !closed;
  }

}

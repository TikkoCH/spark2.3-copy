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

package org.apache.spark.network.server;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;

/**
 * 用来处理TransportClient通过sendRPC()方法发送的消息的handler <br>
 * Handler for sendRPC() messages sent by {@link org.apache.spark.network.client.TransportClient}s.
 */
public abstract class RpcHandler {

  private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayRpcCallback();

  /**
   *
   * 接收单个RPC消息。 在此方法中抛出的任何异常将作为标准RPC失败以字符串形式发送回客户端。<br>
   * 对于单个TransportClient，此方法不会并行调用.
   * Receive a single RPC message. Any exception thrown while in this method will be sent back to
   * the client in string form as a standard RPC failure.
   *
   * This method will not be called in parallel for a single TransportClient (i.e., channel).
   *
   * @param client 一个客户端channel对象,该客户端中的handler能够处理请求返回给发起者.
   *               对于特定Channel,该对象始终是一个对象。
   *               <br>
   *               A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message   序列化的字节,就是传送的消息
   *                    The serialized bytes of the RPC.
   * @param callback 回调对象,无论成功失败,都会调用该对象.
   *                Callback which should be invoked exactly once upon success or failure of the
   *                 RPC.
   */
  public abstract void receive(
      TransportClient client,
      ByteBuffer message,
      RpcResponseCallback callback);

  /**
   * 返回包含TransportClient获得的当前流的状态StreamManager.<br>
   *
   * Returns the StreamManager which contains the state about which streams are currently being
   * fetched by a TransportClient.
   */
  public abstract StreamManager getStreamManager();

  /**
   * 重载receive,不需要响应的receive.默认实现会调用上面的receive方法,默认callback是ONE_WAY_CALLBACK.
   * Receives an RPC message that does not expect a reply. The default implementation will
   * call "{@link #receive(TransportClient, ByteBuffer, RpcResponseCallback)}" and log a warning if
   * any of the callback methods are called.
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message The serialized bytes of the RPC.
   */
  public void receive(TransportClient client, ByteBuffer message) {
    receive(client, message, ONE_WAY_CALLBACK);
  }

  /**
   * 当与客户端关联的channel是active状态调用.<br>
   * Invoked when the channel associated with the given client is active.
   */
  public void channelActive(TransportClient client) { }

  /**
   * 当与客户端关联的channel是非活动状态调用
   * Invoked when the channel associated with the given client is inactive.
   * No further requests will come from this client.
   */
  public void channelInactive(TransportClient client) { }

  /**
   * channel产生异常的时候调用
   * @param cause 异常原因
   * @param client 客户端
   */
  public void exceptionCaught(Throwable cause, TransportClient client) { }

  /**
   * 上面receive双参方法中的默认callback参数的实现类,回调会记录warn日志.
   */
  private static class OneWayRpcCallback implements RpcResponseCallback {

    private static final Logger logger = LoggerFactory.getLogger(OneWayRpcCallback.class);

    @Override
    public void onSuccess(ByteBuffer response) {
      logger.warn("Response provided for one-way RPC.");
    }

    @Override
    public void onFailure(Throwable e) {
      logger.error("Error response provided for one-way RPC.", e);
    }

  }

}

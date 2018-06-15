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

import io.netty.channel.Channel;

/**
 *
 * 客户端连接到服务器后，在传输服务器的客户端通道上执行的引导程序。
 * 然后可以自定义客户端通道用来诸如SASL认证之类的事情。
 * A bootstrap which is executed on a TransportServer's client channel once a client connects
 * to the server. This allows customizing the client channel to allow for things such as SASL
 * authentication.
 */
public interface TransportServerBootstrap {
  /**
   *
   * 如果需要的话可以自定义channel来包含一些新特性
   * Customizes the channel to include new features, if needed.
   *
   * @param channel  客户端打开的已连接channel The connected channel opened by the client.
   * @param rpcHandler 服务端的RPChandler The RPC handler for the server.
   * @return 返回channel使用的RPC handler The RPC handler to use for the channel.
   */
  RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler);
}

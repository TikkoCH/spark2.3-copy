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

import org.apache.spark.network.protocol.Message;

/**
 *
 * 处理来自Netty的请求或响应消息.
 * MessageHandler实例与单个Netty Channel关联（尽管它可能在同一个Channel上有多个客户端.<br>
 * Handles either request or response messages coming off of Netty. A MessageHandler instance
 * is associated with a single Netty Channel (though it may have multiple clients on the same
 * Channel.)
 */
public abstract class MessageHandler<T extends Message> {
  /**
   * 处理接受的单个信息
   * Handles the receipt of a single message. */
  public abstract void handle(T message) throws Exception;

  /**
   * 当本实例所在的channel是活动状态时调用.<br>
   * Invoked when the channel this MessageHandler is on is active. */
  public abstract void channelActive();

  /**
   * 当channel异常是调用<br>
   * Invoked when an exception was caught on the Channel. */
  public abstract void exceptionCaught(Throwable cause);

  /**
   * 当本实例所在的channel是非活动状态时调用.<br>
   * Invoked when the channel this MessageHandler is on is inactive. */
  public abstract void channelInactive();
}

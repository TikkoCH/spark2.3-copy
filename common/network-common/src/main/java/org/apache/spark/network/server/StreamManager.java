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

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;

/**
 *这个StreamManager用来从流中获取单个块.TransportRequestHandler使用这个类用来响应fetchChunk()的请求.
 *流的创建超出了传输层的范围，但给定的流保证只被一个客户端连接读取，这意味着特定流的getChunk（）将被串行调用，
 * 并且一旦与流关联的连接 已关闭，该流不会再被使用.<br>
 * The StreamManager is used to fetch individual chunks from a stream. This is used in
 * {@link TransportRequestHandler} in order to respond to fetchChunk() requests. Creation of the
 * stream is outside the scope of the transport layer, but a given stream is guaranteed to be read
 * by only one client connection, meaning that getChunk() for a particular stream will be called
 * serially and that once the connection associated with the stream is closed, that stream will
 * never be used again.
 */
public abstract class StreamManager {
  /**
   * 本方法用于响应fetchChunk()方法的请求.返回的buffer将按照原样传给client.
   * 单个流将与单个TCP连接关联，因此不会为特定的流并行调用此方法.
   * 可以按任何顺序请求块，并且可以重复请求，但实现不需要支持此行为.
   *  返回的ManagedBuffer在将数据写入网络之后会调用release()方法来释放资源.
   * Called in response to a fetchChunk() request. The returned buffer will be passed as-is to the
   * client. A single stream will be associated with a single TCP connection, so this method
   * will not be called in parallel for a particular stream.
   *
   * Chunks may be requested in any order, and requests may be repeated, but it is not required
   * that implementations support this behavior.
   *
   * The returned ManagedBuffer will be release()'d after being written to the network.
   *
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @param chunkIndex 0-indexed chunk of the stream that's requested
   */
  public abstract ManagedBuffer getChunk(long streamId, int chunkIndex);

  /**
   * 用来响应stream()请求.返回的数据是通过TCP连接流入client.
   * 请注意，streamId参数与getChunk（long，int）方法中类似命名的参数无关<br>
   * Called in response to a stream() request. The returned data is streamed to the client
   * through a single TCP connection.
   *
   * Note the <code>streamId</code> argument is not related to the similarly named argument in the
   * {@link #getChunk(long, int)} method.
   *
   * @param streamId 之前在StreamManager中注册过的stream的id<br>
   *                id of a stream that has been previously registered with the StreamManager.
   * @return 返回流的托管buffer,如果流未找到返回null<br>
   * A managed buffer for the stream, or null if the stream was not found.
   */
  public ManagedBuffer openStream(String streamId) {
    throw new UnsupportedOperationException();
  }

  /**
   * 用来关联单个客户端和流,保证流只有一个接收客户端.getChunk()方法会在连接上串行调用并且一旦连接关闭,
   *流不会再被使用,并且可以进行清理.在流的第一个getChunk()方法之前必须先执行本方法,但是可能被一个channel
   * 和streamId多次调用.<br>
   * Associates a stream with a single client connection, which is guaranteed to be the only reader
   * of the stream. The getChunk() method will be called serially on this connection and once the
   * connection is closed, the stream will never be used again, enabling cleanup.
   *
   * This must be called before the first getChunk() on the stream, but it may be invoked multiple
   * times with the same channel and stream id.
   */
  public void registerChannel(Channel channel, long streamId) { }

  /**
   *  代表指定的channel已经被关闭了.当该方法执行之后,要保证不再从关联的流中读取数据,所以任何状态都可以清除.<br>
   * Indicates that the given channel has been terminated. After this occurs, we are guaranteed not
   * to read from the associated streams again, so any state can be cleaned up.
   */
  public void connectionTerminated(Channel channel) { }

  /**
   *确定客户端是否有权从流中读取数据,如果无权则抛出异常SecurityException<br>
   * Verify that the client is authorized to read from the given stream.
   *
   * @throws SecurityException If client is not authorized.
   */
  public void checkAuthorization(TransportClient client, long streamId) { }

  /**
   * 返回在此StreamManager中正在传输但尚未完成的块数。
   * Return the number of chunks being transferred and not finished yet in this StreamManager.
   */
  public long chunksBeingTransferred() {
    return 0;
  }

  /**
   * 当开始传送数据块调用该方法
   * Called when start sending a chunk.
   */
  public void chunkBeingSent(long streamId) { }

  /**
   * 当开始传送流调用该方法
   * Called when start sending a stream.
   */
  public void streamBeingSent(String streamId) { }

  /**
   * 当块传输成功时调用该方法
   * Called when a chunk is successfully sent.
   */
  public void chunkSent(long streamId) { }

  /**
   * 当流传输成功调用该方法
   * Called when a stream is successfully sent.
   */
  public void streamSent(String streamId) { }

}

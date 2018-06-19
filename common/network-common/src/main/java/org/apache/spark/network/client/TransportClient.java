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

package org.apache.spark.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.OneWayMessage;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.StreamRequest;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * 客户端用来获取之前确定的流的连续数据块.这个API用于提高大体量数据的传输效率,将其分解成几百k到几兆的数据.
 * 注意当客户端从流中获取数据块时,实际的流的设置是在传输层之外完成的.`sendRPC`方法可以控制客户端和服务端
 * 之间的平面通信来对此设置<br>
 * 可以通过TransportClientFactory创建TransportClient实例.一个传输客户端可以用于多个流，
 * 但是任何给定的流必须被限制为单个客户端，以避免乱序响应。此类用于向服务器发送请求，而TransportResponseHandler
 * 负责处理来自服务器的响应。
 * 线程安全,可被多个线程调用<br>
 * Client for fetching consecutive chunks of a pre-negotiated stream. This API is intended to allow
 * efficient transfer of a large amount of data, broken up into chunks with size ranging from
 * hundreds of KB to a few MB.
 *
 * Note that while this client deals with the fetching of chunks from a stream (i.e., data plane),
 * the actual setup of the streams is done outside the scope of the transport layer. The convenience
 * method "sendRPC" is provided to enable control plane communication between the client and server
 * to perform this setup.
 *
 * For example, a typical workflow might be:
 * client.sendRPC(new OpenFile("/foo")) --&gt; returns StreamId = 100
 * client.fetchChunk(streamId = 100, chunkIndex = 0, callback)
 * client.fetchChunk(streamId = 100, chunkIndex = 1, callback)
 * ...
 * client.sendRPC(new CloseStream(100))
 *
 * Construct an instance of TransportClient using {@link TransportClientFactory}. A single
 * TransportClient may be used for multiple streams, but any given stream must be restricted to a
 * single client, in order to avoid out-of-order responses.
 *
 * NB: This class is used to make requests to the server, while {@link TransportResponseHandler} is
 * responsible for handling responses from the server.
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  private final Channel channel;
  private final TransportResponseHandler handler;
  @Nullable private String clientId;
  private volatile boolean timedOut;

  public TransportClient(Channel channel, TransportResponseHandler handler) {
    this.channel = Preconditions.checkNotNull(channel);
    this.handler = Preconditions.checkNotNull(handler);
    this.timedOut = false;
  }

  public Channel getChannel() {
    return channel;
  }

  public boolean isActive() {
    return !timedOut && (channel.isOpen() || channel.isActive());
  }

  public SocketAddress getSocketAddress() {
    return channel.remoteAddress();
  }

  /**
   * 验证可用时,返回客户端用来验证自身的id.如果验证不可用返回null.<br>
   * Returns the ID used by the client to authenticate itself when authentication is enabled.
   *
   * @return The client ID, or null if authentication is disabled.
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * 设置验证过的ID,身份验证会使用这个.尝试设置额外的clientID会抛出异常.<br>
   * Sets the authenticated client ID. This is meant to be used by the authentication layer.
   *
   * Trying to set a different client ID after it's been set will result in an exception.
   */
  public void setClientId(String id) {
    Preconditions.checkState(clientId == null, "Client ID has already been set.");
    this.clientId = id;
  }

  /**
   *
   * 根据之前获得的streamId向远程请求单个数据块.块索引从0开始,对单个数据块请求多次也是可以的,
   * 但是有些流不支持这种操作.多个fetchChunk可能同时请求,但是可以保证按照请求顺序返回,这种情况
   * 建立在假设只有一个TransportClient用来获取数据块.<br>
   * Requests a single chunk from the remote side, from the pre-negotiated streamId.
   *
   * Chunk indices go from 0 onwards. It is valid to request the same chunk multiple times, though
   * some streams may not support this.
   *
   * Multiple fetchChunk requests may be outstanding simultaneously, and the chunks are guaranteed
   * to be returned in the same order that they were requested, assuming only a single
   * TransportClient is used to fetch the chunks.
   *
   * @param streamId Identifier that refers to a stream in the remote StreamManager. This should
   *                 be agreed upon by client and server beforehand.
   * @param chunkIndex 0-based index of the chunk to fetch
   * @param callback Callback invoked upon successful receipt of chunk, or upon any failure.
   */
  public void fetchChunk(
      long streamId,
      int chunkIndex,
      ChunkReceivedCallback callback) {
    long startTime = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.debug("Sending fetch chunk request {} to {}", chunkIndex, getRemoteAddress(channel));
    }
    //创建一个StreamChunkId对象
    StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
    //在handler中添加fetch请求
    handler.addFetchRequest(streamChunkId, callback);
    //writeAndFlush就是将数据写出了,写出的是streamChunkId对象,并且添加了listener.对成功和失败进行处理.
    channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(future -> {
      if (future.isSuccess()) {
        long timeTaken = System.currentTimeMillis() - startTime;
        if (logger.isTraceEnabled()) {
          logger.trace("Sending request {} to {} took {} ms", streamChunkId,
            getRemoteAddress(channel), timeTaken);
        }
      } else {
        String errorMsg = String.format("Failed to send request %s to %s: %s", streamChunkId,
          getRemoteAddress(channel), future.cause());
        logger.error(errorMsg, future.cause());
        handler.removeFetchRequest(streamChunkId);
        channel.close();
        try {
          callback.onFailure(chunkIndex, new IOException(errorMsg, future.cause()));
        } catch (Exception e) {
          logger.error("Uncaught exception in RPC response callback handler!", e);
        }
      }
    });
  }

  /**
   *  根据streamID请求远端流数据
   *  Request to stream the data with the given stream ID from the remote end.
   *
   * @param streamId The stream to fetch.
   * @param callback Object to call with the stream data.
   */
  public void stream(String streamId, StreamCallback callback) {
    long startTime = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.debug("Sending stream request for {} to {}", streamId, getRemoteAddress(channel));
    }
    // 这里需要同步保证回调被添加到队列和RPC被写进socket是原子性的,而且这样能保证响应到达时回调的调用顺序是正确的
    // Need to synchronize here so that the callback is added to the queue and the RPC is
    // written to the socket atomically, so that callbacks are called in the right order
    // when responses arrive.
    synchronized (this) {
      handler.addStreamCallback(streamId, callback);
      channel.writeAndFlush(new StreamRequest(streamId)).addListener(future -> {
        if (future.isSuccess()) {
          long timeTaken = System.currentTimeMillis() - startTime;
          if (logger.isTraceEnabled()) {
            logger.trace("Sending request for {} to {} took {} ms", streamId,
              getRemoteAddress(channel), timeTaken);
          }
        } else {
          String errorMsg = String.format("Failed to send request for %s to %s: %s", streamId,
            getRemoteAddress(channel), future.cause());
          logger.error(errorMsg, future.cause());
          channel.close();
          try {
            callback.onFailure(streamId, new IOException(errorMsg, future.cause()));
          } catch (Exception e) {
            logger.error("Uncaught exception in RPC response callback handler!", e);
          }
        }
      });
    }
  }

  /**
   * 发送不透明的消息给服务端的RpcHandler.当服务端响应或失败时回调会调用.
   * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
   * with the server's response or upon any failure.
   *
   * @param message The message to send.
   * @param callback Callback to handle the RPC's reply.
   * @return The RPC's id.
   */
  public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
    long startTime = System.currentTimeMillis();
    if (logger.isTraceEnabled()) {
      logger.trace("Sending RPC to {}", getRemoteAddress(channel));
    }
    //生成一个正数uuid.
    long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
    //在handler中添加请求.
    handler.addRpcRequest(requestId, callback);
    //发送RPC请求,请求包含了requestId,和NioManagedBuffer包装的message,添加监听,对响应结果做处理.
    channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message)))
        .addListener(future -> {
          if (future.isSuccess()) {
            //如果发送成功,只记录下日志
            long timeTaken = System.currentTimeMillis() - startTime;
            if (logger.isTraceEnabled()) {
              logger.trace("Sending request {} to {} took {} ms", requestId,
                getRemoteAddress(channel), timeTaken);
            }
          } else {
            String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
              getRemoteAddress(channel), future.cause());
            logger.error(errorMsg, future.cause());
            //如果发送失败,除了打印日志还会删除handler中的缓存.
            handler.removeRpcRequest(requestId);
            channel.close();
            try {
              callback.onFailure(new IOException(errorMsg, future.cause()));
            } catch (Exception e) {
              logger.error("Uncaught exception in RPC response callback handler!", e);
            }
          }
        });

    return requestId;
  }

  /**
   * 同步地发送非透明消息给服务端的RPChandler,一直阻塞到超时.
   * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to
   * a specified timeout for a response.
   */
  public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
    //这是google的并发包中封装的future
    final SettableFuture<ByteBuffer> result = SettableFuture.create();
    //重写了sendRpc方法
    sendRpc(message, new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        ByteBuffer copy = ByteBuffer.allocate(response.remaining());
        copy.put(response);
        //Nio的缓冲区API:ByteBuffer.切换读写模式需要flip()
        // flip "copy" to make it readable
        copy.flip();
        result.set(copy);
      }

      @Override
      public void onFailure(Throwable e) {
        result.setException(e);
      }
    });

    try {
      //这个方法会一直阻塞,直到获得结果
      return result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * 发送非透明消息给服务端的RPChandler.无回复,无消息传递保证.OneWayMessage
   * Sends an opaque message to the RpcHandler on the server-side. No reply is expected for the
   * message, and no delivery guarantees are made.
   *
   * @param message The message to send.
   */
  public void send(ByteBuffer message) {
    channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
  }

  /**
   * 删除给定RPC的相关的所有状态
   * Removes any state associated with the given RPC.
   *
   * @param requestId The RPC id returned by {@link #sendRpc(ByteBuffer, RpcResponseCallback)}.
   */
  public void removeRpcRequest(long requestId) {
    handler.removeRpcRequest(requestId);
  }

  /**
   * 标记该channel的超时属性为true
   * Mark this channel as having timed out. */
  public void timeOut() {
    this.timedOut = true;
  }

  @VisibleForTesting
  public TransportResponseHandler getHandler() {
    return handler;
  }

  @Override
  public void close() {
    // close is a local operation and should finish with milliseconds; timeout just to be safe
    channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("remoteAdress", channel.remoteAddress())
      .add("clientId", clientId)
      .add("isActive", isActive())
      .toString();
  }
}

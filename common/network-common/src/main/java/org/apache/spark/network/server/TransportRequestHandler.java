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

import java.net.SocketAddress;
import java.nio.ByteBuffer;

import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.OneWayMessage;
import org.apache.spark.network.protocol.RequestMessage;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.protocol.StreamFailure;
import org.apache.spark.network.protocol.StreamRequest;
import org.apache.spark.network.protocol.StreamResponse;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 *
 * 处理来自客户端的请求并将块数据写回的处理程序.每个处理程序连接到一个Netty通道，
 * 并跟踪哪些流通过此通道获取，以便在通道终止时清除它们.
 * 这些消息应该由TransportServer通过管道设置进行处理.<br>
 * A handler that processes requests from clients and writes chunk data back. Each handler is
 * attached to a single Netty channel, and keeps track of which streams have been fetched via this
 * channel, in order to clean them up if the channel is terminated (see #channelUnregistered).
 *
 * The messages should have been processed by the pipeline setup by {@link TransportServer}.
 */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {
  private static final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);

  /** The Netty channel that this handler is associated with. */
  private final Channel channel;

  /** Client on the same channel allowing us to talk back to the requester. */
  private final TransportClient reverseClient;

  /** Handles all RPC messages. */
  private final RpcHandler rpcHandler;

  /** Returns each chunk part of a stream. */
  private final StreamManager streamManager;

  /** The max number of chunks being transferred and not finished yet. */
  private final long maxChunksBeingTransferred;

  public TransportRequestHandler(
      Channel channel,
      TransportClient reverseClient,
      RpcHandler rpcHandler,
      Long maxChunksBeingTransferred) {
    this.channel = channel;
    this.reverseClient = reverseClient;
    this.rpcHandler = rpcHandler;
    this.streamManager = rpcHandler.getStreamManager();
    this.maxChunksBeingTransferred = maxChunksBeingTransferred;
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    rpcHandler.exceptionCaught(cause, reverseClient);
  }

  @Override
  public void channelActive() {
    rpcHandler.channelActive(reverseClient);
  }

  @Override
  public void channelInactive() {
    if (streamManager != null) {
      try {
        streamManager.connectionTerminated(channel);
      } catch (RuntimeException e) {
        logger.error("StreamManager connectionTerminated() callback failed.", e);
      }
    }
    rpcHandler.channelInactive(reverseClient);
  }

  //整个类的核心逻辑从这里开始,if判断不同Message类型的处理方式
  @Override
  public void handle(RequestMessage request) {
    if (request instanceof ChunkFetchRequest) {
      processFetchRequest((ChunkFetchRequest) request);
    } else if (request instanceof RpcRequest) {
      processRpcRequest((RpcRequest) request);
    } else if (request instanceof OneWayMessage) {
      processOneWayMessage((OneWayMessage) request);
    } else if (request instanceof StreamRequest) {
      processStreamRequest((StreamRequest) request);
    } else {
      throw new IllegalArgumentException("Unknown request type: " + request);
    }
  }

  /**
   * 如果是ChunkFetchRequest类型,则使用这个方法进行处理.
   * @param req
   */
  private void processFetchRequest(final ChunkFetchRequest req) {
    if (logger.isTraceEnabled()) {
      logger.trace("Received req from {} to fetch block {}", getRemoteAddress(channel),
        req.streamChunkId);
    }
    //获取正在传输的块的数量
    long chunksBeingTransferred = streamManager.chunksBeingTransferred();
    //如果大于等于本类中的属性maxChunksBeingTransferred,关闭channel.这可能是2.3新增的
    if (chunksBeingTransferred >= maxChunksBeingTransferred) {
      logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
        chunksBeingTransferred, maxChunksBeingTransferred);
      channel.close();
      return;
    }
    ManagedBuffer buf;
    try {
      //校验客户端是否有权限读取,没有权限抛出异常
      streamManager.checkAuthorization(reverseClient, req.streamChunkId.streamId);
      //注册管道
      streamManager.registerChannel(channel, req.streamChunkId.streamId);
      //获取单个数据块,被封装成了ManagedBuffer
      buf = streamManager.getChunk(req.streamChunkId.streamId, req.streamChunkId.chunkIndex);
    } catch (Exception e) {
      logger.error(String.format("Error opening block %s for request from %s",
        req.streamChunkId, getRemoteAddress(channel)), e);
      respond(new ChunkFetchFailure(req.streamChunkId, Throwables.getStackTraceAsString(e)));
      return;
    }
    //开始传送数据
    streamManager.chunkBeingSent(req.streamChunkId.streamId);
    //流ID和ManagedBuffer封装成ChunkFetchSuccess返回给客户端,并且对respond返回的handler添加监听器
    respond(new ChunkFetchSuccess(req.streamChunkId, buf)).addListener(future -> {
      streamManager.chunkSent(req.streamChunkId.streamId);
    });
  }

  /**
   * 处理StreamRequest类型数据的方法
   * @param req
   */
  private void processStreamRequest(final StreamRequest req) {
    if (logger.isTraceEnabled()) {
      logger.trace("Received req from {} to fetch stream {}", getRemoteAddress(channel),
        req.streamId);
    }
    //和上面方法逻辑大体相同
    long chunksBeingTransferred = streamManager.chunksBeingTransferred();
    if (chunksBeingTransferred >= maxChunksBeingTransferred) {
      logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
        chunksBeingTransferred, maxChunksBeingTransferred);
      channel.close();
      return;
    }
    ManagedBuffer buf;
    //获取流buf,封装成了ManagedBuffer
    try {
      buf = streamManager.openStream(req.streamId);
    } catch (Exception e) {
      logger.error(String.format(
        "Error opening stream %s for request from %s", req.streamId, getRemoteAddress(channel)), e);
      respond(new StreamFailure(req.streamId, Throwables.getStackTraceAsString(e)));
      return;
    }
    //如果buf不等于空,封装StreamResponse(req.streamId, buf.size(), buf)返回给客户端,并为respond返回的handler添加监听
    if (buf != null) {
      streamManager.streamBeingSent(req.streamId);
      respond(new StreamResponse(req.streamId, buf.size(), buf)).addListener(future -> {
        streamManager.streamSent(req.streamId);
      });
    } else {
      respond(new StreamFailure(req.streamId, String.format(
        "Stream '%s' was not found.", req.streamId)));
    }
  }

  private void processRpcRequest(final RpcRequest req) {
    try {
      rpcHandler.receive(reverseClient, req.body().nioByteBuffer(), new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
        }

        @Override
        public void onFailure(Throwable e) {
          respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        }
      });
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
      respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
    } finally {
      req.body().release();
    }
  }

  private void processOneWayMessage(OneWayMessage req) {
    try {
      rpcHandler.receive(reverseClient, req.body().nioByteBuffer());
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() for one-way message.", e);
    } finally {
      req.body().release();
    }
  }

  /**
   * Responds to a single message with some Encodable object. If a failure occurs while sending,
   * it will be logged and the channel closed.
   */
  private ChannelFuture respond(Encodable result) {
    SocketAddress remoteAddress = channel.remoteAddress();
    return channel.writeAndFlush(result).addListener(future -> {
      if (future.isSuccess()) {
        logger.trace("Sent result {} to client {}", result, remoteAddress);
      } else {
        logger.error(String.format("Error sending result %s to %s; closing connection",
          result, remoteAddress), future.cause());
        channel.close();
      }
    });
  }
}

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

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;

/**
 * 允许Iterator<ManagedBuffer>注册的StreamManager.被客户端单独获取为chunk.
 * 每个注册的buffer是一个chunk.
 * StreamManager which allows registration of an Iterator&lt;ManagedBuffer&gt;, which are
 * individually fetched as chunks by the client. Each registered buffer is one chunk.
 */
public class OneForOneStreamManager extends StreamManager {
  private static final Logger logger = LoggerFactory.getLogger(OneForOneStreamManager.class);

  private final AtomicLong nextStreamId;
  // 缓存stremId和StreamState
  private final ConcurrentHashMap<Long, StreamState> streams;

  /**
   * 单个流的状态
   * State of a single stream. */
  private static class StreamState {
    // appId,应用id
    final String appId;
    // ManagedBuffer的缓冲
    final Iterator<ManagedBuffer> buffers;
    // 当前流关联的Channel
    // The channel associated to the stream
    Channel associatedChannel = null;
    // 为了保证客户端按顺序每次请求一个chunk,所以使用此属性跟踪客户端接收到的ManagedBuffer索引
    // Used to keep track of the index of the buffer that the user has retrieved, just to ensure
    // that the caller only requests each chunk one at a time, in order.
    int curChunk = 0;
    // 用于追踪已经传输但还未完成的chunk数量.
    // Used to keep track of the number of chunks being transferred and not finished yet.
    volatile long chunksBeingTransferred = 0L;

    StreamState(String appId, Iterator<ManagedBuffer> buffers) {
      this.appId = appId;
      this.buffers = Preconditions.checkNotNull(buffers);
    }
  }

  public OneForOneStreamManager() {
    // For debugging purposes, start with a random stream id to help identifying different streams.
    // This does not need to be globally unique, only unique to this class.
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streams = new ConcurrentHashMap<>();
  }
  // 用于注册管道,实际上是将流和客户端tcp连接关联起来.
  @Override
  public void registerChannel(Channel channel, long streamId) {
    if (streams.containsKey(streamId)) {
      streams.get(streamId).associatedChannel = channel;
    }
  }
  // 用于获取单个chunk,封装成ManagedBuffer
  @Override
  public ManagedBuffer getChunk(long streamId, int chunkIndex) {
    // 根据stremId从缓存获取state
    StreamState state = streams.get(streamId);
    // 如果不相等,说明顺序有问题.抛异常
    if (chunkIndex != state.curChunk) {
      throw new IllegalStateException(String.format(
        "Received out-of-order chunk index %s (expected %s)", chunkIndex, state.curChunk));
    } else if (!state.buffers.hasNext()) {
      // 如果buffers没有下一个了,说明索引越界了,抛异常
      throw new IllegalStateException(String.format(
        "Requested chunk index beyond end %s", chunkIndex));
    }
    // curChunk+1,为下次接受请求准备.
    state.curChunk += 1;
    // 获取buffer
    ManagedBuffer nextChunk = state.buffers.next();

    if (!state.buffers.hasNext()) {
      // 如果已经到达结尾了,说明数据已被客户端完全拿走,删除streamId
      logger.trace("Removing stream id {}", streamId);
      streams.remove(streamId);
    }
    // 返回
    return nextChunk;
  }

  @Override
  public ManagedBuffer openStream(String streamChunkId) {
    Pair<Long, Integer> streamChunkIdPair = parseStreamChunkId(streamChunkId);
    return getChunk(streamChunkIdPair.getLeft(), streamChunkIdPair.getRight());
  }

  public static String genStreamChunkId(long streamId, int chunkId) {
    return String.format("%d_%d", streamId, chunkId);
  }

  // Parse streamChunkId to be stream id and chunk id. This is used when fetch remote chunk as a
  // stream.
  public static Pair<Long, Integer> parseStreamChunkId(String streamChunkId) {
    String[] array = streamChunkId.split("_");
    assert array.length == 2:
      "Stream id and chunk index should be specified.";
    long streamId = Long.valueOf(array[0]);
    int chunkIndex = Integer.valueOf(array[1]);
    return ImmutablePair.of(streamId, chunkIndex);
  }

  @Override
  public void connectionTerminated(Channel channel) {
    // Close all streams which have been associated with the channel.
    for (Map.Entry<Long, StreamState> entry: streams.entrySet()) {
      StreamState state = entry.getValue();
      if (state.associatedChannel == channel) {
        streams.remove(entry.getKey());

        // Release all remaining buffers.
        while (state.buffers.hasNext()) {
          state.buffers.next().release();
        }
      }
    }
  }

  @Override
  public void checkAuthorization(TransportClient client, long streamId) {
    if (client.getClientId() != null) {
      // 如果客户端id不为null
      StreamState state = streams.get(streamId);
      // 检查
      Preconditions.checkArgument(state != null, "Unknown stream ID.");
      if (!client.getClientId().equals(state.appId)) {
        // 如果客户端id不等于appid,抛异常
        throw new SecurityException(String.format(
          "Client %s not authorized to read stream %d (app %s).",
          client.getClientId(),
          streamId,
          state.appId));
      }
    }
  }

  @Override
  public void chunkBeingSent(long streamId) {
    StreamState streamState = streams.get(streamId);
    if (streamState != null) {
      streamState.chunksBeingTransferred++;
    }

  }

  @Override
  public void streamBeingSent(String streamId) {
    chunkBeingSent(parseStreamChunkId(streamId).getLeft());
  }

  @Override
  public void chunkSent(long streamId) {
    StreamState streamState = streams.get(streamId);
    if (streamState != null) {
      streamState.chunksBeingTransferred--;
    }
  }

  @Override
  public void streamSent(String streamId) {
    chunkSent(OneForOneStreamManager.parseStreamChunkId(streamId).getLeft());
  }

  @Override
  public long chunksBeingTransferred() {
    long sum = 0L;
    for (StreamState streamState: streams.values()) {
      sum += streamState.chunksBeingTransferred;
    }
    return sum;
  }

  /**
   * 用于向OneForOneStreamManager中注册流.
   * 注册ManagedBuffers流，一次一个地向调用者提供单独的块。
   * 每个ManagedBuffer在线路上传输后都将是release（）。
   * 如果在迭代器完全耗尽之前关闭了客户端连接，则剩余的缓冲区将全部为release（）。
   * 如果提供了应用ID，则只允许使用给定应用ID进行身份验证的呼叫者从此流中获取。<br>
   * Registers a stream of ManagedBuffers which are served as individual chunks one at a time to
   * callers. Each ManagedBuffer will be release()'d after it is transferred on the wire. If a
   * client connection is closed before the iterator is fully drained, then the remaining buffers
   * will all be release()'d.
   *
   * If an app ID is provided, only callers who've authenticated with the given app ID will be
   * allowed to fetch from this stream.
   */
  public long registerStream(String appId, Iterator<ManagedBuffer> buffers) {
    // 生辰stremId
    long myStreamId = nextStreamId.getAndIncrement();
    // 创建StreamState,并和id放入缓存
    streams.put(myStreamId, new StreamState(appId, buffers));
    return myStreamId;
  }

}

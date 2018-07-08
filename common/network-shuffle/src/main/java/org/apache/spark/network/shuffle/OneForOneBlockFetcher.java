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

package org.apache.spark.network.shuffle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.network.util.TransportConf;

/**
 * TransportClient顶部的简单包装器，它将每个块解释为整个块，并相应地调用BlockFetchingListener。
 * 只要存在一条返回ShuffleStreamHandle的“open blocks”消息，
 * 并且使用了Java序列化，该类就与实际的RPC处理程序无关。
 * Simple wrapper on top of a TransportClient which interprets each chunk as a whole block, and
 * invokes the BlockFetchingListener appropriately. This class is agnostic to the actual RPC
 * handler, as long as there is a single "open blocks" message which returns a ShuffleStreamHandle,
 * and Java serialization is used.
 *
 * Note that this typically corresponds to a
 * {@link org.apache.spark.network.server.OneForOneStreamManager} on the server side.
 */
public class OneForOneBlockFetcher {
  private static final Logger logger = LoggerFactory.getLogger(OneForOneBlockFetcher.class);
  // 用于向服务端发送请求的client
  private final TransportClient client;
  // 携带远程节点的appId,execId,blocksIds.这用于知道从远程哪个实例获取哪些block
  // 并知道远端Executor生成的Block.
  private final OpenBlocks openMessage;
  // BlockId数组.与openMessage的BlockIds一致
  private final String[] blockIds;
  // 用于获取Block成功或失败时的回调
  private final BlockFetchingListener listener;
  // 获取块结果的回调,配合listener使用
  private final ChunkReceivedCallback chunkCallback;
  // 传输配置
  private final TransportConf transportConf;
  // 临时文件管理
  private final TempFileManager tempFileManager;
  // client发送给server OpenBlocks消息后,server会在strems中缓存读取到的ManagedBufferlist,
  // 并且生成对应的streamId,将streamId和ManagedBuffer封装成StreamHandle返回客户端.
  // 客户端的streamHandle属性将持有该消息.
  private StreamHandle streamHandle = null;

  public OneForOneBlockFetcher(
    TransportClient client,
    String appId,
    String execId,
    String[] blockIds,
    BlockFetchingListener listener,
    TransportConf transportConf) {
    this(client, appId, execId, blockIds, listener, transportConf, null);
  }

  public OneForOneBlockFetcher(
      TransportClient client,
      String appId,
      String execId,
      String[] blockIds,
      BlockFetchingListener listener,
      TransportConf transportConf,
      TempFileManager tempFileManager) {
    this.client = client;
    this.openMessage = new OpenBlocks(appId, execId, blockIds);
    this.blockIds = blockIds;
    this.listener = listener;
    this.chunkCallback = new ChunkCallback();
    this.transportConf = transportConf;
    this.tempFileManager = tempFileManager;
  }

  /** Callback invoked on receipt of each chunk. We equate a single chunk to a single block. */
  private class ChunkCallback implements ChunkReceivedCallback {
    @Override
    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
      // On receipt of a chunk, pass it upwards as a block.
      listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
    }

    @Override
    public void onFailure(int chunkIndex, Throwable e) {
      // On receipt of a failure, fail every block from chunkIndex onwards.
      String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
      failRemainingBlocks(remainingBlockIds, e);
    }
  }

  /**
   * 这个方法用于获取远端的block,每次获取都会调用listener.给定的消息会被java序列化,并且Rpc
   * 会返回一个StreamHandle.我们会立即发送所有获取消息.
   * Begins the fetching process, calling the listener with every block fetched.
   * The given message will be serialized with the Java serializer, and the RPC must return a
   * {@link StreamHandle}. We will send all fetch requests immediately, without throttling.
   */
  public void start() {
    // 校验数量
    if (blockIds.length == 0) {
      throw new IllegalArgumentException("Zero-sized blockIds array");
    }
    // 发送openMessage
    client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {
      // 匿名内部类回调
      @Override
      public void onSuccess(ByteBuffer response) {
        try {
          // 成功的话,从响应中获取streamHandle
          streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
          logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);
          // 立即请求所有的chunk.我们期望由于[[ShuffleBlockFetcherIterator]]中更高级别的分块，
          // 请求的总大小是合理的。
          // Immediately request all chunks -- we expect that the total size of the request is
          // reasonable due to higher level chunking in [[ShuffleBlockFetcherIterator]].
          for (int i = 0; i < streamHandle.numChunks; i++) {
            if (tempFileManager != null) {
              // 如果临时文件管理不为null,根据streamId获取远程数据流
              client.stream(OneForOneStreamManager.genStreamChunkId(streamHandle.streamId, i),
                new DownloadCallback(i));
            } else {
              // 调用fetchChunk获取数据
              client.fetchChunk(streamHandle.streamId, i, chunkCallback);
            }
          }
        } catch (Exception e) {
          logger.error("Failed while starting block fetches after success", e);
          failRemainingBlocks(blockIds, e);
        }
      }

      @Override
      public void onFailure(Throwable e) {
        logger.error("Failed while starting block fetches", e);
        // 失败的话会为每个blockid回调该方法
        failRemainingBlocks(blockIds, e);
      }
    });
  }

  /** Invokes the "onBlockFetchFailure" callback for every listed block id. */
  private void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
    for (String blockId : failedBlockIds) {
      try {
        listener.onBlockFetchFailure(blockId, e);
      } catch (Exception e2) {
        logger.error("Error in block fetch failure callback", e2);
      }
    }
  }

  private class DownloadCallback implements StreamCallback {

    private WritableByteChannel channel = null;
    private File targetFile = null;
    private int chunkIndex;

    DownloadCallback(int chunkIndex) throws IOException {
      this.targetFile = tempFileManager.createTempFile();
      this.channel = Channels.newChannel(new FileOutputStream(targetFile));
      this.chunkIndex = chunkIndex;
    }

    @Override
    public void onData(String streamId, ByteBuffer buf) throws IOException {
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
    }

    @Override
    public void onComplete(String streamId) throws IOException {
      channel.close();
      ManagedBuffer buffer = new FileSegmentManagedBuffer(transportConf, targetFile, 0,
        targetFile.length());
      listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
      if (!tempFileManager.registerTempFileToClean(targetFile)) {
        targetFile.delete();
      }
    }

    @Override
    public void onFailure(String streamId, Throwable cause) throws IOException {
      channel.close();
      // On receipt of a failure, fail every block from chunkIndex onwards.
      String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
      failRemainingBlocks(remainingBlockIds, cause);
      targetFile.delete();
    }
  }
}

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

package org.apache.spark.network.netty
// scalastyle:off
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks, StreamHandle, UploadBlock}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, StorageLevel}

/**
 * 通过简单地为每个请求的块注册一个块来提供open block的请求.处理打开和上传任意BlockManager的block。
  * 打开的block使用“一对一”策略注册，这意味着每个传输层chunk相当于一个Spark级别的shuffle block.
  * Serves requests to open blocks by simply registering one chunk per block requested.
 * Handles opening and uploading arbitrary BlockManager blocks.
 *
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 */
class NettyBlockRpcServer(
    appId: String,
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  private val streamManager = new OneForOneStreamManager()
  // 接收消息
  override def receive(
      client: TransportClient,
      rpcMessage: ByteBuffer,
      responseContext: RpcResponseCallback): Unit = {
    // 获取message
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    logTrace(s"Received request: $message")
    // 消息两种类型:openBlocks和uploadBlock
    message match {
      case openBlocks: OpenBlocks =>
        // 取其blockIds长度
        val blocksNum = openBlocks.blockIds.length
        // 根据blockid的遍历,生成managedbuffer序列
        val blocks = for (i <- (0 until blocksNum).view)
          yield blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i)))
        // 将上面的blocks注册到streamManager缓存streams中
        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava)
        logTrace(s"Registered streamId $streamId with $blocksNum buffers")
        // 创建StreamHandle,并通过Context响应客户端
        responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)

      case uploadBlock: UploadBlock =>
        // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
        // 通过反序列化得到StorageLevel和Block类型
        val (level: StorageLevel, classTag: ClassTag[_]) = {
          serializer
            .newInstance()
            .deserialize(ByteBuffer.wrap(uploadBlock.metadata))
            .asInstanceOf[(StorageLevel, ClassTag[_])]
        }
        // Block数据封装成NioManagedBuffer
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
        // 获取uploadBlock中的BlockId
        val blockId = BlockId(uploadBlock.blockId)
        // 将Block存入本地
        blockManager.putBlockData(blockId, data, level, classTag)
        // 响应
        responseContext.onSuccess(ByteBuffer.allocate(0))
    }
  }

  override def getStreamManager(): StreamManager = streamManager
}

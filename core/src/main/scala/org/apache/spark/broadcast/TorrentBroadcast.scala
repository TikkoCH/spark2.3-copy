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

package org.apache.spark.broadcast
// scalastyle:off
import java.io._
import java.nio.ByteBuffer
import java.util.zip.Adler32

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * A BitTorrent-like implementation of [[org.apache.spark.broadcast.Broadcast]].
 *
 * The mechanism is as follows:
 *
 * The driver divides the serialized object into small chunks and
 * stores those chunks in the BlockManager of the driver.
 *
 * On each executor, the executor first attempts to fetch the object from its BlockManager. If
 * it does not exist, it then uses remote fetches to fetch the small chunks from the driver and/or
 * other executors if available. Once it gets the chunks, it puts the chunks in its own
 * BlockManager, ready for other executors to fetch from.
 *
 * This prevents the driver from being the bottleneck in sending out multiple copies of the
 * broadcast data (one per executor).
 *
 * When initialized, TorrentBroadcast objects read SparkEnv.get.conf.
 *
 * @param obj object to broadcast
 * @param id A unique identifier for the broadcast variable.
 */
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
   * 从Executor或者Driver上读取广播block的值.由readBroadcastBlock获取广播对象.
    * 懒加载意味着构造TorrentBroadcast时并不会构造.等明确调用_value值时创建.
    * 调用下面的getValue方法会构造该值.
    * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
   * which builds this value by reading blocks from the driver and/or other executors.
   *
   * On the driver, if the value is required, it is read lazily from the block manager.
   */
  @transient private lazy val _value: T = readBroadcastBlock()

  /**
    * 使用的压缩编解码器.如果禁用的话就是空.可以通过spark.broadcast.compress设置.默认true.
    * 默认解码器和Serializer中的CompressionCodec是一致的.
    * The compression codec to use, or None if compression is disabled */
  @transient private var compressionCodec: Option[CompressionCodec] = _
  /**
    * 每个块的大小.默认是4mb.broadcaster的只读属性.可通过spark.broadcast.blockSize设置.
    * Size of each block. Default value is 4MB.  This value is only read by the broadcaster. */
  @transient private var blockSize: Int = _

  private def setConf(conf: SparkConf) {
    compressionCodec = if (conf.getBoolean("spark.broadcast.compress", true)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    // Note: use getSizeAsKb (not bytes) to maintain compatibility if no units are provided
    blockSize = conf.getSizeAsKb("spark.broadcast.blockSize", "4m").toInt * 1024
    checksumEnabled = conf.getBoolean("spark.broadcast.checksum", true)
  }
  setConf(SparkEnv.get.conf)
  // 广播id.id是BroadcastManager的原子变量nextBroadCastId自增产生的
  private val broadcastId = BroadcastBlockId(id)

  /**
    * 该实例包含block数量.
    * Total number of blocks this broadcast variable contains. */
  private val numBlocks: Int = writeBlocks(obj)

  /**
    * 是否生成校验和
    * Whether to generate checksum for blocks or not. */
  private var checksumEnabled: Boolean = false
  /**
    * 校验和的数组
    * The checksum for all the blocks. */
  private var checksums: Array[Int] = _

  override protected def getValue() = {
    _value
  }
  /** 生成校验和*/
  private def calcChecksum(block: ByteBuffer): Int = {
    val adler = new Adler32()
    if (block.hasArray) {
      adler.update(block.array, block.arrayOffset + block.position(), block.limit()
        - block.position())
    } else {
      val bytes = new Array[Byte](block.remaining())
      block.duplicate.get(bytes)
      adler.update(bytes)
    }
    adler.getValue.toInt
  }

  /**
   * 将对象分成多个block,并且将其放入block manager
    * Divide the object into multiple blocks and put those blocks in the block manager.
   *
   * @param value the object to divide
   * @return number of blocks this broadcast variable is divided into
   */
  private def writeBlocks(value: T): Int = {
    import StorageLevel._
    // 将广播变量的副本存储在驱动程序中,一边驱动程序运行task
    // 不要创建广播变量值的重复副本.
    // Store a copy of the broadcast variable in the driver so that tasks run on the driver
    // do not create a duplicate copy of the broadcast variable's value.
    val blockManager = SparkEnv.get.blockManager   // 先从env中获取blockManager
    // 如果已经存储过,抛异常.这个putSingle方法会将广播变量对象写入本地存储体系.
    // 其实最终调用的是BloackManager中的doPutIterator方法.
    // 如果是local模式,广播变量会写入Driver本地存储体系.以便task在Driver上执行.
    // MEMORY_AND_DISK对应的副本数量为1,所以指挥将广播变量写入本地存储体系.
    if (!blockManager.putSingle(broadcastId, value, MEMORY_AND_DISK, tellMaster = false)) {
      throw new SparkException(s"Failed to store $broadcastId in BlockManager")
    }
    // 将对象转换成一系列的块,每块由blockSize确定.生成ByteBufer数组.
    // 使用的是SparkEnv中的JavaSerializer序列化,TorrentBroadcast自身的Codec进行压缩.
    val blocks =
      TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)
    // 如果开启校验,则创建校验和数组.
    if (checksumEnabled) {
      checksums = new Array[Int](blocks.length)
    }
    //
    blocks.zipWithIndex.foreach { case (block, i) =>
      if (checksumEnabled) {
        // 生成校验和
        checksums(i) = calcChecksum(block)
      }
      // 给分片广播块生成BroadcastBlockId,
      val pieceId = BroadcastBlockId(id, "piece" + i)
      val bytes = new ChunkedByteBuffer(block.duplicate())
      // 将分片广播块以序列化方式写入Driver本地存储.
      // MEMORY_AND_DISK_SER对应副本数也是1,所以也只写一份.
      if (!blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, tellMaster = true)) {
        throw new SparkException(s"Failed to store $pieceId of $broadcastId in local BlockManager")
      }
    }
    // 返回块的数量.
    blocks.length
  }

  /**
    * 从Driver或Executor中获取块.
    * Fetch torrent blocks from the driver and/or other executors. */
  private def readBlocks(): Array[BlockData] = {
    // 获取数据块。 请注意，所有这些块都存储在BlockManager中并报告给driver，
    // 所以其他executor也可以从这个executor中获取这些块
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    // 新建分片广播块数组
    val blocks = new Array[BlockData](numBlocks)
    // 获取blockManager
    val bm = SparkEnv.get.blockManager
    // 对分片广播进行随机洗牌,避免出现获取热点
    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      val pieceId = BroadcastBlockId(id, "piece" + pid)
      logDebug(s"Reading piece $pieceId of $broadcastId")
      // 首先尝试getLocalBytes，因为以前尝试获取广播块的机会已经取得了一些块。
      // 这种情况下，一些块将在本地可用（在此执行程序上）。
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      bm.getLocalBytes(pieceId) match {
        case Some(block) =>
          // 如果本地直接获取到,就可以释放锁,返回对象了
          blocks(pid) = block
          releaseLock(pieceId)
        case None =>
          // 本地没有的话,从远程存储体系中获取分片广播块.
          bm.getRemoteBytes(pieceId) match {
            case Some(b) =>
              // 开启校验和的情况,验证校验和
              if (checksumEnabled) {
                val sum = calcChecksum(b.chunks(0))
                // 校验和不相等抛异常
                if (sum != checksums(pid)) {
                  throw new SparkException(s"corrupt remote block $pieceId of $broadcastId:" +
                    s" $sum != ${checksums(pid)}")
                }
              }
              // 放入本地存储体系,以便后续获取广播块可以直接获取.
              // We found the block from remote executors/driver's BlockManager, so put the block
              // in this executor's BlockManager.
              if (!bm.putBytes(pieceId, b, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
                throw new SparkException(
                  s"Failed to store $pieceId of $broadcastId in local BlockManager")
              }
              blocks(pid) = new ByteBufferBlockData(b, true)
            case None =>
              throw new SparkException(s"Failed to get $pieceId of $broadcastId")
          }
      }
    }
    blocks
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors.
   */
  override protected def doUnpersist(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors
   * and driver.
   */
  override protected def doDestroy(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }

  private def readBroadcastBlock(): T = Utils.tryOrIOException {
    TorrentBroadcast.synchronized {
      // 获取blockManager
      val broadcastCache = SparkEnv.get.broadcastManager.cachedValues
      // 根据Id获取object,然后转换成T类型,转换之后如果获取不到,就执行getOrElse中的代码
      Option(broadcastCache.get(broadcastId)).map(_.asInstanceOf[T]).getOrElse {
        // setConf中设置了Codec,blockSize和是否开启校验和
        setConf(SparkEnv.get.conf)
        val blockManager = SparkEnv.get.blockManager
        // 从本地blockmanager中获取block,结果是一个Java对象迭代器,被Option[]包装.
        blockManager.getLocalValues(broadcastId) match {
            // 如果可以获取广播对象
          case Some(blockResult) =>
            if (blockResult.data.hasNext) {
              // 转换成T对应实例
              val x = blockResult.data.next().asInstanceOf[T]
              // 释放锁,该锁保证了如果某个task使用这个block,别的块无法使用,所以获取之后即可释放锁了.
              releaseLock(broadcastId)

              if (x != null) {
                broadcastCache.put(broadcastId, x)
              }

              x
            } else {
              // 迭代器中hasNext返回false的话抛出异常
              throw new SparkException(s"Failed to get locally stored broadcast data: $broadcastId")
            }
          case None =>
            // 如果本地存储体系中没有获取广播对象,说明是序列化存储的.
            logInfo("Started reading broadcast variable " + id)
            val startTimeMs = System.currentTimeMillis()
            // 从Driver或Executor中存储体系获取块.
            val blocks = readBlocks()
            logInfo("Reading broadcast variable " + id + " took" + Utils.getUsedTimeMs(startTimeMs))

            try {
              // 将分片广播块转换成原来的广播对象
              val obj = TorrentBroadcast.unBlockifyObject[T](
                // 三个参数:Array<InputStream),序列化器,压缩编码解码器
                blocks.map(_.toInputStream()), SparkEnv.get.serializer, compressionCodec)
              // Store the merged copy in BlockManager so other tasks on this executor don't
              // need to re-fetch it.
              val storageLevel = StorageLevel.MEMORY_AND_DISK
              // 将其放入本地存储体系,以便下次读取对象走 上面的case
              if (!blockManager.putSingle(broadcastId, obj, storageLevel, tellMaster = false)) {
                throw new SparkException(s"Failed to store $broadcastId in BlockManager")
              }

              if (obj != null) {
                broadcastCache.put(broadcastId, obj)
              }

              obj
            } finally {
              blocks.foreach(_.dispose())
            }
        }
      }
    }
  }

  /**
   * 如果在任务中运行，则在任务完成时注册给定块的锁以释放。
    * 如果没有在任务中运行，则立即释放锁。
    * If running in a task, register the given block's locks for release upon task completion.
   * Otherwise, if not running in a task then immediately release the lock.
   */
  private def releaseLock(blockId: BlockId): Unit = {
    val blockManager = SparkEnv.get.blockManager
    Option(TaskContext.get()) match {
      case Some(taskContext) =>
        taskContext.addTaskCompletionListener(_ => blockManager.releaseLock(blockId))
      case None =>

        // 这应该只发生在driver上，其中广播变量可以在正在运行的任务之外访问
        // （例如，在计算rdd.partitions（）时）。为了允许广播变量被垃圾收集，
        // 我们需要释放这个稍微不安全的引用，但技术上没问题，因为广播变量不是堆外存储的。
        // This should only happen on the driver, where broadcast variables may be accessed
        // outside of running tasks (e.g. when computing rdd.partitions()). In order to allow
        // broadcast variables to be garbage collected we need to free the reference here
        // which is slightly unsafe but is technically okay because broadcast variables aren't
        // stored off-heap.
        blockManager.releaseLock(blockId)
    }
  }

}


private object TorrentBroadcast extends Logging {

  def blockifyObject[T: ClassTag](
      obj: T,
      blockSize: Int,
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
    val cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)
    val out = compressionCodec.map(c => c.compressedOutputStream(cbbos)).getOrElse(cbbos)
    val ser = serializer.newInstance()
    val serOut = ser.serializeStream(out)
    Utils.tryWithSafeFinally {
      serOut.writeObject[T](obj)
    } {
      serOut.close()
    }
    cbbos.toChunkedByteBuffer.getChunks()
  }

  def unBlockifyObject[T: ClassTag](
      blocks: Array[InputStream],
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    val is = new SequenceInputStream(blocks.iterator.asJavaEnumeration)
    val in: InputStream = compressionCodec.map(c => c.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = Utils.tryWithSafeFinally {
      serIn.readObject[T]()
    } {
      serIn.close()
    }
    obj
  }

  /**
   * 删除所有executor上和本对象关联的持久化blocks,如果removeFromDriver是true,driver上的也删除
    * Remove all persisted blocks associated with this torrent broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver.
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    logDebug(s"Unpersisting TorrentBroadcast $id")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}

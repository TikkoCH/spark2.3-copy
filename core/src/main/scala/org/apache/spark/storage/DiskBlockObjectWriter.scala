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

package org.apache.spark.storage
// scalastyle:off
import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.util.Utils

/**
 * 一个用于将jvm内存中对象写入磁盘的类.允许追加.
  * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block. For efficiency, it retains the underlying file channel across
 * multiple commits. This channel is kept open until close() is called. In case of faults,
 * callers should instead close with revertPartialWritesAndClose() to atomically revert the
 * uncommitted partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 */
private[spark] class DiskBlockObjectWriter(
    val file: File, // 要写入的文件
    serializerManager: SerializerManager, // 序列化管理器
    serializerInstance: SerializerInstance, // 序列化实例
    bufferSize: Int, // 缓冲大小
    syncWrites: Boolean, // 是否同步写
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetrics, // 用于对shuffle中间结果写到磁盘进行度量统计.
    val blockId: BlockId = null)  // blockid
  extends OutputStream
  with Logging {

  /**
   * Guards against close calls, e.g. from a wrapping stream.
   * Call manualClose to close the stream that was extended by this trait.
   * Commit uses this trait to close object streams without paying the
   * cost of closing and opening the underlying file.
   */
  private trait ManualCloseOutputStream extends OutputStream {
    abstract override def close(): Unit = {
      flush()
    }

    def manualClose(): Unit = {
      super.close()
    }
  }

  /**
    * 文件管道,用于存储删除文件
    * The file channel, used for repositioning / truncating the file. */
  private var channel: FileChannel = null
  private var mcs: ManualCloseOutputStream = null
  private var bs: OutputStream = null
  private var fos: FileOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false // 是否初始化
  private var streamOpen = false // 是否打开流
  private var hasBeenClosed = false // 是否已关闭

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxxxx|----------|-----|
   *           ^          ^     ^
   *           |          |    channel.position()
   *           |        reportedPosition
   *         committedPosition
   *
   * reportedPosition: Position at the time of the last update to the write metrics.
   * committedPosition: Offset after last committed write.
   * -----: Current writes to the underlying file.
   * xxxxx: Committed contents of the file.
   */
  private var committedPosition = file.length()  // 文件上次写提交的位置
  private var reportedPosition = committedPosition // 报告度量系统文件位置

  /**
   * 跟踪写入的记录数量，并使用它来定期输出写入的字节，因为后者对于每个记录来说都很昂贵。
    * 我们在每次调用commitAndGet后重置它。
    * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   * And we reset it after every commitAndGet called.
   */
  private var numRecordsWritten = 0
  /** 初始化*/
  private def initialize(): Unit = {
    // 创建文件输出流
    fos = new FileOutputStream(file, true)
    // 获取channel,其实都是流处理的模版代码
    channel = fos.getChannel()
    // 时间跟踪输出流
    ts = new TimeTrackingOutputStream(writeMetrics, fos)
    // 这是在方法中定义类签名吗,没太看懂,反正就是创建了个ManualCloseOutputStream实现类
    // 并且继承了BufferedOutputStream,然后将mcs赋值
    class ManualCloseBufferedOutputStream
      extends BufferedOutputStream(ts, bufferSize) with ManualCloseOutputStream
    mcs = new ManualCloseBufferedOutputStream
  }
  /** 打开流,返回DiskBlockObjectWriter*/
  def open(): DiskBlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    // 如果没初始化,初始化
    if (!initialized) {
      initialize()
      initialized = true
    }
    // 设置成员变量
    bs = serializerManager.wrapStream(blockId, mcs)
    objOut = serializerInstance.serializeStream(bs)
    streamOpen = true
    this // 返回本对象
  }

  /**
   * 关闭并且释放资源
    * Close and cleanup all resources.
   * Should call after committing or reverting partial writes.
   */
  private def closeResources(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        mcs.manualClose()
      } {
        channel = null
        mcs = null
        bs = null
        fos = null
        ts = null
        objOut = null
        initialized = false
        streamOpen = false
        hasBeenClosed = true
      }
    }
  }

  /**
   * Commits any remaining partial writes and closes resources.
   */
  override def close() {
    if (initialized) {
      Utils.tryWithSafeFinally {
        commitAndGet()
      } {
        closeResources()
      }
    }
  }

  /**
   * 用于将输出流中的数据写入磁盘.刷新部分写入并将它们作为单个原子块提交。
    * 提交可以写入额外的字节来构成原子块。
    * Flush the partial writes and commit them as a single atomic block.
   * A commit may write additional bytes to frame the atomic block.
   *
   * @return file segment with previous offset and length committed on this call.
   */
  def commitAndGet(): FileSegment = {
    if (streamOpen) {
      // 因为Kryo不刷新底层流，所以我们显式刷新了串行器流和低级流。
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      objOut.close()
      streamOpen = false // 关闭状态符

      if (syncWrites) {
        // 强制对磁盘进行未完成的写入并跟踪所需的时间
        // Force outstanding writes to disk and track how long it takes
        val start = System.nanoTime()
        fos.getFD.sync()
        // 更新度量器
        writeMetrics.incWriteTime(System.nanoTime() - start)
      }
      // 获取channel的位置
      val pos = channel.position()
      // 创建文件描述符
      val fileSegment = new FileSegment(file, committedPosition, pos - committedPosition)
      // 修改提交位置
      committedPosition = pos
      // 在某些压缩编解码器中，在关闭流之后写入更多字节
      // In certain compression codecs, more bytes are written after streams are closed
      writeMetrics.incBytesWritten(committedPosition - reportedPosition)
      reportedPosition = committedPosition
      numRecordsWritten = 0
      // 返回文件描述符
      fileSegment
    } else {
      // 如果流没有打开,创建FileSegment,返回
      new FileSegment(file, committedPosition, 0)
    }
  }


  /**
   * Reverts writes that haven't been committed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   *
   * @return the file that this DiskBlockObjectWriter wrote to.
   */
  def revertPartialWritesAndClose(): File = {
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    Utils.tryWithSafeFinally {
      if (initialized) {
        writeMetrics.decBytesWritten(reportedPosition - committedPosition)
        writeMetrics.decRecordsWritten(numRecordsWritten)
        streamOpen = false
        closeResources()
      }
    } {
      var truncateStream: FileOutputStream = null
      try {
        truncateStream = new FileOutputStream(file, true)
        truncateStream.getChannel.truncate(committedPosition)
      } catch {
        case e: Exception =>
          logError("Uncaught exception while reverting partial writes to file " + file, e)
      } finally {
        if (truncateStream != null) {
          truncateStream.close()
          truncateStream = null
        }
      }
    }
    file
  }

  /**
   * 写入键值对
    * Writes a key-value pair.
   */
  def write(key: Any, value: Any) {
    if (!streamOpen) {
      open()
    }

    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!streamOpen) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  /**
   * 勇敢与对写入的记录数进行统计和度量
    * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  def recordWritten(): Unit = {
    // numRecordsWritten+1
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)
    // 如果写入记录数是16384倍数,更新
    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  /**
   * 报告在此writer的shuffle写入度量中写入的字节数。 请注意，这仅在基础流关闭之前有效。
    * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten() {
    val pos = channel.position()
    writeMetrics.incBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  // For testing
  private[spark] override def flush() {
    objOut.flush()
    bs.flush()
  }
}

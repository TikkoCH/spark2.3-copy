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

import java.io.{File, InputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, ShuffleClient, TempFileManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

/**
 * 获取多个block的迭代器.对于本地block,从本地blockManager中获取block.对于远程block,通过BlockTransferService
  * 获取block.该类会创建一个(BlockId,InputStream)的迭代器,因此调用者可以在收到Block时以流水线方式处理Block。
  * 其实现抑制了远程获取block的获取block的最大大小(maxBytesInFlight参数进行限制)来避免使用过多内存.<br>
  * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *
 * @param context TaskContext,用于更新metrics度量系统<br>[[TaskContext]], used for metrics update
 * @param shuffleClient 用于获取远程block<br>[[ShuffleClient]] for fetching remote blocks
 * @param blockManager 用于读取本地Block<br>[[BlockManager]] for reading local blocks
 * @param blocksByAddress  通过BlockManagerId分组的Block列表.对于每个Block,
  *                         我们还需要大小（以字节为单位的长字段）以限制内存使用量.<br>
  *                        list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require the size (in bytes as a long field) in
 *                        order to throttle the memory usage.
 * @param streamWrapper 用于包装返回的输入流的函数<br>
  *                      A function to wrap the returned input stream.
 * @param maxBytesInFlight 任何端点获取远程block的最大大小.
  *                         max size (in bytes) of remote blocks to fetch at any given point.
 * @param maxReqsInFlight  在任何给定端点获取block的最大远程请求数。
  *                        max number of remote requests to fetch blocks at any given point.
 * @param maxBlocksInFlightPerAddress 给定主机:端口的任何给定端点获取
  *                                    max number of shuffle blocks being fetched at any given point
 *                                    for a given remote host:port.
 * @param maxReqSizeShuffleToMem 内存中可shuffle的请求的最大值.
  *                               max size (in bytes) of a request that can be shuffled to memory.
 * @param detectCorrupt 是否检测已获取块中的任何损坏。
  *                      whether to detect any corruption in fetched blocks.
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    maxReqSizeShuffleToMem: Long,
    detectCorrupt: Boolean)
  extends Iterator[(BlockId, InputStream)] with TempFileManager with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * 要获取block的总数.这可能小于[[blocksByAddress]]中的块总数，
    * 因为我们在[[initialize]]中过滤掉了零大小的块。该值应该=localBlocks.size + remoteBlocks.size
    * Total number of blocks to fetch. This can be smaller than the total number of blocks
   * in [[blocksByAddress]] because we filter out zero-sized blocks in [[initialize]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
   */
  private[this] var numBlocksToFetch = 0

  /**
   * 调用者处理block数量。 numBlocksProcessed == numBlocksToFetch时，迭代器到头。
    * The number of blocks processed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  private[this] var numBlocksProcessed = 0
  /**
    * 开始时间
    * */
  private[this] val startTime = System.currentTimeMillis

  /** Local blocks to fetch, excluding zero-sized blocks. */
  private[this] val localBlocks = new ArrayBuffer[BlockId]()

  /**
    * 要获取的远程block,不包括0大小的block
    * Remote blocks to fetch, excluding zero-sized blocks. */
  private[this] val remoteBlocks = new HashSet[BlockId]()

  /**
   * 保存结果的队列。 这将org.apache.spark.network.BlockTransferService
    * 提供的异步模型转换为同步模型（迭代器）.因为他是LinkedBlockingQueue所以阻塞.
    * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * 当前正在处理的FetchResult.
    * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   */
  @volatile private[this] var currentResult: SuccessFetchResult = null

  /**
   * FetchRequest的队列.渐进式拉去请求来确保拉取字节数不超过maxBytesInFlight.
    * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /**
   * 获取请求的队列，这些请求在第一次出列时无法发布.当满足提取约束时，再次尝试这些请求。
    * Queue of fetch requests which could not be issued the first time they were dequeued. These
   * requests are tried again when the fetch constraints are satisfied.
   */
  private[this] val deferredFetchRequests = new HashMap[BlockManagerId, Queue[FetchRequest]]()

  /**
    * 我们请求的当前字节数
    * Current bytes in flight from our requests */
  private[this] var bytesInFlight = 0L

  /**
    * 当前请求的次数
    * Current number of requests in flight */
  private[this] var reqsInFlight = 0

  /**
    * 每个地址的当前block数量
    * Current number of blocks in flight per host:port */
  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()

  /**
   * 无法成功解压的block,它用于保证我们最多为那些损坏的block重试一次。
    * The blocks that can't be decompressed successfully, it is used to guarantee that we retry
   * at most once for those corrupted blocks.
   */
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()
  /**
    * 度量系统  TempShuffleReadMetrics
    * */
  private[this] val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()

  /**
   * 当前iterater是否还是活动状态.如果是僵尸模式,回调接口不会再将获取的block放入results.
    * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
   */
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
   * 存储用于shuffle远程较大block文件的集合.当cleanup()时会将本集合中的文件删除.这是防止磁盘文件泄漏的一层防御。
    * A set to store the files used for shuffling remote huge blocks. Files in this set will be
   * deleted when cleanup. This is a layer of defensiveness against disk file leaks.
   */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[File]()

  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  /** 减少缓冲区引用计数。 currentResult设置为null以防止在cleanup时再次释放缓冲区（）*/
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    // 如果currentResult!=null,释放
    if (currentResult != null) {
      currentResult.buf.release()
    }
    // 清空引用,这样会被gc回收
    currentResult = null
  }
  /** 创建临时文件*/
  override def createTempFile(): File = {
    blockManager.diskBlockManager.createTempLocalBlock()._2
  }
  /** 注册临时文件,当不再使用时候会将其删除.返回注册是否成功,如果不成功调用者应该将文件删除.*/
  override def registerTempFileToClean(file: File): Boolean = synchronized {
    if (isZombie) {
      // 僵尸模式返回false
      false
    } else {
      // 缓存中添加该文件
      shuffleFilesSet += file
      // 返回true
      true
    }
  }

  /**
   * 将Iterator标记为僵尸模式,并且清空那些还没有被反序列化的缓存
    * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[this] def cleanup() {
    synchronized {
      // 标记为僵尸模式
      isZombie = true
    }
    // 减少缓冲区引用计数
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    // 获取阻塞队列的迭代器
    val iter = results.iterator()
    // 遍历迭代器
    while (iter.hasNext) {
      val result = iter.next()
      result match {
          // 如果是SuccessFetchResult
          // 其实都是更新度量,最后释放ManagerBuffer
        case SuccessFetchResult(_, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            // 更新读取远程字节数
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          buf.release()
        case _ =>
      }
    }
    // 最后删除文件夹
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning("Failed to cleanup shuffle fetch temp file " + file.getAbsolutePath())
      }
    }
  }
  /**
    * 发送获取block的请求
    * */
  private[this] def sendRequest(req: FetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    // 更新成员变量bytesInFlight和reqsInFlight
    bytesInFlight += req.size
    reqsInFlight += 1

    // so we can look up the size of each blockID
    // 查看我们每个BlockId的大小
    // 将(blockId,size)映射成(blockId.name,size)
    val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    // 创建block.name的set
    val remainingBlocks = new HashSet[String]() ++= sizeMap.keys
    // 生成blockId的list
    val blockIds = req.blocks.map(_._1.toString)
    // 获取地址
    val address = req.address
    // 创建一个BlockFetchingListener匿名内部类
    val blockFetchingListener = new BlockFetchingListener {
      // 重写onBlockFetchSuccess方法
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // 只有在iterator不是僵尸模式的条件下将buffer添加到resultsQueue中.也就是cleanup()未被调用
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.
        ShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            // 如果不是僵尸模式
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            // remainingBlocks减去该blockId,这算是闭包
            remainingBlocks -= blockId
            // 队列中放入新创建的SuccessFetchResult
            results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf,
              remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
          }
        }
        logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
      }
      // 重写获取block失败方法
      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
        // 在队列中放入新创建的FailureFetchResult
        results.put(new FailureFetchResult(BlockId(blockId), address, e))
      }
    }
    // 请求过大时，将远程shuffle块提取到磁盘。 由于shuffle数据已经通过线路加密和压缩）,
    // 我们可以直接获取数据并将其写入文件。
    // Fetch remote shuffle blocks to disk when the request is too large. Since the shuffle data is
    // already encrypted and compressed over the wire(w.r.t. the related configs), we can just fetch
    // the data and write it to file directly.
    if (req.size > maxReqSizeShuffleToMem) {
      // 如果req.size>内存中可shuffle的请求的最大值.将fetchBlocks的tmpFIleManager参数设为this
      // idea中ctrl+q查看fetchBlocks描述,eclipse中可能是ctrl+1
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, this)
    } else {
      // 否则tmpFIleManager参数设为null
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, null)
    }
  }
  /**
    * 切分本地远程block.
    * */
  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // 将远程请求的最大长度保持在maxBytesInFlight / 5以下.原因就是将请求大小保持小于五分之maxBytesInFlight
    // 可以并行地向五个节点请求而不是阻塞读取一个节点的输出
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    // 先计算出5分之一
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize
      + ", maxBlocksInFlightPerAddress: " + maxBlocksInFlightPerAddress)
    // 远程块进一步拆分为大小最多为maxBytesInFlight的FetchRequests，以限制传输中的数据量。
    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    val remoteRequests = new ArrayBuffer[FetchRequest]

    // Tracks total number of blocks (including zero sized blocks)
    // 跟踪block总数
    var totalBlocks = 0
    // 遍历blocksByAddress
    for ((address, blockInfos) <- blocksByAddress) {
      // 增加totalBlocks
      totalBlocks += blockInfos.size
      // 如果是本地的
      if (address.executorId == blockManager.blockManagerId.executorId) {
        // Filter out zero-sized blocks
        // 过滤掉size=0的block并且获取blockId数组添加到localBlocks中
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        // 更新numBlocksToFetch
        numBlocksToFetch += localBlocks.size
      } else {
        // 不是本地的,获取blockInfos的迭代器
        val iterator = blockInfos.iterator
        // 当前请求大小
        var curRequestSize = 0L
        // 当前block
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          // Skip empty blocks
          // 过滤空block
          if (size > 0) {
            // 更新curBlocks
            curBlocks += ((blockId, size))
            // 更新远程blocks
            remoteBlocks += blockId
            // 更新numBlocksToFetch
            numBlocksToFetch += 1
            // 更新局部变量curRequestSize
            curRequestSize += size
          } else if (size < 0) {
            // 抛异常
            throw new BlockException(blockId, "Negative block size " + size)
          }
          // 如果当前请求大小>=限制的请求大小,或者当前blocks的个数>= 最大限制
          if (curRequestSize >= targetRequestSize ||
              curBlocks.size >= maxBlocksInFlightPerAddress) {
            // Add this FetchRequest
            // 创建一个FetchRequest添加到remoteRequests中
            remoteRequests += new FetchRequest(address, curBlocks)
            logDebug(s"Creating fetch request of $curRequestSize at $address "
              + s"with ${curBlocks.size} blocks")
            // 清零curBlocks和curRequestSize
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            curRequestSize = 0
          }
        }
        // Add in the final request
        // 循环结束将请求添加到remoteRequests
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
    // 返回
    remoteRequests
  }

  /**
   * 当政获取远程block时获取本地block.因为当我们创建输入流时ManagerBuffer的内存是延迟分配的,所以
    * 是ok的.我们跟踪的内存是ManagerBuffer引用自身.
    * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * `ManagedBuffer`'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   */
  private[this] def fetchLocalBlocks() {
    // 获取bendiblock的迭代器
    val iter = localBlocks.iterator
    // 遍历
    while (iter.hasNext) {
      // 获取每个blockId
      val blockId = iter.next()
      try {
        // 获取block数据,实际是个ManagerBuffer
        val buf = blockManager.getBlockData(blockId)
        // 更新度量系统
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        // 增加ManagerBuffer的引用计数
        buf.retain()
        // SuccessFetchResult放入结果队列
        results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf, false))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          // 异常的话讲放入FailureFetchResult
          results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }
  }
  /** 初始化,构建对象时会执行该方法*/
  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    // 添加监听器的回调,成功失败都会cleanup
    context.addTaskCompletionListener(_ => cleanup())

    // Split local and remote blocks.
    // 切分本地和远程blocks
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    // 将远程请求打乱顺序再放入fetchRequests
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    // 发送block初始化请求,取决于maxBytesInFlight
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * 获取下一个（BlockId，InputStream）.如果任务失败，则使用TaskCompletionListener注册的
    * cleanup（）方法将释放每个InputStream底层的ManagedBuffers。 但是，
    * 调用者应该在不再需要它们时立即close（）这些InputStream，以便尽早释放内存。
    * 如果无法获取下一个block，则抛出FetchFailedException。
    * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  override def next(): (BlockId, InputStream) = {
    // 迭代器到头了,抛出异常
    if (!hasNext) {
      throw new NoSuchElementException
    }
    // 更新numBlocksProcessed
    numBlocksProcessed += 1

    var result: FetchResult = null
    var input: InputStream = null
    // 获取下一个获取的结果并尝试解压缩以检测数据损坏，如果它已损坏则再次获取它，如果第二次
    // 获取也损坏则抛出FailureFetchResult，因此可以重试之前的stage。 对于本地shuffle块，
    // 为第一个IOException抛出FailureFetchResult。
    // Take the next fetched result and try to decompress it to detect data corruption,
    // then fetch it one more time if it's corrupt, throw FailureFetchResult if the second fetch
    // is also corrupt, so the previous stage could be retried.
    // For local shuffle block, throw FailureFetchResult for the first IOException.
    while (result == null) {
      val startFetchWait = System.currentTimeMillis()
      // 获取阻塞队列中的一个FetchResult
      result = results.take()
      val stopFetchWait = System.currentTimeMillis()
      // 更新度量
      shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

      result match {
          // 提一下@这个东西,这是将SuccessFetchResult的值绑定到r上,r变量就等于SuccessFetchResult
        case r @ SuccessFetchResult(blockId, address, size, buf, isNetworkReqDone) =>
          if (address != blockManager.blockManagerId) {
            // 如果这个SuccessFetchResult中的address(BlockManagerId)不等于本对象的
            // blockManager的blockManagerId,说明是获取远程数据的FetchResult
            // 所以应该减去numBlocksInFlightPerAddress中该地址的block数量
            numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
            // 更新度量
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          // 更新bytesInFlight
          bytesInFlight -= size
          // 是否是最后一个请求
          if (isNetworkReqDone) {
            // 如果是,reqsInFlight-= 1
            reqsInFlight -= 1
            logDebug("Number of requests in flight " + reqsInFlight)
          }
          // 创建输入流
          val in = try {
            buf.createInputStream()
          } catch {
            // The exception could only be throwed by local shuffle block
            case e: IOException =>
              assert(buf.isInstanceOf[FileSegmentManagedBuffer])
              logError("Failed to create input stream from local block", e)
              buf.release()
              throwFetchFailedException(blockId, address, e)
          }
          // streamWrapper是一个函数,构造iterator时传入的
          input = streamWrapper(blockId, in)
          // 只有当它通过压缩或加密包装时才复制流，block也很小（解压缩的块小于maxBytesInFlight）
          // Only copy the stream if it's wrapped by compression or encryption, also the size of
          // block is small (the decompressed block is smaller than maxBytesInFlight)
          // 只有成员变量detectCorrupt(是否监测到block损坏)为true,input!=in,size<maxBytesInFlight / 3
          if (detectCorrupt && !input.eq(in) && size < maxBytesInFlight / 3) {
            val originalInput = input
            // 创建输出流
            val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
            try {
              // 立即解压缩整个块以检测任何损坏，这可能会增加内存使用量，从而增加OOM的可能性.
              // Decompress the whole block at once to detect any corruption, which could increase
              // the memory usage tne potential increase the chance of OOM.
              // TODO: manage the memory used here, and spill it into disk in case of OOM.
              // 拷贝
              Utils.copyStream(input, out)
              out.close()
              input = out.toChunkedByteBuffer.toInputStream(dispose = true)
            } catch {
              case e: IOException =>
                buf.release()
                if (buf.isInstanceOf[FileSegmentManagedBuffer]
                  || corruptedBlocks.contains(blockId)) {
                  throwFetchFailedException(blockId, address, e)
                } else {
                  logWarning(s"got an corrupted block $blockId from $address, fetch again", e)
                  corruptedBlocks += blockId
                  fetchRequests += FetchRequest(address, Array((blockId, size)))
                  result = null
                }
            } finally {
              // TODO: release the buf here to free memory earlier
              originalInput.close()
              in.close()
            }
          }

        case FailureFetchResult(blockId, address, e) =>
          throwFetchFailedException(blockId, address, e)
      }
      // 发送获取block请求,不超过maxBytesInFlight
      // Send fetch requests up to maxBytesInFlight
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    // 返回元组(BlockId,流)
    (currentResult.blockId, new BufferReleasingInputStream(input, this))
  }
  /**
    * 发送获取block请求最多为maxBytesInFlight.如果不能从远程host立即获取,会延迟到远程能够处理时再获取.
    * */
  private def fetchUpToMaxBytes(): Unit = {
    // Send fetch requests up to maxBytesInFlight. If you cannot fetch from a remote host
    // immediately, defer the request until the next time it can be processed.

    // Process any outstanding deferred fetch requests if possible.
    // 如果可能，处理任何未完成的延迟获取block的请求。
    if (deferredFetchRequests.nonEmpty) {
      // 遍历deferredFetchRequests
      for ((remoteAddress, defReqQueue) <- deferredFetchRequests) {
        while (isRemoteBlockFetchable(defReqQueue) &&
            !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          // 获取队列第一个元素
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred fetch request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          // 发送请求
          send(remoteAddress, request)
          // 如果这个队列都处理完了,deferredFetchRequests缓存将其删除
          if (defReqQueue.isEmpty) {
            deferredFetchRequests -= remoteAddress
          }
        }
      }
    }

    // Process any regular fetch requests if possible.
    while (isRemoteBlockFetchable(fetchRequests)) {
      // 获取fetchRequests队列中的元素
      val request = fetchRequests.dequeue()
      // 获取请求地址
      val remoteAddress = request.address
      // 检查获取block数量有没有超过最大值
      if (isRemoteAddressMaxedOut(remoteAddress, request)) {
        logDebug(s"Deferring fetch request for $remoteAddress with ${request.blocks.size} blocks")
        // 如果超过了,获取deferredFetchRequests中对应地址的队列,默认是空的
        val defReqQueue = deferredFetchRequests.getOrElse(remoteAddress, new Queue[FetchRequest]())
        // 将请求添加到队列中
        defReqQueue.enqueue(request)
        // deferredFetchRequests中更新地址->队列映射
        deferredFetchRequests(remoteAddress) = defReqQueue
      } else {
        // 没超过直接发送
        send(remoteAddress, request)
      }
    }
    /** 发送获取block的请求,更新numBlocksInFlightPerAddress*/
    def send(remoteAddress: BlockManagerId, request: FetchRequest): Unit = {
      sendRequest(request)
      numBlocksInFlightPerAddress(remoteAddress) =
        numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size
    }
    /** 远程block是否能获取*/
    def isRemoteBlockFetchable(fetchReqQueue: Queue[FetchRequest]): Boolean = {
      // fetchReqQueue非空&&(当前请求字节==0||(当前请求次数+1<=最大请求次数&&总共请求字节<=maxBytesInFlight)
      fetchReqQueue.nonEmpty &&
        (bytesInFlight == 0 ||
          (reqsInFlight + 1 <= maxReqsInFlight &&
            bytesInFlight + fetchReqQueue.front.size <= maxBytesInFlight))
    }

    // Checks if sending a new fetch request will exceed the max no. of blocks being fetched from a
    // given remote address.
    /** 检查向给定地址获取请求的block数量是否会超过最大值*/
    def isRemoteAddressMaxedOut(remoteAddress: BlockManagerId, request: FetchRequest): Boolean = {
      numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
        maxBlocksInFlightPerAddress
    }
  }

  private def throwFetchFailedException(blockId: BlockId, address: BlockManagerId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

/**
 * 确保在InputStream.close（）上释放ManagedBuffer的辅助类
  * Helper class that ensures a ManagedBuffer is released upon InputStream.close()
 */
private class BufferReleasingInputStream(
    private val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * 一个用于从远程BLockManager获取Block的请求
    * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    val size = blocks.map(_._2).sum
  }

  /**
   * 获取远程block的结果
    * Result of a fetch from a remote block.
   */
  private[storage] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  /**
   * 成功获取远程block的结果
    * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param address 获取block的BlockManager,即服务端BlockManagerId
    *                BlockManager that the block was fetched from.
   * @param size 估计的block大小,用于计算bytesInFlight,不是精确值
    *             estimated size of the block, used to calculate bytesInFlight.
   *             Note that this is NOT the exact bytes.
   * @param buf 内容的ManagedBuffer
    *            `ManagedBuffer` for the content.
   * @param isNetworkReqDone 是否是从该host获取数据请求的最后一个请求
    *                         Is this the last network request for this host in this fetch request.
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * 获取远程block的结果失败
    * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e 异常
    *          the failure exception
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult
}

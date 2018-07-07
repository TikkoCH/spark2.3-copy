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

import java.io._
import java.lang.ref.{ReferenceQueue => JReferenceQueue, WeakReference}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal

import com.codahale.metrics.{MetricRegistry, MetricSet}

import org.apache.spark._
import org.apache.spark.executor.{DataReadMethod, ShuffleWriteMetrics}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.metrics.source.Source
import org.apache.spark.network._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.{ExternalShuffleClient, TempFileManager}
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage.memory._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

/**
  * 本类封装了从本地BlockManager中获取的block数据及Block相关联的度量数据
  * Class for returning a fetched block and associated metrics. */
private[spark] class BlockResult(
    val data: Iterator[Any], // Block及相关联度量数据
    // 读取Block的方法.Memory, Disk, Hadoop, Network这四种枚举值.
    val readMethod: DataReadMethod.Value,
    val bytes: Long)// 读取Block的字节长度

/**
 * Abstracts away how blocks are stored and provides different ways to read the underlying block
 * data. Callers should call [[dispose()]] when they're done with the block.
 */
private[spark] trait BlockData {

  def toInputStream(): InputStream

  /**
   * Returns a Netty-friendly wrapper for the block's data.
   *
   * Please see `ManagedBuffer.convertToNetty()` for more details.
   */
  def toNetty(): Object

  def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer

  def toByteBuffer(): ByteBuffer

  def size: Long

  def dispose(): Unit

}

private[spark] class ByteBufferBlockData(
    val buffer: ChunkedByteBuffer,
    val shouldDispose: Boolean) extends BlockData {

  override def toInputStream(): InputStream = buffer.toInputStream(dispose = false)

  override def toNetty(): Object = buffer.toNetty

  override def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    buffer.copy(allocator)
  }

  override def toByteBuffer(): ByteBuffer = buffer.toByteBuffer

  override def size: Long = buffer.size

  override def dispose(): Unit = {
    if (shouldDispose) {
      buffer.dispose()
    }
  }

}

/**
 * Mnager运行在每个节点上(Driver和Executors),提供对本地或远端不同存储方式(内存,磁盘,非堆)上Block的存储和检索接口.
  * initialize方法必须在BlockManager可用之前调用.
  * Manager running on every node (driver and executors) which provides interfaces for putting and
 * retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap).
 *
 * Note that [[initialize()]] must be called before the BlockManager is usable.
 */
private[spark] class BlockManager(
    executorId: String,
    rpcEnv: RpcEnv,
    val master: BlockManagerMaster,
    val serializerManager: SerializerManager,
    val conf: SparkConf,
    memoryManager: MemoryManager,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    val blockTransferService: BlockTransferService,
    securityManager: SecurityManager,
    numUsableCores: Int)
  extends BlockDataManager with BlockEvictionHandler with Logging {

  private[spark] val externalShuffleServiceEnabled =
    conf.getBoolean("spark.shuffle.service.enabled", false)

  val diskBlockManager = {
    // Only perform cleanup if an external service is not serving our shuffle files.
    val deleteFilesOnStop =
      !externalShuffleServiceEnabled || executorId == SparkContext.DRIVER_IDENTIFIER
    new DiskBlockManager(conf, deleteFilesOnStop)
  }

  // Visible for testing
  private[storage] val blockInfoManager = new BlockInfoManager

  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))

  // Actual storage of where blocks are kept
  private[spark] val memoryStore =
    new MemoryStore(conf, blockInfoManager, serializerManager, memoryManager, this)
  private[spark] val diskStore = new DiskStore(conf, diskBlockManager, securityManager)
  memoryManager.setMemoryStore(memoryStore)

  // Note: depending on the memory manager, `maxMemory` may actually vary over time.
  // However, since we use this only for reporting and logging, what we actually want here is
  // the absolute maximum value that `maxMemory` can ever possibly reach. We may need
  // to revisit whether reporting this value as the "max" is intuitive to the user.
  private val maxOnHeapMemory = memoryManager.maxOnHeapStorageMemory
  private val maxOffHeapMemory = memoryManager.maxOffHeapStorageMemory

  // Port used by the external shuffle service. In Yarn mode, this may be already be
  // set through the Hadoop configuration as the server is launched in the Yarn NM.
  private val externalShuffleServicePort = {
    val tmpPort = Utils.getSparkOrYarnConfig(conf, "spark.shuffle.service.port", "7337").toInt
    if (tmpPort == 0) {
      // for testing, we set "spark.shuffle.service.port" to 0 in the yarn config, so yarn finds
      // an open port.  But we still need to tell our spark apps the right port to use.  So
      // only if the yarn config has the port set to 0, we prefer the value in the spark config
      conf.get("spark.shuffle.service.port").toInt
    } else {
      tmpPort
    }
  }

  var blockManagerId: BlockManagerId = _

  // Address of the server that serves this executor's shuffle files. This is either an external
  // service, or just our own Executor's BlockManager.
  private[spark] var shuffleServerId: BlockManagerId = _

  // Client to read other executors' shuffle files. This is either an external service, or just the
  // standard BlockTransferService to directly connect to other Executors.
  private[spark] val shuffleClient = if (externalShuffleServiceEnabled) {
    val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores)
    new ExternalShuffleClient(transConf, securityManager,
      securityManager.isAuthenticationEnabled(), conf.get(config.SHUFFLE_REGISTRATION_TIMEOUT))
  } else {
    blockTransferService
  }

  // Max number of failures before this block manager refreshes the block locations from the driver
  private val maxFailuresBeforeLocationRefresh =
    conf.getInt("spark.block.failures.beforeLocationRefresh", 5)

  private val slaveEndpoint = rpcEnv.setupEndpoint(
    "BlockManagerEndpoint" + BlockManager.ID_GENERATOR.next,
    new BlockManagerSlaveEndpoint(rpcEnv, this, mapOutputTracker))

  // Pending re-registration action being executed asynchronously or null if none is pending.
  // Accesses should synchronize on asyncReregisterLock.
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object

  // Field related to peer block managers that are necessary for block replication
  @volatile private var cachedPeers: Seq[BlockManagerId] = _
  private val peerFetchLock = new Object
  private var lastPeerFetchTime = 0L

  private var blockReplicationPolicy: BlockReplicationPolicy = _

  // A TempFileManager used to track all the files of remote blocks which above the
  // specified memory threshold. Files will be deleted automatically based on weak reference.
  // Exposed for test
  private[storage] val remoteBlockTempFileManager =
    new BlockManager.RemoteBlockTempFileManager(this)
  private val maxRemoteBlockToMem = conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)

  /**
   * 使用给定的appId初始化BlockManager。 这不是在构造函数中执行的，
    * 因为在BlockManager实例化时可能不知道appId（特别是对于驱动程序，它仅在使用TaskScheduler注册后才知道）.
    * 此方法初始化BlockTransferService和ShuffleClient，向BlockManagerMaster注册，
    * 启动BlockManagerWorker端点，并在配置时注册本地shuffle服务。
    * Initializes the BlockManager with the given appId. This is not performed in the constructor as
   * the appId may not be known at BlockManager instantiation time (in particular for the driver,
   * where it is only learned after registration with the TaskScheduler).
   *
   * This method initializes the BlockTransferService and ShuffleClient, registers with the
   * BlockManagerMaster, starts the BlockManagerWorker endpoint, and registers with a local shuffle
   * service if configured.
   */
  def initialize(appId: String): Unit = {
    // 初始化BlockTransferService.NettyBlockTransferService.
    blockTransferService.init(this)
    // 初始化shuffle客户端
    shuffleClient.init(appId)
    // block的复制策略,可通过spark.storage.replication.policy设置,默认是RandomBlockReplicationPolicy
    blockReplicationPolicy = {
      val priorityClass = conf.get(
        "spark.storage.replication.policy", classOf[RandomBlockReplicationPolicy].getName)
      val clazz = Utils.classForName(priorityClass)
      val ret = clazz.newInstance.asInstanceOf[BlockReplicationPolicy]
      logInfo(s"Using $priorityClass for block replication policy")
      ret
    }
    // 生成当前BlockManager的BlockManagerId.BlockManager在本地创建BlockManagerId,
    // 实际是在向BlockManagerMaster注册BlockManager时,提供BlockManagerMaster引用,BlockManagerMaster
    // 会创建一个包含拓扑信息的新BlockManagerId作为正式分配给BlockManger的身份标识
    val id =
      BlockManagerId(executorId, blockTransferService.hostName, blockTransferService.port, None)
    // 从BlockManagerMaster那获得的正式的BlockManager身份Id.
    val idFromMaster = master.registerBlockManager(
      id,
      maxOnHeapMemory,
      maxOffHeapMemory,
      slaveEndpoint)
    // 为BlockManagerId赋值,如果idFromMaster不为null就是idFromMaster,否则是id.
    blockManagerId = if (idFromMaster != null) idFromMaster else id
    // 生成shuffleServer的id.如果开启外部shuffleservice
    shuffleServerId = if (externalShuffleServiceEnabled) {
      logInfo(s"external shuffle service port = $externalShuffleServicePort")
      // 构造一个idFromMaster
      BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
    } else {
      // shuffleServerId为自身BlockManagerId.
      blockManagerId
    }
    // 向本地shuffle服务注册Executor的配置.
    // Register Executors' configuration with the local shuffle service, if one should exist.
    if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
      registerWithExternalShuffleServer()
    }

    logInfo(s"Initialized BlockManager: $blockManagerId")
  }

  def shuffleMetricsSource: Source = {
    import BlockManager._

    if (externalShuffleServiceEnabled) {
      new ShuffleMetricsSource("ExternalShuffle", shuffleClient.shuffleMetrics())
    } else {
      new ShuffleMetricsSource("NettyBlockTransfer", shuffleClient.shuffleMetrics())
    }
  }

  private def registerWithExternalShuffleServer() {
    logInfo("Registering executor with local external shuffle service.")
    val shuffleConfig = new ExecutorShuffleInfo(
      diskBlockManager.localDirs.map(_.toString),
      diskBlockManager.subDirsPerLocalDir,
      shuffleManager.getClass.getName)

    val MAX_ATTEMPTS = conf.get(config.SHUFFLE_REGISTRATION_MAX_ATTEMPTS)
    val SLEEP_TIME_SECS = 5

    for (i <- 1 to MAX_ATTEMPTS) {
      try {
        // Synchronous and will throw an exception if we cannot connect.
        shuffleClient.asInstanceOf[ExternalShuffleClient].registerWithShuffleServer(
          shuffleServerId.host, shuffleServerId.port, shuffleServerId.executorId, shuffleConfig)
        return
      } catch {
        case e: Exception if i < MAX_ATTEMPTS =>
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}"
            + s" more times after waiting $SLEEP_TIME_SECS seconds...", e)
          Thread.sleep(SLEEP_TIME_SECS * 1000)
        case NonFatal(e) =>
          throw new SparkException("Unable to register with external shuffle server due to : " +
            e.getMessage, e)
      }
    }
  }

  /**
   * 向BlockManager再次报告所有block信息.如果我们被BlockManager删除并且返回,或者我们能从执行崩溃之后
    * 恢复磁盘上的block,我们需要这样做.如果master返回false（表示slave需要重新注册）,那么该函数故意失败.
    * 将通过下一次心跳尝试或新的Block注册再次检测错误条件，然后另一次尝试重新注册所有块.
    * Report all blocks to the BlockManager again. This may be necessary if we are dropped
   * by the BlockManager and come back or if we become capable of recovering blocks on disk after
   * an executor crash.
   *
   * This function deliberately fails silently if the master returns false (indicating that
   * the slave needs to re-register). The error condition will be detected again by the next
   * heart beat attempt or new block registration and another try to re-register all blocks
   * will be made then.
   */
  private def reportAllBlocks(): Unit = {
    logInfo(s"Reporting ${blockInfoManager.size} blocks to the master.")
    for ((blockId, info) <- blockInfoManager.entries) {
      // 获取Block当前状态
      val status = getCurrentBlockStatus(blockId, info)
      // 如果需要将Block的BlockStatus汇报给BlockManagermaster,则调用tryToReportBlockStatus.
      if (info.tellMaster && !tryToReportBlockStatus(blockId, status)) {
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
  }

  /**
   * 用于向BlockManaerMaster重新注册BlockManager,并向BlockManagerMaster报告所有Block信息.
    * 如果我们向BlockManager发送的心跳表明我们还未注册,该方法会被心跳线程调用.
    * 请注意，必须在不持有任何BlockInfo锁的情况下调用此方法.
    *
    * Re-register with the master and report all blocks to it. This will be called by the heart beat
   * thread if our heartbeat to the block manager indicates that we were not registered.
   *
   * Note that this method must be called without any BlockInfo locks held.
   */
  def reregister(): Unit = {
    // TODO: We might need to rate limit re-registering.
    logInfo(s"BlockManager $blockManagerId re-registering with master")
    // 注册BlockManager.
    master.registerBlockManager(blockManagerId, maxOnHeapMemory, maxOffHeapMemory, slaveEndpoint)
    // 报告所有Block信息.
    reportAllBlocks()
  }

  /**
   * 很快就会重新注册到master上.
    * Re-register with the master sometime soon.
   */
  private def asyncReregister(): Unit = {
    // 锁住异步注册锁
    asyncReregisterLock.synchronized {
      // 如果锁不为空
      if (asyncReregisterTask == null) {

        asyncReregisterTask = Future[Unit] {
          // 这是个阻塞操作,并且应该在缓存线程池futureExecutionContext中运行.
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          // 注册
          reregister()
          asyncReregisterLock.synchronized {
            // 释放task
            asyncReregisterTask = null
          }
        }(futureExecutionContext)
      }
    }
  }

  /**
   * For testing. Wait for any pending asynchronous re-registration; otherwise, do nothing.
   */
  def waitForAsyncReregister(): Unit = {
    val task = asyncReregisterTask
    if (task != null) {
      try {
        ThreadUtils.awaitReady(task, Duration.Inf)
      } catch {
        case NonFatal(t) =>
          throw new Exception("Error occurred while waiting for async. reregistration", t)
      }
    }
  }

  /**
   * 获取本地block数据接口.如果找不到或读取不成功抛出异常.
    * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   */
  override def getBlockData(blockId: BlockId): ManagedBuffer = {
    if (blockId.isShuffle) {// shuffleblock情况
      // 通过ShuffleBlockResolver.getBlockData获取block数据
      shuffleManager.shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
    } else { // 不是shuffleblock
      // 本对象方法getLocalBytes获取block数据
      getLocalBytes(blockId) match {
        case Some(blockData) =>
          // 创建BlockManagerManagedBuffer
          new BlockManagerManagedBuffer(blockInfoManager, blockId, blockData, true)
        case None =>
          // If this block manager receives a request for a block that it doesn't have then it's
          // likely that the master has outdated block statuses for this block. Therefore, we send
          // an RPC so that this block is marked as being unavailable from this block manager.
          // 报告一下
          reportBlockStatus(blockId, BlockStatus.empty)
          throw new BlockNotFoundException(blockId.toString)
      }
    }
  }

  /**
   * Put the block locally, using the given storage level.
   *
   * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
   * so may corrupt or change the data stored by the `BlockManager`.
   */
  override def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Boolean = {
    putBytes(blockId, new ChunkedByteBuffer(data.nioByteBuffer()), level)(classTag)
  }

  /**
   * Get the BlockStatus for the block identified by the given ID, if it exists.
   * NOTE: This is mainly for testing.
   */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    blockInfoManager.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
      BlockStatus(info.level, memSize = memSize, diskSize = diskSize)
    }
  }

  /**
   * Get the ids of existing blocks that match the given filter. Note that this will
   * query the blocks stored in the disk block manager (that the block manager
   * may not know of).
   */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    // The `toArray` is necessary here in order to force the list to be materialized so that we
    // don't try to serialize a lazy iterator when responding to client requests.
    (blockInfoManager.entries.map(_._1) ++ diskBlockManager.getAllBlocks())
      .filter(filter)
      .toArray
      .toSeq
  }

  /**
   * 告诉master block的当前存储状态.该方法会发送一个反射当前block状态的更新消息,不是blockinfo的存储级别.
    * 例如,MEMORY_AND_DISK存储级别的block可能已经存到了磁盘上.
    * Tell the master about the current storage status of a block. This will send a block update
   * message reflecting the current status, *not* the desired storage level in its block info.
   * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
   *
   * droppedMemorySize exists to account for when the block is dropped from memory to disk (so
   * it is still valid). This ensures that update in master will compensate for the increase in
   * memory on slave.
   */
  private def reportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Unit = {
    // 向blockmanagerMaster回报Blockstatus.
    val needReregister = !tryToReportBlockStatus(blockId, status, droppedMemorySize)
    // 如果true,说明需要重新向BlockManagerMaster一步注册BlockManager
    if (needReregister) {
      logInfo(s"Got told to re-register updating block $blockId")
      // 另起线程调用reregister,异步注册Blockmanager
      // Re-registering will report our new block for free.
      asyncReregister()
    }
    logDebug(s"Told master about block $blockId")
  }

  /**
   * 实际上发送更新BlockInfo的消息.返回master的响应.如果block成功被记录下返回true,如果
    * 从节点需要重新注册返回false
    * Actually send a UpdateBlockInfo message. Returns the master's response,
   * which will be true if the block was successfully recorded and false if
   * the slave needs to re-register.
   */
  private def tryToReportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Boolean = {
    val storageLevel = status.storageLevel
    val inMemSize = Math.max(status.memSize, droppedMemorySize)
    val onDiskSize = status.diskSize
    master.updateBlockInfo(blockManagerId, blockId, storageLevel, inMemSize, onDiskSize)
  }

  /**
   * 返回指定BlockId的Block更新过的存储状态.如果Block从内存中被删除并且有可能添加到磁盘,返回新的存储级别
    * 并且更新使用内存和磁盘大小的信息.
    * Return the updated storage status of the block with the given ID. More specifically, if
   * the block is dropped from memory and possibly added to disk, return the new storage level
   * and the updated in-memory and on-disk sizes.
   */
  private def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus = {
    info.synchronized {
      // 匹配存储级别
      info.level match {
          // 没有的话就BlockStatus.empty
        case null =>
          BlockStatus.empty
          // 如果有级别的话
        case level =>
          // 是否使用内存
          val inMem = level.useMemory && memoryStore.contains(blockId)
          // 是否使用磁盘
          val onDisk = level.useDisk && diskStore.contains(blockId)
          // 是否序列化
          val deserialized = if (inMem) level.deserialized else false
          // 副本个数
          val replication = if (inMem  || onDisk) level.replication else 1
          // 构造存储级别
          val storageLevel = StorageLevel(
            useDisk = onDisk,
            useMemory = inMem,
            useOffHeap = level.useOffHeap,
            deserialized = deserialized,
            replication = replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          // 返回
          BlockStatus(storageLevel, memSize, diskSize)
      }
    }
  }

  /**
   * Get locations of an array of blocks.
   */
  private def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]] = {
    val startTimeMs = System.currentTimeMillis
    val locations = master.getLocations(blockIds).toArray
    logDebug("Got multiple block location in %s".format(Utils.getUsedTimeMs(startTimeMs)))
    locations
  }

  /**
   * Cleanup code run in response to a failed local read.
   * Must be called while holding a read lock on the block.
   */
  private def handleLocalReadFailure(blockId: BlockId): Nothing = {
    releaseLock(blockId)
    // Remove the missing block so that its unavailability is reported to the driver
    removeBlock(blockId)
    throw new SparkException(s"Block $blockId was not found even though it's read-locked")
  }

  /**
   * 从本地block管理获取block,转换成java对象的迭代器
    * Get block from local block manager as an iterator of Java objects.
   */
  def getLocalValues(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting local block $blockId")
    blockInfoManager.lockForReading(blockId) match {
      case None =>
        logDebug(s"Block $blockId was not found")
        None
      case Some(info) =>
        val level = info.level
        logDebug(s"Level for block $blockId is $level")
        val taskAttemptId = Option(TaskContext.get()).map(_.taskAttemptId())
        if (level.useMemory && memoryStore.contains(blockId)) {
          val iter: Iterator[Any] = if (level.deserialized) {
            memoryStore.getValues(blockId).get
          } else {
            serializerManager.dataDeserializeStream(
              blockId, memoryStore.getBytes(blockId).get.toInputStream())(info.classTag)
          }
          // We need to capture the current taskId in case the iterator completion is triggered
          // from a different thread which does not have TaskContext set; see SPARK-18406 for
          // discussion.
          val ci = CompletionIterator[Any, Iterator[Any]](iter, {
            releaseLock(blockId, taskAttemptId)
          })
          Some(new BlockResult(ci, DataReadMethod.Memory, info.size))
        } else if (level.useDisk && diskStore.contains(blockId)) {
          val diskData = diskStore.getBytes(blockId)
          val iterToReturn: Iterator[Any] = {
            if (level.deserialized) {
              val diskValues = serializerManager.dataDeserializeStream(
                blockId,
                diskData.toInputStream())(info.classTag)
              maybeCacheDiskValuesInMemory(info, blockId, level, diskValues)
            } else {
              val stream = maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
                .map { _.toInputStream(dispose = false) }
                .getOrElse { diskData.toInputStream() }
              serializerManager.dataDeserializeStream(blockId, stream)(info.classTag)
            }
          }
          val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, {
            releaseLockAndDispose(blockId, diskData, taskAttemptId)
          })
          Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
        } else {
          handleLocalReadFailure(blockId)
        }
    }
  }

  /**
   * 从本地block管理获取block的序列化字节.
    * Get block from the local block manager as serialized bytes.
   */
  def getLocalBytes(blockId: BlockId): Option[BlockData] = {
    logDebug(s"Getting local block $blockId as bytes")
    // 作为获取map输出的优化，如果该block用于shuffle，则返回它而不获取锁定;
    // 磁盘存储永远不会删除（最近）项目，所以这应该可以.
    // As an optimization for map output fetches, if the block is for a shuffle, return it
    // without acquiring a lock; the disk store never deletes (recent) items so this should work
    if (blockId.isShuffle) {
      val shuffleBlockResolver = shuffleManager.shuffleBlockResolver
      // TODO: This should gracefully handle case where local block is not available. Currently
      // downstream code will throw an exception.
      // 如果Block是shuffleBlock,那么调用getBlockData构造ChunkedByteBuffer
      val buf = new ChunkedByteBuffer(
        shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId]).nioByteBuffer())
      // 再封装一层ByteBufferBlockData返回.
      Some(new ByteBufferBlockData(buf, true))
    } else {
      // 如果不是shuffleBlock,获取读锁,然后调用doGetLocalBytes获取数据
      blockInfoManager.lockForReading(blockId).map { info => doGetLocalBytes(blockId, info) }
    }
  }

  /**
   * 从本地block管理获取block序列化字节.调用时必须持有该block读锁.
    * 异常释放锁,成功保持锁定
    * Get block from the local block manager as serialized bytes.
   *
   * Must be called while holding a read lock on the block.
   * Releases the read lock upon exception; keeps the read lock upon successful return.
   */
  private def doGetLocalBytes(blockId: BlockId, info: BlockInfo): BlockData = {
    val level = info.level
    logDebug(s"Level for block $blockId is $level")
    // 按顺序，尝试从内存读取序列化字节，然后从磁盘读取，然后回到序列化内存中对象，
    // 最后，如果块不存在则抛出异常.
    // In order, try to read the serialized bytes from memory, then from disk, then fall back to
    // serializing in-memory objects, and, finally, throw an exception if the block does not exist.
    if (level.deserialized) { // 没被序列化,按照磁盘,内存顺序获取数据
      // 尝试通过从磁盘读取预序列化副本来避免昂贵的序列化
      // Try to avoid expensive serialization by reading a pre-serialized copy from disk:
      if (level.useDisk && diskStore.contains(blockId)) {
        // 注意：我们故意不尝试将block放回内存。 由于此分支处理反序列化的block，因此该块只能作为对象缓存在内存中，
        // 而不是序列化的字节。 因为调用者只请求了字节，所以缓存block的反序列化对象没有意义，因为缓存可能没有收益。
        // Note: we purposely do not try to put the block back into memory here. Since this branch
        // handles deserialized blocks, this block may only be cached in memory as objects, not
        // serialized bytes. Because the caller only requested bytes, it doesn't make sense to
        // cache the block's deserialized objects since that caching may not have a payoff.
        // 根据blockid获取字节
        diskStore.getBytes(blockId)
      } else if (level.useMemory && memoryStore.contains(blockId)) {
        // 没有在磁盘上,序列化一个内存副本
        // The block was not found on disk, so serialize an in-memory copy:
        new ByteBufferBlockData(serializerManager.dataSerializeWithExplicitClassTag(
          blockId, memoryStore.getValues(blockId).get, info.classTag), true)
      } else {
        handleLocalReadFailure(blockId)  //本地读取失败
      }
    } else {  // storage level is serialized block被序列化了,那么按照内存,磁盘顺序获取数据
      if (level.useMemory && memoryStore.contains(blockId)) {
        new ByteBufferBlockData(memoryStore.getBytes(blockId).get, false)
      } else if (level.useDisk && diskStore.contains(blockId)) {
        val diskData = diskStore.getBytes(blockId)
        maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
          .map(new ByteBufferBlockData(_, false))
          .getOrElse(diskData)
      } else {
        handleLocalReadFailure(blockId) //失败
      }
    }
  }

  /**
   * Get block from remote block managers.
   *
   * This does not acquire a lock on this block in this JVM.
   */
  private def getRemoteValues[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    val ct = implicitly[ClassTag[T]]
    getRemoteBytes(blockId).map { data =>
      val values =
        serializerManager.dataDeserializeStream(blockId, data.toInputStream(dispose = true))(ct)
      new BlockResult(values, DataReadMethod.Network, data.size)
    }
  }

  /**
   * Return a list of locations for the given block, prioritizing the local machine since
   * multiple block managers can share the same host, followed by hosts on the same rack.
   */
  private def sortLocations(locations: Seq[BlockManagerId]): Seq[BlockManagerId] = {
    val locs = Random.shuffle(locations)
    val (preferredLocs, otherLocs) = locs.partition { loc => blockManagerId.host == loc.host }
    blockManagerId.topologyInfo match {
      case None => preferredLocs ++ otherLocs
      case Some(_) =>
        val (sameRackLocs, differentRackLocs) = otherLocs.partition {
          loc => blockManagerId.topologyInfo == loc.topologyInfo
        }
        preferredLocs ++ sameRackLocs ++ differentRackLocs
    }
  }

  /**
   * Get block from remote block managers as serialized bytes.
   */
  def getRemoteBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    logDebug(s"Getting remote block $blockId")
    require(blockId != null, "BlockId is null")
    var runningFailureCount = 0
    var totalFailureCount = 0

    // Because all the remote blocks are registered in driver, it is not necessary to ask
    // all the slave executors to get block status.
    val locationsAndStatus = master.getLocationsAndStatus(blockId)
    val blockSize = locationsAndStatus.map { b =>
      b.status.diskSize.max(b.status.memSize)
    }.getOrElse(0L)
    val blockLocations = locationsAndStatus.map(_.locations).getOrElse(Seq.empty)

    // If the block size is above the threshold, we should pass our FileManger to
    // BlockTransferService, which will leverage it to spill the block; if not, then passed-in
    // null value means the block will be persisted in memory.
    val tempFileManager = if (blockSize > maxRemoteBlockToMem) {
      remoteBlockTempFileManager
    } else {
      null
    }

    val locations = sortLocations(blockLocations)
    val maxFetchFailures = locations.size
    var locationIterator = locations.iterator
    while (locationIterator.hasNext) {
      val loc = locationIterator.next()
      logDebug(s"Getting remote block $blockId from $loc")
      val data = try {
        blockTransferService.fetchBlockSync(
          loc.host, loc.port, loc.executorId, blockId.toString, tempFileManager).nioByteBuffer()
      } catch {
        case NonFatal(e) =>
          runningFailureCount += 1
          totalFailureCount += 1

          if (totalFailureCount >= maxFetchFailures) {
            // Give up trying anymore locations. Either we've tried all of the original locations,
            // or we've refreshed the list of locations from the master, and have still
            // hit failures after trying locations from the refreshed list.
            logWarning(s"Failed to fetch block after $totalFailureCount fetch failures. " +
              s"Most recent failure cause:", e)
            return None
          }

          logWarning(s"Failed to fetch remote block $blockId " +
            s"from $loc (failed attempt $runningFailureCount)", e)

          // If there is a large number of executors then locations list can contain a
          // large number of stale entries causing a large number of retries that may
          // take a significant amount of time. To get rid of these stale entries
          // we refresh the block locations after a certain number of fetch failures
          if (runningFailureCount >= maxFailuresBeforeLocationRefresh) {
            locationIterator = sortLocations(master.getLocations(blockId)).iterator
            logDebug(s"Refreshed locations from the driver " +
              s"after ${runningFailureCount} fetch failures.")
            runningFailureCount = 0
          }

          // This location failed, so we retry fetch from a different one by returning null here
          null
      }

      if (data != null) {
        return Some(new ChunkedByteBuffer(data))
      }
      logDebug(s"The value of block $blockId is null")
    }
    logDebug(s"Block $blockId not found")
    None
  }

  /**
   * Get a block from the block manager (either local or remote).
   *
   * This acquires a read lock on the block if the block was stored locally and does not acquire
   * any locks if the block was fetched from a remote block manager. The read lock will
   * automatically be freed once the result's `data` iterator is fully consumed.
   */
  def get[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    val local = getLocalValues(blockId)
    if (local.isDefined) {
      logInfo(s"Found block $blockId locally")
      return local
    }
    val remote = getRemoteValues[T](blockId)
    if (remote.isDefined) {
      logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
  }

  /**
   * Downgrades an exclusive write lock to a shared read lock.
   */
  def downgradeLock(blockId: BlockId): Unit = {
    blockInfoManager.downgradeLock(blockId)
  }

  /**
   * Release a lock on the given block with explicit TID.
   * The param `taskAttemptId` should be passed in case we can't get the correct TID from
   * TaskContext, for example, the input iterator of a cached RDD iterates to the end in a child
   * thread.
   */
  def releaseLock(blockId: BlockId, taskAttemptId: Option[Long] = None): Unit = {
    blockInfoManager.unlock(blockId, taskAttemptId)
  }

  /**
   * Registers a task with the BlockManager in order to initialize per-task bookkeeping structures.
   */
  def registerTask(taskAttemptId: Long): Unit = {
    blockInfoManager.registerTask(taskAttemptId)
  }

  /**
   * Release all locks for the given task.
   *
   * @return the blocks whose locks were released.
   */
  def releaseAllLocksForTask(taskAttemptId: Long): Seq[BlockId] = {
    blockInfoManager.releaseAllLocksForTask(taskAttemptId)
  }

  /**
   * Retrieve the given block if it exists, otherwise call the provided `makeIterator` method
   * to compute the block, persist it, and return its values.
   *
   * @return either a BlockResult if the block was successfully cached, or an iterator if the block
   *         could not be cached.
   */
  def getOrElseUpdate[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      makeIterator: () => Iterator[T]): Either[BlockResult, Iterator[T]] = {
    // Attempt to read the block from local or remote storage. If it's present, then we don't need
    // to go through the local-get-or-put path.
    get[T](blockId)(classTag) match {
      case Some(block) =>
        return Left(block)
      case _ =>
        // Need to compute the block.
    }
    // Initially we hold no locks on this block.
    doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
      case None =>
        // doPut() didn't hand work back to us, so the block already existed or was successfully
        // stored. Therefore, we now hold a read lock on the block.
        val blockResult = getLocalValues(blockId).getOrElse {
          // Since we held a read lock between the doPut() and get() calls, the block should not
          // have been evicted, so get() not returning the block indicates some internal error.
          releaseLock(blockId)
          throw new SparkException(s"get() failed for block $blockId even though we held a lock")
        }
        // We already hold a read lock on the block from the doPut() call and getLocalValues()
        // acquires the lock again, so we need to call releaseLock() here so that the net number
        // of lock acquisitions is 1 (since the caller will only call release() once).
        releaseLock(blockId)
        Left(blockResult)
      case Some(iter) =>
        // The put failed, likely because the data was too large to fit in memory and could not be
        // dropped to disk. Therefore, we need to pass the input iterator back to the caller so
        // that they can decide what to do with the values (e.g. process them without caching).
       Right(iter)
    }
  }

  /**
   * @return true if the block was stored or false if an error occurred.
   */
  def putIterator[T: ClassTag](
      blockId: BlockId,
      values: Iterator[T],
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(values != null, "Values is null")
    doPutIterator(blockId, () => values, level, implicitly[ClassTag[T]], tellMaster) match {
      case None =>
        true
      case Some(iter) =>
        // Caller doesn't care about the iterator values, so we can close the iterator here
        // to free resources earlier
        iter.close()
        false
    }
  }

  /**
   * A short circuited method to get a block writer that can write data directly to disk.
   * The Block will be appended to the File specified by filename. Callers should handle error
   * cases.
   */
  def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializerInstance: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): DiskBlockObjectWriter = {
    val syncWrites = conf.getBoolean("spark.shuffle.sync", false)
    new DiskBlockObjectWriter(file, serializerManager, serializerInstance, bufferSize,
      syncWrites, writeMetrics, blockId)
  }

  /**
   * 将新Block序列化字节写入BlockManager.调用者不能改变或释放字节数据缓冲区.
    * 这样做可能会损坏或更改BlockManager存储的数据
    * Put a new block of serialized bytes to the block manager.
   *
   * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
   * so may corrupt or change the data stored by the `BlockManager`.
   *
   * @return true if the block was stored or false if an error occurred.
   */
  def putBytes[T: ClassTag](
      blockId: BlockId,
      bytes: ChunkedByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(bytes != null, "Bytes is null")
    doPutBytes(blockId, bytes, level, implicitly[ClassTag[T]], tellMaster)
  }

  /**
   * 根据指定的block存储级别将给定字节存入,副本数是必须的.如果block早已存在,本方法会覆盖其写入.
    * 调用者不能改变或释放字节数据缓冲区.这样做可能会损坏或更改BlockManager存储的数据
    * Put the given bytes according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * If the block already exists, this method will not overwrite it.
   *
   * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
   * so may corrupt or change the data stored by the `BlockManager`.
   *
   * @param keepReadLock 如果是true,本方法在返回之后仍会持有读锁.如果false,不会持有.
    *                     if true, this method will hold the read lock when it returns (even if the
   *                     block already exists). If false, this method will hold no locks when it
   *                     returns.
   * @return true if the block was already present or if the put succeeded, false otherwise.
   */
  private def doPutBytes[T](
      blockId: BlockId,
      bytes: ChunkedByteBuffer,
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false): Boolean = {
    doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
      val startTimeMs = System.currentTimeMillis
      // Since we're storing bytes, initiate the replication before storing them locally.
      // This is faster as data is already serialized and ready to send.
      val replicationFuture = if (level.replication > 1) {
        Future {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool. The ByteBufferBlockData wrapper is not disposed of to avoid releasing
          // buffers that are owned by the caller.
          replicate(blockId, new ByteBufferBlockData(bytes, false), level, classTag)
        }(futureExecutionContext)
      } else {
        null
      }

      val size = bytes.size

      if (level.useMemory) {
        // Put it in memory first, even if it also has useDisk set to true;
        // We will drop it to disk later if the memory store can't hold it.
        val putSucceeded = if (level.deserialized) {
          val values =
            serializerManager.dataDeserializeStream(blockId, bytes.toInputStream())(classTag)
          memoryStore.putIteratorAsValues(blockId, values, classTag) match {
            case Right(_) => true
            case Left(iter) =>
              // If putting deserialized values in memory failed, we will put the bytes directly to
              // disk, so we don't need this iterator and can close it to free resources earlier.
              iter.close()
              false
          }
        } else {
          val memoryMode = level.memoryMode
          memoryStore.putBytes(blockId, size, memoryMode, () => {
            if (memoryMode == MemoryMode.OFF_HEAP &&
                bytes.chunks.exists(buffer => !buffer.isDirect)) {
              bytes.copy(Platform.allocateDirectBuffer)
            } else {
              bytes
            }
          })
        }
        if (!putSucceeded && level.useDisk) {
          logWarning(s"Persisting block $blockId to disk instead.")
          diskStore.putBytes(blockId, bytes)
        }
      } else if (level.useDisk) {
        diskStore.putBytes(blockId, bytes)
      }

      val putBlockStatus = getCurrentBlockStatus(blockId, info)
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) {
        // Now that the block is in either the memory or disk store,
        // tell the master about it.
        info.size = size
        if (tellMaster && info.tellMaster) {
          reportBlockStatus(blockId, putBlockStatus)
        }
        addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
      }
      logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))
      if (level.replication > 1) {
        // Wait for asynchronous replication to finish
        try {
          ThreadUtils.awaitReady(replicationFuture, Duration.Inf)
        } catch {
          case NonFatal(t) =>
            throw new Exception("Error occurred while waiting for replication to finish", t)
        }
      }
      if (blockWasSuccessfullyStored) {
        None
      } else {
        Some(bytes)
      }
    }.isEmpty
  }

  /**
   * 从doPutBytes和doPutIterator方法中抽象出来的辅助方法
    * Helper method used to abstract common code from [[doPutBytes()]] and [[doPutIterator()]].
   *
   * @param putBody a function which attempts the actual put() and returns None on success
   *                or Some on failure.
   */
  private def doPut[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_],
      tellMaster: Boolean,
      keepReadLock: Boolean)(putBody: BlockInfo => Option[T]): Option[T] = {

    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")
    // 放入block的信息
    val putBlockInfo = {
      val newInfo = new BlockInfo(level, classTag, tellMaster)
      // 获取写锁
      if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) {
        newInfo
      } else {
        logWarning(s"Block $blockId already exists on this machine; not re-adding it")
        if (!keepReadLock) {
          // 如果Block已经存在并且不需要持有读锁,释放当前读锁
          // lockNewBlockForWriting returned a read lock on the existing block, so we must free it:
          releaseLock(blockId)
        }
        return None
      }
    }
    // 开始时间
    val startTimeMs = System.currentTimeMillis
    // 是否抛出异常
    var exceptionWasThrown: Boolean = true
    val result: Option[T] = try {
      // 将block写入,putbody是偏函数,需要传入
      val res = putBody(putBlockInfo)
      // 是否抛出异常
      exceptionWasThrown = false
      // 如果
      if (res.isEmpty) {
        // 写入成功
        // the block was successfully stored
        if (keepReadLock) {
          // 如果需要持有读锁将写锁降级为读锁
          blockInfoManager.downgradeLock(blockId)
        } else {
          // 不需要持有读锁,直接释放锁
          blockInfoManager.unlock(blockId)
        }
      } else {
        // 如果写入失败,移除Block
        removeBlockInternal(blockId, tellMaster = false)
        // log一下
        logWarning(s"Putting block $blockId failed")
      }
      // 赋值result
      res
    } catch {
      // 因为removeBlockInternal可能抛出异常,我们需要打印异常信息来展示原因
      // Since removeBlockInternal may throw exception,
      // we should print exception first to show root cause.
      case NonFatal(e) =>
        logWarning(s"Putting block $blockId failed due to exception $e.")
        throw e
    } finally {
      // finally块执行是为了避免捕获并重新抛出InterruptedException
      // This cleanup is performed in a finally block rather than a `catch` to avoid having to
      // catch and properly re-throw InterruptedException.
      if (exceptionWasThrown) {
        // 如果抛出异常了,那就移除block并且更新任务度量信息.
        // If an exception was thrown then it's possible that the code in `putBody` has already
        // notified the master about the availability of this block, so we need to send an update
        // to remove this block location.
        removeBlockInternal(blockId, tellMaster = tellMaster)
        // The `putBody` code may have also added a new block status to TaskMetrics, so we need
        // to cancel that out by overwriting it with an empty block status. We only do this if
        // the finally block was entered via an exception because doing this unconditionally would
        // cause us to send empty block statuses for every block that failed to be cached due to
        // a memory shortage (which is an expected failure, unlike an uncaught exception).
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
      }
    }
    if (level.replication > 1) {
      logDebug("Putting block %s with replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    } else {
      logDebug("Putting block %s without replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    }
    // 返回
    result
  }

  /**
   * Put the given block according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * If the block already exists, this method will not overwrite it.
   *
   * @param keepReadLock if true, this method will hold the read lock when it returns (even if the
   *                     block already exists). If false, this method will hold no locks when it
   *                     returns.
   * @return None if the block was already present or if the put succeeded, or Some(iterator)
   *         if the put failed.
   */
  private def doPutIterator[T](
      blockId: BlockId,
      iterator: () => Iterator[T],
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false): Option[PartiallyUnrolledIterator[T]] = {
    doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
      val startTimeMs = System.currentTimeMillis
      var iteratorFromFailedMemoryStorePut: Option[PartiallyUnrolledIterator[T]] = None
      // Size of the block in bytes
      var size = 0L
      if (level.useMemory) {
        // Put it in memory first, even if it also has useDisk set to true;
        // We will drop it to disk later if the memory store can't hold it.
        if (level.deserialized) {
          memoryStore.putIteratorAsValues(blockId, iterator(), classTag) match {
            case Right(s) =>
              size = s
            case Left(iter) =>
              // Not enough space to unroll this block; drop to disk if applicable
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  serializerManager.dataSerializeStream(blockId, out, iter)(classTag)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(iter)
              }
          }
        } else { // !level.deserialized
          memoryStore.putIteratorAsBytes(blockId, iterator(), classTag, level.memoryMode) match {
            case Right(s) =>
              size = s
            case Left(partiallySerializedValues) =>
              // Not enough space to unroll this block; drop to disk if applicable
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  partiallySerializedValues.finishWritingToStream(out)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(partiallySerializedValues.valuesIterator)
              }
          }
        }

      } else if (level.useDisk) {
        diskStore.put(blockId) { channel =>
          val out = Channels.newOutputStream(channel)
          serializerManager.dataSerializeStream(blockId, out, iterator())(classTag)
        }
        size = diskStore.getSize(blockId)
      }

      val putBlockStatus = getCurrentBlockStatus(blockId, info)
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) {
        // Now that the block is in either the memory or disk store, tell the master about it.
        info.size = size
        if (tellMaster && info.tellMaster) {
          reportBlockStatus(blockId, putBlockStatus)
        }
        addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
        logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))
        if (level.replication > 1) {
          val remoteStartTime = System.currentTimeMillis
          val bytesToReplicate = doGetLocalBytes(blockId, info)
          // [SPARK-16550] Erase the typed classTag when using default serialization, since
          // NettyBlockRpcServer crashes when deserializing repl-defined classes.
          // TODO(ekl) remove this once the classloader issue on the remote end is fixed.
          val remoteClassTag = if (!serializerManager.canUseKryo(classTag)) {
            scala.reflect.classTag[Any]
          } else {
            classTag
          }
          try {
            replicate(blockId, bytesToReplicate, level, remoteClassTag)
          } finally {
            bytesToReplicate.dispose()
          }
          logDebug("Put block %s remotely took %s"
            .format(blockId, Utils.getUsedTimeMs(remoteStartTime)))
        }
      }
      assert(blockWasSuccessfullyStored == iteratorFromFailedMemoryStorePut.isEmpty)
      iteratorFromFailedMemoryStorePut
    }
  }

  /**
   * Attempts to cache spilled bytes read from disk into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the bytes from the memory store if the put succeeded, otherwise None.
   *         If this returns bytes from the memory store then the original disk store bytes will
   *         automatically be disposed and the caller should not continue to use them. Otherwise,
   *         if this returns None then the original disk store bytes will be unaffected.
   */
  private def maybeCacheDiskBytesInMemory(
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskData: BlockData): Option[ChunkedByteBuffer] = {
    require(!level.deserialized)
    if (level.useMemory) {
      // Synchronize on blockInfo to guard against a race condition where two readers both try to
      // put values read from disk into the MemoryStore.
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          diskData.dispose()
          Some(memoryStore.getBytes(blockId).get)
        } else {
          val allocator = level.memoryMode match {
            case MemoryMode.ON_HEAP => ByteBuffer.allocate _
            case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
          }
          val putSucceeded = memoryStore.putBytes(blockId, diskData.size, level.memoryMode, () => {
            // https://issues.apache.org/jira/browse/SPARK-6076
            // If the file size is bigger than the free memory, OOM will happen. So if we
            // cannot put it into MemoryStore, copyForMemory should not be created. That's why
            // this action is put into a `() => ChunkedByteBuffer` and created lazily.
            diskData.toChunkedByteBuffer(allocator)
          })
          if (putSucceeded) {
            diskData.dispose()
            Some(memoryStore.getBytes(blockId).get)
          } else {
            None
          }
        }
      }
    } else {
      None
    }
  }

  /**
   * Attempts to cache spilled values read from disk into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the iterator. The original iterator passed this method should no longer
   *         be used after this method returns.
   */
  private def maybeCacheDiskValuesInMemory[T](
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskIterator: Iterator[T]): Iterator[T] = {
    require(level.deserialized)
    val classTag = blockInfo.classTag.asInstanceOf[ClassTag[T]]
    if (level.useMemory) {
      // Synchronize on blockInfo to guard against a race condition where two readers both try to
      // put values read from disk into the MemoryStore.
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          // Note: if we had a means to discard the disk iterator, we would do that here.
          memoryStore.getValues(blockId).get
        } else {
          memoryStore.putIteratorAsValues(blockId, diskIterator, classTag) match {
            case Left(iter) =>
              // The memory store put() failed, so it returned the iterator back to us:
              iter
            case Right(_) =>
              // The put() succeeded, so we can read the values back:
              memoryStore.getValues(blockId).get
          }
        }
      }.asInstanceOf[Iterator[T]]
    } else {
      diskIterator
    }
  }

  /**
   * Get peer block managers in the system.
   */
  private def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = {
    peerFetchLock.synchronized {
      val cachedPeersTtl = conf.getInt("spark.storage.cachedPeersTtl", 60 * 1000) // milliseconds
      val timeout = System.currentTimeMillis - lastPeerFetchTime > cachedPeersTtl
      if (cachedPeers == null || forceFetch || timeout) {
        cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode)
        lastPeerFetchTime = System.currentTimeMillis
        logDebug("Fetched peers from master: " + cachedPeers.mkString("[", ",", "]"))
      }
      cachedPeers
    }
  }

  /**
   * Called for pro-active replenishment of blocks lost due to executor failures
   *
   * @param blockId blockId being replicate
   * @param existingReplicas existing block managers that have a replica
   * @param maxReplicas maximum replicas needed
   */
  def replicateBlock(
      blockId: BlockId,
      existingReplicas: Set[BlockManagerId],
      maxReplicas: Int): Unit = {
    logInfo(s"Using $blockManagerId to pro-actively replicate $blockId")
    blockInfoManager.lockForReading(blockId).foreach { info =>
      val data = doGetLocalBytes(blockId, info)
      val storageLevel = StorageLevel(
        useDisk = info.level.useDisk,
        useMemory = info.level.useMemory,
        useOffHeap = info.level.useOffHeap,
        deserialized = info.level.deserialized,
        replication = maxReplicas)
      // we know we are called as a result of an executor removal, so we refresh peer cache
      // this way, we won't try to replicate to a missing executor with a stale reference
      getPeers(forceFetch = true)
      try {
        replicate(blockId, data, storageLevel, info.classTag, existingReplicas)
      } finally {
        logDebug(s"Releasing lock for $blockId")
        releaseLockAndDispose(blockId, data)
      }
    }
  }

  /**
   * Replicate block to another node. Note that this is a blocking call that returns after
   * the block has been replicated.
   */
  private def replicate(
      blockId: BlockId,
      data: BlockData,
      level: StorageLevel,
      classTag: ClassTag[_],
      existingReplicas: Set[BlockManagerId] = Set.empty): Unit = {

    val maxReplicationFailures = conf.getInt("spark.storage.maxReplicationFailures", 1)
    val tLevel = StorageLevel(
      useDisk = level.useDisk,
      useMemory = level.useMemory,
      useOffHeap = level.useOffHeap,
      deserialized = level.deserialized,
      replication = 1)

    val numPeersToReplicateTo = level.replication - 1
    val startTime = System.nanoTime

    val peersReplicatedTo = mutable.HashSet.empty ++ existingReplicas
    val peersFailedToReplicateTo = mutable.HashSet.empty[BlockManagerId]
    var numFailures = 0

    val initialPeers = getPeers(false).filterNot(existingReplicas.contains)

    var peersForReplication = blockReplicationPolicy.prioritize(
      blockManagerId,
      initialPeers,
      peersReplicatedTo,
      blockId,
      numPeersToReplicateTo)

    while(numFailures <= maxReplicationFailures &&
      !peersForReplication.isEmpty &&
      peersReplicatedTo.size < numPeersToReplicateTo) {
      val peer = peersForReplication.head
      try {
        val onePeerStartTime = System.nanoTime
        logTrace(s"Trying to replicate $blockId of ${data.size} bytes to $peer")
        blockTransferService.uploadBlockSync(
          peer.host,
          peer.port,
          peer.executorId,
          blockId,
          new BlockManagerManagedBuffer(blockInfoManager, blockId, data, false),
          tLevel,
          classTag)
        logTrace(s"Replicated $blockId of ${data.size} bytes to $peer" +
          s" in ${(System.nanoTime - onePeerStartTime).toDouble / 1e6} ms")
        peersForReplication = peersForReplication.tail
        peersReplicatedTo += peer
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to replicate $blockId to $peer, failure #$numFailures", e)
          peersFailedToReplicateTo += peer
          // we have a failed replication, so we get the list of peers again
          // we don't want peers we have already replicated to and the ones that
          // have failed previously
          val filteredPeers = getPeers(true).filter { p =>
            !peersFailedToReplicateTo.contains(p) && !peersReplicatedTo.contains(p)
          }

          numFailures += 1
          peersForReplication = blockReplicationPolicy.prioritize(
            blockManagerId,
            filteredPeers,
            peersReplicatedTo,
            blockId,
            numPeersToReplicateTo - peersReplicatedTo.size)
      }
    }
    logDebug(s"Replicating $blockId of ${data.size} bytes to " +
      s"${peersReplicatedTo.size} peer(s) took ${(System.nanoTime - startTime) / 1e6} ms")
    if (peersReplicatedTo.size < numPeersToReplicateTo) {
      logWarning(s"Block $blockId replicated to only " +
        s"${peersReplicatedTo.size} peer(s) instead of $numPeersToReplicateTo peers")
    }

    logDebug(s"block $blockId replicated to ${peersReplicatedTo.mkString(", ")}")
  }

  /**
   * Read a block consisting of a single object.
   */
  def getSingle[T: ClassTag](blockId: BlockId): Option[T] = {
    get[T](blockId).map(_.data.next().asInstanceOf[T])
  }

  /**
   * Write a block consisting of a single object.
   *
   * @return true if the block was stored or false if the block was already stored or an
   *         error occurred.
   */
  def putSingle[T: ClassTag](
      blockId: BlockId,
      value: T,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    putIterator(blockId, Iterator(value), level, tellMaster)
  }

  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * The caller of this method must hold a write lock on the block before calling this method.
   * This method does not release the write lock.
   *
   * @return the block's new effective StorageLevel.
   */
  private[storage] override def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel = {
    logInfo(s"Dropping block $blockId from memory")
    val info = blockInfoManager.assertBlockIsLockedForWriting(blockId)
    var blockIsUpdated = false
    val level = info.level

    // Drop to disk, if storage level requires
    if (level.useDisk && !diskStore.contains(blockId)) {
      logInfo(s"Writing block $blockId to disk")
      data() match {
        case Left(elements) =>
          diskStore.put(blockId) { channel =>
            val out = Channels.newOutputStream(channel)
            serializerManager.dataSerializeStream(
              blockId,
              out,
              elements.toIterator)(info.classTag.asInstanceOf[ClassTag[T]])
          }
        case Right(bytes) =>
          diskStore.putBytes(blockId, bytes)
      }
      blockIsUpdated = true
    }

    // Actually drop from memory store
    val droppedMemorySize =
      if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
    val blockIsRemoved = memoryStore.remove(blockId)
    if (blockIsRemoved) {
      blockIsUpdated = true
    } else {
      logWarning(s"Block $blockId could not be dropped from memory as it does not exist")
    }

    val status = getCurrentBlockStatus(blockId, info)
    if (info.tellMaster) {
      reportBlockStatus(blockId, status, droppedMemorySize)
    }
    if (blockIsUpdated) {
      addUpdatedBlockStatusToTaskMetrics(blockId, status)
    }
    status.storageLevel
  }

  /**
   * Remove all blocks belonging to the given RDD.
   *
   * @return The number of blocks removed.
   */
  def removeRdd(rddId: Int): Int = {
    // TODO: Avoid a linear scan by creating another mapping of RDD.id to blocks.
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfoManager.entries.flatMap(_._1.asRDDId).filter(_.rddId == rddId)
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    blocksToRemove.size
  }

  /**
   * Remove all blocks belonging to the given broadcast.
   */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logDebug(s"Removing broadcast $broadcastId")
    val blocksToRemove = blockInfoManager.entries.map(_._1).collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    blocksToRemove.size
  }

  /**
   * Remove a block from both memory and disk.
   */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logDebug(s"Removing block $blockId")
    blockInfoManager.lockForWriting(blockId) match {
      case None =>
        // The block has already been removed; do nothing.
        logWarning(s"Asked to remove block $blockId, which does not exist")
      case Some(info) =>
        removeBlockInternal(blockId, tellMaster = tellMaster && info.tellMaster)
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
    }
  }

  /**
   * removeBlock方法的内部版本,调用者需要已经获取了block的写锁.
    * Internal version of [[removeBlock()]] which assumes that the caller already holds a write
   * lock on the block.
   */
  private def removeBlockInternal(blockId: BlockId, tellMaster: Boolean): Unit = {
    // 移除对于磁盘存储和内存存储是幂等的,最坏的情况我们也会得到警告.
    // Removals are idempotent in disk store and memory store. At worst, we get a warning.
    // 从memoryStore中移除Block
    val removedFromMemory = memoryStore.remove(blockId)
    // 从diskStore上移除Block
    val removedFromDisk = diskStore.remove(blockId)
    // 全都没移除,warning一下
    if (!removedFromMemory && !removedFromDisk) {
      logWarning(s"Block $blockId could not be removed as it was not found on disk or in memory")
    }
    // 从BlockInfoManager中移除Block
    blockInfoManager.removeBlock(blockId)
    // 如果需要向BlockManagerMaster回报Block状态,报告一下
    if (tellMaster) {
      reportBlockStatus(blockId, BlockStatus.empty)
    }
  }

  private def addUpdatedBlockStatusToTaskMetrics(blockId: BlockId, status: BlockStatus): Unit = {
    if (conf.get(config.TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES)) {
      Option(TaskContext.get()).foreach { c =>
        c.taskMetrics().incUpdatedBlockStatuses(blockId -> status)
      }
    }
  }

  def releaseLockAndDispose(
      blockId: BlockId,
      data: BlockData,
      taskAttemptId: Option[Long] = None): Unit = {
    releaseLock(blockId, taskAttemptId)
    data.dispose()
  }

  def stop(): Unit = {
    blockTransferService.close()
    if (shuffleClient ne blockTransferService) {
      // Closing should be idempotent, but maybe not for the NioBlockTransferService.
      shuffleClient.close()
    }
    remoteBlockTempFileManager.stop()
    diskBlockManager.stop()
    rpcEnv.stop(slaveEndpoint)
    blockInfoManager.clear()
    memoryStore.clear()
    futureExecutionContext.shutdownNow()
    logInfo("BlockManager stopped")
  }
}


private[spark] object BlockManager {
  private val ID_GENERATOR = new IdGenerator

  def blockIdsToHosts(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {

    // blockManagerMaster != null is used in tests
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }

    val blockManagers = new HashMap[BlockId, Seq[String]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i).map(_.host)
    }
    blockManagers.toMap
  }

  private class ShuffleMetricsSource(
      override val sourceName: String,
      metricSet: MetricSet) extends Source {

    override val metricRegistry = new MetricRegistry
    metricRegistry.registerAll(metricSet)
  }

  class RemoteBlockTempFileManager(blockManager: BlockManager)
      extends TempFileManager with Logging {

    private class ReferenceWithCleanup(file: File, referenceQueue: JReferenceQueue[File])
        extends WeakReference[File](file, referenceQueue) {
      private val filePath = file.getAbsolutePath

      def cleanUp(): Unit = {
        logDebug(s"Clean up file $filePath")

        if (!new File(filePath).delete()) {
          logDebug(s"Fail to delete file $filePath")
        }
      }
    }

    private val referenceQueue = new JReferenceQueue[File]
    private val referenceBuffer = Collections.newSetFromMap[ReferenceWithCleanup](
      new ConcurrentHashMap)

    private val POLL_TIMEOUT = 1000
    @volatile private var stopped = false

    private val cleaningThread = new Thread() { override def run() { keepCleaning() } }
    cleaningThread.setDaemon(true)
    cleaningThread.setName("RemoteBlock-temp-file-clean-thread")
    cleaningThread.start()

    override def createTempFile(): File = {
      blockManager.diskBlockManager.createTempLocalBlock()._2
    }

    override def registerTempFileToClean(file: File): Boolean = {
      referenceBuffer.add(new ReferenceWithCleanup(file, referenceQueue))
    }

    def stop(): Unit = {
      stopped = true
      cleaningThread.interrupt()
      cleaningThread.join()
    }

    private def keepCleaning(): Unit = {
      while (!stopped) {
        try {
          Option(referenceQueue.remove(POLL_TIMEOUT))
            .map(_.asInstanceOf[ReferenceWithCleanup])
            .foreach { ref =>
              referenceBuffer.remove(ref)
              ref.cleanUp()
            }
        } catch {
          case _: InterruptedException =>
            // no-op
          case NonFatal(e) =>
            logError("Error in cleaning thread", e)
        }
      }
    }
  }
}

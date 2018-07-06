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

package org.apache.spark.memory

import javax.annotation.concurrent.GuardedBy

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore

/**
 * 管理用于存储的可调整大小的内存池.主要用于数据存储的内存池.
  * Performs bookkeeping for managing an adjustable-size pool of memory that is used for storage
 * (caching).
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
 */
private[memory] class StorageMemoryPool(
    lock: Object,
    memoryMode: MemoryMode // 内存模式,堆外内存或堆内存
  ) extends MemoryPool(lock) with Logging {
  /** 内存池名称,堆内堆外两种:on-heap storage;ff-heap storage*/
  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap storage"
    case MemoryMode.OFF_HEAP => "off-heap storage"
  }
  /** 已使用内存,单位Byte*/
  @GuardedBy("lock")
  private[this] var _memoryUsed: Long = 0L
  /** get方法,获取已使用内存大小*/
  override def memoryUsed: Long = lock.synchronized {
    _memoryUsed
  }
  /** 当前StorageMemoryPool关联的MemoryStore*/
  private var _memoryStore: MemoryStore = _
  /** get方法,获取memoryStore*/
  def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalStateException("memory store not initialized yet")
    }
    _memoryStore
  }

  /**
   * 设置当前StorageMemoryPool关联的MemoryStore.由于初始化排序约束，必须在构造后设置.
    * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /**
   * 获取N字节内存来缓存指定Block,必要时删除已存在Block缓存
    * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireMemory(blockId: BlockId, numBytes: Long): Boolean = lock.synchronized {
    val numBytesToFree = math.max(0, numBytes - memoryFree)
    acquireMemory(blockId, numBytes, numBytesToFree)
  }

  /**
   *
    * 获取N字节内存来缓存指定Block,必要时删除已存在Block缓存
    * Acquire N bytes of storage memory for the given block, evicting existing ones if necessary.
   *
   * @param blockId the ID of the block we are acquiring storage memory for
   * @param numBytesToAcquire the size of this block
   * @param numBytesToFree the amount of space to be freed through evicting blocks
   * @return whether all N bytes were successfully granted.
   */
  def acquireMemory(
      blockId: BlockId,
      numBytesToAcquire: Long,
      numBytesToFree: Long): Boolean = lock.synchronized {
    assert(numBytesToAcquire >= 0)
    assert(numBytesToFree >= 0)
    assert(memoryUsed <= poolSize)
    if (numBytesToFree > 0) {  // numBytesToFree需要腾出内存空间大小
      // 腾出numBytesToFree空间
      memoryStore.evictBlocksToFreeSpace(Some(blockId), numBytesToFree, memoryMode)
    }
    // 如果内存存储驱逐部分Block，那么这些驱逐将同步回调到这个StorageMemoryPool以释放内存.
    // 因此，这些变量应该已经更新.
    // NOTE: If the memory store evicts blocks, then those evictions will synchronously call
    // back into this StorageMemoryPool in order to free memory. Therefore, these variables
    // should have been updated.
    val enoughMemory = numBytesToAcquire <= memoryFree
    if (enoughMemory) { // 如果内存充足
      // 逻辑上添加已使用内存
      _memoryUsed += numBytesToAcquire
    }
    // 是否获得了用于存储Block的内存空间
    enoughMemory
  }
  /** 释放内存*/
  def releaseMemory(size: Long): Unit = lock.synchronized {
    if (size > _memoryUsed) { // 如果释放内存大小大于已使用内存,逻辑上直接清空已使用内存
      logWarning(s"Attempted to release $size bytes of storage " +
        s"memory when we only have ${_memoryUsed} bytes")
      _memoryUsed = 0
    } else {
      // 否则 减去需要释放内存大小
      _memoryUsed -= size
    }
  }
  /** 释放当前内存池所有内存*/
  def releaseAllMemory(): Unit = lock.synchronized {
    _memoryUsed = 0
  }

  /**
   * 释放spaceToFree参数字节大小空间来缩小内存池空间.该方法不会真正减少内存池大小,依赖调用者完成实际操作.
    * Free space to shrink the size of this storage memory pool by `spaceToFree` bytes.
   * Note: this method doesn't actually reduce the pool size but relies on the caller to do so.
   *
   * @return number of bytes to be removed from the pool's capacity.
   */
  def freeSpaceToShrinkPool(spaceToFree: Long): Long = lock.synchronized {
    // 取参数spaceToFree和memoryFree的最小值
    val spaceFreedByReleasingUnusedMemory = math.min(spaceToFree, memoryFree)
    // spaceToFree-上面的值,得到保留空间中需要释放的值.
    // 总结一下,我们将内存池大小指定为spaceToFree,如果,空闲空间memoryFree大于等于spaceToFree,
    // 我们可以直接返回spaceToFree,因为直接将memoryFree之中的部分内存划分成新内存储就可以了.
    // 如果memoryFree不足够,我们需要驱逐部分Block存储来得到需要的空间大小
    val remainingSpaceToFree = spaceToFree - spaceFreedByReleasingUnusedMemory
    if (remainingSpaceToFree > 0) {
      // 如果回收空闲内存没有充分缩小内存池，则开始逐出Block
      // If reclaiming free memory did not adequately shrink the pool, begin evicting blocks:
      val spaceFreedByEviction =
        memoryStore.evictBlocksToFreeSpace(None, remainingSpaceToFree, memoryMode)
      // 释放Block时，BlockManager.dropFromMemory()方法中调用releaseMemory(),
      // 因此我们不需要在此处减少_memoryUsed.虽然需要减小内存池大小,但是本方法不需要减小.
      // When a block is released, BlockManager.dropFromMemory() calls releaseMemory(), so we do
      // not need to decrement _memoryUsed here. However, we do need to decrement the pool size.
      spaceFreedByReleasingUnusedMemory + spaceFreedByEviction
    } else {
      spaceFreedByReleasingUnusedMemory
    }
  }
}

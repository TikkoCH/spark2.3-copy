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

import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

import com.google.common.collect.{ConcurrentHashMultiset, ImmutableMultiset}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging


/**
 * 单个block的追踪元信息.本类对象不是线程安全的.通过BlockInfoManager中的锁进行同步.
  * Tracks metadata for an individual block.
 *
 * Instances of this class are _not_ thread-safe and are protected by locks in the
 * [[BlockInfoManager]].
 *
 * @param level the block's storage level. This is the requested persistence level, not the
 *              effective storage level of the block (i.e. if this is MEMORY_AND_DISK, then this
 *              does not imply that the block is actually resident in memory).
 * @param classTag the block's [[ClassTag]], used to select the serializer
 * @param tellMaster whether state changes for this block should be reported to the master. This
 *                   is true for most blocks, but is false for broadcast blocks.
 */
private[storage] class BlockInfo(
    val level: StorageLevel, // blockInfo描述的Block的存储级别
    val classTag: ClassTag[_], // BlockInfo描述的Block类型
    val tellMaster: Boolean) { // 是否通知Master

  /**
   * block的字节大小.无参方法类似get方法,有参方法类似set方法.
    * The size of the block (in bytes)
   */
  def size: Long = _size
  def size_=(s: Long): Unit = {
    _size = s
    checkInvariants()
  }
  private[this] var _size: Long = 0

  /**
   * Block锁定读取次数
    * The number of times that this block has been locked for reading.
   */
  def readerCount: Int = _readerCount
  def readerCount_=(c: Int): Unit = {
    _readerCount = c
    checkInvariants()
  }
  private[this] var _readerCount: Int = 0

  /**
   * task在尝试对Block进行写操作之前,首先获取BlockInfo的写锁.writerTask用于保存task尝试的Id.
    * The task attempt id of the task which currently holds the write lock for this block, or
   * [[BlockInfo.NON_TASK_WRITER]] if the write lock is held by non-task code, or
   * [[BlockInfo.NO_WRITER]] if this block is not locked for writing.
   */
  def writerTask: Long = _writerTask
  def writerTask_=(t: Long): Unit = {
    _writerTask = t
    checkInvariants()
  }
  private[this] var _writerTask: Long = BlockInfo.NO_WRITER

  private def checkInvariants(): Unit = {
    // A block's reader count must be non-negative:
    assert(_readerCount >= 0)
    // A block is either locked for reading or for writing, but not for both at the same time:
    assert(_readerCount == 0 || _writerTask == BlockInfo.NO_WRITER)
  }

  checkInvariants()
}

private[storage] object BlockInfo {

  /**
   * Special task attempt id constant used to mark a block's write lock as being unlocked.
   */
  val NO_WRITER: Long = -1

  /**
   * Special task attempt id constant used to mark a block's write lock as being held by
   * a non-task thread (e.g. by a driver thread or by unit test code).
   */
  val NON_TASK_WRITER: Long = -1024
}

/**
 * BlockManager的内部组件,用于跟踪block的元信息并且控制block的锁管理.读锁是共享锁,写锁是排他锁.
  * 此类公开的锁定接口是读写器锁定. 每次锁定获取都会自动与正在运行的任务相关联,
  * 并且在任务完成或失败时会自动释放锁定.<br>
  * Component of the [[BlockManager]] which tracks metadata for blocks and manages block locking.
 *
 * The locking interface exposed by this class is readers-writer lock. Every lock acquisition is
 * automatically associated with a running task and locks are automatically released upon task
 * completion or failure.
 *
 * This class is thread-safe.
 */
private[storage] class BlockInfoManager extends Logging {

  private type TaskAttemptId = Long

  /**
   * 用来查询单个block,是BlockId和BlockInfo的映射.
    * 键值对通过set-if-not-exists原子操作lockNewBlockForWriting添加,通过removeBlock删除.
    * Used to look up metadata for individual blocks. Entries are added to this map via an atomic
   * set-if-not-exists operation ([[lockNewBlockForWriting()]]) and are removed
   * by [[removeBlock()]].
   */
  @GuardedBy("this")
  private[this] val infos = new mutable.HashMap[BlockId, BlockInfo]

  /**
   * 每次task执行尝试的标识TaskAttemptId,与执行获取的Block的写锁之间的映射关系.
    * TaskAttemptId与写锁之间是1对多的关系,即一次任务尝试执行会获取0到多个Block写锁.
    * Tracks the set of blocks that each task has locked for writing.
   */
  @GuardedBy("this")
  private[this] val writeLocksByTask =
    new mutable.HashMap[TaskAttemptId, mutable.Set[BlockId]]
      with mutable.MultiMap[TaskAttemptId, BlockId]

  /**
   * 跟踪每个任务已锁定以供读取的block集合,以及block被锁定的次数(因为读取锁是可重入的).
    * Tracks the set of blocks that each task has locked for reading, along with the number of times
   * that a block has been locked (since our read locks are re-entrant).
   */
  @GuardedBy("this")
  private[this] val readLocksByTask =
    new mutable.HashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]]

  // ----------------------------------------------------------------------------------------------

  // Initialization for special task attempt ids:
  registerTask(BlockInfo.NON_TASK_WRITER)

  // ----------------------------------------------------------------------------------------------

  /**
   * task开始时调用,用以在本BlockInfoManager中注册task.
    * 必须在从该任务调用任何其他BlockInfoManager方法之前调用此方法.
    * Called at the start of a task in order to register that task with this [[BlockInfoManager]].
   * This must be called prior to calling any other BlockInfoManager methods from that task.
   */
  def registerTask(taskAttemptId: TaskAttemptId): Unit = synchronized {
    require(!readLocksByTask.contains(taskAttemptId),
      s"Task attempt $taskAttemptId is already registered")
    readLocksByTask(taskAttemptId) = ConcurrentHashMultiset.create()
  }

  /**
   * 返回当前task的任务尝试id,如果TaskContext中没有TaskAttemptId,返回BlockInfo.NON_TASK_WRITER.
    * Returns the current task's task attempt id (which uniquely identifies the task), or
   * [[BlockInfo.NON_TASK_WRITER]] if called by a non-task thread.
   */
  private def currentTaskAttemptId: TaskAttemptId = {
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(BlockInfo.NON_TASK_WRITER)
  }

  /**
   * 锁定正在读取的block并且返回元信息.如果其他task读取时已经锁定了该block,lock计数会增加,任务也能执行.
    * 如果其他task写入时锁定了该block并且blocking属性 = false,那么调用会阻塞到写入完成释放锁.
    * 单个任务可以多次锁住block的读锁,每个锁需要单独释放.
    * Lock a block for reading and return its metadata.
   *
   * If another task has already locked this block for reading, then the read lock will be
   * immediately granted to the calling task and its lock count will be incremented.
   *
   * If another task has locked this block for writing, then this call will block until the write
   * lock is released or will return immediately if `blocking = false`.
   *
   * A single task can lock a block multiple times for reading, in which case each lock will need
   * to be released separately.
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for reading).
   */
  def lockForReading(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire read lock for $blockId")
    do {
      // 从缓存中获取BlockId对应的BlockInfo.
      infos.get(blockId) match {
        case None => return None
        case Some(info) =>
          if (info.writerTask == BlockInfo.NO_WRITER) {
            // 如果写锁没被其他任务尝试线程占用,则由当前线程持有读锁.
            info.readerCount += 1  // 计数加一
            readLocksByTask(currentTaskAttemptId).add(blockId)
            logTrace(s"Task $currentTaskAttemptId acquired read lock for $blockId")
            // 返回info
            return Some(info)
          }
      }
      if (blocking) {
        // 如果blocking为true,则当前线程等待,知道写锁释线程释放Block写锁唤醒当前线程
        // 如果一直不施放写锁,那就一直等
        wait()
      }
    } while (blocking)
    None
  }

  /**
   * 锁住block的写锁并返回其元信息.如果另一个task已经锁定了写锁,
    * 并且blocking属性 = false,那么调用会阻塞到写入完成释放锁.
    * Lock a block for writing and return its metadata.
   *
   * If another task has already locked this block for either reading or writing, then this call
   * will block until the other locks are released or will return immediately if `blocking = false`.
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for writing).
   */
  def lockForWriting(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire write lock for $blockId")
    do {
      // 从缓存中获取BlockId对应的BlockInfo.
      infos.get(blockId) match {
        case None => return None
        case Some(info) =>
          if (info.writerTask == BlockInfo.NO_WRITER && info.readerCount == 0) {
            // 如果写锁没被其他任务尝试线程占用并且读锁计数=0,则由当前线程持有读锁.
            info.writerTask = currentTaskAttemptId
            writeLocksByTask.addBinding(currentTaskAttemptId, blockId)
            logTrace(s"Task $currentTaskAttemptId acquired write lock for $blockId")
            return Some(info)  // 返回BlockInfo
          }
      }
      if (blocking) {
        // 阻塞
        wait()
      }
    } while (blocking)
    None
  }

  /**
   * Throws an exception if the current task does not hold a write lock on the given block.
   * Otherwise, returns the block's BlockInfo.
   */
  def assertBlockIsLockedForWriting(blockId: BlockId): BlockInfo = synchronized {
    infos.get(blockId) match {
      case Some(info) =>
        if (info.writerTask != currentTaskAttemptId) {
          throw new SparkException(
            s"Task $currentTaskAttemptId has not locked block $blockId for writing")
        } else {
          info
        }
      case None =>
        throw new SparkException(s"Block $blockId does not exist")
    }
  }

  /**
   * 获取BlockId对应的BlockInfo并且不需要锁定.该方法只供BlockManager.getStatus()使用,并且不应该在本类之外调用.
    * Get a block's metadata without acquiring any locks. This method is only exposed for use by
   * [[BlockManager.getStatus()]] and should not be called by other code outside of this class.
   */
  private[storage] def get(blockId: BlockId): Option[BlockInfo] = synchronized {
    infos.get(blockId)
  }

  /**
   * 降级对共享读锁定的独占写锁定.
    * Downgrades an exclusive write lock to a shared read lock.
   */
  def downgradeLock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId downgrading write lock for $blockId")
    // 获取BlockId对应的BlockInfo
    val info = get(blockId).get
    require(info.writerTask == currentTaskAttemptId,
      s"Task $currentTaskAttemptId tried to downgrade a write lock that it does not hold on" +
        s" block $blockId")
    // 调用unlock释放当前任务尝试线程从获取的写锁.
    unlock(blockId)
    // 获取读锁.
    val lockOutcome = lockForReading(blockId, blocking = false)
    assert(lockOutcome.isDefined)
  }

  /**
   * 释放blockId对应的block上的锁.如果TaskContext未正确传播到任务的所有子线程，
    * 我们无法从TaskContext获取TID，因此我们必须显式传递TID值以释放锁。
    * Release a lock on the given block.
   * In case a TaskContext is not propagated properly to all child threads for the task, we fail to
   * get the TID from TaskContext, so we have to explicitly pass the TID value to release the lock.
   *
   * See SPARK-18406 for more discussion of this issue.
   */
  def unlock(blockId: BlockId, taskAttemptId: Option[TaskAttemptId] = None): Unit = synchronized {
    // 获取taskAttemptId
    val taskId = taskAttemptId.getOrElse(currentTaskAttemptId)
    logTrace(s"Task $taskId releasing lock for $blockId")
    // 获取BlockId对应的BlockInfo
    val info = get(blockId).getOrElse {
      throw new IllegalStateException(s"Block $blockId not found")
    }
    if (info.writerTask != BlockInfo.NO_WRITER) {
      // 如果当前任务获得了Block写锁,释放写锁
      info.writerTask = BlockInfo.NO_WRITER
      writeLocksByTask.removeBinding(taskId, blockId)
    } else {
      // 如果没有获得写锁,释放当前任务读锁.减少读锁计数.
      assert(info.readerCount > 0, s"Block $blockId is not locked for reading")
      info.readerCount -= 1
      val countsForTask = readLocksByTask(taskId)
      val newPinCountForTask: Int = countsForTask.remove(blockId, 1) - 1
      assert(newPinCountForTask >= 0,
        s"Task $taskId release lock on block $blockId more times than it acquired it")
    }
    // 唤醒wait线程.
    notifyAll()
  }

  /**
   * 尝试为写新block操作获取合适的锁.这里施行了先写先得语义.如果我们首先对block写入,
    * 可以继续写并且锁是归我们所有.如果别的线程先写,我们在获得读锁前需要等待其写入完成.
    * Attempt to acquire the appropriate lock for writing a new block.
   * This enforces the first-writer-wins semantics. If we are the first to write the block,
   * then just go ahead and acquire the write lock. Otherwise, if another thread is already
   * writing the block, then we wait for the write to finish before acquiring the read lock.
   *
   * @return true if the block did not already exist, false otherwise. If this returns false, then
   *         a read lock on the existing block will be held. If this returns true, a write lock on
   *         the new block will be held.
   */
  def lockNewBlockForWriting(
      blockId: BlockId,
      newBlockInfo: BlockInfo): Boolean = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to put $blockId")
    // 获取读锁
    lockForReading(blockId) match {
      case Some(info) =>
        // 说明BlockId对应的Block已经存在,这种情况说明已有线程取得先机,当前线程没必要继续获得写锁写入数据
        // 直接返回false即可.
        // Block already exists. This could happen if another thread races with us to compute
        // the same block. In this case, just keep the read lock and return.
        false
      case None =>
        // 如果没有获得读锁,说明还不存在Block,那就向BlockId和新的BlockInfo放入infos,
        // 然后获取Block对应的写锁,返回true.
        // Block does not yet exist or is removed, so we are free to acquire the write lock
        infos(blockId) = newBlockInfo
        lockForWriting(blockId)
        true
    }
  }

  /**
   * 释放指定task持有的所有锁,并通通知所有等待获取锁的线程.
    * Release all lock held by the given task, clearing that task's pin bookkeeping
   * structures and updating the global pin counts. This method should be called at the
   * end of a task (either by a task completion handler or in `TaskRunner.run()`).
   *
   * @return the ids of blocks whose pins were released
   */
  def releaseAllLocksForTask(taskAttemptId: TaskAttemptId): Seq[BlockId] = synchronized {
    val blocksWithReleasedLocks = mutable.ArrayBuffer[BlockId]()

    val readLocks = readLocksByTask.remove(taskAttemptId).getOrElse(ImmutableMultiset.of[BlockId]())
    val writeLocks = writeLocksByTask.remove(taskAttemptId).getOrElse(Seq.empty)

    for (blockId <- writeLocks) {
      infos.get(blockId).foreach { info =>
        assert(info.writerTask == taskAttemptId)
        info.writerTask = BlockInfo.NO_WRITER
      }
      blocksWithReleasedLocks += blockId
    }

    readLocks.entrySet().iterator().asScala.foreach { entry =>
      val blockId = entry.getElement
      val lockCount = entry.getCount
      blocksWithReleasedLocks += blockId
      get(blockId).foreach { info =>
        info.readerCount -= lockCount
        assert(info.readerCount >= 0)
      }
    }

    notifyAll()

    blocksWithReleasedLocks
  }

  /** Returns the number of locks held by the given task.  Used only for testing. */
  private[storage] def getTaskLockCount(taskAttemptId: TaskAttemptId): Int = {
    readLocksByTask.get(taskAttemptId).map(_.size()).getOrElse(0) +
      writeLocksByTask.get(taskAttemptId).map(_.size).getOrElse(0)
  }

  /**
   * 返回跟踪的blocks数量.
    * Returns the number of blocks tracked.
   */
  def size: Int = synchronized {
    infos.size
  }

  /**
   * Return the number of map entries in this pin counter's internal data structures.
   * This is used in unit tests in order to detect memory leaks.
   */
  private[storage] def getNumberOfMapEntries: Long = synchronized {
    size +
      readLocksByTask.size +
      readLocksByTask.map(_._2.size()).sum +
      writeLocksByTask.size +
      writeLocksByTask.map(_._2.size).sum
  }

  /**
   * 返回所有Block元信息的快照的迭代器.请注意，此迭代器中的各个键值对是可变的，
    * 因此可能反映在遍历迭代器时删除的block。
    * Returns an iterator over a snapshot of all blocks' metadata. Note that the individual entries
   * in this iterator are mutable and thus may reflect blocks that are deleted while the iterator
   * is being traversed.
   */
  def entries: Iterator[(BlockId, BlockInfo)] = synchronized {
    infos.toArray.toIterator
  }

  /**
   * 删除指定BlockId对应的BlockInfo.
    * Removes the given block and releases the write lock on it.
   *
   * This can only be called while holding a write lock on the given block.
   */
  def removeBlock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to remove block $blockId")
    // 获取blockInfo
    infos.get(blockId) match {
      case Some(blockInfo) =>
        // 如果正在写入的任务尝试线程不是当前线程,抛异常
        if (blockInfo.writerTask != currentTaskAttemptId) {
          throw new IllegalStateException(
            s"Task $currentTaskAttemptId called remove() on block $blockId without a write lock")
        } else {
          // 移除
          infos.remove(blockId)
          // 读线程数清零
          blockInfo.readerCount = 0
          // writerTask设置为NO_WRITER
          blockInfo.writerTask = BlockInfo.NO_WRITER
          // 清除任务尝试线程与BlockId的关系
          writeLocksByTask.removeBinding(currentTaskAttemptId, blockId)
        }
      case None =>
        throw new IllegalArgumentException(
          s"Task $currentTaskAttemptId called remove() on non-existent block $blockId")
    }
    // 唤醒其他所有等待线程
    notifyAll()
  }

  /**
   * Delete all state. Called during shutdown.
   */
  def clear(): Unit = synchronized {
    infos.valuesIterator.foreach { blockInfo =>
      blockInfo.readerCount = 0
      blockInfo.writerTask = BlockInfo.NO_WRITER
    }
    infos.clear()
    readLocksByTask.clear()
    writeLocksByTask.clear()
    notifyAll()
  }

}

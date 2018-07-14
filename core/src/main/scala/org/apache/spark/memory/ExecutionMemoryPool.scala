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

import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * 以便在任务之间共享可调大小的内存池。 尝试确保每个任务获得合理的内存份额，
  * 而不是首先增加大量任务然后导致其他任务重复溢出到磁盘。 如果有N个任务，
  * 它确保每个任务在泄漏之前至少可以获取1 / 2N的内存，最多1 / N.因为N动态变化，
  * 我们会跟踪活动任务的集合并重做 每当该组改变时，等待任务中的1 / 2N和1 / N的计算。
  * 这一切都是通过同步对可变状态的访问并使用wait（）和notifyAll（）来向调用者发出更改信号来完成的。
  * 在Spark 1.6之前，跨任务的内存仲裁由ShuffleMemoryManager执行。
  * Implements policies and bookkeeping for sharing an adjustable-sized pool of memory between tasks.
 *
 * Tries to ensure that each task gets a reasonable share of memory, instead of some task ramping up
 * to a large amount first and then causing others to spill to disk repeatedly.
 *
 * If there are N tasks, it ensures that each task can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever this
 * set changes. This is all done by synchronizing access to mutable state and using wait() and
 * notifyAll() to signal changes to callers. Prior to Spark 1.6, this arbitration of memory across
 * tasks was performed by the ShuffleMemoryManager.
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
 */
private[memory] class ExecutionMemoryPool(
    lock: Object,
    memoryMode: MemoryMode  // 内存模式.堆内,堆外两种
  ) extends MemoryPool(lock) with Logging {
  /** 内存池名称,ON_HEAP => "on-heap execution",OFF_HEAP => "off-heap execution"*/
  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap execution"
    case MemoryMode.OFF_HEAP => "off-heap execution"
  }

  /**
   * 任务尝试id和内存消费大小的映射缓存,单位byte
    * Map from taskAttemptId -> memory consumption in bytes
   */
  @GuardedBy("lock")
  private val memoryForTask = new mutable.HashMap[Long, Long]()
  /**
    * 已经使用的内存大小.
    * */
  override def memoryUsed: Long = lock.synchronized {
    memoryForTask.values.sum
  }

  /**
   * 获取尝试任务使用的内存大小.
    * Returns the memory consumption, in bytes, for the given task.
   */
  def getMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    memoryForTask.getOrElse(taskAttemptId, 0L)
  }

  /**
   * 用于给尝试任务获取指定的(numBytes)内存大小.该方法在某些情况下会阻塞直到有足够的内存.
    * 假设有N个线程,必须保证每个线程在spill(内存不足写入硬盘)之前获得1/2N的内存,并且每个线程最多获取
    * 1/N的内存.由于N是动态变化的变量,所以要持续对这些线程追踪,以便变化时,重新按照1/2N或1/N计算.
    * Try to acquire up to `numBytes` of memory for the given task and return the number of bytes
   * obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   *
   * @param numBytes number of bytes to acquire
   * @param taskAttemptId the task attempt acquiring memory
   * @param maybeGrowPool a callback that potentially grows the size of this pool. It takes in
   *                      one parameter (Long) that represents the desired amount of memory by
   *                      which this pool should be expanded.
   * @param computeMaxPoolSize a callback that returns the maximum allowable size of this pool
   *                           at this given moment. This is not a field because the max pool
   *                           size is variable in certain cases. For instance, in unified
   *                           memory management, the execution pool can be expanded by evicting
   *                           cached blocks, thereby shrinking the storage pool.
   *
   * @return the number of bytes granted to the task.
   */
  private[memory] def acquireMemory(
      numBytes: Long,
      taskAttemptId: Long,
      maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => Unit,
      computeMaxPoolSize: () => Long = () => poolSize): Long = lock.synchronized {
    assert(numBytes > 0, s"invalid number of bytes requested: $numBytes")

    // TODO: clean up this clunky method signature

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
    // 如果不包含taskAttemptId
    if (!memoryForTask.contains(taskAttemptId)) {
      // 放入缓存
      memoryForTask(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      // 唤醒其他等待获取ExecutionMemoryPool的锁的线程
      lock.notifyAll()
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // TODO: simplify this to limit each task to its own slot
    while (true) {
      // 获取当前激活task数量
      val numActiveTasks = memoryForTask.keys.size
      // 获取当前尝试task所消费的内存
      val curMem = memoryForTask(taskAttemptId)

      // In every iteration of this loop, we should first try to reclaim any borrowed execution
      // space from storage. This is necessary because of the potential race condition where new
      // storage blocks may steal the free execution memory that this task was waiting for.
      // 从StorageMemoryPool中回收或借用内存,有可能会驱赶StorageMemoryPool中的block
      maybeGrowPool(numBytes - memoryFree)

      // Maximum size the pool would have after potentially growing the pool.
      // This is used to compute the upper bound of how much memory each task can occupy. This
      // must take into account potential free memory as well as the amount this pool currently
      // occupies. Otherwise, we may run into SPARK-12155 where, in unified memory management,
      // we did not take into account space that could have been freed by evicting cached blocks.
      // 计算内存池的最大大小
      val maxPoolSize = computeMaxPoolSize()
      // 计算每个任务尝试最大可使用内存
      val maxMemoryPerTask = maxPoolSize / numActiveTasks
      // 计算每个任务尝试最小保证使用的内存大小
      val minMemoryPerTask = poolSize / (2 * numActiveTasks)
      // How much we can grant this task; keep its share within 0 <= X <= 1 / numActiveTasks
      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))
      // Only give it as much memory as is free, which might be none if it reached 1 / numTasks
      // 计算当前任务尝试真正可以申请获取的内存大小
      val toGrant = math.min(maxToGrant, memoryFree)

      // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
      // if we can't give it this much now, wait for other tasks to free up memory
      // (this happens if older tasks allocated lots of memory before N grew)
      // 如果每个任务尝试最基本的大小都得不到满足的情况下
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        logInfo(s"TID $taskAttemptId waiting for at least 1/2N of $poolName pool to be free")
        // 内存不足,线程等待
        lock.wait()
      } else {
        memoryForTask(taskAttemptId) += toGrant
        return toGrant
      }
    }
    0L  // Never reached
  }

  /**
   * 给指定任务尝试释放指定大小内存
    * Release `numBytes` of memory acquired by the given task.
   */
  def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    // 获取当前任务消费内存
    val curMem = memoryForTask.getOrElse(taskAttemptId, 0L)
    // 计算能够释放内存大小
    var memoryToFree = if (curMem < numBytes) {
      logWarning(
        s"Internal error: release called on $numBytes bytes but task only has $curMem bytes " +
          s"of memory from the $poolName pool")
      // 如果当前任务消费内存小于numBytes,那么能够释放curMem大小内存
      curMem
    } else {
      // 否则,释放numbytes那么多内存
      numBytes
    }
    // 同步缓存,逻辑上释放内存
    if (memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) -= memoryToFree
      if (memoryForTask(taskAttemptId) <= 0) {
        memoryForTask.remove(taskAttemptId)
      }
    }
    // 唤醒所有申请获得内存但处于等待状态的线程
    lock.notifyAll() // Notify waiters in acquireMemory() that memory has been freed
  }

  /**
   * 释放taskAttemptId对应任务所消费的所有内存
    * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * @return the number of bytes freed.
   */
  def releaseAllMemoryForTask(taskAttemptId: Long): Long = lock.synchronized {
    val numBytesToFree = getMemoryUsageForTask(taskAttemptId)
    releaseMemory(numBytesToFree, taskAttemptId)
    numBytesToFree
  }

}

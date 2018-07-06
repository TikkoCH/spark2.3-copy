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

/**
 *  管理可调大小的内存区域的对象.该类是MemoryManager的内部类.
  * Manages bookkeeping for an adjustable-sized region of memory. This class is internal to
 * the [[MemoryManager]]. See subclasses for more details.
 *
 * @param lock 一个MemoryManager实例，用于同步.我们故意将类型擦除为object以避免编程错误，
  *             因为此对象应仅用于同步目的。
  *             a [[MemoryManager]] instance, used for synchronization. We purposely erase the type
 *             to `Object` to avoid programming errors, since this object should only be used for
 *             synchronization purposes.
 */
private[memory] abstract class MemoryPool(lock: Object) {
  /** 内存池大小,单位字节Byte*/
  @GuardedBy("lock")
  private[this] var _poolSize: Long = 0

  /**
   * get方法,返回_poolSize
    * Returns the current size of the pool, in bytes.
   */
  final def poolSize: Long = lock.synchronized {
    _poolSize
  }

  /**
   * 获取内存池的空闲空间.
    * Returns the amount of free memory in the pool, in bytes.
   */
  final def memoryFree: Long = lock.synchronized {
    _poolSize - memoryUsed
  }

  /**
   * 扩展内存池容量,参数delta为扩展大小.
    * Expands the pool by `delta` bytes.
   */
  final def incrementPoolSize(delta: Long): Unit = lock.synchronized {
    require(delta >= 0)
    _poolSize += delta
  }

  /**
   * 缩小内存池容量,参数delta为缩小字节数
    * Shrinks the pool by `delta` bytes.
   */
  final def decrementPoolSize(delta: Long): Unit = lock.synchronized {
    require(delta >= 0)
    require(delta <= _poolSize)
    require(_poolSize - delta >= memoryUsed)
    _poolSize -= delta
  }

  /**
   * 获取使用的内存大小,单位byte
    * Returns the amount of used memory in this pool (in bytes).
   */
  def memoryUsed: Long
}

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

package org.apache.spark.unsafe.memory;

public interface MemoryAllocator {

  /**
   * 是否分别用0xa5和0x5a字节填充新分配和释放的内存。 这有助于捕获对未初始化或释放的内存的滥用，但会产生一些开销。
   * Whether to fill newly allocated and deallocated memory with 0xa5 and 0x5a bytes respectively.
   * This helps catch misuse of uninitialized or freed memory, but imposes some overhead.
   */
  boolean MEMORY_DEBUG_FILL_ENABLED = Boolean.parseBoolean(
    System.getProperty("spark.memory.debugFill", "false"));

  // Same as jemalloc's debug fill values.
  byte MEMORY_DEBUG_FILL_CLEAN_VALUE = (byte)0xa5;
  byte MEMORY_DEBUG_FILL_FREED_VALUE = (byte)0x5a;

  /**
   * 分配指定大小的连续内存块,按照这种方式分配的内存不能保证清零,如果需要调用MemoryLocak上的fill(0)
   * Allocates a contiguous block of memory. Note that the allocated memory is not guaranteed
   * to be zeroed out (call `fill(0)` on the result if this is necessary).
   */
  MemoryBlock allocate(long size) throws OutOfMemoryError;
  /**
   * 释放内存块
   * */
  void free(MemoryBlock memory);

  MemoryAllocator UNSAFE = new UnsafeMemoryAllocator();

  MemoryAllocator HEAP = new HeapMemoryAllocator();
}

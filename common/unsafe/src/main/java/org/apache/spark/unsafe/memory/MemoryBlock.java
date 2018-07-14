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

import javax.annotation.Nullable;

import org.apache.spark.unsafe.Platform;

/**
 * 一个连续的内存块,从具有固定大小的memorylocation开始,
 * A consecutive block of memory, starting at a {@link MemoryLocation} with a fixed size.
 */
public class MemoryBlock extends MemoryLocation {

  /**
   * 首先理解一下page,操作系统中page是一个内存块,操作系统读取数据,往往先确定page,然后根据page的偏移量
   * 和所读取数据的长度从page中读取数据.<br>
   * 这是一个特殊的pageNum,不是被TaskMemoryManagers分配的就是该值.
   * Special `pageNumber` value for pages which were not allocated by TaskMemoryManagers */
  public static final int NO_PAGE_NUMBER = -1;

  /**
   * 特殊的`pageNumber`值，用于标记已在TaskMemoryManager中释放的页面。
   * 我们在TaskMemoryManager.freePage（）中将`pageNumber`设置为此值，
   * 以便MemoryAllocator可以检测在传递给MemoryAllocator.free（）
   * 之前是否已在TMM中释放由TaskMemoryManager分配的页面（分配页面时出错）
   * 在TaskMemoryManager中然后直接在MemoryAllocator中释放它而不通过TMM freePage（）调用）
   * Special `pageNumber` value for marking pages that have been freed in the TaskMemoryManager.
   * We set `pageNumber` to this value in TaskMemoryManager.freePage() so that MemoryAllocator
   * can detect if pages which were allocated by TaskMemoryManager have been freed in the TMM
   * before being passed to MemoryAllocator.free() (it is an error to allocate a page in
   * TaskMemoryManager and then directly free it in a MemoryAllocator without going through
   * the TMM freePage() call).
   */
  public static final int FREED_IN_TMM_PAGE_NUMBER = -2;

  /**
   * MemoryAllocator释放的页面的特殊“pageNumber”值。 这允许我们检测双重释放。
   * Special `pageNumber` value for pages that have been freed by the MemoryAllocator. This allows
   * us to detect double-frees.
   */
  public static final int FREED_IN_ALLOCATOR_PAGE_NUMBER = -3;
  /** 当前MemoryBlock的连续内存块长度
   * */
  private final long length;

  /**
   * 当前MemoryBlock的页码(pageNumber).TaskMemoryManager分配由MemoryBlock标识Page时,使用该属性.
   * Optional page number; used when this MemoryBlock represents a page allocated by a
   * TaskMemoryManager. This field is public so that it can be modified by the TaskMemoryManager,
   * which lives in a different package.
   */
  public int pageNumber = NO_PAGE_NUMBER;

  public MemoryBlock(@Nullable Object obj, long offset, long length) {
    super(obj, offset);
    this.length = length;
  }

  /**
   * 返回memoryblock的大小
   * Returns the size of the memory block.
   */
  public long size() {
    return length;
  }

  /**
   * 创建指向由长整形数组使用的内存MemoryBlock.
   * Creates a memory block pointing to the memory used by the long array.
   */
  public static MemoryBlock fromLongArray(final long[] array) {
    return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, array.length * 8L);
  }

  /**
   * 以指定字节填充整个MemoryBlock,即将obj对象从offset开始,长度为length的堆内存
   * 替换为指定字节的值.下面那个Platform封装了sun.misc.Unsafe的api.
   * Fills the memory block with the specified byte value.
   */
  public void fill(byte value) {
    Platform.setMemory(obj, offset, length, value);
  }
}

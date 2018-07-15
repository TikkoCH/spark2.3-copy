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

package org.apache.spark.memory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * 用于管理单个任务尝试的内存分配与释放.TaskMemoryManager实际上依赖MemoryManager提供的内存管理能力,
 * 多个TaskMemoryManager将共享memoryManager所管理的内存.一次任务尝试有很多组件需要使用内存,这些组件
 * 都借助于TaskMemoryManager提供的服务对实际的物理内存进行消费,他们统称为内存消费者.
 * Manages the memory allocated by an individual task.
 * <p>
 * Most of the complexity in this class deals with encoding of off-heap addresses into 64-bit longs.
 * In off-heap mode, memory can be directly addressed with 64-bit longs. In on-heap mode, memory is
 * addressed by the combination of a base Object reference and a 64-bit offset within that object.
 * This is a problem when we want to store pointers to data structures inside of other structures,
 * such as record pointers inside hashmaps or sorting buffers. Even if we decided to use 128 bits
 * to address memory, we can't just store the address of the base object since it's not guaranteed
 * to remain stable as the heap gets reorganized due to GC.
 * <p>
 * Instead, we use the following approach to encode record pointers in 64-bit longs: for off-heap
 * mode, just store the raw address, and for on-heap mode use the upper 13 bits of the address to
 * store a "page number" and the lower 51 bits to store an offset within this page. These page
 * numbers are used to index into a "page table" array inside of the MemoryManager in order to
 * retrieve the base object.
 * <p>
 * This allows us to address 8192 pages. In on-heap mode, the maximum page size is limited by the
 * maximum size of a long[] array, allowing us to address 8192 * (2^31 - 1) * 8 bytes, which is
 * approximately 140 terabytes of memory.
 */
public class TaskMemoryManager {

  private static final Logger logger = LoggerFactory.getLogger(TaskMemoryManager.class);

  /**
   * 用于寻址Page的位数.在64位长整形中将使用高位的13位存储页号
   * The number of bits used to address the page table. */
  private static final int PAGE_NUMBER_BITS = 13;

  /**
   * 用于保存编码后的偏移量的位数.64位长整形中使用低位的51位存储偏移量
   * The number of bits used to encode offsets in data pages. */
  @VisibleForTesting
  static final int OFFSET_BITS = 64 - PAGE_NUMBER_BITS;  // 51

  /**
   * Page表中的Page数量.1左移13位,8192
   * The number of entries in the page table. */
  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

  /**
   * 最大的page大小.(2^32-1)*8.原则上来讲,最大可分配page大小是2pb多.堆上内存分配器的最大page大小被可存储
   * 进Long数组中数据的总量2^32-1限制了,所以,最大值是2^32-1.
   * Maximum supported data page size (in bytes). In principle, the maximum addressable page size is
   * (1L &lt;&lt; OFFSET_BITS) bytes, which is 2+ petabytes. However, the on-heap allocator's
   * maximum page size is limited by the maximum amount of data that can be stored in a long[]
   * array, which is (2^31 - 1) * 8 bytes (or about 17 gigabytes). Therefore, we cap this at 17
   * gigabytes.
   */
  public static final long MAXIMUM_PAGE_SIZE_BYTES = ((1L << 31) - 1) * 8L;

  /**
   * 长整型的低51位的位掩码
   * Bit mask for the lower 51 bits of a long. */
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  /**
   * Page表.市纪委Page的数组,数组长度为PAGE_TABLE_SIZE.
   * 与操作系统的页表类似，此数组将页码映射到基础对象指针，允许我们在哈希表的内部64位地址表示
   * 和baseObject +偏移表示之间进行转换，我们使用它来支持堆内和堆外地址。 使用堆外分配器时，
   * 此映射中的每个条目都将为“null”。 使用堆内分配器时，此映射中的条目将指向页面的基础对象。
   * 在分配新数据页时，会将条目添加到此映射中。
   * Similar to an operating system's page table, this array maps page numbers into base object
   * pointers, allowing us to translate between the hashtable's internal 64-bit address
   * representation and the baseObject+offset representation which we use to support both in- and
   * off-heap addresses. When using an off-heap allocator, every entry in this map will be `null`.
   * When using an in-heap allocator, the entries in this map will point to pages' base objects.
   * Entries are added to this map as new data pages are allocated.
   */
  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

  /**
   * 用于跟踪空闲Page的BitSet
   * Bitmap for tracking free pages.
   */
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

  private final MemoryManager memoryManager;
  /** TaskMemoryManager锁管理任务尝试的id*/
  private final long taskAttemptId;

  /**
   * Tugsten内存模式,TaskMemoryManager的getTungstenMemoryMode方法专门用于返回tungstenMemoryMode的值.
   * 跟踪我们是在堆中还是在堆外。 对于堆外，我们将大多数这些方法短路而不进行任何屏蔽或查找。
   * 由于这个分支应该由JIT很好地预测，所以这个额外的间接/抽象层希望不应该太昂贵。
   * Tracks whether we're in-heap or off-heap. For off-heap, we short-circuit most of these methods
   * without doing any masking or lookups. Since this branching should be well-predicted by the JIT,
   * this extra layer of indirection / abstraction hopefully shouldn't be too expensive.
   */
  final MemoryMode tungstenMemoryMode;

  /**
   * 用于跟踪可溢出的MemoryConsumer
   * Tracks spillable memory consumers.
   */
  @GuardedBy("this")
  private final HashSet<MemoryConsumer> consumers;

  /**
   * 任务尝试已经获得但并未使用的内存大小
   * The amount of memory that is acquired but not used.
   */
  private volatile long acquiredButNotUsed = 0L;

  /**
   * Construct a new TaskMemoryManager.
   */
  public TaskMemoryManager(MemoryManager memoryManager, long taskAttemptId) {
    this.tungstenMemoryMode = memoryManager.tungstenMemoryMode();
    this.memoryManager = memoryManager;
    this.taskAttemptId = taskAttemptId;
    this.consumers = new HashSet<>();
  }

  /**
   * 获取内存消费者指定大小的内存.当Task没有足够内存时,调用MemoryConsumer的spill方法释放内存.
   * Acquire N bytes of memory for a consumer. If there is no enough memory, it will call
   * spill() of consumers to release more memory.
   *
   * @return number of bytes successfully granted (<= N).
   */
  public long acquireExecutionMemory(long required, MemoryConsumer consumer) {
    assert(required >= 0);
    assert(consumer != null);
    // 内存消费者的模式
    MemoryMode mode = consumer.getMode();
    // 如果我们在堆外分配Tungsten页面并在此处接收分配堆内存的请求，那么溢出可能没有意义，
    // 因为这样只能最终释放堆外内存。 但是，这可能会发生变化，因此，
    // 如果我们在进行更改时忘记稍后撤消它，现在进行此优化可能会有风险。
    // If we are allocating Tungsten pages off-heap and receive a request to allocate on-heap
    // memory here, then it may not make sense to spill since that would only end up freeing
    // off-heap memory. This is subject to change, though, so it may be risky to make this
    // optimization now in case we forget to undo it late when making changes.
    synchronized (this) {
      // 为当前的任务尝试按照指定的存储模式获取指定大小的内存
      long got = memoryManager.acquireExecutionMemory(required, taskAttemptId, mode);
      // 首先从消费者中释放内存,然后我们可以减少spill的频率,避免有太多溢出文件.
      // Try to release memory from other consumers first, then we can reduce the frequency of
      // spilling, avoid to have too many spilled files.
      // 如果获得的小于需求的
      if (got < required) {
        // 调用消费者上的spill来释放内存,就是写到磁盘上.根据消费者内存的使用排序.
        // 因此，我们避免溢出最后几次溢出的同一消费者，并且重新溢出它将产生许多小的溢出文件。
        // Call spill() on other consumers to release memory
        // Sort the consumers according their memory usage. So we avoid spilling the same consumer
        // which is just spilled in last few times and re-spilling on it will produce many small
        // spill files.
        TreeMap<Long, List<MemoryConsumer>> sortedConsumers = new TreeMap<>();
        for (MemoryConsumer c: consumers) {
          if (c != consumer && c.getUsed() > 0 && c.getMode() == mode) {
            long key = c.getUsed();
            List<MemoryConsumer> list =
                sortedConsumers.computeIfAbsent(key, k -> new ArrayList<>(1));
            list.add(c);
          }
        }
        // 遍历排完序的消费者
        while (!sortedConsumers.isEmpty()) {
          // 使用最少的内存而不是剩余的所需内存来获取consumer.
          // Get the consumer using the least memory more than the remaining required memory.
          // ceilingEntry返回大于等于指定键的最近的一个键,没有返回null
          Map.Entry<Long, List<MemoryConsumer>> currentEntry =
            sortedConsumers.ceilingEntry(required - got);  // 需要的-已获得=还差多少
          // 没有消费者已经使用的内存比保留需要的内存
          // No consumer has used memory more than the remaining required memory.
          // Get the consumer of largest used memory.
          if (currentEntry == null) {
            // 如果为null,说明没有获取比required - got还大的值,那就取最后一个,那是最大的
            currentEntry = sortedConsumers.lastEntry();
          }
          // 拿到MemoryConsumer的列表
          List<MemoryConsumer> cList = currentEntry.getValue();
          // 删除并获得最后一个MemoryConsumer
          MemoryConsumer c = cList.remove(cList.size() - 1);
          if (cList.isEmpty()) {
            // 如果cList已经为空了,从排完序的sortedConsumers删除该key
            sortedConsumers.remove(currentEntry.getKey());
          }
          try {
            // 对上面拿到的MemoryConsumer溢出到磁盘
            long released = c.spill(required - got, consumer);
            // 如果释放的内存大于0
            if (released > 0) {
              logger.debug("Task {} released {} from {} for {}", taskAttemptId,
                Utils.bytesToString(released), c, consumer);
              // got(已获得)就变大了,获取执行内存
              got += memoryManager.acquireExecutionMemory(required - got, taskAttemptId, mode);
              // 如果获取大于需求,那可以跳出循环了,不然继续遍历sortedConsumers
              if (got >= required) {
                break;
              }
            }
          } catch (ClosedByInterruptException e) {
            // This called by user to kill a task (e.g: speculative task).
            logger.error("error while calling spill() on " + c, e);
            throw new RuntimeException(e.getMessage());
          } catch (IOException e) {
            logger.error("error while calling spill() on " + c, e);
            throw new SparkOutOfMemoryError("error while calling spill() on " + c + " : "
              + e.getMessage());
          }
        }
      }

      // call spill() on itself
      if (got < required) {
        // 到这说明已经遍历完了sortedConsumers,说明其中的元素都被spill了,
        // 并且没有got >= required,所以也没break循环.说明获取的空间还不够
        try {
          // 尝试对传进来的消费者进行溢出到磁盘,释放内存
          long released = consumer.spill(required - got, consumer);
          if (released > 0) {
            logger.debug("Task {} released {} from itself ({})", taskAttemptId,
              Utils.bytesToString(released), consumer);
            // 然后更新已获得的内存
            got += memoryManager.acquireExecutionMemory(required - got, taskAttemptId, mode);
          }
        } catch (ClosedByInterruptException e) {
          // This called by user to kill a task (e.g: speculative task).
          logger.error("error while calling spill() on " + consumer, e);
          throw new RuntimeException(e.getMessage());
        } catch (IOException e) {
          logger.error("error while calling spill() on " + consumer, e);
          throw new SparkOutOfMemoryError("error while calling spill() on " + consumer + " : "
            + e.getMessage());
        }
      }
      // 总的来说所有的MemoryConsumer都是我们的可压榨对象,不断跟踪不断在需要时候进行压榨.
      // 整个方法就是不断压榨MemoryConsumer的过程.
      // 缓存中添加consumer
      consumers.add(consumer);
      logger.debug("Task {} acquired {} for {}", taskAttemptId, Utils.bytesToString(got), consumer);
      // 返回已获得内存
      return got;
    }
  }

  /**
   * 为内存消费者释放指定大小的内存,单位byte
   * Release N bytes of execution memory for a MemoryConsumer.
   */
  public void releaseExecutionMemory(long size, MemoryConsumer consumer) {
    logger.debug("Task {} release {} from {}", taskAttemptId, Utils.bytesToString(size), consumer);
    // 实际调用memoryManager的releaseExecutionMemory
    memoryManager.releaseExecutionMemory(size, taskAttemptId, consumer.getMode());
  }

  /**
   * 所有consumer的内存使用情况
   * Dump the memory usage of all consumers.
   */
  public void showMemoryUsage() {
    logger.info("Memory used in task " + taskAttemptId);
    synchronized (this) {
      long memoryAccountedForByConsumers = 0;
      for (MemoryConsumer c: consumers) {
        long totalMemUsage = c.getUsed();
        memoryAccountedForByConsumers += totalMemUsage;
        if (totalMemUsage > 0) {
          logger.info("Acquired by " + c + ": " + Utils.bytesToString(totalMemUsage));
        }
      }
      long memoryNotAccountedFor =
        memoryManager.getExecutionMemoryUsageForTask(taskAttemptId) - memoryAccountedForByConsumers;
      logger.info(
        "{} bytes of memory were used by task {} but are not associated with specific consumers",
        memoryNotAccountedFor, taskAttemptId);
      logger.info(
        "{} bytes of memory are used for execution and {} bytes of memory are used for storage",
        memoryManager.executionMemoryUsed(), memoryManager.storageMemoryUsed());
    }
  }

  /**
   * 返回page的小,单位字节
   * Return the page size in bytes.
   */
  public long pageSizeBytes() {
    return memoryManager.pageSizeBytes();
  }

   /**
   * 用于给MemoryConsumer分配指定大小的memoryBlock.分配的内存块会在MemoryManager的pagetable中被追踪;
    * 这是为了分配将在运操作者之间共享的大块Tungsten内存。
   * Allocate a block of memory that will be tracked in the MemoryManager's page table; this is
   * intended for allocating large blocks of Tungsten memory that will be shared between operators.
   *
   * Returns `null` if there was not enough memory to allocate the page. May return a page that
   * contains fewer bytes than requested, so callers should verify the size of returned pages.
   *
   * @throws TooLargePageException
   */
  public MemoryBlock allocatePage(long size, MemoryConsumer consumer) {
    assert(consumer != null);
    assert(consumer.getMode() == tungstenMemoryMode);
    if (size > MAXIMUM_PAGE_SIZE_BYTES) { // 请求获得的page大小不能超过限制
      throw new TooLargePageException(size);
    }
    // 获取逻辑内存,如果获取小于等于0,返回null
    long acquired = acquireExecutionMemory(size, consumer);
    if (acquired <= 0) {
      return null;
    }

    final int pageNumber;
    synchronized (this) {
      // 获取还未分配的页码
      pageNumber = allocatedPages.nextClearBit(0);
      if (pageNumber >= PAGE_TABLE_SIZE) {
        releaseExecutionMemory(acquired, consumer);
        throw new IllegalStateException(
          "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
      }
      // 标记为已分配
      allocatedPages.set(pageNumber);
    }
    MemoryBlock page = null;
    try {
      // 获取tugsten采用的内存分配器分配内存
      page = memoryManager.tungstenMemoryAllocator().allocate(acquired);
    } catch (OutOfMemoryError e) {
      logger.warn("Failed to allocate a page ({} bytes), try again.", acquired);
      // there is no enough memory actually, it means the actual free memory is smaller than
      // MemoryManager thought, we should keep the acquired memory.
      synchronized (this) {
        // 更新缓存
        acquiredButNotUsed += acquired;
        allocatedPages.clear(pageNumber);
      }
      // 再次调用allocatePage,这可能触发溢出到硬盘的操作来释放一些内存page
      // this could trigger spilling to free some pages.
      return allocatePage(size, consumer);
    }
    // 给MemoryBlock指定页码,并存入pageTable中
    page.pageNumber = pageNumber;
    pageTable[pageNumber] = page;
    if (logger.isTraceEnabled()) {
      logger.trace("Allocate page number {} ({} bytes)", pageNumber, acquired);
    }
    // 返回MemoryBlock
    return page;
  }

  /**
   * Free a block of memory allocated via {@link TaskMemoryManager#allocatePage}.
   */
  public void freePage(MemoryBlock page, MemoryConsumer consumer) {
    assert (page.pageNumber != MemoryBlock.NO_PAGE_NUMBER) :
      "Called freePage() on memory that wasn't allocated with allocatePage()";
    assert (page.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
      "Called freePage() on a memory block that has already been freed";
    assert (page.pageNumber != MemoryBlock.FREED_IN_TMM_PAGE_NUMBER) :
            "Called freePage() on a memory block that has already been freed";
    assert(allocatedPages.get(page.pageNumber));
    // 清理pageTable中保存的MemoryBlock
    pageTable[page.pageNumber] = null;
    synchronized (this) {
      // 清空allocatedPages对MemoryBlock的页码的跟踪
      allocatedPages.clear(page.pageNumber);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Freed page number {} ({} bytes)", page.pageNumber, page.size());
    }
    // 获取page大小
    long pageSize = page.size();
    // Clear the page number before passing the block to the MemoryAllocator's free().
    // Doing this allows the MemoryAllocator to detect when a TaskMemoryManager-managed
    // page has been inappropriately directly freed without calling TMM.freePage().
    // 修改pageNumber值
    page.pageNumber = MemoryBlock.FREED_IN_TMM_PAGE_NUMBER;
    // 释放内存
    memoryManager.tungstenMemoryAllocator().free(page);
    // 释放MemoryManager管理的内存逻辑
    releaseExecutionMemory(pageSize, consumer);
  }

  /**
   * 根据给定的Page(即MemoryBlock)和Page中偏移量的地址,返回页号和相对于内存块起始地址的偏移量.
   * Given a memory page and offset within that page, encode this address into a 64-bit long.
   * This address will remain valid as long as the corresponding page has not been freed.
   *
   * @param page a data page allocated by {@link TaskMemoryManager#allocatePage}/
   * @param offsetInPage an offset in this page which incorporates the base offset. In other words,
   *                     this should be the value that you would pass as the base offset into an
   *                     UNSAFE call (e.g. page.baseOffset() + something).
   * @return an encoded page address.
   */
  public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
    if (tungstenMemoryMode == MemoryMode.OFF_HEAP) {
      // In off-heap mode, an offset is an absolute address that may require a full 64 bits to
      // encode. Due to our page size limitation, though, we can convert this into an offset that's
      // relative to the page's base offset; this relative offset will fit in 51 bits.
      // 获取page偏移量
      offsetInPage -= page.getBaseOffset();
    }
    //
    return encodePageNumberAndOffset(page.pageNumber, offsetInPage);
  }
  // 获取页号相对于内存起始地址的偏移量
  @VisibleForTesting
  public static long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
    assert (pageNumber >= 0) : "encodePageNumberAndOffset called with invalid page";
    // 位运算将pageNumber存储到64位长整形的高13位中,并将偏移量存储到64位长整形的低51位中
    return (((long) pageNumber) << OFFSET_BITS) | (offsetInPage & MASK_LONG_LOWER_51_BITS);
  }
  /** 用于将64位长整形右移51位,然后转换为整型获得pageNumber*/
  @VisibleForTesting
  public static int decodePageNumber(long pagePlusOffsetAddress) {
    return (int) (pagePlusOffsetAddress >>> OFFSET_BITS);
  }
  /** 用于将64位的长整形与51位的掩码按位进行与运算,获得在Page中的偏移量*/
  private static long decodeOffset(long pagePlusOffsetAddress) {
    return (pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS);
  }

  /**
   * 通过64位长整形,获取Page在内存中的对象.Tungsten的对内存模式有效.
   * Get the page associated with an address encoded by
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   */
  public Object getPage(long pagePlusOffsetAddress) {
    if (tungstenMemoryMode == MemoryMode.ON_HEAP) {
      // 堆内存模式,获取pageNumber
      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
      // 从pageTable中取出MemoryBlock
      final MemoryBlock page = pageTable[pageNumber];
      assert (page != null);
      assert (page.getBaseObject() != null);
      // 获取对象
      return page.getBaseObject();
    } else {
      // 非堆直接返回null.
      return null;
    }
  }

  /**
   * 通过64位长整形,获取在Page中的偏移量
   * Get the offset associated with an address encoded by
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   */
  public long getOffsetInPage(long pagePlusOffsetAddress) {
    // 获得Page中的偏移量
    final long offsetInPage = decodeOffset(pagePlusOffsetAddress);
    if (tungstenMemoryMode == MemoryMode.ON_HEAP) {
      // Tungsten内存模式是堆内存
      // 返回
      return offsetInPage;
    } else {
      // In off-heap mode, an offset is an absolute address. In encodePageNumberAndOffset, we
      // converted the absolute address into a relative address. Here, we invert that operation:
      // 获取pageNumber
      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
      // 获取对应的MemoryBlock
      final MemoryBlock page = pageTable[pageNumber];
      assert (page != null);
      // 返回Page在操作系统内存中的偏移量
      return page.getBaseOffset() + offsetInPage;
    }
  }

  /**
   * Clean up all allocated memory and pages. Returns the number of bytes freed. A non-zero return
   * value can be used to detect memory leaks.
   */
  public long cleanUpAllAllocatedMemory() {
    synchronized (this) {
      for (MemoryConsumer c: consumers) {
        if (c != null && c.getUsed() > 0) {
          // In case of failed task, it's normal to see leaked memory
          logger.debug("unreleased " + Utils.bytesToString(c.getUsed()) + " memory from " + c);
        }
      }
      consumers.clear();

      for (MemoryBlock page : pageTable) {
        if (page != null) {
          logger.debug("unreleased page: " + page + " in task " + taskAttemptId);
          page.pageNumber = MemoryBlock.FREED_IN_TMM_PAGE_NUMBER;
          memoryManager.tungstenMemoryAllocator().free(page);
        }
      }
      Arrays.fill(pageTable, null);
    }

    // release the memory that is not used by any consumer (acquired for pages in tungsten mode).
    memoryManager.releaseExecutionMemory(acquiredButNotUsed, taskAttemptId, tungstenMemoryMode);

    return memoryManager.releaseAllExecutionMemoryForTask(taskAttemptId);
  }

  /**
   * Returns the memory consumption, in bytes, for the current task.
   */
  public long getMemoryConsumptionForThisTask() {
    return memoryManager.getExecutionMemoryUsageForTask(taskAttemptId);
  }

  /**
   * Returns Tungsten memory mode
   */
  public MemoryMode getTungstenMemoryMode() {
    return tungstenMemoryMode;
  }
}

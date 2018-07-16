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

package org.apache.spark.util.collection

import java.util.Comparator

import com.google.common.hash.Hashing

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A simple open hash table optimized for the append-only use case, where keys
 * are never removed, but the value for each key may be changed.
 *
 * This implementation uses quadratic probing with a power-of-2 hash table
 * size, which is guaranteed to explore all spaces for each key (see
 * http://en.wikipedia.org/wiki/Quadratic_probing).
 *
 * The map can support up to `375809638 (0.7 * 2 ^ 29)` elements.
 *
 * TODO: Cache the hash values of each key? java.util.HashMap does that.
 */
@DeveloperApi
class AppendOnlyMap[K, V](initialCapacity: Int = 64) // 容器初始容量
  extends Iterable[(K, V)] with Serializable {

  import AppendOnlyMap._

  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")
  // 用于计算data数组容量增长的阈值的负载因子.容量到这个比重,扩容
  private val LOAD_FACTOR = 0.7
  // data数组的当前容量
  private var capacity = nextPowerOf2(initialCapacity)
  // 计算数据存放位置的掩码
  private var mask = capacity - 1
  private var curSize = 0
  private var growThreshold = (LOAD_FACTOR * capacity).toInt

  // Holds keys and values in the same array for memory locality; specifically, the order of
  // elements is key0, value0, key1, value1, key2, value2, etc.
  // 用于保存key和聚合值的数组.key和聚合值各占一位
  private var data = new Array[AnyRef](2 * capacity)

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  // data数组是否已经有了null值
  private var haveNullValue = false
  // 空值
  private var nullValue: V = null.asInstanceOf[V]

  // Triggered by destructiveSortedIterator; the underlying data array may no longer be used
  // data数组是否不再使用
  private var destroyed = false
  // 就是destroyed为true时打印消息的内容
  private val destructionMessage = "Map state is invalid from destructive sorting!"

  /**
    * 获取指定键的值
    * Get the value for a given key */
  def apply(key: K): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        return data(2 * pos + 1).asInstanceOf[V]
      } else if (curKey.eq(null)) {
        return null.asInstanceOf[V]
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V]
  }

  /**
    * 为指定key设置值
    * Set the value for a key */
  def update(key: K, value: V): Unit = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      // 如果key是null
      if (!haveNullValue) {
        // 如果不包含null值,扩充
        incrementSize()
      }
      // 更新成员变量
      nullValue = value
      haveNullValue = true
      return
    }
    // 不为null
    // 根据key的哈希值和掩码计算元素放入data数组的索引位置
    var pos = rehash(key.hashCode) & mask
    var i = 1
    while (true) {
      // 获取data(2 * pos)位置的当前key
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        // 如果curKey为null,说明索引位置还没放元素,将k放入该位置
        data(2 * pos) = k
        // 将值放入该位置+1位置
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        // 扩容
        incrementSize()  // Since we added a new key
        return
      } else if (k.eq(curKey) || k.equals(curKey)) {
        // 如果curKey不等于null并且等于k,说明data数组的,说明那个位置的键就是k,
        // 只放入值就可以了
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        return
      } else {
        // 如果k不等于null,不等于key,换位置
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }

  /**
   * 将key的值设置为updateFunc（hadValue，oldValue），
    * 其中oldValue将是key的旧值（如果有），否则为null。 返回新更新的值。
    * updateFunc是一个函数,接受两个参数,分别是boolean和泛型v.v标识key曾经添加到AppendOnlyMap的data数组
    * 进行聚合时生成的聚合值,新一轮的聚合将在之前的聚合值上累积.
    * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
   */
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    // 对key是null值的缓存集合
    if (k.eq(null)) {
      // 如果没有null值
      if (!haveNullValue) {
        // 扩容
        incrementSize()
      }
      // 更新成员变量,使用updateFunc值进行聚合
      nullValue = updateFunc(haveNullValue, nullValue)
      haveNullValue = true
      return nullValue
    }
    // 根据key的哈希值和掩码计算索引位置pos
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      // 获取data(2 * pos)的当前key
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        // curkey为null,放入key和值
        val newValue = updateFunc(false, null.asInstanceOf[V])
        data(2 * pos) = k
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        incrementSize()
        return newValue
      } else if (k.eq(curKey) || k.equals(curKey)) {
        // 如果k等于curkey,将之前的值与现在的值聚合获取新值
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        // 放入值
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else {
        // 修改位置继续循环
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }

  /** Iterator method from Iterable */
  override def iterator: Iterator[(K, V)] = {
    assert(!destroyed, destructionMessage)
    new Iterator[(K, V)] {
      var pos = -1

      /** Get the next value we should return from next(), or null if we're finished iterating */
      def nextValue(): (K, V) = {
        if (pos == -1) {    // Treat position -1 as looking at the null value
          if (haveNullValue) {
            return (null.asInstanceOf[K], nullValue)
          }
          pos += 1
        }
        while (pos < capacity) {
          if (!data(2 * pos).eq(null)) {
            return (data(2 * pos).asInstanceOf[K], data(2 * pos + 1).asInstanceOf[V])
          }
          pos += 1
        }
        null
      }

      override def hasNext: Boolean = nextValue() != null

      override def next(): (K, V) = {
        val value = nextValue()
        if (value == null) {
          throw new NoSuchElementException("End of iterator")
        }
        pos += 1
        value
      }
    }
  }

  override def size: Int = curSize

  /**
    * 扩充容量,增加1,如果需要的话会重新哈希
    * Increase table size by 1, rehashing if necessary */
  private def incrementSize() {
    curSize += 1
    if (curSize > growThreshold) {
      growTable()
    }
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   */
  private def rehash(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()

  /**
    * 容量扩充一倍,并且重新哈希
    * Double the table's size and re-hash everything */
  protected def growTable() {
    // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow
    val newCapacity = capacity * 2
    require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")
    // 创建一个两倍容量的新数组
    val newData = new Array[AnyRef](2 * newCapacity)
    // 新数组掩码
    val newMask = newCapacity - 1
    // Insert all our old values into the new array. Note that because our old keys are
    // unique, there's no need to check for equality here when we insert.
    var oldPos = 0
    while (oldPos < capacity) {
      // 将老数组中元素拷贝到新书组的指定索引位置
      if (!data(2 * oldPos).eq(null)) {
        val key = data(2 * oldPos)
        val value = data(2 * oldPos + 1)
        var newPos = rehash(key.hashCode) & newMask
        var i = 1
        var keepGoing = true
        while (keepGoing) {
          val curKey = newData(2 * newPos)
          if (curKey.eq(null)) {
            newData(2 * newPos) = key
            newData(2 * newPos + 1) = value
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }
    // 将新数组作为扩充容量后的data数组
    data = newData
    // 容量
    capacity = newCapacity
    // 掩码修改为新计算的掩码
    mask = newMask
    // 重新计算增长阈值
    growThreshold = (LOAD_FACTOR * newCapacity).toInt
  }
  /**
    * 取大于等于n最近的一个2平方数,如nextPowerOf2(63)=64
    * */
  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }

  /**
   * 以排序顺序返回map的迭代器。 这提供了一种在不使用额外内存的情况下对map进行排序的方法，
    * 其代价是破坏了map的有效性。
    * Return an iterator of the map in sorted order. This provides a way to sort the map without
   * using additional memory, at the expense of destroying the validity of the map.
   */
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    destroyed = true
    // Pack KV pairs into the front of the underlying array
    // 将data数组中的元素向前(index=0方向)整理
    var keyIndex, newIndex = 0
    while (keyIndex < capacity) {
      // 实际上这会跳过keyIndex==null的key和value,就是松散的数组,往前挤压去掉空值
      if (data(2 * keyIndex) != null) {
        data(2 * newIndex) = data(2 * keyIndex)
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)
        newIndex += 1
      }
      keyIndex += 1
    }
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0))
    // 利用Sorter,KVArraySortDataFormat及指定的keyComparator进行排序,用的timsort,优化版归并排序.
    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)
    // 创建访问data数组的迭代器
    new Iterator[(K, V)] {
      var i = 0
      var nullValueReady = haveNullValue
      def hasNext: Boolean = (i < newIndex || nullValueReady)
      def next(): (K, V) = {
        if (nullValueReady) {
          nullValueReady = false
          (null.asInstanceOf[K], nullValue)
        } else {
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
          i += 1
          item
        }
      }
    }
  }

  /**
   * Return whether the next insert will cause the map to grow
   */
  def atGrowThreshold: Boolean = curSize == growThreshold
}

private object AppendOnlyMap {
  val MAXIMUM_CAPACITY = (1 << 29)
}

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

import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.collection.WritablePartitionedPairCollection._

/**
 * 跟人感觉和AppendOnlyMap很像,可以查看AppendOnlyMap中的注释.
  * 两者有些区别:<br>
  * (1)AppendOnlyMap会对元素在内存中进行更新和聚合,PartitionedPairBuffer只起数据缓冲作用.
  * (2)AppendOnlyMap行为上更像map,元素以散列的方式放入data数组,PartitionedPairBuffer行为更像collection,
  * 元素从0,1往后连续放入的.
  * (3) AppendOnlyMap没有继承SizeTracker,不支持采样和大小估算.而PartitionedPairBuffer继承SizeTracker,
  * 所以支持采样和大小估算,但是AppendOnlyMap的子类SizeTrackingAppendOnlyMap支持采样和大小估算.
  * (4)AppendOnlyMap没有继承WritablePartitionedPairCollection,不支持基于内存进行有效排序的迭代器,
  * 也不可以创建将集合内容按照字节写入磁盘的WritablePartitionedIterator.PartitionedPairBuffer继承了
  * WritablePartitionedPairCollection.但是AppendOnlyMap有继承了WritablePartitionedPairCollection
  * 的子类.
  * Append-only buffer of key-value pairs, each with a corresponding partition ID, that keeps track
 * of its estimated size in bytes.
 *
 * The buffer can support up to 1073741819 elements.
 */
private[spark] class PartitionedPairBuffer[K, V](initialCapacity: Int = 64)// 初始容量,默认64
  extends WritablePartitionedPairCollection[K, V] with SizeTracker
{
  import PartitionedPairBuffer._

  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  // Basic growable array data structure. We use a single array of AnyRef to hold both the keys
  // and the values, so that we can sort them efficiently with KVArraySortDataFormat.
  // data数组当前容量.初始值是initialCapacity
  private var capacity = initialCapacity
  // 记录当前已经放入data的key和value的数量
  private var curSize = 0
  // 用于存储key和value,key和value各占一个位置
  private var data = new Array[AnyRef](2 * initialCapacity)

  /**
    * 向数组中添加元素
    * Add an element into the buffer */
  def insert(partition: Int, key: K, value: V): Unit = {
    // 如果满了,扩容
    if (curSize == capacity) {
      growArray()
    }
    // 键放入data
    data(2 * curSize) = (partition, key.asInstanceOf[AnyRef])
    // 值放入data
    data(2 * curSize + 1) = value.asInstanceOf[AnyRef]
    curSize += 1
    // 对集合大小采样
    afterUpdate()
  }

  /**
    * 双倍扩充数组
    * Double the size of the array because we've reached capacity */
  private def growArray(): Unit = {
    // 超过容量限制抛出异常
    if (capacity >= MAXIMUM_CAPACITY) {
      throw new IllegalStateException(s"Can't insert more than ${MAXIMUM_CAPACITY} elements")
    }
    // 扩容,原容量*2,不超过最大容量
    val newCapacity =
      if (capacity * 2 > MAXIMUM_CAPACITY) { // Overflow
        MAXIMUM_CAPACITY
      } else {
        capacity * 2
      }
    // 创建两倍容量新数组
    val newArray = new Array[AnyRef](2 * newCapacity)
    // 拷贝
    System.arraycopy(data, 0, newArray, 0, 2 * capacity)
    // 更新数组
    data = newArray
    // 更新容量
    capacity = newCapacity
    // 对样本重置
    resetSamples()
  }

  /**
    * Iterate through the data in a given order. For this class this is not really destructive. */
  override def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    // 生成comparator
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    // 利用生成comparator,Sorter,KVArraySortDataFormat进行排序
    new Sorter(new KVArraySortDataFormat[(Int, K), AnyRef]).sort(data, 0, curSize, comparator)
    // 创建迭代器
    iterator
  }

  private def iterator(): Iterator[((Int, K), V)] = new Iterator[((Int, K), V)] {
    var pos = 0

    override def hasNext: Boolean = pos < curSize

    override def next(): ((Int, K), V) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val pair = (data(2 * pos).asInstanceOf[(Int, K)], data(2 * pos + 1).asInstanceOf[V])
      pos += 1
      pair
    }
  }
}

private object PartitionedPairBuffer {
  val MAXIMUM_CAPACITY: Int = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 2
}

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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.util.Utils

@DeveloperApi
class RDDInfo(
    val id: Int, // RDD的id
    var name: String, // 名称
    val numPartitions: Int, // 分区数量
    var storageLevel: StorageLevel, // 存储级别
    val parentIds: Seq[Int], // 父RDD的idlist,说明RDD会有0到多个父RDD
    val callSite: String = "", // RDD的用户调用栈信息
    val scope: Option[RDDOperationScope] = None) // RDD操作范围
  extends Ordered[RDDInfo] {
  // 缓存的分区数量
  var numCachedPartitions = 0
  // 使用的内存大小
  var memSize = 0L
  // 使用的磁盘大下
  var diskSize = 0L
  // Block存储在外部的大小
  var externalBlockStoreSize = 0L
  // 是否缓存
  def isCached: Boolean = (memSize + diskSize > 0) && numCachedPartitions > 0

  override def toString: String = {
    import Utils.bytesToString
    ("RDD \"%s\" (%d) StorageLevel: %s; CachedPartitions: %d; TotalPartitions: %d; " +
      "MemorySize: %s; DiskSize: %s").format(
        name, id, storageLevel.toString, numCachedPartitions, numPartitions,
        bytesToString(memSize), bytesToString(diskSize))
  }
  // 重写compare用于排序,根据id大小排序
  override def compare(that: RDDInfo): Int = {
    this.id - that.id
  }
}

private[spark] object RDDInfo {
  // 用于从RDD中构建对应的RDDInfo
  def fromRdd(rdd: RDD[_]): RDDInfo = {
    // 获取当前RDD的name属性作为RDDInfo的name,没有从工具类中生成
    val rddName = Option(rdd.name).getOrElse(Utils.getFormattedClassName(rdd))
    // 获取当前RDD以来的所有父RDD的身份标识作为parentsId属性
    val parentIds = rdd.dependencies.map(_.rdd.id)
    // 从RDD中获取信息,然后创建RDDInfo
    new RDDInfo(rdd.id, rddName, rdd.partitions.length,
      rdd.getStorageLevel, parentIds, rdd.creationSite.shortForm, rdd.scope)
  }
}

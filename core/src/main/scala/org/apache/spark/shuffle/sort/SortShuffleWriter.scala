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
// scalastyle:off
package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter

private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {
  /** BaseShuffleHandle的依赖(ShuffleDependency)*/
  private val dep = handle.dependency
  /** SparkEnv的BlockManager*/
  private val blockManager = SparkEnv.get.blockManager
  /** ExternalSorter对象*/
  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  /** 是否已经停止*/
  private var stopping = false
  /** map任务的状态*/
  private var mapStatus: MapStatus = null
  /** 对Shuffle写入的度量系统*/
  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  /**
    * 将批量记录写入本任务的输出
    * Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      // 如果shuffle依赖的map段结合为true
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      // 根据dep.aggregator和dep.keyOrdering创建一个ExternalSorter
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      // 如果mapSideCombine!=true.聚集器是None,排序也是None.
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    // 使用ExternalSorter的insertAll方法将数据写入磁盘,等待reduce获取数据
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    // 获取输出文件
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    // 返回临时文件的路径，该路径与path在同一目录中。
    val tmp = Utils.tempFileWith(output)
    try {
      // 创建bllockId
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      // sorter会将数据写入磁盘,并返回文件的每个分区的长度数组
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      // 将block索引写入索引文件
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      // 创建一个mapstatus
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }

  /**
    * 请关闭该writer，参数是该map阶段是否已完成。
    * Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      // 如果已经停止了就返回None
      if (stopping) {
        return None
      }
      stopping = true
      // 如果成功了,返回本对象的mapStatus
      if (success) {
        return Option(mapStatus)
      } else {
        // 否则返回None
        return None
      }
    } finally {
      // 清理我们的分类器，它可能有自己的中间文件
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        // 调用sorter的stop
        sorter.stop()
        // 更新度量系统
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        // 清空sorter的引用
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
  // 是否绕过合并排序,取决于dep.mapSideCombine,如果true不绕过,
  // 如果false还要看dep.partitioner.numPartitions是否小于等于bypassMergeThreshold
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      false
    } else {
      val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}

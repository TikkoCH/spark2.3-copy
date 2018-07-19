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

package org.apache.spark.shuffle.sort
// scalastyle:off
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._

/**
 * 在基于排序的shuffle中,进入的记录按照目标分区ID排序,然后输出到一个单独的map文件中.
  * reduce为了独处map输出,需要获取map输出文件的连续内容.当map的输出数据太大已经不适合放在内存中时,
  * 排序后的输出子集将被溢出到文件中,这些磁盘上的文件将被合并生成最终的输出文件.
  * In sort-based shuffle, incoming records are sorted according to their target partition ids, then
 * written to a single map output file. Reducers fetch contiguous regions of this file in order to
 * read their portion of the map output. In cases where the map output data is too large to fit in
 * memory, sorted subsets of the output can are spilled to disk and those on-disk files are merged
 * to produce the final output file.
 *
 * Sort-based shuffle has two different write paths for producing its map output files:
 *
 *  - Serialized sorting: used when all three of the following conditions hold:
 *    1. The shuffle dependency specifies no aggregation or output ordering.
 *    2. The shuffle serializer supports relocation of serialized values (this is currently
 *       supported by KryoSerializer and Spark SQL's custom serializers).
 *    3. The shuffle produces fewer than 16777216 output partitions.
 *  - Deserialized sorting: used to handle all other cases.
 *
 * -----------------------
 * Serialized sorting mode
 * -----------------------
 *
 * In the serialized sorting mode, incoming records are serialized as soon as they are passed to the
 * shuffle writer and are buffered in a serialized form during sorting. This write path implements
 * several optimizations:
 *
 *  - Its sort operates on serialized binary data rather than Java objects, which reduces memory
 *    consumption and GC overheads. This optimization requires the record serializer to have certain
 *    properties to allow serialized records to be re-ordered without requiring deserialization.
 *    See SPARK-4550, where this optimization was first proposed and implemented, for more details.
 *
 *  - It uses a specialized cache-efficient sorter ([[ShuffleExternalSorter]]) that sorts
 *    arrays of compressed record pointers and partition ids. By using only 8 bytes of space per
 *    record in the sorting array, this fits more of the array into cache.
 *
 *  - The spill merging procedure operates on blocks of serialized records that belong to the same
 *    partition and does not need to deserialize records during the merge.
 *
 *  - When the spill compression codec supports concatenation of compressed data, the spill merge
 *    simply concatenates the serialized and compressed spill partitions to produce the final output
 *    partition.  This allows efficient data copying methods, like NIO's `transferTo`, to be used
 *    and avoids the need to allocate decompression or copying buffers during the merge.
 *
 * For more details on these optimizations, see SPARK-7081.
 */
private[spark] class SortShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
        " Shuffle will continue to spill to disk when necessary.")
  }

  /**
   * ShuflleId与此Shuffle生成输出的map任务的数量之间的映射关系
    * A mapping from shuffle ids to the number of mappers producing output for those shuffles.
   */
  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  override val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)

  /**
   * 根据不同条件创建ShuffleHandle实例
    * Obtains a [[ShuffleHandle]] to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      // 需要绕过合并和排序则创建BypassMergeSortShuffleHandle
      new BypassMergeSortShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      // 如果可以使用序列化Shuffle创建SerializedShuffleHandle
      new SerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      // 其他情况BaseShuffleHandle
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /**
    * 为指定的分区获取writer.map任务所在的executor会调用该方法
    * Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {
    numMapsForShuffle.putIfAbsent(
      handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    val env = SparkEnv.get
    handle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new UnsafeShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf)
      case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        new BypassMergeSortShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          bypassMergeSortHandle,
          mapId,
          context,
          env.conf)
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new SortShuffleWriter(shuffleBlockResolver, other, mapId, context)
    }
  }

  /**
    * 根据指定的shuffleId删除此Shuffle郭成的所有mao任务数据文件和索引文件.
    * Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps).foreach { mapId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}


private[spark] object SortShuffleManager extends Logging {

  /**
   * The maximum number of shuffle output partitions that SortShuffleManager supports when
   * buffering map outputs in a serialized form. This is an extreme defensive programming measure,
   * since it's extremely unlikely that a single shuffle produces over 16 million output partitions.
   * */
  val MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE =
    PackedRecordPointer.MAXIMUM_PARTITION_ID + 1

  /**
   * Helper method for determining whether a shuffle should use an optimized serialized shuffle
   * path or whether it should fall back to the original path that operates on deserialized objects.
   */
  def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val shufId = dependency.shuffleId
    val numPartitions = dependency.partitioner.numPartitions
    if (!dependency.serializer.supportsRelocationOfSerializedObjects) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because the serializer, " +
        s"${dependency.serializer.getClass.getName}, does not support object relocation")
      false
    } else if (dependency.aggregator.isDefined) {
      log.debug(
        s"Can't use serialized shuffle for shuffle $shufId because an aggregator is defined")
      false
    } else if (numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because it has more than " +
        s"$MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE partitions")
      false
    } else {
      log.debug(s"Can use serialized shuffle for shuffle $shufId")
      true
    }
  }
}

/**
 * 用于确定何时选择序列化的shuffle
  * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * serialized shuffle.
 */
private[spark] class SerializedShuffleHandle[K, V](
  shuffleId: Int,
  numMaps: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

/**
 * 用于确定何时选择合并和排序的Shuffle路径
  * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * bypass merge sort shuffle path.
 */
private[spark] class BypassMergeSortShuffleHandle[K, V](
  shuffleId: Int,
  numMaps: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

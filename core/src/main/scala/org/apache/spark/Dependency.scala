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

package org.apache.spark

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  /** 返回当前以来的RDD*/
  def rdd: RDD[T]
}


/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * 为子分区获取父分区列表
    * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}


/**
 * :: DeveloperApi ::
  * 表示在shuffle阶段的输出上的依赖.请注意，在shuffle的情况下,RDD是transient,因为我们在执行程序端不需要它。
 * Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,
 * the RDD is transient since we don't need it on the executor side.
 *
 * @param _rdd 父rdd,泛型要求必须是Product2[K,V]及其子类的RDD<br>the parent RDD
 * @param partitioner 用于分区shuffle输出的分区器<br>
  *                    partitioner used to partition the shuffle output
 * @param serializer sparkEnv中的序列化器.<br>
  *                   [[org.apache.spark.serializer.Serializer Serializer]] to use. If not set
 *                   explicitly then the default serializer, as specified by `spark.serializer`
 *                   config option, will be used.
 * @param keyOrdering RDD的shuffle是否按泛型K排序.<br>
  *                    key ordering for RDD's shuffles
 * @param aggregator m对map任务输出数据进行聚合的聚合器
  *                   map/reduce-side aggregator for RDD's shuffle
 * @param mapSideCombine 是否在map端进行合并,默认false
  *                       whether to perform partial aggregation (also known as map-side combine)
 */
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Serializer = SparkEnv.get.serializer,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false)
  extends Dependency[Product2[K, V]] {

  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]
  // K的类名
  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName
  // V的类名
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName
  // combiner类名.如果使用PairRDDFunctions中的combineByKey方法
  // 而不是combineByKeyWithClassTag，则组合器类标记可能为null。
  // Note: It's possible that the combiner class tag is null, if the combineByKey
  // methods in PairRDDFunctions are used instead of combineByKeyWithClassTag.
  private[spark] val combinerClassName: Option[String] =
    Option(reflect.classTag[C]).map(_.runtimeClass.getName)
  // 当前ShuffleDependency的标识
  val shuffleId: Int = _rdd.context.newShuffleId()
  // 当前ShuffleDependency的处理器.
  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, _rdd.partitions.length, this)

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
}


/**
 * :: DeveloperApi ::
  * 表示父子RDD之间分区一对一的依赖关系
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
  * 表示父子RDD之间分区一对一的依赖关系,一个RDD可能依赖两个父RDD,但是分区还是一对一,子RDD的部分分区依赖
  * 父RDD1,部分依赖父RDD2,但是分区都是一对一依赖.
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd 父rdd the parent RDD
 * @param inStart 父RDD的范围起始 the start of the range in the parent RDD
 * @param outStart 子RDD的范围起始 the start of the range in the child RDD
 * @param length 范围大小 the length of the range
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}

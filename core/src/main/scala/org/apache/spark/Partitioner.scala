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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.log10
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.util.random.SamplingUtils

/**
  * 只有键值对RDD有分区器,分区器定义了键值对RDD中元素根据键分区的方式.
  * 每个key都会对应一个分区ID,从0到分区数量-1
 * An object that defines how the elements in a key-value pair RDD are partitioned by key.
 * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
 */
abstract class Partitioner extends Serializable {
  /**
    * 分区数量
    * @return
    */
  def numPartitions: Int

  /**
    * 获取分区信息
    * @param key 键
    * @return 分区号码
    */
  def getPartition(key: Any): Int
}

object Partitioner {
  /**
    *
    * 选择一个分区器用于多个RDD之间的类似cogroup的操作.
    * 如果已经设置了spark的默认并行度,我们会使用SparkContext的defaultParallelism作为
    * RDD之间类似cogroup操作的并行度值.如果可用的话,我们选择rdd中拥有最大分区数的rdd的分区器.
    * 1.这个分区器符合条件(分区的数量与rdd中最大分区数量级一致),
    * 2.或拥有的分区数量高于默认分区数量
    * 满足条件之一我们就可以使用其作为分区器
    *
    * 否则,我们会以默认分区数量new一个新HashPartitioner,并以其作为分区器.
    * 除非spark.default.parallelism已经被设置了,否则分区的数量会和上游RDD中最大的分区数量相同.
    * 这样可以最大限度避免内存溢出异常.
    *我们使用两个参数(rdd,others)来强迫调用者至少传入一个RDD.
    *
   * Choose a partitioner to use for a cogroup-like operation between a number of RDDs.
   *
   * If spark.default.parallelism is set, we'll use the value of SparkContext defaultParallelism
   * as the default partitions number, otherwise we'll use the max number of upstream partitions.
   *
   * When available, we choose the partitioner from rdds with maximum number of partitions. If this
   * partitioner is eligible (number of partitions within an order of maximum number of partitions
   * in rdds), or has partition number higher than default partitions number - we use this
   * partitioner.
   *
   * Otherwise, we'll use a new HashPartitioner with the default partitions number.
   *
   * Unless spark.default.parallelism is set, the number of partitions will be the same as the
   * number of partitions in the largest upstream RDD, as this should be least likely to cause
   * out-of-memory errors.
   *
   * We use two method parameters (rdd, others) to enforce callers passing at least 1 RDD.
   */
  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    //将两个参数合并成Seq
    val rdds = (Seq(rdd) ++ others)
    //筛选分区数大于0的RDD
    val hasPartitioner = rdds.filter(_.partitioner.exists(_.numPartitions > 0))
    //选择分区数最大的RDD
    val hasMaxPartitioner: Option[RDD[_]] = if (hasPartitioner.nonEmpty) {
      Some(hasPartitioner.maxBy(_.partitions.length))
    } else {
      None
    }
    //如果conf中有默认并行数,为defaultNumPartitions赋值,否则选择rdds中分区数最大的值
    val defaultNumPartitions = if (rdd.context.conf.contains("spark.default.parallelism")) {
      rdd.context.defaultParallelism
    } else {
      rdds.map(_.partitions.length).max
    }
    //如果存在的最大分区器是合格的,或者它的分区数量比默认分区数量大,则使用存在的分区器.
    // If the existing max partitioner is an eligible one, or its partitions number is larger
    // than the default number of partitions, use the existing partitioner.
    if (hasMaxPartitioner.nonEmpty && (isEligiblePartitioner(hasMaxPartitioner.get, rdds) ||
        defaultNumPartitions < hasMaxPartitioner.get.getNumPartitions)) {
      hasMaxPartitioner.get.partitioner.get
    } else {
      new HashPartitioner(defaultNumPartitions)
    }
  }

  /**
    * 如果RDD的分区数量大于或小于最大上游分区数量的一个数量级(数量级在这里就是log10())，则返回true，否则返回false。
   * Returns true if the number of partitions of the RDD is either greater than or is less than and
   * within a single order of magnitude of the max number of upstream partitions, otherwise returns
   * false.
   */
  private def isEligiblePartitioner(
     hasMaxPartitioner: RDD[_],
     rdds: Seq[RDD[_]]): Boolean = {
    val maxPartitions = rdds.map(_.partitions.length).max
    log10(maxPartitions) - log10(hasMaxPartitioner.getNumPartitions) < 1
  }
}

/**
  * HashPartitioner是根据Java的hashCode方法实现的Partitioner
  * Java数组的hashCode是基于数组的标识而不是其内容.
  * 所以试图对RDD[Array[_] ] 或 RDD[(Array[_], _)]可能会得到意想不到的结果或者错误的结果
 * A [[org.apache.spark.Partitioner]] that implements hash-based partitioning using
 * Java's `Object.hashCode`.
 *
 * Java arrays have hashCodes that are based on the arrays' identities rather than their contents,
 * so attempting to partition an RDD[Array[_] ] or RDD[(Array[_], _)] using a HashPartitioner will
 * produce an unexpected or incorrect result.
 */
class HashPartitioner(partitions: Int) extends Partitioner {
  //如果partitions<0,就扔个IllegalArgumentException,并且打印一下分区不能负数
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")
  //返回分区数
  def numPartitions: Int = partitions
  //返回分区
  //null是0分区,其他是键的hashcod模除分区数的值
  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  /**
    * 重写equals两个分区数相同则相同....
    */
  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

/**
  * 范围分区器.将有序的记录分到大约相等的多个区间中.范围是由rdd传入的采样内容决定.
  * 注意一点,在采样数据的情况下,创建的实际分区数量可能不同于构造器中partitions参数的值,会低于实际的partitions的值.
 * A [[org.apache.spark.Partitioner]] that partitions sortable records by range into roughly
 * equal ranges. The ranges are determined by sampling the content of the RDD passed in.
 *
 * @note The actual number of partitions created by the RangePartitioner might not be the same
 * as the `partitions` parameter, in the case where the number of sampled records is less than
 * the value of `partitions`.
 */
class RangePartitioner[K : Ordering : ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    private var ascending: Boolean = true,
    val samplePointsPerPartitionHint: Int = 20)
  extends Partitioner {

  // A constructor declared in order to maintain backward compatibility for Java, when we add the
  // 4th constructor parameter samplePointsPerPartitionHint. See SPARK-22160.
  // This is added to make sure from a bytecode point of view, there is still a 3-arg ctor.
  //声明这个构造器就是为了当添加第四个参数时兼容Java.这是为了确保从字节码的角度看,这仍有一个三参构造器.
  def this(partitions: Int, rdd: RDD[_ <: Product2[K, V]], ascending: Boolean) = {
    this(partitions, rdd, ascending, samplePointsPerPartitionHint = 20)
  }

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  //当在默认设置下排序空RDD时,允许partitions=0
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")
  require(samplePointsPerPartitionHint > 0,
    s"Sample points per partition must be greater than 0 but found $samplePointsPerPartitionHint")

  private var ordering = implicitly[Ordering[K]]
  /**
    * 1.1版本采样算法
    */
  // An array of upper bounds for the first (partitions - 1) partitions
//  private val rangeBounds: Array[K] = {
  //如果分区数是1,则返回一个empty数组
//    if (partitions == 1) {
//      Array()
//    } else {
  //计算rdd元素总数
//      val rddSize = rdd.count()
//      val maxSampleSize = partitions * 20.0//最大采样为分区数*20,这里被写死了
  //抽样比例:(maxSampleSize/(rddSize和1)的最大值, 1)中取最小值.
//      val frac = math.min(maxSampleSize / math.max(rddSize, 1), 1.0)
//      val rddSample = rdd.sample(false, frac, 1).map(_._1).collect().sorted //计算出抽样RDD
//      if (rddSample.length == 0) {
//        Array()
//      } else {
//        val bounds = new Array[K](partitions - 1) //长度为分区数-1的数组
  //这一步填充bounds数组
//        for (i <- 0 until partitions - 1) {
  //计算下标:对已经排序的rddSample平均切分
//          val index = (rddSample.length - 1) * (i + 1) / partitions
//          bounds(i) = rddSample(index)
//        }
//        bounds
//      }
//    }
//  }
  // An array of upper bounds for the first (partitions - 1) partitions
  //范围边界数组
  private var rangeBounds: Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      // Cast to double to avoid overflowing ints or longs
      //我们需要大致平衡输出分区的样本量，最高值为1M。强转成double避免int或long的溢出
      //样本大小=构造参数中的samplePointsPerPartitionHint*分区数和10的6次方的最小值.(上限小于十的6次方)
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      //假设输入分区大致平衡并过度采样一点
      //每个分区的样本量=3*采样数量/分区数
      //乘于3的目的就是保证数据量小的分区能够采样到足够的数据，而对于数据量大的分区会进行第二次采样。
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      //水塘抽样算法获得了(元素总数,Array(分区Id,分区中元素数量,抽样样本)
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        //因为rangepartitioner就是为了防止数据倾斜,所以第一次抽样不合理的话会再来一次
        //如果分区包含的内容远远多于平均数量，我们会从中重新抽样，以确保从该分区收集足够的元素样本。
        //这个因子和1.1版本一样
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0) //sampleSize/numItems
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) => //Array(分区Id,分区中元素数量,抽样样本)
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx //如果fraction * n > sampleSizePerPartition,就是需要重新采样的分区
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))//candidates是(sample中的每项,权重)
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        //确定边界
        RangePartitioner.determineBounds(candidates, math.min(partitions, candidates.size))
      }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}

private[spark] object RangePartitioner {

  /**
    * 水塘算法实现
   * Sketches the input RDD via reservoir sampling on each partition.
   *
   * @param rdd the input RDD to sketch 需要抽样的输入rdd
   * @param sampleSizePerPartition max sample size per partition 每个分区的最大样本数
   * @return (元素总数,Array(分区Id,分区中元素数量,抽样样本)
    *         (total number of items, an array of (partitionId, number of items, sample))
   */
  def sketch[K : ClassTag](
      rdd: RDD[K],
      sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    val shift = rdd.id //rdd的唯一id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object 避免序列化整个分区器对象
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      //种子的计算方法,idx是键值对RDD的键,shift左移16位保证了高位有值
      //异或键保证低位有值
      val seed = byteswap32(idx ^ (shift << 16))
      //下面reservoirSampleAndCount就是水塘抽样算法实现了
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()//对每个分区进行水塘抽样
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }

  /**
   * Determines the bounds for range partitioning from candidates with weights indicating how many
   * items each represents. Usually this is 1 over the probability used to sample this candidate.
   *
   * @param candidates unordered candidates with weights
   * @param partitions number of partitions
   * @return selected bounds
   */
  def determineBounds[K : Ordering : ClassTag](
      candidates: ArrayBuffer[(K, Float)],
      partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }
}

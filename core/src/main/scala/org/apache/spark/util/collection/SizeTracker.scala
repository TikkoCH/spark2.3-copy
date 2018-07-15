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
// scalastyle:off
import scala.collection.mutable

import org.apache.spark.util.SizeEstimator

/**
 *  集合的通用接口，以跟踪其估计的字节数。我们使用size估计值来进行缓慢的指数回溯，
  *  以摊销时间，因为每个对size估计值的调用都有些昂贵（大约几毫秒）。
  * A general interface for collections to keep track of their estimated sizes in bytes.
 * We sample with a slow exponential back-off using the SizeEstimator to amortize the time,
 * as each call to SizeEstimator is somewhat expensive (order of a few milliseconds).
 */
private[spark] trait SizeTracker {

  import SizeTracker._

  /**
   * 采样增长速率.例如,速率为2时,分别对1,2,4,8位置上的元素进行采样.
    * Controls the base of the exponential which governs the rate of sampling.
   * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
   */
  private val SAMPLE_GROWTH_RATE = 1.1

  /**
    * 样本队列,最后两个样本用于估算
    * Samples taken since last resetSamples(). Only the last two are kept for extrapolation. */
  private val samples = new mutable.Queue[Sample]

  /**
    * 在最后两个样本之间的每个更新的平均字节数。
    * The average number of bytes per update between our last two samples. */
  private var bytesPerUpdate: Double = _

  /**
    * 上次resetSamples()之后,更新操作的总次数
    * Total number of insertions and updates into the map since the last resetSamples(). */
  private var numUpdates: Long = _

  /**
    * 我们将在下一个样本中使用numupdate的值
    * The value of 'numUpdates' at which we will take our next sample. */
  private var nextSampleNum: Long = _

  resetSamples()

  /**
   * 重置目前采样.这应该在集合经历了一个巨大的变化之后被调用
    * Reset samples collected so far.
   * This should be called after the collection undergoes a dramatic change in size.
   */
  protected def resetSamples(): Unit = {
    numUpdates = 1
    nextSampleNum = 1
    samples.clear()
    takeSample()
  }

  /**
   * 每次更新后需要调用的回调
    * Callback to be invoked after every update.
   */
  protected def afterUpdate(): Unit = {
    numUpdates += 1
    if (nextSampleNum == numUpdates) {
      takeSample()
    }
  }

  /**
   * 根据当前集合大小获取新的采样
    * Take a new sample of the current collection's size.
   */
  private def takeSample(): Unit = {
    // 估算集合的大小作为样本
    samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates))
    // Only use the last two samples to extrapolate
    // 保留队列的最后两个样本
    if (samples.size > 2) {
      samples.dequeue()
    }
    val bytesDelta = samples.toList.reverse match {
      case latest :: previous :: tail =>
        // (本次采样大小-上次采样大小)/(本次采样编号-上次采样编号)
        (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates)
      // If fewer than 2 samples, assume no change
      case _ => 0
    }
    // 计算每次更新的字节数
    bytesPerUpdate = math.max(0, bytesDelta)
    // 挑选下次采样的采样号码
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }

  /**
   * Estimate the current size of the collection in bytes. O(1) time.
   */
  def estimateSize(): Long = {
    assert(samples.nonEmpty)
    // 样本平均字节*(当前采样numUpdates-上次采样numUpdates)
    val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
    // 上次采样集合大小+extrapolatedDelta
    (samples.last.size + extrapolatedDelta).toLong
  }
}

private object SizeTracker {
  case class Sample(size: Long, numUpdates: Long)
}

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

package org.apache.spark.scheduler

import scala.collection.mutable.HashSet

import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * ShuffleMapStages是执行DAG的中间阶段，它为shuffle生成数据。 它们恰好在每次shuffle操作之前发生，
  * 并且可能在此之前包含多个流水线操作（例如map和filter）。 执行时，它们会保存map输出文件，
  * 以后可以通过reduce任务获取这些文件。 `shuffleDep`字段描述了每个阶段所属的shuffle，
  * 而诸如`outputLocs`和`numAvailableOutputs`之类的变量跟踪了准备好多少个map输出。
  * ShuffleMapStages也可以作为具有DAGScheduler.submitMapStage的作业独立提交。
  * 对于这样的阶段，提交它们的ActiveJobs在`mapStageJobs`中被跟踪。
  * 请注意，可能有多个ActiveJobs尝试计算相同的shuffle map阶段。
  * ShuffleMapStages are intermediate stages in the execution DAG that produce data for a shuffle.
 * They occur right before each shuffle operation, and might contain multiple pipelined operations
 * before that (e.g. map and filter). When executed, they save map output files that can later be
 * fetched by reduce tasks. The `shuffleDep` field describes the shuffle each stage is part of,
 * and variables like `outputLocs` and `numAvailableOutputs` track how many map outputs are ready.
 *
 * ShuffleMapStages can also be submitted independently as jobs with DAGScheduler.submitMapStage.
 * For such stages, the ActiveJobs that submitted them are tracked in `mapStageJobs`. Note that
 * there can be multiple ActiveJobs trying to compute the same shuffle map stage.
 */
private[spark] class ShuffleMapStage(
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _], // 与ShuffleMapStage相对应的ShuffleDependency
    mapOutputTrackerMaster: MapOutputTrackerMaster)
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {
  // 与ShuffleMapStage相关联的ActiveJob列表
  private[this] var _mapStageJobs: List[ActiveJob] = Nil

  /**
   * 仍然没有完成计算的或在executor上计算但是已经失去联络了的分区,需要重新计算.这个变量用于DAGScheduler
    * 判断stage何时完成.stage的主动尝试或该stage的早期尝试中的task成功可能会导致分区id从
    * pendingPartitions中删除.因此，此变量可能与TaskSetManager中用于stage的活动尝试的挂起任务不一致
    * （此处存储的分区将始终是TaskSetManager认为待处理的分区的子集）。
    * Partitions that either haven't yet been computed, or that were computed on an executor
   * that has since been lost, so should be re-computed.  This variable is used by the
   * DAGScheduler to determine when a stage has completed. Task successes in both the active
   * attempt for the stage or in earlier attempts for this stage can cause paritition ids to get
   * removed from pendingPartitions. As a result, this variable may be inconsistent with the pending
   * tasks in the TaskSetManager for the active attempt for the stage (the partitions stored here
   * will always be a subset of the partitions that the TaskSetManager thinks are pending).
   */
  val pendingPartitions = new HashSet[Int]

  override def toString: String = "ShuffleMapStage " + id

  /**
   * 返回活动job列表.
    * Returns the list of active jobs,
   * i.e. map-stage jobs that were submitted to execute this stage independently (if any).
   */
  def mapStageJobs: Seq[ActiveJob] = _mapStageJobs

  /**
    * 将job添加到活动job列表
    * Adds the job to the active job list. */
  def addActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = job :: _mapStageJobs
  }

  /**
    * 从活动job列表中删除job
    * Removes the job from the active job list. */
  def removeActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = _mapStageJobs.filter(_ != job)
  }

  /**
   * ShuffleMapStage可用的map任务的输出数量,也代表map成功任务数量
    * Number of partitions that have shuffle outputs.
   * When this reaches [[numPartitions]], this map stage is ready.
   */
  def numAvailableOutputs: Int = mapOutputTrackerMaster.getNumAvailableOutputs(shuffleDep.shuffleId)

  /**
   * 如果map阶段已经就绪,返回true.也意味着当ShuffleMapStage的所有分区的map任务都执行成功后,
    * ShuffleMapStage才是可用的
    * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   */
  def isAvailable: Boolean = numAvailableOutputs == numPartitions

  /**
    * 返回所有还未执行成功而需要计算的分区.
    * Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  override def findMissingPartitions(): Seq[Int] = {
    mapOutputTrackerMaster
      .findMissingPartitions(shuffleDep.shuffleId)
      .getOrElse(0 until numPartitions)
  }
}

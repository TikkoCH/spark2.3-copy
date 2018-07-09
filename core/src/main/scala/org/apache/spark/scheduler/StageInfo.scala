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

import scala.collection.mutable.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.RDDInfo

/**
 * :: DeveloperApi ::
 * Stores information about a stage to pass from the scheduler to SparkListeners.
 */
@DeveloperApi
class StageInfo(
    val stageId: Int,             //Stage的id
    @deprecated("Use attemptNumber instead", "2.3.0") val attemptId: Int,
    val name: String,             // 名称
    val numTasks: Int,            // 任务数量
    val rddInfos: Seq[RDDInfo],   // RDD信息列表
    val parentIds: Seq[Int],      // 父StageId的序列
    val details: String,          // 详细线程栈信息
    val taskMetrics: TaskMetrics = null, // Task度量信息
    private[spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty) { //存储任务本地行偏好
  /**
    * DAGScheduler将当前Stage提交给TaskScheduler的时间
    * When this stage was submitted from the DAGScheduler to a TaskScheduler. */
  var submissionTime: Option[Long] = None
  /**
    * Stage中所有Task完成的时间,或Stage被取消的时间
    * Time when all tasks in the stage completed or when the stage was cancelled. */
  var completionTime: Option[Long] = None
  /**
    * 如果Stage失败,记录原因
    * If the stage failed, the reason why. */
  var failureReason: Option[String] = None

  /**
   * Stage更新过的聚合器的最终值,包含所有用户定义的聚合器
    * Terminal values of accumulables updated during this stage, including all the user-defined
   * accumulators.
   */
  val accumulables = HashMap[Long, AccumulableInfo]()
  // 保存失败原因和完成时间
  def stageFailed(reason: String) {
    failureReason = Some(reason)
    completionTime = Some(System.currentTimeMillis)
  }

  def attemptNumber(): Int = attemptId

  private[spark] def getStatusString: String = {
    if (completionTime.isDefined) {
      if (failureReason.isDefined) {
        "failed"
      } else {
        "succeeded"
      }
    } else {
      "running"
    }
  }
}

private[spark] object StageInfo {
  /**
   * 用于在Stage中构建StageInfo.每一个Stage都关联了一个或多个RDD,和shuffle依赖为Stage标记的边界.
    * 因此，通过一系列窄依赖关系与此Stage的RDD相关的所有祖先RDD也应该与此阶段相关联.
    * Construct a StageInfo from a Stage.
   *
   * Each Stage is associated with one or many RDDs, with the boundary of a Stage marked by
   * shuffle dependencies. Therefore, all ancestor RDDs related to this Stage's RDD through a
   * sequence of narrow dependencies should also be associated with this Stage.
   */
  def fromStage(
      stage: Stage,
      attemptId: Int,
      numTasks: Option[Int] = None,
      taskMetrics: TaskMetrics = null,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty
    ): StageInfo = {
    // 获取RDD的祖先以来中属于窄依赖的RDD序列.对于ancestorRddInfos中的每个RDD,创建RDDInfo对象
    val ancestorRddInfos = stage.rdd.getNarrowAncestors.map(RDDInfo.fromRdd)
    // 给当前Stage的RDD创建对应的RDDInfo,然后结合上一步的祖先RDDInfo列表
    val rddInfos = Seq(RDDInfo.fromRdd(stage.rdd)) ++ ancestorRddInfos
    // 创建StageInfo对象
    new StageInfo(
      stage.id,
      attemptId,
      stage.name,
      numTasks.getOrElse(stage.numTasks),
      rddInfos,
      stage.parents.map(_.id),
      stage.details,
      taskMetrics,
      taskLocalityPreferences)
  }
}

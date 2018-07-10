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
// scalastyle:off
/**
 * An interface for sort algorithm
 * FIFO: FIFO algorithm between TaskSetManagers
 * FS: FS algorithm between Pools, and FIFO or FS within Pools
 */
private[spark] trait SchedulingAlgorithm {
  def comparator(s1: Schedulable, s2: Schedulable): Boolean
}

private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val priority1 = s1.priority
    val priority2 = s2.priority
    // 比较s1和s2的优先级
    var res = math.signum(priority1 - priority2)
    if (res == 0) {
      val stageId1 = s1.stageId
      val stageId2 = s2.stageId
      // 如果优先级相等,那么比较stageId
      res = math.signum(stageId1 - stageId2)
    }
    // 如果小于0,优先s1,否则s2
    res < 0
  }
}

private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
  // 如果s1中的runningTask数量小于s1的minShare,并且S2中runningTask数量
  // 大于等于S2中的minShare,优先s1.
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val minShare1 = s1.minShare
    val minShare2 = s2.minShare
    val runningTasks1 = s1.runningTasks
    val runningTasks2 = s2.runningTasks
    val s1Needy = runningTasks1 < minShare1
    val s2Needy = runningTasks2 < minShare2
    val minShareRatio1 = runningTasks1.toDouble / math.max(minShare1, 1.0)
    val minShareRatio2 = runningTasks2.toDouble / math.max(minShare2, 1.0)
    val taskToWeightRatio1 = runningTasks1.toDouble / s1.weight.toDouble
    val taskToWeightRatio2 = runningTasks2.toDouble / s2.weight.toDouble

    var compare = 0

    if (s1Needy && !s2Needy) {
      // 如果s1中的runningTask数量小于s1的minShare,并且S2中runningTask数量
      // 大于等于S2中的minShare,优先s1.
      return true
    } else if (!s1Needy && s2Needy) {
      // 和上面相反,优先s2
      return false
    } else if (s1Needy && s2Needy) {
      // 如果s1和s2的runningTask<minShare,求runningTasks/math.max(minShare, 1.0)
      compare = minShareRatio1.compareTo(minShareRatio2)
    } else {
      // 如果s1和s2的runningTask>=minShare,求runningTasks / s.weight
      compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
    }
    // 然后比较compare
    if (compare < 0) {
      // 小于0,优先s1
      true
    } else if (compare > 0) {
      // 大于0 优先s2
      false
    } else {
      // 仍然相等,比较名称
      s1.name < s2.name
    }
  }
}


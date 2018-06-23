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

/**
 * A client that communicates with the cluster manager to request or kill executors.
 * This is currently supported only in YARN mode.
 */
private[spark] trait ExecutorAllocationClient {


  /** Get the list of currently active executors */
  private[spark] def getExecutorIds(): Seq[String]

  /**
   * 根据计划需要更新cluster manager,包含三bit的信息帮助其做决定<br>
    * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   * @param numExecutors 我们想要的executor的总数量.cluster manager不应该杀死正在运行的executor来达到这个数值.
    *                     但是如果所有存在的executor已经死亡,我们想要的数量就可以分配了.<br>
    *                     The total number of executors we'd like to have. The cluster manager
   *                     shouldn't kill any running executor to reach this number, but,
   *                     if all existing executors were to die, this is the number of executors
   *                     we'd want to be allocated.
   * @param localityAwareTasks  所有拥有本地性偏好的活动阶段的task数量,包含运行的,挂起的,完成的.<br>
    *                           The number of tasks in all active stages that have a locality
   *                           preferences. This includes running, pending, and completed tasks.
   * @param hostToLocalTaskCount host与想要在本台机器运行task的数量之间的映射.<br>
    *                             A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   * @return 请求是否通知了cluster manager<br>
    *           whether the request is acknowledged by the cluster manager.
   */
  private[spark] def requestTotalExecutors(
      numExecutors: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int]): Boolean

  /**
   * Request an additional number of executors from the cluster manager.
   * @return whether the request is acknowledged by the cluster manager.
   */
  def requestExecutors(numAdditionalExecutors: Int): Boolean

  /**
   * Request that the cluster manager kill the specified executors.
   *
   * When asking the executor to be replaced, the executor loss is considered a failure, and
   * killed tasks that are running on the executor will count towards the failure limits. If no
   * replacement is being requested, then the tasks will not count towards the limit.
   *
   * @param executorIds identifiers of executors to kill
   * @param replace whether to replace the killed executors with new ones, default false
   * @param force whether to force kill busy executors, default false
   * @return the ids of the executors acknowledged by the cluster manager to be removed.
   */
  def killExecutors(
    executorIds: Seq[String],
    replace: Boolean = false,
    force: Boolean = false): Seq[String]

  /**
   * Request that the cluster manager kill every executor on the specified host.
   *
   * @return whether the request is acknowledged by the cluster manager.
   */
  def killExecutorsOnHost(host: String): Boolean

  /**
   * Request that the cluster manager kill the specified executor.
   * @return whether the request is acknowledged by the cluster manager.
   */
  def killExecutor(executorId: String): Boolean = {
    val killedExecutors = killExecutors(Seq(executorId))
    killedExecutors.nonEmpty && killedExecutors(0).equals(executorId)
  }
}

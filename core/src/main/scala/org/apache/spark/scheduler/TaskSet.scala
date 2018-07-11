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

import java.util.Properties

/**
 * DAGScheduler将Task提交给TaskScheduler时,将多个Task打包成TaskSet,该类对象是Task调度管理的基本单位.
  * A set of tasks submitted together to the low-level TaskScheduler, usually representing
 * missing partitions of a particular stage.
 */
private[spark] class TaskSet(
    val tasks: Array[Task[_]],  // Task数组
    val stageId: Int,           // stageId,所属stage
    val stageAttemptId: Int,    // stage尝试id
    val priority: Int,          // 优先级,通常以JobId作为优先级
    val properties: Properties) { // job有关调度,job group,描述等属性
  val id: String = stageId + "." + stageAttemptId

  override def toString: String = "TaskSet " + id
}

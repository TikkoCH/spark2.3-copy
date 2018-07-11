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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * 表示池或TaskSetManager的集合的可调度实体
  * A Schedulable entity that represents collection of Pools or TaskSetManagers
 */
private[spark] class Pool(
    val poolName: String,  // 名称
    val schedulingMode: SchedulingMode,  // 调度模式,FAIR,FIFO,NONE三种
    initMinShare: Int, // minShare初始值
    initWeight: Int)   // 初始权重
  extends Schedulable with Logging {
  // 用于存储Schedulable
  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  // 调度名称与Schedulable对应关系
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  // 用于公平调度算法的权重
  val weight = initWeight
  // 用于公平调度算法的参考值
  val minShare = initMinShare
  // 当前正在运行的任务数量
  var runningTasks = 0
  // 进行调度的优先级
  val priority = 0
  // 池的stageid,用于打破调度关系.
  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1
  val name = poolName
  // 当前pool的父pool
  var parent: Pool = null

  private val taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
      case _ =>
        val msg = s"Unsupported scheduling mode: $schedulingMode. Use FAIR or FIFO instead."
        throw new IllegalArgumentException(msg)
    }
  }
  /** 用于将Schedulable添加到schedulableQueue和schedulableNameToSchedulable中,
    * 并将参数中Schedulable中的parent设置为this对象.
    * */
  override def addSchedulable(schedulable: Schedulable) {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }
  /**
    * 移除schedulableQueue和schedulableNameToSchedulable中指定的Schedulable
    * */
  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }
  /**
    * 根据指定名称查找Schedulable.
    * */
  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      // 先从缓存中查找
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue.asScala) {
      // 从子Schedulable中查找,递归
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }
  /** 当某个Executor丢失后,调用当前Pool的schedulableQueue中的各个Schedulable的
    * executorLost方法.
    * */
  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason) {
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }
  /** 检查当前Pool中是否有需要推断执行的任务.实际调用子Schedulable中的checkSpeculableTasks*/
  override def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks(minTimeToSpeculation)
    }
    shouldRevive
  }
  /** 用于对当前Pool中的所有TaskSetManager按照调度算法进行排序,并返回排序后的TaskManager.*/
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    // 排序
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    // 每个子Schedulable的getSortedTaskSetQueue添加到sortedTaskSetQueue
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue // 返回
  }
  /** 增加当前Pool及其父Pool中记录的当前正在运行的task数量*/
  def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }
  /** 减少当前Pool及其父Pool中记录的当前正在运行的task数量*/
  def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}

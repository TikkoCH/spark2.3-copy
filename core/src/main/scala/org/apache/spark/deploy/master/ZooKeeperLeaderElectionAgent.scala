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

package org.apache.spark.deploy.master

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.internal.Logging

private[master] class ZooKeeperLeaderElectionAgent(val masterInstance: LeaderElectable,
    conf: SparkConf) extends LeaderLatchListener with LeaderElectionAgent with Logging  {
  /** zk的工作目录*/
  val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/leader_election"
  /** zk客户端*/
  private var zk: CuratorFramework = _
  /** 使用zk进行领导选举的客户端*/
  private var leaderLatch: LeaderLatch = _
  /** 领导选举的状态,有领导和无领导两个状态*/
  private var status = LeadershipStatus.NOT_LEADER

  start()
  /** 启动基于zk的领导选举*/
  private def start() {
    // 本类实现了LeaderLatchListener,领导选举时,leaderLatch会回调isLeader或notLeader方法
    logInfo("Starting ZooKeeper LeaderElection agent")
    zk = SparkCuratorUtil.newClient(conf)
    leaderLatch = new LeaderLatch(zk, WORKING_DIR)
    leaderLatch.addListener(this)
    leaderLatch.start()
  }
  // 关闭leaderLatch和zk客户端
  override def stop() {
    leaderLatch.close()
    zk.close()
  }
  /** 告知ZooKeeperLeaderElectionAgent所属master节点被选为领导,更新领导关系状态*/
  override def isLeader() {
    synchronized {
      // could have lost leadership by now.
      if (!leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have gained leadership")
      updateLeadershipStatus(true)
    }
  }
  /** 告知ZooKeeperLeaderElectionAgent所属master节点没被选为领导,更新领导关系状态*/
  override def notLeader() {
    synchronized {
      // could have gained leadership by now.
      if (leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have lost leadership")
      updateLeadershipStatus(false)
    }
  }
  /** 更新领导关系状态*/
  private def updateLeadershipStatus(isLeader: Boolean) {
    // 如果master节点之前不是领导
    if (isLeader && status == LeadershipStatus.NOT_LEADER) {
      status = LeadershipStatus.LEADER
      // 被选为领导
      masterInstance.electedLeader()
    } else if (!isLeader && status == LeadershipStatus.LEADER) {
      // master节点之前是领导,当没有被选为领导时,将其状态设置为NOT_LEADER
      status = LeadershipStatus.NOT_LEADER
      // 撤销Master领导关系
      masterInstance.revokedLeadership()
    }
  }

  private object LeadershipStatus extends Enumeration {
    type LeadershipStatus = Value
    val LEADER, NOT_LEADER = Value
  }
}

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

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, ReliableRDDCheckpointData}
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, ThreadUtils, Utils}

/**
 * 用于清理任务的类
  * Classes that represent cleaning tasks.
 */
private sealed trait CleanupTask
private case class CleanRDD(rddId: Int) extends CleanupTask
private case class CleanShuffle(shuffleId: Int) extends CleanupTask
private case class CleanBroadcast(broadcastId: Long) extends CleanupTask
private case class CleanAccum(accId: Long) extends CleanupTask
private case class CleanCheckpoint(rddId: Int) extends CleanupTask

/**
 * 与清理任务关联的弱引用,当引用对象变成只有弱可达性时,相应的CleanupTaskWeakReference是自动添加到
  * 指定的引用队列
  * A WeakReference associated with a CleanupTask.
 *
 * When the referent object becomes only weakly reachable, the corresponding
 * CleanupTaskWeakReference is automatically added to the given reference queue.
 */
private class CleanupTaskWeakReference(
    val task: CleanupTask,
    referent: AnyRef,
    referenceQueue: ReferenceQueue[AnyRef])
  extends WeakReference(referent, referenceQueue)

/**
 * rdd,shuffle和广播状态的异步cleaner.这保存了每个rdd,shuffle依赖和订阅广播的弱引用.
  * 当关联对象
  * An asynchronous cleaner for RDD, shuffle, and broadcast state.
 *
 * This maintains a weak reference for each RDD, ShuffleDependency, and Broadcast of interest,
 * to be processed when the associated object goes out of scope of the application. Actual
 * cleanup is performed in a separate daemon thread.
 */
private[spark] class ContextCleaner(sc: SparkContext) extends Logging {

  /**
   * 用来缓存虚引用,不放进reference queue来防止被回收
    * A buffer to ensure that `CleanupTaskWeakReference`s are not garbage collected as long as they
   * have not been handled by the reference queue.
   */
  private val referenceBuffer =
    Collections.newSetFromMap[CleanupTaskWeakReference](new ConcurrentHashMap)
  /** 缓存顶级AnyRef引用 */
  private val referenceQueue = new ReferenceQueue[AnyRef]
  // 清理工作的监听器数组
  private val listeners = new ConcurrentLinkedQueue[CleanerListener]()
  // 用于具体清理工作的线程.为守护线程,名称为spark Context Cleaner
  private val cleaningThread = new Thread() { override def run() { keepCleaning() }}
  // 用于执行GC的调度线程池,此线程池只包含一个线程,启动的线程名称以context-cleaner-periodic-gc开头
  private val periodicGCService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("context-cleaner-periodic-gc")

  /**
   * 执行gc的间隔,可通过spark.cleaner.periodicGC.interval属性配置,默认是30分钟.
    * 只有当弱引用被垃圾收集时，这个上下文清理器才会触发清理。
    * 在具有大型驱动程序JVM的长时间运行的应用程序中，驱动程序的内存压力很小，
    * 这可能偶尔会发生，或根本不会发生。 根本不清理可能会导致执行者在一段时间后耗尽磁盘空间。<br>
    * How often to trigger a garbage collection in this JVM.
   *
   * This context cleaner triggers cleanups only when weak references are garbage collected.
   * In long-running applications with large driver JVMs, where there is little memory pressure
   * on the driver, this may happen very occasionally or not at all. Not cleaning at all may
   * lead to executors running out of disk space after a while.
   */
  private val periodicGCInterval =
    sc.conf.getTimeAsSeconds("spark.cleaner.periodicGC.interval", "30min")

  /**
   * 设置清理非shuffle的其他数据是否是阻塞的,spark.cleaner.referenceTracking.blocking.shuffle默认为true.
    *
    * 由于SPARK-3015(该bug会产生一个死锁,好奇的话可以查看SPARK-3015描述),这个值默认设置为true.
    * 这只是针对该问题的暂时解决方法，最终是由BlockManager端点在高频率下发出互相阻止RPC消息的方式造成的。
    * 例如，当驱动程序执行GC并清除不在范围内的所有广播块时，会发生这种情况。
    * Whether the cleaning thread will block on cleanup tasks (other than shuffle, which
   * is controlled by the `spark.cleaner.referenceTracking.blocking.shuffle` parameter).
   *
   * Due to SPARK-3015, this is set to true by default. This is intended to be only a temporary
   * workaround for the issue, which is ultimately caused by the way the BlockManager endpoints
   * issue inter-dependent blocking RPC messages to each other at high frequencies. This happens,
   * for instance, when the driver performs a GC and cleans up all broadcast blocks that are no
   * longer in scope.
   */
  private val blockOnCleanupTasks = sc.conf.getBoolean(
    "spark.cleaner.referenceTracking.blocking", true)

  /**
   * 清理shuffle数据是否是阻塞式的.可通过spark.cleaner.referenceTracking.blocking.shuffle属性设置,默认是false
    * 清理shuffle数据包括清理MapOutputTracker中指定的ShuffleId对应的map任务状态
    * 和ShuffleManager中注册的ShuffleId对应的Shuffle元数据.
    * 当上下文清理器被配置为阻塞每个删除请求时，它可能在清理混洗块时抛出超时异常，如SPARK-3139中所报告的。
    * 为了避免这种情况，默认情况下该参数禁用随机清理上的阻塞。 请注意，这不会影响RDD和广播的清理。
    * 这是一个临时解决方法，直到真正的RPC问题（在上面的注释中提到的`blockOnCleanupTasks`）被解决。
    * Whether the cleaning thread will block on shuffle cleanup tasks.
   *
   * When context cleaner is configured to block on every delete request, it can throw timeout
   * exceptions on cleanup of shuffle blocks, as reported in SPARK-3139. To avoid that, this
   * parameter by default disables blocking on shuffle cleanups. Note that this does not affect
   * the cleanup of RDDs and broadcasts. This is intended to be a temporary workaround,
   * until the real RPC issue (referred to in the comment above `blockOnCleanupTasks`) is
   * resolved.
   */
  private val blockOnShuffleCleanupTasks = sc.conf.getBoolean(
    "spark.cleaner.referenceTracking.blocking.shuffle", false)

  @volatile private var stopped = false

  /** Attach a listener object to get information of when objects are cleaned. */
  def attachListener(listener: CleanerListener): Unit = {
    listeners.add(listener)
  }

  /** Start the cleaner. */
  def start(): Unit = {
    cleaningThread.setDaemon(true)  // 将cleaningThread设置为守护线程
    cleaningThread.setName("Spark Context Cleaner") // 将其命名为Spark Context Cleaner
    cleaningThread.start()  // 启动cleaner
    // 为periodicGCService设置以periordicGCInternal作为间隔执行GC操作的任务.
    periodicGCService.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = System.gc()
    }, periodicGCInterval, periodicGCInterval, TimeUnit.SECONDS)
  }

  /**
   * Stop the cleaning thread and wait until the thread has finished running its current task.
   */
  def stop(): Unit = {
    stopped = true
    // Interrupt the cleaning thread, but wait until the current task has finished before
    // doing so. This guards against the race condition where a cleaning thread may
    // potentially clean similarly named variables created by a different SparkContext,
    // resulting in otherwise inexplicable block-not-found exceptions (SPARK-6132).
    synchronized {
      cleaningThread.interrupt()
    }
    cleaningThread.join()
    periodicGCService.shutdown()
  }
  // 总体来说还是监听器模式,异步线程处理.
  /** Register an RDD for cleanup when it is garbage collected. */
  def registerRDDForCleanup(rdd: RDD[_]): Unit = {
    registerForCleanup(rdd, CleanRDD(rdd.id))
  }

  def registerAccumulatorForCleanup(a: AccumulatorV2[_, _]): Unit = {
    registerForCleanup(a, CleanAccum(a.id))
  }

  /** Register a ShuffleDependency for cleanup when it is garbage collected. */
  def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _, _]): Unit = {
    registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
  }

  /** Register a Broadcast for cleanup when it is garbage collected. */
  def registerBroadcastForCleanup[T](broadcast: Broadcast[T]): Unit = {
    registerForCleanup(broadcast, CleanBroadcast(broadcast.id))
  }

  /** Register a RDDCheckpointData for cleanup when it is garbage collected. */
  def registerRDDCheckpointDataForCleanup[T](rdd: RDD[_], parentId: Int): Unit = {
    registerForCleanup(rdd, CleanCheckpoint(parentId))
  }

  /** Register an object for cleanup. */
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit = {
    referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue))
  }

  /**
    * 持续清理RDD,shuffle,和广播状态
    * Keep cleaning RDD, shuffle, and broadcast state. */
  private def keepCleaning(): Unit = Utils.tryOrStopSparkContext(sc) {
    while (!stopped) {
      try {
        val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        // Synchronize here to avoid being interrupted on stop()
        // 这里是避免调用Stop方法打断清理工作
        synchronized {
          reference.foreach { ref =>
            logDebug("Got cleaning task " + ref.task)
            referenceBuffer.remove(ref)
            // 模式匹配,对不同的引用执行不同的清理工作
            ref.task match {
              case CleanRDD(rddId) =>
                doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
              case CleanShuffle(shuffleId) =>
                doCleanupShuffle(shuffleId, blocking = blockOnShuffleCleanupTasks)
              case CleanBroadcast(broadcastId) =>
                doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
              case CleanAccum(accId) =>
                doCleanupAccum(accId, blocking = blockOnCleanupTasks)
              case CleanCheckpoint(rddId) =>
                doCleanCheckpoint(rddId)
            }
          }
        }
      } catch {
        case ie: InterruptedException if stopped => // ignore
        case e: Exception => logError("Error in cleaning thread", e)
      }
    }
  }

  /** Perform RDD cleanup. */
  def doCleanupRDD(rddId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning RDD " + rddId)  // 日志记录清理的RDDId
      sc.unpersistRDD(rddId, blocking)  // 从persistentRdds中移除RDD的跟踪
      // 监视器中的rddCleaned方法 源码:toBeCleanedRDDIds是一个hashSet.
      // toBeCleanedRDDIds.synchronized { toBeCleanedRDDIds -= rddId }
      listeners.asScala.foreach(_.rddCleaned(rddId))
      logInfo("Cleaned RDD " + rddId)
    } catch {
      case e: Exception => logError("Error cleaning RDD " + rddId, e)
    }
  }

  /** Perform shuffle cleanup. */
  def doCleanupShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning shuffle " + shuffleId)
      mapOutputTrackerMaster.unregisterShuffle(shuffleId)
      blockManagerMaster.removeShuffle(shuffleId, blocking)
      listeners.asScala.foreach(_.shuffleCleaned(shuffleId))
      logInfo("Cleaned shuffle " + shuffleId)
    } catch {
      case e: Exception => logError("Error cleaning shuffle " + shuffleId, e)
    }
  }

  /** Perform broadcast cleanup. */
  def doCleanupBroadcast(broadcastId: Long, blocking: Boolean): Unit = {
    try {
      logDebug(s"Cleaning broadcast $broadcastId")
      broadcastManager.unbroadcast(broadcastId, true, blocking)
      listeners.asScala.foreach(_.broadcastCleaned(broadcastId))
      logDebug(s"Cleaned broadcast $broadcastId")
    } catch {
      case e: Exception => logError("Error cleaning broadcast " + broadcastId, e)
    }
  }

  /** Perform accumulator cleanup. */
  def doCleanupAccum(accId: Long, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning accumulator " + accId)
      AccumulatorContext.remove(accId)
      listeners.asScala.foreach(_.accumCleaned(accId))
      logInfo("Cleaned accumulator " + accId)
    } catch {
      case e: Exception => logError("Error cleaning accumulator " + accId, e)
    }
  }

  /**
   * Clean up checkpoint files written to a reliable storage.
   * Locally checkpointed files are cleaned up separately through RDD cleanups.
   */
  def doCleanCheckpoint(rddId: Int): Unit = {
    try {
      logDebug("Cleaning rdd checkpoint data " + rddId)
      ReliableRDDCheckpointData.cleanCheckpoint(sc, rddId)
      listeners.asScala.foreach(_.checkpointCleaned(rddId))
      logInfo("Cleaned rdd checkpoint data " + rddId)
    }
    catch {
      case e: Exception => logError("Error cleaning rdd checkpoint data " + rddId, e)
    }
  }

  private def blockManagerMaster = sc.env.blockManager.master
  private def broadcastManager = sc.env.broadcastManager
  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}

private object ContextCleaner {
  private val REF_QUEUE_POLL_TIMEOUT = 100
}

/**
 * Listener class used for testing when any item has been cleaned by the Cleaner class.
 */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int): Unit
  def shuffleCleaned(shuffleId: Int): Unit
  def broadcastCleaned(broadcastId: Long): Unit
  def accumCleaned(accId: Long): Unit
  def checkpointCleaned(rddId: Long): Unit
}

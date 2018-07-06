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

package org.apache.spark.storage

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * 负责为逻辑的Block与数据写入磁盘的位置之间建立逻辑映射关系.一个block对应一个BlockId作为名称的文件.
  * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 */
private[spark] class DiskBlockManager(conf: SparkConf,
  // 停止DiskBlockManager时是否删除本地目录,如果spark.shuffle.service.enabled=false或当前实例是driver,
                                      // 该属性为true
                                      deleteFilesOnStop: Boolean) extends Logging {
  // 磁盘存储的本地子目录数量.由spark.diskStore.subDirectories属性配置,默认64
  private[spark] val subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories", 64)

  /**
    * 本地目录数组.为spark.local.dir属性中包含的每个路径创建一个本地目录;在该目录之中,创建多个子目录,
    * 我们将文件用哈希函数分散到这些子目录中,这是为了避免在顶层目录有大量的索引节点.
    * Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }
  // 本地子目录的二维数组.
  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))
  // 在初始化时,调用addShutdownHook()为DiskBlockManager设置关闭钩子.
  private val shutdownHook = addShutdownHook()

  /**
    * 根据指定的文件名获取文件.
    * Looks up a file by hashing it into one of our local subdirectories. */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
  def getFile(filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    // 获取文件名的非负hash值
    val hash = Utils.nonNegativeHash(filename)
    // 取余获取一级目录
    val dirId = hash % localDirs.length
    // 取余获取耳机目录
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

    // Create the subdirectory if it doesn't already exist
    // 如果不存在二级目录,创建二级目录
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {
        old   // 如果存在,直接赋值
      } else {
        // 如果不存在,创建目录
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException(s"Failed to create local dir in $newDir.")
        }
        // 文件二维数组缓存中添加目录
        subDirs(dirId)(subDirId) = newDir
        // 赋值给新创建的目录
        newDir
      }
    }
    // 返回二级目录下的文件
    new File(subDir, filename)
  }
  /** 重载方法,根据BlockId获取文件*/
  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /**
    * 检查本地目录是否包含BlockId对应的文件.
    * Check if disk block manager has a block. */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  /**
    * 获取本地目录所有文件
    * List all the files currently stored on disk by the disk manager. */
  def getAllFiles(): Seq[File] = {
    // 获取所有目录
    // Get all the files inside the array of array of directories
    subDirs.flatMap { dir =>
      dir.synchronized {// 同步加克隆,确保线程安全.
        // 拷贝目录内容,因为可能被其他线程修改
        // Copy the content of dir because it may be modified in other threads
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files else Seq.empty
    }
  }

  /**
    * 获取本地目录中所有Block的BlockId
    * List all the blocks currently stored on disk by the disk manager. */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().flatMap { f =>
      try {
        Some(BlockId(f.getName))
      } catch {
        case _: UnrecognizedBlockId =>
          // 跳过不对应Block的文件,如SortShuffleWriter创建的临时文件
          // Skip files which do not correspond to blocks, for example temporary
          // files created by [[SortShuffleWriter]].
          None
      }
    }
  }

  /**
    * 生成一个唯一的BlockID和适合存储本地中间结果的文件。
    * Produces a unique block id and File suitable for storing local intermediate results. */
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = new TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /**
    * 创建唯一的BlockId和文件用于存储Shuffle的中间结果(map任务输出).
    * Produces a unique block id and File suitable for storing shuffled intermediate results. */
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = new TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempShuffleBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /**
   * 创建用于存储Block数据的本地文件夹.这些目录位于已配置的本地目录中并且使用外部shuffle服务时,
    * 不会随着jvm退出被删除掉.
    * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   */
  private def createLocalDirs(conf: SparkConf): Array[File] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }

  private def addShutdownHook(): AnyRef = {
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop()
    }
  }

  /**
    * 清空本地文件夹并且停止shuffle的发送
    * Cleanup local dirs and stop shuffle sender. */
  private[spark] def stop() {
    // 删除shutdown钩子,如果不删的话会内存泄漏
    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    try {
      // 移除
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }

  private def doStop(): Unit = {
    // 是否在停止时删除属性确定是否删除,如果创建本类对象时候设置为true,删除
    if (deleteFilesOnStop) {
      localDirs.foreach { localDir =>
        // 如果是文件夹并且文件夹存在
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              // 递归删除
              Utils.deleteRecursively(localDir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
  }
}

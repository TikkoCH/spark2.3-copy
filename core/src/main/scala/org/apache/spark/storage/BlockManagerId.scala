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

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

/**
 * 该类对象代表一个BlockManager的唯一标识.本类中前两个构造器声明为private来确保本类对象
  * 只能通过半生对象的apply方法来创建.构造器中的参数也是私有的,所以外部无法改变参数.
  * :: DeveloperApi ::
 * This class represent an unique identifier for a BlockManager.
 *
 * The first 2 constructors of this class are made private to ensure that BlockManagerId objects
 * can be created only using the apply method in the companion object. This allows de-duplication
 * of ID objects. Also, constructor parameters are private to ensure that parameters cannot be
 * modified from outside this class.
 */
@DeveloperApi
class BlockManagerId private (
    // 当前BlockManager所在实例的Id,如果是Driver,id为driver,否则Master负责给Executor分配
    // 格式为app-日期-数字
    private var executorId_ : String,
    private var host_ : String, // 主机域名或ip
    private var port_ : Int,  // 端口号,使用了BlockManager中BlockTransferService对外服务的端口
    private var topologyInfo_ : Option[String]) // 拓扑信息
  extends Externalizable {

  private def this() = this(null, null, 0, None)  // For deserialization only
  // 返回executorId的值
  def executorId: String = executorId_
  // 检查host地址
  if (null != host_) {
    Utils.checkHost(host_)
    assert (port_ > 0)
  }
  // 获取域名端口
  def hostPort: String = {
    // DEBUG code
    Utils.checkHost(host)
    assert (port > 0)
    host + ":" + port
  }
  // 获取host
  def host: String = host_
  // 获取port
  def port: Int = port_
  // 获取topologyInfo
  def topologyInfo: Option[String] = topologyInfo_
  // 判断BlockManager所在实例是否是Driver
  def isDriver: Boolean = {
    executorId == SparkContext.DRIVER_IDENTIFIER ||
      executorId == SparkContext.LEGACY_DRIVER_IDENTIFIER
  }
  /**将BlockManagerId的所有信息序列化后写到外部二进制流ObjectOutput*/
  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeUTF(executorId_)
    out.writeUTF(host_)
    out.writeInt(port_)
    out.writeBoolean(topologyInfo_.isDefined)
    // we only write topologyInfo if we have it
    topologyInfo.foreach(out.writeUTF(_))
  }
  /** 从外部二进制流读取BlockManagerId的所有信息*/
  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    executorId_ = in.readUTF()
    host_ = in.readUTF()
    port_ = in.readInt()
    val isTopologyInfoAvailable = in.readBoolean()
    topologyInfo_ = if (isTopologyInfoAvailable) Option(in.readUTF()) else None
  }

  @throws(classOf[IOException])
  private def readResolve(): Object = BlockManagerId.getCachedBlockManagerId(this)

  override def toString: String = s"BlockManagerId($executorId, $host, $port, $topologyInfo)"

  override def hashCode: Int =
    ((executorId.hashCode * 41 + host.hashCode) * 41 + port) * 41 + topologyInfo.hashCode

  override def equals(that: Any): Boolean = that match {
    case id: BlockManagerId =>
      executorId == id.executorId &&
        port == id.port &&
        host == id.host &&
        topologyInfo == id.topologyInfo
    case _ =>
      false
  }
}


private[spark] object BlockManagerId {

  /**
   * Returns a [[org.apache.spark.storage.BlockManagerId]] for the given configuration.
   *
   * @param execId ID of the executor.
   * @param host Host name of the block manager.
   * @param port Port of the block manager.
   * @param topologyInfo topology information for the blockmanager, if available
   *                     This can be network topology information for use while choosing peers
   *                     while replicating data blocks. More information available here:
   *                     [[org.apache.spark.storage.TopologyMapper]]
   * @return A new [[org.apache.spark.storage.BlockManagerId]].
   */
  def apply(
      execId: String,
      host: String,
      port: Int,
      topologyInfo: Option[String] = None): BlockManagerId =
    getCachedBlockManagerId(new BlockManagerId(execId, host, port, topologyInfo))

  def apply(in: ObjectInput): BlockManagerId = {
    val obj = new BlockManagerId()
    obj.readExternal(in)
    getCachedBlockManagerId(obj)
  }

  val blockManagerIdCache = new ConcurrentHashMap[BlockManagerId, BlockManagerId]()

  def getCachedBlockManagerId(id: BlockManagerId): BlockManagerId = {
    blockManagerIdCache.putIfAbsent(id, id)
    blockManagerIdCache.get(id)
  }
}

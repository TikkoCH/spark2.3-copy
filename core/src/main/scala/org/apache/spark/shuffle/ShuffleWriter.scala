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

package org.apache.spark.shuffle

import java.io.IOException

import org.apache.spark.scheduler.MapStatus

/**
 * 从maptask中获得用于将记录写入shuffle系统
  * Obtained inside a map task to write out records to the shuffle system.
 */
private[spark] abstract class ShuffleWriter[K, V] {
  /**
    * 将map任务的结果写入到磁盘
    * Write a sequence of records to this task's output */
  @throws[IOException]
  def write(records: Iterator[Product2[K, V]]): Unit

  /**
    * 用于关闭ShuffleWriter
    * Close this writer, passing along whether the map completed */
  def stop(success: Boolean): Option[MapStatus]
}

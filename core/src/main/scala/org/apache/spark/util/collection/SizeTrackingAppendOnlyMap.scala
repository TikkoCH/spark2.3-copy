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

package org.apache.spark.util.collection

/**
 * AppendOnlyMap和SizeTracker结合实现体.既可以在内存中对任务执行结果进行更新
  * 和聚合运算,也可以对自身大小进行样本采集和大小估算
  * An append-only map that keeps track of its estimated size in bytes.
 */
private[spark] class SizeTrackingAppendOnlyMap[K, V]
  extends AppendOnlyMap[K, V] with SizeTracker
{
  override def update(key: K, value: V): Unit = {
    // 更新
    super.update(key, value)
    // 完成采样
    super.afterUpdate()
  }

  override def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    // 聚合
    val newValue = super.changeValue(key, updateFunc)
    // 完成采样
    super.afterUpdate()
    newValue
  }

  override protected def growTable(): Unit = {
    // 扩容
    super.growTable()
    // 样本重置
    resetSamples()
  }
}

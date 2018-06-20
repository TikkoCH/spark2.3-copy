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

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source
// scalastyle:off
/**
  * 集成应用程序的性能监测度量
  * @param application
  */
private[master] class ApplicationSource(val application: ApplicationInfo) extends Source {
  override val metricRegistry = new MetricRegistry() //metrics的api
  override val sourceName = "%s.%s.%s".format("application", application.desc.name,
    System.currentTimeMillis())//source的名称
  //status 有这几个枚举状态:WAITING, RUNNING, FINISHED, FAILED, KILLED, UNKNOWN
  metricRegistry.register(MetricRegistry.name("status"), new Gauge[String] {
    override def getValue: String = application.state.toString
  })
  //集成运行时间的监测
  metricRegistry.register(MetricRegistry.name("runtime_ms"), new Gauge[Long] {
    override def getValue: Long = application.duration
  })
  //集成授权核心的监测
  metricRegistry.register(MetricRegistry.name("cores"), new Gauge[Int] {
    override def getValue: Int = application.coresGranted
  })

}

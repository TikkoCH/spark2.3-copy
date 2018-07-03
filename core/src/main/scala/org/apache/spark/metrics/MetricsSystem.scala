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

package org.apache.spark.metrics
// scalastyle:off
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import com.codahale.metrics.{Metric, MetricFilter, MetricRegistry}
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.metrics.sink.{MetricsServlet, Sink}
import org.apache.spark.metrics.source.{Source, StaticSources}
import org.apache.spark.util.Utils

/**
 * Spark Metrics System, created by a specific "instance", combined by source,
 * sink, periodically polls source metrics data to sink destinations.
 *
 * "instance" specifies "who" (the role) uses the metrics system. In Spark, there are several roles
 * like master, worker, executor, client driver. These roles will create metrics system
 * for monitoring. So, "instance" represents these roles. Currently in Spark, several instances
 * have already implemented: master, worker, executor, driver, applications.
 *
 * "source" specifies "where" (source) to collect metrics data from. In metrics system, there exists
 * two kinds of source:
 *   1. Spark internal source, like MasterSource, WorkerSource, etc, which will collect
 *   Spark component's internal state, these sources are related to instance and will be
 *   added after a specific metrics system is created.
 *   2. Common source, like JvmSource, which will collect low level state, is configured by
 *   configuration and loaded through reflection.
 *
 * "sink" specifies "where" (destination) to output metrics data to. Several sinks can
 * coexist and metrics can be flushed to all these sinks.
 *
 * Metrics configuration format is like below:
 * [instance].[sink|source].[name].[options] = xxxx
 *
 * [instance] can be "master", "worker", "executor", "driver", "applications" which means only
 * the specified instance has this property.
 * wild card "*" can be used to replace instance name, which means all the instances will have
 * this property.
 *
 * [sink|source] means this property belongs to source or sink. This field can only be
 * source or sink.
 *
 * [name] specify the name of sink or source, if it is custom defined.
 *
 * [options] represent the specific property of this source or sink.
 */
private[spark] class MetricsSystem private (
    val instance: String,  // 实例名称
    conf: SparkConf,
    securityMgr: SecurityManager)
  extends Logging {
  // 度量系统的配置信息.MetricsConfig提供了对配置的设置,加载,转换等功能.其中也包含Sink和Source.
  private[this] val metricsConfig = new MetricsConfig(conf)
  // 参考flume的sink概念,就是输出目的地的意思
  private val sinks = new mutable.ArrayBuffer[Sink]
  // 参考flume的source概念,就是数据源.
  private val sources = new mutable.ArrayBuffer[Source]
  // 用于度量系统的注册.source和sink都是通过其注册到度量仓库中的.
  private val registry = new MetricRegistry()
  // 标记当前MetricsSystem是否正在运行.
  private var running: Boolean = false
  // 添加到ServletContextHandler后可通过webUi展示.
  // Treat MetricsServlet as a special sink as it should be exposed to add handlers to web ui
  private var metricsServlet: Option[MetricsServlet] = None

  /**
   * 获取WebUI 的handler用于本注册系统,只有在start()方法调用之后才能调用该方法.
    * Get any UI handlers used by this metrics system; can only be called after start().
   */
  def getServletHandlers: Array[ServletContextHandler] = {
    require(running, "Can only call getServletHandlers on a running MetricsSystem")
    metricsServlet.map(_.getHandlers(conf)).getOrElse(Array())
  }

  metricsConfig.initialize()
  /** 用于启动MetricsSystem*/
  def start() {
    // 如果running为true则已经运行了,不需要start了.
    require(!running, "Attempting to start a MetricsSystem that is already running")
    // 将runing设为true
    running = true
    // 将静态的度量来源CodegenMetrics和HiveCatalogMetrics注册到MetricRegistry.
    StaticSources.allSources.foreach(registerSource)
    registerSources()
    registerSinks()
    sinks.foreach(_.start)
  }

  def stop() {
    if (running) {
      sinks.foreach(_.stop)
    } else {
      logWarning("Stopping a MetricsSystem that is not running")
    }
    running = false
  }

  def report() {
    sinks.foreach(_.report())
  }

  /**
   * 为每个度量源构建唯一标识名称.构成如下:
    * <app ID>.<executor ID (or "driver")>.<source name>,
    * 如果无法获取某个Id,则默认使用<source name> <br>
    * Build a name that uniquely identifies each metric source.
   * The name is structured as follows: <app ID>.<executor ID (or "driver")>.<source name>.
   * If either ID is not available, this defaults to just using <source name>.
   *
   * @param source Metric source to be named by this method.
   * @return An unique metric name for each combination of
   *         application, executor/driver and metric source.
   */
  private[spark] def buildRegistryName(source: Source): String = {
    // 默认读取spark.app.id作为命名空间.可通过saprk.metrics.namespace控制.
    val metricsNamespace = conf.get(METRICS_NAMESPACE).orElse(conf.getOption("spark.app.id"))
    // 获取spark.executor.id
    val executorId = conf.getOption("spark.executor.id")
    // 获取默认名称.
    val defaultName = MetricRegistry.name(source.sourceName)
    // 如果是driver或executor
    if (instance == "driver" || instance == "executor") {
      // 如果metricsNamespace和executorId已定义.
      if (metricsNamespace.isDefined && executorId.isDefined) {
        // 生成${metricesNamespace}.${executorId}.${defaultName}格式注册名
        MetricRegistry.name(metricsNamespace.get, executorId.get, source.sourceName)
      } else {
        // Only Driver and Executor set spark.app.id and spark.executor.id.
        // Other instance types, e.g. Master and Worker, are not related to a specific application.
        if (metricsNamespace.isEmpty) {
          logWarning(s"Using default name $defaultName for source because neither " +
            s"${METRICS_NAMESPACE.key} nor spark.app.id is set.")
        }
        if (executorId.isEmpty) {
          logWarning(s"Using default name $defaultName for source because spark.executor.id is " +
            s"not set.")
        }
        // 否则默认名称.
        defaultName
      }
    } else { defaultName }
  }

  def getSourcesByName(sourceName: String): Seq[Source] =
    sources.filter(_.sourceName == sourceName)

  def registerSource(source: Source) {
    sources += source
    try {
      val regName = buildRegistryName(source)
      registry.register(regName, source.metricRegistry)
    } catch {
      case e: IllegalArgumentException => logInfo("Metrics already registered", e)
    }
  }

  def removeSource(source: Source) {
    sources -= source
    val regName = buildRegistryName(source)
    registry.removeMatching(new MetricFilter {
      def matches(name: String, metric: Metric): Boolean = name.startsWith(regName)
    })
  }
  /** 注册度量数据源*/
  private def registerSources() {
    // 获取当前实例的度量属性.
    val instConfig = metricsConfig.getInstance(instance)
    // 通过匹配正则表达式"^source\\.(.+)\\.(.+)",获取所有度量源细粒度属性和实例.
    val sourceConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SOURCE_REGEX)

    // Register all the sources related to instance
    // 注册所有相关实例
    sourceConfigs.foreach { kv =>
      // 获取class{ath
      val classPath = kv._2.getProperty("class")
      try {
        // 反射获取实例对象
        val source = Utils.classForName(classPath).newInstance()
        // 注册
        registerSource(source.asInstanceOf[Source])
      } catch {
        case e: Exception => logError("Source class " + classPath + " cannot be instantiated", e)
      }
    }
  }
  // 注册度量输出目的地
  private def registerSinks() {
    // 获取当前实例的度量属性
    val instConfig = metricsConfig.getInstance(instance)
    // 通过正则匹配获取所有细粒度实例及属性
    val sinkConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SINK_REGEX)
    // 通过反射生成实例注册sink目的地.
    sinkConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      if (null != classPath) {
        try {
          val sink = Utils.classForName(classPath)
            .getConstructor(classOf[Properties], classOf[MetricRegistry], classOf[SecurityManager])
            .newInstance(kv._2, registry, securityMgr)
          if (kv._1 == "servlet") {
            // 如果当前实例是servlet,则由metricsServlet持有此servlet引用
            metricsServlet = Some(sink.asInstanceOf[MetricsServlet])
          } else {
            // 否则将度量输出实例注册到数组缓冲sinks中.
            sinks += sink.asInstanceOf[Sink]
          }
        } catch {
          case e: Exception =>
            logError("Sink class " + classPath + " cannot be instantiated")
            throw e
        }
      }
    }
  }
}

private[spark] object MetricsSystem {
  val SINK_REGEX = "^sink\\.(.+)\\.(.+)".r
  val SOURCE_REGEX = "^source\\.(.+)\\.(.+)".r

  private[this] val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
  private[this] val MINIMAL_POLL_PERIOD = 1

  def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int) {
    val period = MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit)
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }


  def createMetricsSystem(
      instance: String, conf: SparkConf, securityMgr: SecurityManager): MetricsSystem = {
    new MetricsSystem(instance, conf, securityMgr)
  }
}

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
import java.io.{FileInputStream, InputStream}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] class MetricsConfig(conf: SparkConf) extends Logging {

  private val DEFAULT_PREFIX = "*"
  private val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  private val DEFAULT_METRICS_CONF_FILENAME = "metrics.properties"
  // 度量属性信息
  private[metrics] val properties = new Properties()
  // 每个实例的子属性,缓存每个实例与其属性的映射关系
  private[metrics] var perInstanceSubProperties: mutable.HashMap[String, Properties] = null
  // 设置默认属性
  private def setDefaultProperties(prop: Properties) {
    prop.setProperty("*.sink.servlet.class", "org.apache.spark.metrics.sink.MetricsServlet")
    prop.setProperty("*.sink.servlet.path", "/metrics/json")
    prop.setProperty("master.sink.servlet.path", "/metrics/master/json")
    prop.setProperty("applications.sink.servlet.path", "/metrics/applications/json")
  }

  /**
   * 从不同的地方加载属性,如果相同的属性在之后的方法中设置了,会覆盖之前的值.
    * Load properties from various places, based on precedence
   * If the same property is set again latter on in the method, it overwrites the previous value
   */
  def initialize() {
    // Add default properties in case there's no properties file
    // 设置默认属性
    setDefaultProperties(properties)
    // 从文件中加载度量属性.可以通过spark.metrics.conf指定属性文件.
    loadPropertiesFromFile(conf.getOption("spark.metrics.conf"))

    // Also look for the properties in provided Spark configuration
    // 从sparkConf中查找spark.metrics.conf.作为前缀的属性
    val prefix = "spark.metrics.conf."
    conf.getAll.foreach {
      case (k, v) if k.startsWith(prefix) =>
        // 截取spark.metrics.conf.后的部分作为key
        properties.setProperty(k.substring(prefix.length()), v)
      case _ =>
    }

    // //现在，让我们填充每个实例的子属性列表，实例是在属性名称中第一个点之前出现的前缀。
    // 添加到每个实例的子属性，默认属性（前缀为“*”的属性），
    // 如果它们没有已经定义的完全相同的子属性。
    //  例如，如果属性有（“* .class” - >“default_class”，“*.path” - >“default_path”，
    // “driver.path” - >“driver_path”），对于驱动程序特定的子属性，
    // 我们会 像输出一样
    // （“driver” - > Map（“path” - >“driver_path”，“class” - >“default_class”）
     // 注意如何基于默认属性添加类，但路径保持不变，因为“driver.path”已经存在并且优先于“* .path"
    // Now, let's populate a list of sub-properties per instance, instance being the prefix that
    // appears before the first dot in the property name.
    // Add to the sub-properties per instance, the default properties (those with prefix "*"), if
    // they don't have that exact same sub-property already defined.
    //
    // For example, if properties has ("*.class"->"default_class", "*.path"->"default_path,
    // "driver.path"->"driver_path"), for driver specific sub-properties, we'd like the output to be
    // ("driver"->Map("path"->"driver_path", "class"->"default_class")
    // Note how class got added to based on the default property, but path remained the same
    // since "driver.path" already existed and took precedence over "*.path"
    //
    perInstanceSubProperties = subProperties(properties, INSTANCE_REGEX)
    if (perInstanceSubProperties.contains(DEFAULT_PREFIX)) {
      val defaultSubProperties = perInstanceSubProperties(DEFAULT_PREFIX).asScala
      for ((instance, prop) <- perInstanceSubProperties if (instance != DEFAULT_PREFIX);
           (k, v) <- defaultSubProperties if (prop.get(k) == null)) {
        prop.put(k, v)
      }
    }
  }

  /**
   * 对于properties中的每个属性kv.通过正则表达式匹配找出kve中key的前缀和后缀,
    * 前缀作为实例,后缀作为新的属性kv2的key,kv的value作为新的属性kv2的value.
    * 最后降属性kv2添加到实例对应的属性集合中作为实例的属性.<br>
    * Take a simple set of properties and a regex that the instance names (part before the first dot)
   * have to conform to. And, return a map of the first order prefix (before the first dot) to the
   * sub-properties under that prefix.
   *
   * For example, if the properties sent were Properties("*.sink.servlet.class"->"class1",
   * "*.sink.servlet.path"->"path1"), the returned map would be
   * Map("*" -> Properties("sink.servlet.class" -> "class1", "sink.servlet.path" -> "path1"))
   * Note in the subProperties (value of the returned Map), only the suffixes are used as property
   * keys.
   * If, in the passed properties, there is only one property with a given prefix, it is still
   * "unflattened". For example, if the input was Properties("*.sink.servlet.class" -> "class1"
   * the returned Map would contain one key-value pair
   * Map("*" -> Properties("sink.servlet.class" -> "class1"))
   * Any passed in properties, not complying with the regex are ignored.
   *
   * @param prop the flat list of properties to "unflatten" based on prefixes
   * @param regex the regex that the prefix has to comply with
   * @return an unflatted map, mapping prefix with sub-properties under that prefix
   */
  def subProperties(prop: Properties, regex: Regex): mutable.HashMap[String, Properties] = {
    val subProperties = new mutable.HashMap[String, Properties]
    prop.asScala.foreach { kv =>
      if (regex.findPrefixOf(kv._1.toString).isDefined) {
        val regex(prefix, suffix) = kv._1.toString
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2.toString)
      }
    }
    subProperties
  }
  /** 获取实例的属性*/
  def getInstance(inst: String): Properties = {
    // 从perInstanceSubProperties获取属性
    perInstanceSubProperties.get(inst) match {
      case Some(s) => s
        //如果未获得,返回默认值
      case None => perInstanceSubProperties.getOrElse(DEFAULT_PREFIX, new Properties)
    }
  }

  /**
   * 从指定配置文件中加载配置.如果没有文件路径会尝试从classpath中读取metrics.properties.
    * Loads configuration from a config file. If no config file is provided, try to get file
   * in class path.
   */
  private[this] def loadPropertiesFromFile(path: Option[String]): Unit = {
    var is: InputStream = null
    try {
      is = path match {
        case Some(f) => new FileInputStream(f)
        case None => Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_METRICS_CONF_FILENAME)
      }

      if (is != null) {
        properties.load(is)
      }
    } catch {
      case e: Exception =>
        val file = path.getOrElse(DEFAULT_METRICS_CONF_FILENAME)
        logError(s"Error loading configuration file $file", e)
    } finally {
      if (is != null) {
        is.close()
      }
    }
  }

}

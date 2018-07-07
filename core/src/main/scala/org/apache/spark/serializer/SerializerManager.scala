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

package org.apache.spark.serializer

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.io.CompressionCodec
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.storage._
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * 为各种spark组件配置序列化,压缩和加密的组件,包括自动选择用于shuffle的Serializer
  * Component which configures serialization, compression and encryption for various Spark
 * components, including automatic selection of which [[Serializer]] to use for shuffles.
 */
private[spark] class SerializerManager(
    // 默认序列化器,
    defaultSerializer: Serializer,
    conf: SparkConf,
    // 加密使用的密钥
    encryptionKey: Option[Array[Byte]]) {

  def this(defaultSerializer: Serializer, conf: SparkConf) = this(defaultSerializer, conf, None)
  // google提供的库实现的序列化器
  private[this] val kryoSerializer = new KryoSerializer(conf)

  def setDefaultClassLoader(classLoader: ClassLoader): Unit = {
    kryoSerializer.setDefaultClassLoader(classLoader)
  }
  // 字符串类型标记
  private[this] val stringClassTag: ClassTag[String] = implicitly[ClassTag[String]]
  // 原始数据类型和原始数据类型数组的标记
  private[this] val primitiveAndPrimitiveArrayClassTags: Set[ClassTag[_]] = {
    val primitiveClassTags = Set[ClassTag[_]](
      ClassTag.Boolean,
      ClassTag.Byte,
      ClassTag.Char,
      ClassTag.Double,
      ClassTag.Float,
      ClassTag.Int,
      ClassTag.Long,
      ClassTag.Null,
      ClassTag.Short
    )
    val arrayClassTags = primitiveClassTags.map(_.wrap)
    primitiveClassTags ++ arrayClassTags
  }
  // 是否对广播对象进行压缩
  // Whether to compress broadcast variables that are stored
  private[this] val compressBroadcast = conf.getBoolean("spark.broadcast.compress", true)
  // 是否对Shuffle数据进行压缩
  // Whether to compress shuffle output that are stored
  private[this] val compressShuffle = conf.getBoolean("spark.shuffle.compress", true)
  // 时候对RDD进行压缩
  // Whether to compress RDD partitions that are stored serialized
  private[this] val compressRdds = conf.getBoolean("spark.rdd.compress", false)
  // 是否对溢出到磁盘的shuffle数据进行压缩.
  // Whether to compress shuffle output temporarily spilled to disk
  private[this] val compressShuffleSpill = conf.getBoolean("spark.shuffle.spill.compress", true)

  /*
   * 使用的压缩解码器.必须使用延迟加载,因为我们想将压缩解码器的初始化推迟到第一次使用的时候.
   * 原因是Spark程序可能使用用户自定义解码器的第三方jar包加载到executor中.当BlockManager初始化的时候,
   * 用户级别的jar包还没有被加载.
   * The compression codec to use. Note that the "lazy" val is necessary because we want to delay
   * the initialization of the compression codec until it is first used. The reason is that a Spark
   * program could be using a user-defined codec in a third party jar, which is loaded in
   * Executor.updateDependencies. When the BlockManager is initialized, user level jars hasn't been
   * loaded yet. */
  private lazy val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)
  /** 是否支持加密,如果想要支持加密,必须在构造SerializerManager的时候传入encryptionKey.
    * 可以通过spark.io.encryption.enabled(允许加密)
    * spark.io.encryption.keySizeBits(密钥长度,有128,192,256三种长度.
    * spark.io.encryption.keygen.algorithm(加密算法,默认HmacSHA1)等进行具体配置.
    * */
  def encryptionEnabled: Boolean = encryptionKey.isDefined
  /** 对于指定的类型(ct),能否使用kryoSerializer进行序列化.
    * 当类型标记属于primitiveAndPrimitiveArrayClassTags,返回true.
    * */
  def canUseKryo(ct: ClassTag[_]): Boolean = {
    primitiveAndPrimitiveArrayClassTags.contains(ct) || ct == stringClassTag
  }
  //由于SPARK-13990中的功能现在无法应用于Spark Streaming。
  // 不幸的是基于`Receiver`模式的streaming job无法在Spark 2.x上正常运行。
  // 在第一步中关闭流媒体的“kryo auto pick”功能可能是一个合理的选择。
  // SPARK-18617: As feature in SPARK-13990 can not be applied to Spark Streaming now. The worst
  // result is streaming job based on `Receiver` mode can not run on Spark 2.x properly. It may be
  // a rational choice to close `kryo auto pick` feature for streaming in the first step.
  /** 获取序列化器.如果autoPick为true(BlockId不等于StreamBlockId时),
    * 并且canUseKryo结果为true时选择kryoSerializer,否则defaultSerializer.
    * */
  def getSerializer(ct: ClassTag[_], autoPick: Boolean): Serializer = {
    if (autoPick && canUseKryo(ct)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  /**
   * 获取序列化器,(canUseKryo(keyClassTag) && canUseKryo(valueClassTag))为true使用kryo,
    * 否则默认序列化器.
    * Pick the best serializer for shuffling an RDD of key-value pairs.
   */
  def getSerializer(keyClassTag: ClassTag[_], valueClassTag: ClassTag[_]): Serializer = {
    if (canUseKryo(keyClassTag) && canUseKryo(valueClassTag)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  private def shouldCompress(blockId: BlockId): Boolean = {
    blockId match {
      case _: ShuffleBlockId => compressShuffle
      case _: BroadcastBlockId => compressBroadcast
      case _: RDDBlockId => compressRdds
      case _: TempLocalBlockId => compressShuffleSpill
      case _: TempShuffleBlockId => compressShuffle
      case _ => false
    }
  }

  /**
   * 对Block输入流进行压缩加密
    * Wrap an input stream for encryption and compression
   */
  def wrapStream(blockId: BlockId, s: InputStream): InputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }

  /**
   * 对Block输出流进行压缩加密
    * Wrap an output stream for encryption and compression
   */
  def wrapStream(blockId: BlockId, s: OutputStream): OutputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }

  /**
   * 如果shuffle加密可用,对输入流加密.
    * Wrap an input stream for encryption if shuffle encryption is enabled
   */
  def wrapForEncryption(s: InputStream): InputStream = {
    encryptionKey
      .map { key => CryptoStreamUtils.createCryptoInputStream(s, conf, key) }
      .getOrElse(s)
  }

  /**
   * 如果shuffle加密可用,对输出流加密.
    * Wrap an output stream for encryption if shuffle encryption is enabled
   */
  def wrapForEncryption(s: OutputStream): OutputStream = {
    encryptionKey
      .map { key => CryptoStreamUtils.createCryptoOutputStream(s, conf, key) }
      .getOrElse(s)
  }

  /**
   * 对于block对应类型压缩可用的话,压缩输出流.
    * Wrap an output stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedOutputStream(s) else s
  }

  /**
   * 对于block对应类型压缩可用的话,压缩输入流.
    * Wrap an input stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: BlockId, s: InputStream): InputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedInputStream(s) else s
  }

  /**
    * 对block输出流序列化
    * Serializes into a stream. */
  def dataSerializeStream[T: ClassTag](
      blockId: BlockId,
      outputStream: OutputStream,
      values: Iterator[T]): Unit = {
    val byteStream = new BufferedOutputStream(outputStream)
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = getSerializer(implicitly[ClassTag[T]], autoPick).newInstance()
    ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
  }

  /**
    * 对block输入流序列化
    * Serializes into a chunked byte buffer. */
  def dataSerialize[T: ClassTag](
      blockId: BlockId,
      values: Iterator[T]): ChunkedByteBuffer = {
    dataSerializeWithExplicitClassTag(blockId, values, implicitly[ClassTag[T]])
  }

  /**
    * 使用明确的类型标记,序列化成分块的字节缓冲区
    * Serializes into a chunked byte buffer. */
  def dataSerializeWithExplicitClassTag(
      blockId: BlockId,
      values: Iterator[_],
      classTag: ClassTag[_]): ChunkedByteBuffer = {
    val bbos = new ChunkedByteBufferOutputStream(1024 * 1024 * 4, ByteBuffer.allocate)
    val byteStream = new BufferedOutputStream(bbos)
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = getSerializer(classTag, autoPick).newInstance()
    ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
    bbos.toChunkedByteBuffer
  }

  /**
   *
    * 将输入流反序列化成值的迭代器,在到达迭代器末尾时将其释放.
    * Deserializes an InputStream into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   */
  def dataDeserializeStream[T](
      blockId: BlockId,
      inputStream: InputStream)
      (classTag: ClassTag[T]): Iterator[T] = {
    val stream = new BufferedInputStream(inputStream)
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    getSerializer(classTag, autoPick)
      .newInstance()
      .deserializeStream(wrapForCompression(blockId, stream))
      .asIterator.asInstanceOf[Iterator[T]]
  }
}

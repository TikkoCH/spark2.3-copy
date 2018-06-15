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

package org.apache.spark.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 提供了字节构成的数据不可变视图.其实现应该指定数据的来源.
 * 决堤实现可以在JVM缓冲区之外管理.例如，在NettyManagedBuffer的情况下，缓冲区是引用计数。在这种情况下，
 * 如果缓冲区将传递到另一个线程，则应调用retain / release.<br>
 * This interface provides an immutable view for data in the form of bytes. The implementation
 * should specify how the data is provided:
 *
 * - {@link FileSegmentManagedBuffer}: data backed by part of a file 基于部分文件的ManagedBuffer
 * - {@link NioManagedBuffer}: data backed by a NIO ByteBuffer 基于NIO 缓冲区的ManagedBuffer
 * - {@link NettyManagedBuffer}: data backed by a Netty ByteBuf 基于Netty缓冲区的ManagedBuffer
 *
 * The concrete buffer implementation might be managed outside the JVM garbage collector.
 * For example, in the case of {@link NettyManagedBuffer}, the buffers are reference counted.
 * In that case, if the buffer is going to be passed around to a different thread, retain/release
 * should be called.
 */
public abstract class ManagedBuffer {

  /**
   * 返回数据的长度
   * Number of bytes of the data. */
  public abstract long size();

  /**
   *将此缓冲区的数据按照NIO ByteBuffer类型返回。 更改返回的ByteBuffer的position和limit不应该影响此缓冲区的内容。
   * 这的方法应该被废弃,该方法对内存映射和分配的消耗极高.<br>
   * Exposes this buffer's data as an NIO ByteBuffer. Changing the position and limit of the
   * returned ByteBuffer should not affect the content of this buffer.
   */
  // TODO: Deprecate this, usage may require expensive memory mapping or allocation.
  public abstract ByteBuffer nioByteBuffer() throws IOException;

  /**
   * 将数据按照InputStream类型返回.底层实现不一定检查读取的字节长度，因此调用者负责确保它不超出限制。<br>
   * Exposes this buffer's data as an InputStream. The underlying implementation does not
   * necessarily check for the length of bytes read, so the caller is responsible for making sure
   * it does not go over the limit.
   */
  public abstract InputStream createInputStream() throws IOException;

  /**
   * 新使用者使用此视图时,增加引用计数<br>
   * Increment the reference count by one if applicable.
   */
  public abstract ManagedBuffer retain();

  /**
   *使用者不使用该视图就减少引用计数,当引用计数=0时,释放缓冲区.<br>
   * If applicable, decrement the reference count by one and deallocates the buffer if the
   * reference count reaches zero.
   */
  public abstract ManagedBuffer release();

  /**
   *
   * 将缓冲区转换为Netty对象,用以写出数据,返回值是ByteBuf或FileRegion <br>
   * 如果该方法返回值是ByteBUf,那么buffer的引用计数会增加并且调用者负责释放引用.
   * Convert the buffer into an Netty object, used to write the data out. The return value is either
   * a {@link io.netty.buffer.ByteBuf} or a {@link io.netty.channel.FileRegion}.
   *
   * If this method returns a ByteBuf, then that buffer's reference count will be incremented and
   * the caller will be responsible for releasing this new reference.
   */
  public abstract Object convertToNetty() throws IOException;
}

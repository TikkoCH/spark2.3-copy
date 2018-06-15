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

package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * 在线可传递消息
 * An on-the-wire transmittable message. */
public interface Message extends Encodable {
  /**
   * 定义消息类型
   * Used to identify this request type. */
  Type type();

  /**
   * 返回消息的可选内容主体
   * An optional body for the message. */
  ManagedBuffer body();

  /**
   * 消息的主体是否包含在消息的同一帧中
   * Whether to include the body of the message in the same frame as the message. */
  boolean isBodyInFrame();

  /**
   * 有点像错误码,每种错误对应一个byte;这是每种消息类型对应一个byte.
   * Preceding every serialized Message is its type, which allows us to deserialize it. */
  enum Type implements Encodable {
    /**请求获取流的单个序列块*/
    ChunkFetchRequest(0),
    /**处理ChunkFetchRequest请求成功后的信息*/
    ChunkFetchSuccess(1),
    /**处理ChunkFetchRequest请求失败后的信息*/
    ChunkFetchFailure(2),
    /** 该消息由远程server处理,client请求,server回复*/
    RpcRequest(3),
    /** server返回的消息*/
    RpcResponse(4),
    /**server处理失败*/
    RpcFailure(5),
    /**远程服务发起请求,获取流数据*/
    StreamRequest(6),
    /** StreamRequest成功返回的消息*/
    StreamResponse(7),
    /** StreamRequest失败返回的消息*/
    StreamFailure(8),
    /**单向消息,没有回应,有点像udp*/
    OneWayMessage(9),
    /**用户的消息类型,解码时会抛出异常*/
    User(-1);

    private final byte id;

    Type(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.id = (byte) id;
    }

    public byte id() { return id; }

    @Override public int encodedLength() { return 1; }

    @Override public void encode(ByteBuf buf) { buf.writeByte(id); }

    public static Type decode(ByteBuf buf) {
      byte id = buf.readByte();
      switch (id) {
        case 0: return ChunkFetchRequest;
        case 1: return ChunkFetchSuccess;
        case 2: return ChunkFetchFailure;
        case 3: return RpcRequest;
        case 4: return RpcResponse;
        case 5: return RpcFailure;
        case 6: return StreamRequest;
        case 7: return StreamResponse;
        case 8: return StreamFailure;
        case 9: return OneWayMessage;
        case -1: throw new IllegalArgumentException("User type messages cannot be decoded.");
        default: throw new IllegalArgumentException("Unknown message type: " + id);
      }
    }
  }
}

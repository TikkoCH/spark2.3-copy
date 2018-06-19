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

package org.apache.spark.network.sasl;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.security.sasl.Sasl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * 在委托给一个子RPChandler之前会进行SASL权限验证,只有在指定连接验证成功之后委托才能收到消息.
 * 连接最多可验证一次.注意:验证过程包含多个质询响应对,每一个都是一个远程过程调用.<br>
 * RPC Handler which performs SASL authentication before delegating to a child RPC handler.
 * The delegate will only receive messages if the given connection has been successfully
 * authenticated. A connection may be authenticated at most once.
 *
 * Note that the authentication process consists of multiple challenge-response pairs, each of
 * which are individual RPCs.
 */
public class SaslRpcHandler extends RpcHandler {
  private static final Logger logger = LoggerFactory.getLogger(SaslRpcHandler.class);

  /** Transport的配置信息.Transport configuration. */
  private final TransportConf conf;

  /** 客户端channel.The client channel. */
  private final Channel channel;

  /**建立验证连接之后想要委托的RPChandler RpcHandler we will delegate to for authenticated connections. */
  private final RpcHandler delegate;

  /**
   *提供密钥的类，这些密钥由服务器和客户端在每个应用程序的基础上共享<br>
   * Class which provides secret keys which are shared by server and client on a per-app basis. */
  private final SecretKeyHolder secretKeyHolder;

  private SparkSaslServer saslServer;
  private boolean isComplete;
  private boolean isAuthenticated;

  public SaslRpcHandler(
      TransportConf conf,
      Channel channel,
      RpcHandler delegate,
      SecretKeyHolder secretKeyHolder) {
    this.conf = conf;
    this.channel = channel;
    this.delegate = delegate;
    this.secretKeyHolder = secretKeyHolder;
    this.saslServer = null;
    this.isComplete = false;
    this.isAuthenticated = false;
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    if (isComplete) {
      //如果是已经完成,将消息传递给下游RpcHandler并返回
      // Authentication complete, delegate to base handler.
      delegate.receive(client, message, callback);
      return;
    }
    if (saslServer == null || !saslServer.isComplete()) {
      ByteBuf nettyBuf = Unpooled.wrappedBuffer(message);
      SaslMessage saslMessage;
      try {
        saslMessage = SaslMessage.decode(nettyBuf);//解密数据
      } finally {
        nettyBuf.release();
      }

      if (saslServer == null) {
        //如果saslServer还未创建,则创建SparkSaslServer
        //握手中的第一条消息,设置必要的状态
        // First message in the handshake, setup the necessary state.
        client.setClientId(saslMessage.appId);
        saslServer = new SparkSaslServer(saslMessage.appId, secretKeyHolder,
          conf.saslServerAlwaysEncrypt());
      }
      //声明响应的字节数组
      byte[] response;
      try {
        //通过saslServer响应
        response = saslServer.response(JavaUtils.bufferToArray(
          saslMessage.body().nioByteBuffer()));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
      callback.onSuccess(ByteBuffer.wrap(response));//返回给客户端
    }


    // 在SASL响应发出之后设置加密,否则客户端无法解析响应.因为我们正在处理传入的消息,
    // 所以在此修改channel pipeline是可以的.所以该pipeline处于忙碌状态,并且在方法返回之前,
    // 不会有新消息传入pipeline之内.这需要代码通过其他方式确保在协商进行时没有出站消息正在写入channel.
    // Setup encryption after the SASL response is sent, otherwise the client can't parse the
    // response. It's ok to change the channel pipeline here since we are processing an incoming
    // message, so the pipeline is busy and no new incoming messages will be fed to it before this
    // method returns. This assumes that the code ensures, through other means, that no outbound
    // messages are being written to the channel while negotiation is still going on.
    if (saslServer.isComplete()) {
      //QOP_AUTH_CONF=auth-conf;Sasl.QOP=javax.security.sasl.qop,这两个应该是一定相等的,因为都是final修饰的String
      //我不太明白为什么在这里进行判断.如果不相等就设置complete为true,如果相等就设置false
      if (!SparkSaslServer.QOP_AUTH_CONF.equals(saslServer.getNegotiatedProperty(Sasl.QOP))) {
        logger.debug("SASL authentication successful for channel {}", client);
        complete(true);
        return;
      }
      //对管道进行加密
      logger.debug("Enabling encryption for channel {}", client);
      SaslEncryption.addToChannel(channel, saslServer, conf.maxSaslEncryptedBlockSize());
      complete(false);
      return;
    }
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message) {
    delegate.receive(client, message);
  }

  @Override
  public StreamManager getStreamManager() {
    return delegate.getStreamManager();
  }

  @Override
  public void channelActive(TransportClient client) {
    delegate.channelActive(client);
  }

  @Override
  public void channelInactive(TransportClient client) {
    try {
      delegate.channelInactive(client);
    } finally {
      if (saslServer != null) {
        saslServer.dispose();
      }
    }
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    delegate.exceptionCaught(cause, client);
  }

  private void complete(boolean dispose) {
    if (dispose) {
      try {
        saslServer.dispose();
      } catch (RuntimeException e) {
        logger.error("Error while disposing SASL server", e);
      }
    }

    saslServer = null;
    isComplete = true;
  }

}

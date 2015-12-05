/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.proton.plug.test.minimalclient;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.proton.plug.AMQPClientConnectionContext;
import org.proton.plug.context.client.ProtonClientConnectionContextFactory;

public class SimpleAMQPConnector implements Connector {

   private Bootstrap bootstrap;

   @Override
   public void start() {

      bootstrap = new Bootstrap();
      bootstrap.channel(NioSocketChannel.class);
      bootstrap.group(new NioEventLoopGroup(10));

      bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            public void initChannel(Channel channel) throws Exception {
            }
         });
   }

   @Override
   public AMQPClientConnectionContext connect(String host, int port) throws Exception {
      SocketAddress remoteDestination = new InetSocketAddress(host, port);

      ChannelFuture future = bootstrap.connect(remoteDestination);

      future.awaitUninterruptibly();

      AMQPClientSPI clientConnectionSPI = new AMQPClientSPI(future.channel());

      final AMQPClientConnectionContext connection = (AMQPClientConnectionContext) ProtonClientConnectionContextFactory.getFactory().createConnection(clientConnectionSPI);

      future.channel().pipeline().addLast(new ChannelDuplexHandler() {
            @Override
            public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
               ByteBuf buffer = (ByteBuf) msg;
               connection.inputBuffer(buffer);
            }
         });

      return connection;
   }
}

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
package org.apache.activemq.artemis.tests.integration.stomp;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

public class StompOverHttpTest extends StompTest {

   @Override
   protected void addChannelHandlers(int index, SocketChannel ch) {
      ch.pipeline().addLast(new HttpRequestEncoder());
      ch.pipeline().addLast(new HttpResponseDecoder());
      ch.pipeline().addLast(new HttpHandler());
      ch.pipeline().addLast("decoder", new StringDecoder(StandardCharsets.UTF_8));
      ch.pipeline().addLast("encoder", new StringEncoder(StandardCharsets.UTF_8));
      ch.pipeline().addLast(new StompClientHandler(index));
   }

   @Override
   public String receiveFrame(long timeOut) throws Exception {
      //we are request/response so may need to send an empty request so we get responses piggy backed
      sendFrame(new byte[]{});
      return super.receiveFrame(timeOut);
   }

   class HttpHandler extends ChannelDuplexHandler {

      @Override
      public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
         if (msg instanceof DefaultHttpContent) {
            DefaultHttpContent response = (DefaultHttpContent) msg;
            ctx.fireChannelRead(response.content());
         }
      }

      @Override
      public void write(final ChannelHandlerContext ctx, final Object msg, ChannelPromise promise) throws Exception {
         if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "", buf);
            httpRequest.headers().add(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buf.readableBytes()));
            ctx.write(httpRequest, promise);
         } else {
            ctx.write(msg, promise);
         }
      }
   }
}

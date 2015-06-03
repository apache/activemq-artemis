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


import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.string.StringDecoder;

public class StompOverWebsocketTest extends StompTest
{


   private ChannelPromise handshakeFuture;

   @Override
   protected void addChannelHandlers(SocketChannel ch) throws URISyntaxException
   {
      ch.pipeline().addLast("http-codec", new HttpClientCodec());
      ch.pipeline().addLast("aggregator", new HttpObjectAggregator(8192));
      ch.pipeline().addLast(new WebsocketHandler(WebSocketClientHandshakerFactory.newHandshaker(new URI("ws://localhost:8080/websocket"), WebSocketVersion.V13, null, false, null)));
      ch.pipeline().addLast("decoder", new StringDecoder(StandardCharsets.UTF_8));
      ch.pipeline().addLast(new StompClientHandler());
   }


   @Override
   protected void handshake() throws InterruptedException
   {
      handshakeFuture.sync();
   }

   class WebsocketHandler extends ChannelDuplexHandler
   {
      private WebSocketClientHandshaker handshaker;

      public WebsocketHandler(WebSocketClientHandshaker webSocketClientHandshaker)
      {
         this.handshaker = webSocketClientHandshaker;
      }

      @Override
      public void handlerAdded(ChannelHandlerContext ctx) throws Exception
      {
         handshakeFuture = ctx.newPromise();
      }

      @Override
      public void channelActive(ChannelHandlerContext ctx) throws Exception
      {
         handshaker.handshake(ctx.channel());
      }

      @Override
      public void channelInactive(ChannelHandlerContext ctx) throws Exception
      {
         System.out.println("WebSocket Client disconnected!");
      }

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
      {
         Channel ch = ctx.channel();
         if (!handshaker.isHandshakeComplete())
         {
            handshaker.finishHandshake(ch, (FullHttpResponse) msg);
            System.out.println("WebSocket Client connected!");
            handshakeFuture.setSuccess();
            return;
         }

         if (msg instanceof FullHttpResponse)
         {
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new Exception("Unexpected FullHttpResponse (getStatus=" + response.getStatus() + ", content="
                                   + response.content().toString(StandardCharsets.UTF_8) + ')');
         }

         WebSocketFrame frame = (WebSocketFrame) msg;
         if (frame instanceof BinaryWebSocketFrame)
         {
            BinaryWebSocketFrame dataFrame = (BinaryWebSocketFrame) frame;
            super.channelRead(ctx, dataFrame.content());
         }
         else if (frame instanceof PongWebSocketFrame)
         {
            System.out.println("WebSocket Client received pong");
         }
         else if (frame instanceof CloseWebSocketFrame)
         {
            System.out.println("WebSocket Client received closing");
            ch.close();
         }
      }

      @Override
      public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
      {
         try
         {
            if (msg instanceof String)
            {
               TextWebSocketFrame frame = new TextWebSocketFrame((String) msg);
               ctx.write(frame, promise);
            }
            else
            {
               super.write(ctx, msg, promise);
            }
         }
         catch (Exception e)
         {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
         }
      }
   }
}

/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.core.protocol.stomp;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;

import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.setContentLength;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


/**
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author <a href="http://jmesnil.net/">Jeff Mesnil</a
 * @author <a href="mailto:nmaurer@redhat.com">Norman Maurer</a>
 */
public class WebSocketServerHandler extends SimpleChannelInboundHandler<Object>
{
   private static final String WEBSOCKET_PATH = "/stomp";

   private WebSocketServerHandshaker handshaker;
   private static final BinaryWebSocketEncoder BINARY_WEBSOCKET_ENCODER = new BinaryWebSocketEncoder();

   @Override
   public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception
   {
      if (msg instanceof FullHttpRequest)
      {
         handleHttpRequest(ctx, (FullHttpRequest) msg);
      }
      else if (msg instanceof WebSocketFrame)
      {
         WebSocketFrame frame = (WebSocketFrame) msg;
         boolean handle = handleWebSocketFrame(ctx, frame);
         if (handle)
         {
            ctx.fireChannelRead(frame.content().retain());
         }
      }
   }

   private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception
   {
      // Allow only GET methods.
      if (req.getMethod() != GET)
      {
         sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, FORBIDDEN));
         return;
      }

      // Handshake
      WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
         this.getWebSocketLocation(req), "v10.stomp,v11.stomp", false);
      this.handshaker = wsFactory.newHandshaker(req);
      if (this.handshaker == null)
      {
         WebSocketServerHandshakerFactory.sendUnsupportedWebSocketVersionResponse(ctx.channel());
      }
      else
      {
         ChannelFuture handshake = this.handshaker.handshake(ctx.channel(), req);
         handshake.addListener(new ChannelFutureListener()
         {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception
            {
               if (future.isSuccess())
               {
                  // we need to insert an encoder that takes the underlying ChannelBuffer of a StompFrame.toHornetQBuffer and
                  // wrap it in a binary web socket frame before letting the wsencoder send it on the wire
                  future.channel().pipeline().addAfter("wsencoder", "binary-websocket-encoder", BINARY_WEBSOCKET_ENCODER);
               }
               else
               {
                  // Handshake failed, fire an exceptionCaught event
                  future.channel().pipeline().fireExceptionCaught(future.cause());
               }
            }
         });
      }
   }

   private boolean handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame)
   {

      // Check for closing frame
      if (frame instanceof CloseWebSocketFrame)
      {
         this.handshaker.close(ctx.channel(), ((CloseWebSocketFrame) frame).retain());
         return false;
      }
      else if (frame instanceof PingWebSocketFrame)
      {
         ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
         return false;
      }
      else if (!(frame instanceof TextWebSocketFrame))
      {
         throw new UnsupportedOperationException(String.format("%s frame types not supported", frame.getClass()
            .getName()));
      }
      return true;
   }

   private void sendHttpResponse(ChannelHandlerContext ctx, HttpRequest req, FullHttpResponse res)
   {
      // Generate an error page if response status code is not OK (200).
      if (res.getStatus().code() != 200)
      {
         res.content().writeBytes(res.getStatus().toString().getBytes(StandardCharsets.UTF_8));
         setContentLength(res, res.content().readableBytes());
      }

      // Send the response and close the connection if necessary.
      ChannelFuture f = ctx.writeAndFlush(res);
      if (!isKeepAlive(req) || res.getStatus().code() != 200)
      {
         f.addListener(ChannelFutureListener.CLOSE);
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
   {
      cause.printStackTrace();
      ctx.close();
   }

   private String getWebSocketLocation(HttpRequest req)
   {
      return "ws://" + req.headers().get(HttpHeaders.Names.HOST) + WEBSOCKET_PATH;
   }

   @Sharable
   private static final class BinaryWebSocketEncoder extends ChannelOutboundHandlerAdapter
   {

      @Override
      public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
      {
         if (msg instanceof ByteBuf)
         {
            msg = new BinaryWebSocketFrame((ByteBuf) msg);
         }

         ctx.write(msg, promise);
      }

   }
}
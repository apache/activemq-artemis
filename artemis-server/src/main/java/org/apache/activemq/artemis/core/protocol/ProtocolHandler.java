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
package org.apache.activemq.artemis.core.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.remoting.impl.netty.ConnectionCreator;
import org.apache.activemq.artemis.core.remoting.impl.netty.HttpAcceptorHandler;
import org.apache.activemq.artemis.core.remoting.impl.netty.HttpKeepAliveRunnable;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptor;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.artemis.core.remoting.impl.netty.PartialPooledByteBufAllocator;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.protocol.websocket.WebSocketServerHandler;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.utils.ConfigurationHelper;

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class ProtocolHandler {

   private Map<String, ProtocolManager> protocolMap;

   private NettyAcceptor nettyAcceptor;

   private Map<String, Object> configuration;

   private ScheduledExecutorService scheduledThreadPool;

   private HttpKeepAliveRunnable httpKeepAliveRunnable;

   private final List<String> websocketSubprotocolIds;

   public ProtocolHandler(Map<String, ProtocolManager> protocolMap,
                          NettyAcceptor nettyAcceptor,
                          final Map<String, Object> configuration,
                          ScheduledExecutorService scheduledThreadPool) {
      this.protocolMap = protocolMap;
      this.nettyAcceptor = nettyAcceptor;
      this.configuration = configuration;
      this.scheduledThreadPool = scheduledThreadPool;

      websocketSubprotocolIds = new ArrayList<>();
      for (ProtocolManager pm : protocolMap.values()) {
         if (pm.websocketSubprotocolIdentifiers() != null) {
            websocketSubprotocolIds.addAll(pm.websocketSubprotocolIdentifiers());
         }
      }
   }

   public ChannelHandler getProtocolDecoder() {
      return new ProtocolDecoder(true, false);
   }

   public void close() {
      if (httpKeepAliveRunnable != null) {
         httpKeepAliveRunnable.close();
      }
   }

   public ProtocolManager getProtocol(String name) {
      return this.protocolMap.get(name);
   }

   class ProtocolDecoder extends ByteToMessageDecoder {

      private final boolean http;

      private final boolean httpEnabled;

      ProtocolDecoder(boolean http, boolean httpEnabled) {
         this.http = http;
         this.httpEnabled = httpEnabled;
      }

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
         if (msg instanceof FullHttpRequest) {
            FullHttpRequest request = (FullHttpRequest) msg;
            HttpHeaders headers = request.headers();
            String upgrade = headers.get("upgrade");
            if (upgrade != null && upgrade.equalsIgnoreCase("websocket")) {
               ctx.pipeline().addLast("websocket-handler", new WebSocketServerHandler(websocketSubprotocolIds));
               ctx.pipeline().addLast(new ProtocolDecoder(false, false));
               ctx.pipeline().remove(this);
               ctx.pipeline().remove("http-handler");
               ctx.fireChannelRead(msg);
            } else if (upgrade != null && upgrade.equalsIgnoreCase(NettyConnector.ACTIVEMQ_REMOTING)) { // HORNETQ-1391
               // Send the response and close the connection if necessary.
               ctx.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, FORBIDDEN)).addListener(ChannelFutureListener.CLOSE);
            }
         } else {
            super.channelRead(ctx, msg);
         }
      }

      @Override
      protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
         if (ctx.isRemoved()) {
            return;
         }

         // Will use the first five bytes to detect a protocol.
         if (in.readableBytes() < 8) {
            return;
         }

         final int magic1 = in.getUnsignedByte(in.readerIndex());
         final int magic2 = in.getUnsignedByte(in.readerIndex() + 1);
         if (http && isHttp(magic1, magic2)) {
            switchToHttp(ctx);
            return;
         }
         String protocolToUse = null;
         Set<String> protocolSet = protocolMap.keySet();
         if (!protocolSet.isEmpty()) {
            // Use getBytes(...) as this works with direct and heap buffers.
            // See https://issues.jboss.org/browse/HORNETQ-1406
            byte[] bytes = new byte[8];
            in.getBytes(0, bytes);

            for (String protocol : protocolSet) {
               ProtocolManager protocolManager = protocolMap.get(protocol);
               if (protocolManager.isProtocol(bytes)) {
                  protocolToUse = protocol;
                  break;
               }
            }
         }

         //if we get here we assume we use the core protocol as we match nothing else
         if (protocolToUse == null) {
            for (Map.Entry<String, ProtocolManager> entry : protocolMap.entrySet()) {
               if (entry.getValue().acceptsNoHandshake()) {
                  protocolToUse = entry.getKey();
                  break;
               }
            }
            if (protocolToUse == null) {
               protocolToUse = ActiveMQClient.DEFAULT_CORE_PROTOCOL;
            }
         }
         ProtocolManager protocolManagerToUse = protocolMap.get(protocolToUse);
         ConnectionCreator channelHandler = nettyAcceptor.createConnectionCreator();
         ChannelPipeline pipeline = ctx.pipeline();
         protocolManagerToUse.addChannelHandlers(pipeline);
         pipeline.addLast("handler", channelHandler);
         NettyServerConnection connection = channelHandler.createConnection(ctx, protocolToUse, httpEnabled);
         protocolManagerToUse.handshake(connection, new ChannelBufferWrapper(in));
         pipeline.remove(this);

         // https://issues.apache.org/jira/browse/ARTEMIS-392
         // Application servers or other components may upgrade a regular socket to Netty
         // We need to be able to work normally as with anything else on Artemis
         ctx.channel().config().setAllocator(PartialPooledByteBufAllocator.INSTANCE);
         ctx.flush();
      }

      private boolean isHttp(int magic1, int magic2) {
         return magic1 == 'G' && magic2 == 'E' || // GET
            magic1 == 'P' && magic2 == 'O' || // POST
            magic1 == 'P' && magic2 == 'U' || // PUT
            magic1 == 'H' && magic2 == 'E' || // HEAD
            magic1 == 'O' && magic2 == 'P' || // OPTIONS
            magic1 == 'P' && magic2 == 'A' || // PATCH
            magic1 == 'D' && magic2 == 'E' || // DELETE
            magic1 == 'T' && magic2 == 'R'; // TRACE
         //magic1 == 'C' && magic2 == 'O'; // CONNECT
      }

      private void switchToHttp(ChannelHandlerContext ctx) {
         ChannelPipeline p = ctx.pipeline();
         p.addLast("http-decoder", new HttpRequestDecoder());
         p.addLast("http-aggregator", new HttpObjectAggregator(Integer.MAX_VALUE));
         p.addLast("http-encoder", new HttpResponseEncoder());
         //create it lazily if and when we need it
         if (httpKeepAliveRunnable == null) {
            long httpServerScanPeriod = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_SERVER_SCAN_PERIOD_PROP_NAME, TransportConstants.DEFAULT_HTTP_SERVER_SCAN_PERIOD, configuration);
            httpKeepAliveRunnable = new HttpKeepAliveRunnable();
            Future<?> future = scheduledThreadPool.scheduleAtFixedRate(httpKeepAliveRunnable, httpServerScanPeriod, httpServerScanPeriod, TimeUnit.MILLISECONDS);
            httpKeepAliveRunnable.setFuture(future);
         }
         long httpResponseTime = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_RESPONSE_TIME_PROP_NAME, TransportConstants.DEFAULT_HTTP_RESPONSE_TIME, configuration);
         HttpAcceptorHandler httpHandler = new HttpAcceptorHandler(httpKeepAliveRunnable, httpResponseTime, ctx.channel());
         ctx.pipeline().addLast("http-handler", httpHandler);
         p.addLast(new ProtocolDecoder(false, true));
         p.remove(this);
      }
   }
}

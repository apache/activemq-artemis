/**
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
package org.proton.plug.test.minimalserver;


import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.concurrent.GlobalEventExecutor;

import org.proton.plug.AMQPServerConnectionContext;
import org.proton.plug.context.server.ProtonServerConnectionContextFactory;
import org.proton.plug.test.Constants;

/**
 * A Netty TCP Acceptor that supports SSL
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="tlee@redhat.com">Trustin Lee</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="nmaurer@redhat.com">Norman Maurer</a>
 * @version $Rev$, $Date$
 */
public class MinimalServer
{

   static
   {
      // Disable resource leak detection for performance reasons by default
      ResourceLeakDetector.setEnabled(false);
   }

   private Class<? extends ServerChannel> channelClazz;

   private EventLoopGroup eventLoopGroup;

   private volatile ChannelGroup serverChannelGroup;

   private volatile ChannelGroup channelGroup;

   private ServerBootstrap bootstrap;

   private String host;

   private boolean sasl;

   // Constants.PORT is the default here
   private int port;

   public synchronized void start(String host, int port, final boolean sasl) throws Exception
   {
      this.host = host;
      this.port = port;
      this.sasl = sasl;

      if (channelClazz != null)
      {
         // Already started
         return;
      }

      int threadsToUse = Runtime.getRuntime().availableProcessors() * 3;
      channelClazz = NioServerSocketChannel.class;
      eventLoopGroup = new NioEventLoopGroup(threadsToUse, new SimpleServerThreadFactory("simple-server", true, Thread.currentThread().getContextClassLoader()));

      bootstrap = new ServerBootstrap();
      bootstrap.group(eventLoopGroup);
      bootstrap.channel(channelClazz);


      ChannelInitializer<Channel> factory = new ChannelInitializer<Channel>()
      {
         @Override
         public void initChannel(Channel channel) throws Exception
         {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast("amqp-handler", new ProtocolDecoder());
         }
      };
      bootstrap.childHandler(factory);

      bootstrap.option(ChannelOption.SO_REUSEADDR, true).
         childOption(ChannelOption.SO_REUSEADDR, true).
         childOption(ChannelOption.SO_KEEPALIVE, true).
//       childOption(ChannelOption.AUTO_READ, false).
         childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

      channelGroup = new DefaultChannelGroup("activemq-accepted-channels", GlobalEventExecutor.INSTANCE);

      serverChannelGroup = new DefaultChannelGroup("activemq-acceptor-channels", GlobalEventExecutor.INSTANCE);


      SocketAddress address;
      address = new InetSocketAddress(host, port);
      Channel serverChannel = bootstrap.bind(address).syncUninterruptibly().channel();
      serverChannelGroup.add(serverChannel);

   }

   class ProtocolDecoder extends ByteToMessageDecoder
   {

      AMQPServerConnectionContext connection;


      public ProtocolDecoder()
      {
      }

      @Override
      public void channelActive(ChannelHandlerContext ctx) throws Exception
      {
         super.channelActive(ctx);
         connection = ProtonServerConnectionContextFactory.getFactory().createConnection(new MinimalConnectionSPI(ctx.channel()));
         //ctx.read();
      }

      @Override
      protected void decode(final ChannelHandlerContext ctx, ByteBuf byteIn, List<Object> out) throws Exception
      {
         connection.inputBuffer(byteIn);
         ctx.flush();
//         if (connection.capacity() > 0)
//         {
//            ctx.read();
//         }
      }
   }

   public synchronized void stop()
   {
      serverChannelGroup.close().awaitUninterruptibly();
      ChannelGroupFuture future = channelGroup.close().awaitUninterruptibly();
   }


   public static void main(String[] arg)
   {
      MinimalServer server = new MinimalServer();
      try
      {
         server.start("127.0.0.1", Constants.PORT, true);


         while (true)
         {
            Thread.sleep(360000000);
         }
      }
      catch (Throwable e)
      {
         e.printStackTrace();
      }
   }
}

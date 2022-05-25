package org.apache.activemq.artemis.quorum.etcd;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Proxy to run between client and etcd server to simulate connection failures
 *
 * Derived from etcd-java's LocalNettyProxy: https://github.com/IBM/etcd-java/blob/main/src/test/java/com/ibm/etcd/client/LocalNettyProxy.java
 */
public class EtcdProxyTest implements AutoCloseable {

   private final int proxyPort;

   private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
   private final EventLoopGroup workerGroup = new NioEventLoopGroup();
   private final Set<Channel> inboundChannels = ConcurrentHashMap.newKeySet();

   private Channel channel;

   public EtcdProxyTest(int fromPort) {
      this.proxyPort = fromPort;
   }

   public synchronized EtcdProxyTest start() throws InterruptedException {
      if (channel != null) {
         return this;
      }
      long before = System.nanoTime();
      System.out.println("localproxy about to start on port "+proxyPort);
      ServerBootstrap b = new ServerBootstrap();
      ChannelFuture cf = b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override protected void initChannel(SocketChannel ch) throws Exception {
               inboundChannels.add(ch);
               ch.pipeline().addLast(new FrontendHandler("localhost", 2379));
            }
         })
         .childOption(ChannelOption.AUTO_READ, false)
         .bind(proxyPort);
      channel = cf.channel();
      cf.sync();
      System.out.println("localproxy started on port " + proxyPort + " in " +
                            (System.nanoTime()- before) / 1000_000 + "ms");
      return this;
   }

   public synchronized void kill() throws InterruptedException {
      if (channel == null) {
         return;
      }
      long before = System.nanoTime();
      System.out.println("localproxy on port "+proxyPort+" about to forcibly stop");
      Channel chan = channel;
      channel.eventLoop().execute(() -> chan.unsafe().closeForcibly());
      inboundChannels.forEach(ch -> ch.eventLoop().execute(()
                                                              -> ch.unsafe().closeForcibly()));
      inboundChannels.clear();
      ChannelFuture closeFut = channel.close();
      channel = null;
      // wait for full closure to ensue we can re-bind in subsequent start()
      closeFut.await();
      System.out.println("localproxy on port " + proxyPort + " stopped in " +
                            (System.nanoTime() - before) / 1000_000 + "ms");
   }

   static class FrontendHandler extends ChannelInboundHandlerAdapter {

      private final String remoteHost;
      private final int remotePort;

      // As we use inboundChannel.eventLoop() when building the Bootstrap this does not need to be volatile as
      // the outboundChannel will use the same EventLoop (and therefore Thread) as the inboundChannel.
      private Channel outboundChannel;

      public FrontendHandler(String remoteHost, int remotePort) {
         this.remoteHost = remoteHost;
         this.remotePort = remotePort;
      }

      @Override
      public void channelActive(ChannelHandlerContext ctx) {
         final Channel inboundChannel = ctx.channel();

         // Start the connection attempt.
         Bootstrap b = new Bootstrap();
         b.group(inboundChannel.eventLoop())
            .channel(ctx.channel().getClass())
            .handler(new BackendHandler(inboundChannel))
            .option(ChannelOption.AUTO_READ, false);
         ChannelFuture f = b.connect(remoteHost, remotePort);
         outboundChannel = f.channel();
         f.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
               // connection complete start to read first data
               inboundChannel.read();
            } else {
               // Close the connection if the connection attempt has failed.
               inboundChannel.close();
            }
         });
      }
      @Override
      public void channelRead(final ChannelHandlerContext ctx, Object msg) {
         if (outboundChannel.isActive()) {
            forwardMessage(ctx, msg, outboundChannel);
         }
      }
      @Override
      public void channelInactive(ChannelHandlerContext ctx) {
         closeOnFlush(outboundChannel);
      }
      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
         cause.printStackTrace();
         closeOnFlush(ctx.channel());
      }
   }

   static class BackendHandler extends ChannelInboundHandlerAdapter {
      private final Channel inboundChannel;

      public BackendHandler(Channel inboundChannel) {
         this.inboundChannel = inboundChannel;
      }
      @Override
      public void channelActive(ChannelHandlerContext ctx) {
         ctx.read();
      }
      @Override
      public void channelRead(final ChannelHandlerContext ctx, Object msg) {
         forwardMessage(ctx, msg, inboundChannel);
      }
      @Override
      public void channelInactive(ChannelHandlerContext ctx) {
         closeOnFlush(inboundChannel);
      }
      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
         cause.printStackTrace();
         closeOnFlush(ctx.channel());
      }
   }

   static void forwardMessage(ChannelHandlerContext fromCtx,
                              Object msg, Channel toChannel) {
      toChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
         if (future.isSuccess()) {
            fromCtx.channel().read();
         } else {
            future.channel().close();
         }
      });
   }

   /**
    * Closes the specified channel after all queued write requests are flushed.
    */
   static void closeOnFlush(Channel ch) {
      if (ch != null && ch.isActive()) {
         ch.writeAndFlush(Unpooled.EMPTY_BUFFER)
            .addListener(ChannelFutureListener.CLOSE);
      }
   }

   @Override
   public void close() throws Exception {
      kill();
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
   }

}

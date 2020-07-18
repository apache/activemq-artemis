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
package org.apache.activemq.artemis.tests.unit.core.remoting.impl.netty;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.NoopAddressResolverGroup;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.spi.core.remoting.BufferHandler;
import org.apache.activemq.artemis.spi.core.remoting.ClientConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class SocksProxyTest extends ActiveMQTestBase {

   private static final int SOCKS_PORT = 1080;

   private ExecutorService closeExecutor;
   private ExecutorService threadPool;
   private ScheduledExecutorService scheduledThreadPool;

   private NioEventLoopGroup bossGroup;
   private NioEventLoopGroup workerGroup;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      closeExecutor       = Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory());
      threadPool          = Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory());
      scheduledThreadPool = Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory());

      startSocksProxy();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      closeExecutor.shutdownNow();
      threadPool.shutdownNow();
      scheduledThreadPool.shutdownNow();

      stopSocksProxy();

      super.tearDown();
   }

   @Test
   public void testSocksProxyHandlerAdded() throws Exception {
      InetAddress address = getNonLoopbackAddress();
      Assume.assumeTrue("Cannot find non-loopback address", address != null);

      BufferHandler handler = (connectionID, buffer) -> {
      };

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.HOST_PROP_NAME, address.getHostAddress());
      params.put(TransportConstants.PROXY_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.PROXY_HOST_PROP_NAME, "localhost");

      ClientConnectionLifeCycleListener listener = new ClientConnectionLifeCycleListener() {
         @Override
         public void connectionException(final Object connectionID, final ActiveMQException me) {
         }

         @Override
         public void connectionDestroyed(final Object connectionID) {
         }

         @Override
         public void connectionCreated(final ActiveMQComponent component,
                                       final Connection connection,
                                       final ClientProtocolManager protocol) {
         }

         @Override
         public void connectionReadyForWrites(Object connectionID, boolean ready) {
         }
      };

      NettyConnector connector = new NettyConnector(params, handler, listener, closeExecutor, threadPool, scheduledThreadPool);

      connector.start();
      Assert.assertTrue(connector.isStarted());

      ChannelPipeline pipeline = connector.getBootStrap().register().await().channel().pipeline();
      Assert.assertNotNull(pipeline.get(Socks5ProxyHandler.class));

      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   private InetAddress getNonLoopbackAddress() throws SocketException {
      Enumeration<NetworkInterface> n = NetworkInterface.getNetworkInterfaces();
      InetAddress addr = null;
      for (; n.hasMoreElements(); ) {
         NetworkInterface e = n.nextElement();
         Enumeration<InetAddress> a = e.getInetAddresses();
         boolean found = false;
         for (; a.hasMoreElements(); ) {
            addr = a.nextElement();
            if (!addr.isLoopbackAddress()) {
               found = true;
               break;
            }
         }
         if (found) {
            break;
         }
      }
      return addr;
   }

   @Test
   public void testSocksProxyHandlerNotAddedForLocalhost() throws Exception {
      BufferHandler handler = (connectionID, buffer) -> {
      };

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.HOST_PROP_NAME, "localhost");
      params.put(TransportConstants.PROXY_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.PROXY_HOST_PROP_NAME, "localhost");

      ClientConnectionLifeCycleListener listener = new ClientConnectionLifeCycleListener() {
         @Override
         public void connectionException(final Object connectionID, final ActiveMQException me) {
         }

         @Override
         public void connectionDestroyed(final Object connectionID) {
         }

         @Override
         public void connectionCreated(final ActiveMQComponent component,
                                       final Connection connection,
                                       final ClientProtocolManager protocol) {
         }

         @Override
         public void connectionReadyForWrites(Object connectionID, boolean ready) {
         }
      };

      NettyConnector connector = new NettyConnector(params, handler, listener, closeExecutor, threadPool, scheduledThreadPool);

      connector.start();
      Assert.assertTrue(connector.isStarted());

      ChannelPipeline pipeline = connector.getBootStrap().register().await().channel().pipeline();
      Assert.assertNull(pipeline.get(Socks5ProxyHandler.class));

      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testSocks5hSupport() throws Exception {
      BufferHandler handler = (connectionID, buffer) -> {
      };
      Map<String, Object> params = new HashMap<>();

      params.put(TransportConstants.HOST_PROP_NAME, "only-resolvable-on-proxy");
      params.put(TransportConstants.PROXY_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.PROXY_HOST_PROP_NAME, "localhost");
      params.put(TransportConstants.PROXY_PORT_PROP_NAME, SOCKS_PORT);
      params.put(TransportConstants.PROXY_REMOTE_DNS_PROP_NAME, true);

      ClientConnectionLifeCycleListener listener = new ClientConnectionLifeCycleListener() {
         @Override
         public void connectionException(final Object connectionID, final ActiveMQException me) {
         }

         @Override
         public void connectionDestroyed(final Object connectionID) {
         }

         @Override
         public void connectionCreated(final ActiveMQComponent component,
                                       final Connection connection,
                                       final ClientProtocolManager protocol) {
         }

         @Override
         public void connectionReadyForWrites(Object connectionID, boolean ready) {
         }
      };

      NettyConnector connector = new NettyConnector(params, handler, listener, closeExecutor, threadPool, scheduledThreadPool);

      connector.start();
      Assert.assertTrue(connector.isStarted());

      connector.getBootStrap().register().await().channel().pipeline();

      AddressResolverGroup<?> resolver = connector.getBootStrap().config().resolver();
      Assert.assertSame(resolver, NoopAddressResolverGroup.INSTANCE);

      Connection connection = connector.createConnection(future -> {
         future.awaitUninterruptibly();
         Assert.assertTrue(future.isSuccess());

         Socks5ProxyHandler socks5Handler = future.channel().pipeline().get(Socks5ProxyHandler.class);
         Assert.assertNotNull(socks5Handler);

         InetSocketAddress remoteAddress = (InetSocketAddress)socks5Handler.destinationAddress();
         Assert.assertTrue(remoteAddress.isUnresolved());
      });
      Assert.assertNotNull(connection);

      Assert.assertTrue(connection.isOpen());
      connection.close();
      Assert.assertFalse(connection.isOpen());

      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   private void startSocksProxy() throws Exception {
      bossGroup   = new NioEventLoopGroup();
      workerGroup = new NioEventLoopGroup();

      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup);
      b.channel(NioServerSocketChannel.class);
      b.childHandler(new ChannelInitializer<SocketChannel>() {
         @Override
         protected void initChannel(SocketChannel ch) throws Exception {
            // We can further configure SOCKS, but have to assume Netty is doing the right thing,
            // we just need something listening on the port to make the initial connection
         }
      });

      b.bind(SOCKS_PORT).sync();
   }

   private void stopSocksProxy() {
      bossGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
      workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
   }
}

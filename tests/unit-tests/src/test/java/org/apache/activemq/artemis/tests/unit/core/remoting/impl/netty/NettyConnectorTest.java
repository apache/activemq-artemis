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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.channel.ChannelPipeline;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.ActiveMQChannelHandler;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.remoting.BufferHandler;
import org.apache.activemq.artemis.spi.core.remoting.ClientConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NettyConnectorTest extends ActiveMQTestBase {

   private ActiveMQServer server;
   private ExecutorService executorService;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      executorService = Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory());

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.SSL_PROVIDER, TransportConstants.OPENSSL_PROVIDER);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "openssl-server-side-keystore.jks");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME,  "openssl-server-side-truststore.jks");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, true);
      ConfigurationImpl config = createBasicConfig().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, "nettySSL"));
      server = createServer(false, config);
      server.start();
      waitForServerToStart(server);
   }

   @Override
   public void tearDown() throws Exception {
      executorService.shutdown();
      super.tearDown();
   }

   private ClientConnectionLifeCycleListener listener = new ClientConnectionLifeCycleListener() {
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

   @Test
   public void testStartStop() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };
      Map<String, Object> params = new HashMap<>();

      NettyConnector connector = new NettyConnector(params, handler, listener, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

      connector.start();
      Assert.assertTrue(connector.isStarted());
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testNullParams() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };
      Map<String, Object> params = new HashMap<>();

      try {
         new NettyConnector(params, null, listener, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

         Assert.fail("Should throw Exception");
      } catch (IllegalArgumentException e) {
         // Ok
      }

      try {
         new NettyConnector(params, handler, null, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

         Assert.fail("Should throw Exception");
      } catch (IllegalArgumentException e) {
         // Ok
      }
   }


   /**
    * that java system properties are read
    */
   @Test
   public void testJavaSystemProperty() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };

      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PATH_PROP_NAME, "openssl-client-side-keystore.jks");
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PATH_PROP_NAME, "openssl-client-side-truststore.jks");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);

      NettyConnector connector = new NettyConnector(params, handler, listener, executorService, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

      connector.start();
      Assert.assertTrue(connector.isStarted());
      Connection c = connector.createConnection();
      assertNotNull(c);
      c.close();
      connector.close();
      Assert.assertFalse(connector.isStarted());

   }

   @Test
   public void testOverridesJavaSystemPropertyFail() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };

      //bad system properties will override the transport constants
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PATH_PROP_NAME, "bad path");
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PASSWORD_PROP_NAME, "bad password");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PATH_PROP_NAME, "bad path");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME, "bad password");

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME,  "openssl-client-side-keystore.jks");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME,"openssl-client-side-truststore.jks");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");

      NettyConnector connector = new NettyConnector(params, handler, listener, executorService, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

      connector.start();
      Assert.assertTrue(connector.isStarted());
      Connection c = connector.createConnection();

      //Should have failed because SSL props override transport config options
      assertNull(c);
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testOverridesJavaSystemProperty() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };

      //system properties will override the bad transport constants
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PATH_PROP_NAME, "openssl-client-side-keystore.jks");
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PATH_PROP_NAME, "openssl-client-side-truststore.jks");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME,  "bad path");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "bad password");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "bad path");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "bad password");

      NettyConnector connector = new NettyConnector(params, handler, listener, executorService, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

      connector.start();
      Assert.assertTrue(connector.isStarted());
      Connection c = connector.createConnection();

      //Should not fail because SSL props override transport config options
      assertNotNull(c);
      c.close();
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testOverridesJavaSystemPropertyForceSSLParameters() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };

      //bad system properties will override the transport constants
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PATH_PROP_NAME, "bad path");
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PASSWORD_PROP_NAME, "bad password");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PATH_PROP_NAME, "bad path");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME, "bad password");

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.FORCE_SSL_PARAMETERS, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME,  "openssl-client-side-keystore.jks");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME,"openssl-client-side-truststore.jks");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");

      NettyConnector connector = new NettyConnector(params, handler, listener, executorService, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

      connector.start();
      Assert.assertTrue(connector.isStarted());
      Connection c = connector.createConnection();

      //Should not fail because forceSSLParameters is set
      assertNotNull(c);
      c.close();
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testOverridesJavaSystemPropertyForceSSLParameters2() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };

      //bad system properties will override the transport constants
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PATH_PROP_NAME, "openssl-client-side-keystore.jks");
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PATH_PROP_NAME, "openssl-client-side-truststore.jks");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);

      // forcing SSL parameters will "undo" the values set by the system properties; all properties will be set to default values
      params.put(TransportConstants.FORCE_SSL_PARAMETERS, true);

      NettyConnector connector = new NettyConnector(params, handler, listener, executorService, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

      connector.start();
      Assert.assertTrue(connector.isStarted());
      Connection c = null;

      try {
         connector.createConnection();
      } catch (Exception e) {
         // ignore
      }

      //Should fail because forceSSLParameters is set
      assertNull(c);
   }

   @Test
   public void tesActiveMQSystemProperties() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);

      NettyConnector connector = new NettyConnector(params, handler, listener, executorService, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

      System.setProperty(NettyConnector.ACTIVEMQ_KEYSTORE_PATH_PROP_NAME, "openssl-client-side-keystore.jks");
      System.setProperty(NettyConnector.ACTIVEMQ_KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      System.setProperty(NettyConnector.ACTIVEMQ_TRUSTSTORE_PATH_PROP_NAME, "openssl-client-side-truststore.jks");
      System.setProperty(NettyConnector.ACTIVEMQ_TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");

      connector.start();
      Assert.assertTrue(connector.isStarted());
      Connection c = connector.createConnection();
      assertNotNull(c);
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testActiveMQOverridesSystemProperty() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);

      NettyConnector connector = new NettyConnector(params, handler, listener, executorService, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

      System.setProperty(NettyConnector.ACTIVEMQ_KEYSTORE_PATH_PROP_NAME, "openssl-client-side-keystore.jks");
      System.setProperty(NettyConnector.ACTIVEMQ_KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      System.setProperty(NettyConnector.ACTIVEMQ_TRUSTSTORE_PATH_PROP_NAME, "openssl-client-side-truststore.jks");
      System.setProperty(NettyConnector.ACTIVEMQ_TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");

      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PATH_PROP_NAME, "bad path");
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PASSWORD_PROP_NAME, "bad password");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PATH_PROP_NAME, "bad path");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME, "bad password");

      connector.start();
      Assert.assertTrue(connector.isStarted());
      Connection c = connector.createConnection();
      assertNotNull(c);
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testBadCipherSuite() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME, "myBadCipherSuite");

      NettyConnector connector = new NettyConnector(params, handler, listener, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

      connector.start();
      Assert.assertTrue(connector.isStarted());
      Assert.assertNull(connector.createConnection());
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testBadProtocol() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME, "myBadProtocol");

      NettyConnector connector = new NettyConnector(params, handler, listener, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

      connector.start();
      Assert.assertTrue(connector.isStarted());
      Assert.assertNull(connector.createConnection());
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testChannelHandlerRemovedWhileCreatingConnection() throws Exception {
      BufferHandler handler = (connectionID, buffer) -> {
      };
      Map<String, Object> params = new HashMap<>();
      final ExecutorService closeExecutor = Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory());
      final ExecutorService threadPool = Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory());
      final ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory());
      try {
         NettyConnector connector = new NettyConnector(params, handler, listener, closeExecutor, threadPool, scheduledThreadPool);
         connector.start();
         final Connection connection = connector.createConnection(future -> {
            future.awaitUninterruptibly();
            Assert.assertTrue(future.isSuccess());
            final ChannelPipeline pipeline = future.channel().pipeline();
            final ActiveMQChannelHandler activeMQChannelHandler = pipeline.get(ActiveMQChannelHandler.class);
            Assert.assertNotNull(activeMQChannelHandler);
            pipeline.remove(activeMQChannelHandler);
            Assert.assertNull(pipeline.get(ActiveMQChannelHandler.class));
         });
         Assert.assertNull(connection);
         connector.close();
      } finally {
         closeExecutor.shutdownNow();
         threadPool.shutdownNow();
         scheduledThreadPool.shutdownNow();
      }
   }
}

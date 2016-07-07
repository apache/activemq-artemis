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
import java.util.concurrent.Executors;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
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
import org.junit.Assert;
import org.junit.Test;

public class NettyConnectorTest extends ActiveMQTestBase {

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

   @Test
   public void testJavaSystemPropertyOverrides() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "bad path");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "bad password");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "bad path");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "bad password");

      NettyConnector connector = new NettyConnector(params, handler, listener, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PATH_PROP_NAME, "client-side-keystore.jks");
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PATH_PROP_NAME, "client-side-truststore.jks");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");

      connector.start();
      Assert.assertTrue(connector.isStarted());
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testActiveMQSystemPropertyOverrides() throws Exception {
      BufferHandler handler = new BufferHandler() {
         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "bad path");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "bad password");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "bad path");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "bad password");

      NettyConnector connector = new NettyConnector(params, handler, listener, Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory()), Executors.newScheduledThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));

      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PATH_PROP_NAME, "bad path");
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PASSWORD_PROP_NAME, "bad password");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PATH_PROP_NAME, "bad path");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME, "bad password");

      System.setProperty(NettyConnector.ACTIVEMQ_KEYSTORE_PATH_PROP_NAME, "client-side-keystore.jks");
      System.setProperty(NettyConnector.ACTIVEMQ_KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      System.setProperty(NettyConnector.ACTIVEMQ_TRUSTSTORE_PATH_PROP_NAME, "client-side-truststore.jks");
      System.setProperty(NettyConnector.ACTIVEMQ_TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");

      connector.start();
      Assert.assertTrue(connector.isStarted());
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
}

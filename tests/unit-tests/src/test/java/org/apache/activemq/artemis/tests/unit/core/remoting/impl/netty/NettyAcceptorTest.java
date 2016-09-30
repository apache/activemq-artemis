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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptor;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.BufferHandler;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ServerConnectionLifeCycleListener;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NettyAcceptorTest extends ActiveMQTestBase {

   private ScheduledExecutorService pool2;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      ActiveMQTestBase.checkFreePort(TransportConstants.DEFAULT_PORT);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         ActiveMQTestBase.checkFreePort(TransportConstants.DEFAULT_PORT);
      } finally {
         if (pool2 != null)
            pool2.shutdownNow();
         super.tearDown();
      }
   }

   @Test
   public void testStartStop() throws Exception {
      BufferHandler handler = new BufferHandler() {

         @Override
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         }
      };

      Map<String, Object> params = new HashMap<>();
      ServerConnectionLifeCycleListener listener = new ServerConnectionLifeCycleListener() {

         @Override
         public void connectionException(final Object connectionID, final ActiveMQException me) {
         }

         @Override
         public void connectionDestroyed(final Object connectionID) {
         }

         @Override
         public void connectionCreated(final ActiveMQComponent component,
                                       final Connection connection,
                                       final ProtocolManager protocol) {
         }

         @Override
         public void connectionReadyForWrites(Object connectionID, boolean ready) {
         }
      };
      pool2 = Executors.newScheduledThreadPool(ActiveMQDefaultConfiguration.getDefaultScheduledThreadPoolMaxSize(), ActiveMQThreadFactory.defaultThreadFactory());
      NettyAcceptor acceptor = new NettyAcceptor("netty", null, params, handler, listener, pool2, new HashMap<String, ProtocolManager>());

      addActiveMQComponent(acceptor);
      acceptor.start();
      Assert.assertTrue(acceptor.isStarted());
      acceptor.stop();
      Assert.assertFalse(acceptor.isStarted());
      ActiveMQTestBase.checkFreePort(TransportConstants.DEFAULT_PORT);

      acceptor.start();
      Assert.assertTrue(acceptor.isStarted());
      acceptor.stop();
      Assert.assertFalse(acceptor.isStarted());
      ActiveMQTestBase.checkFreePort(TransportConstants.DEFAULT_PORT);

      pool2.shutdown();

      pool2.awaitTermination(1, TimeUnit.SECONDS);
   }

}

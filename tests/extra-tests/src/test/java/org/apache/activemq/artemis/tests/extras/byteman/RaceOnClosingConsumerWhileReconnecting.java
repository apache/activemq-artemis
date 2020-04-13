/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.extras.byteman;

import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class RaceOnClosingConsumerWhileReconnecting extends ActiveMQTestBase {
   static RemotingConnection conn;

   static ClientConsumer consumer;

   protected ActiveMQServer server = null;

   protected ClientSessionFactoryInternal sf = null;

   protected ClientSessionInternal session = null;

   protected final SimpleString queueName1 = new SimpleString("my_queue_one");

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      conn = null;
      consumer = null;
      server = createServer(true, true);
      server.start();

      SimpleString addressName1 = new SimpleString("my_address_one");

      server.addAddressInfo(new AddressInfo(addressName1, RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(queueName1).setAddress(addressName1).setRoutingType(RoutingType.ANYCAST));

      final long retryInterval = 500;
      final double retryMultiplier = 1d;
      final int reconnectAttempts = 10;
      ServerLocator locator = createFactory(true).setCallFailoverTimeout(0).setCallTimeout(2000).setRetryInterval(retryInterval).setRetryIntervalMultiplier(retryMultiplier).setReconnectAttempts(reconnectAttempts).setConfirmationWindowSize(-1);
      sf = (ClientSessionFactoryInternal) createSessionFactory(locator);
      session = (ClientSessionInternal)sf.createSession(false, true, true);

   }

   @Override
   @After
   public void tearDown() throws Exception {
      if (session != null) {
         session.close();
      }
      if (sf != null) {
         sf.close();
      }
      if (server != null) {
         server.stop();
      }
      conn = null;
      consumer = null;
      super.tearDown();
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "session.removeConsumer wait",
         targetClass = "org.apache.activemq.artemis.core.client.impl.ClientSessionImpl",
         targetMethod = "removeConsumer(org.apache.activemq.artemis.core.client.impl.ClientConsumerInternal)",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.RaceOnClosingConsumerWhileReconnecting.waitForReconnection();")})
   public void testClosingConsumerBeforeReconnecting() throws Exception {
      conn = session.getConnection();

      ClientConsumer clientConsumer1 = session.createConsumer(queueName1);
      ClientConsumer clientConsumer2 = session.createConsumer(queueName1);
      clientConsumer1.close();

      Thread.sleep(500);
      Set<ServerConsumer> serverConsumers = server.getSessionByID(session.getName()).getServerConsumers();
      ServerConsumer serverConsumer = serverConsumers.iterator().next();
      assertEquals(1, serverConsumers.size());
      assertEquals(clientConsumer2.getConsumerContext().getId(), serverConsumer.getID());
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "session.closeConsumer before recreating consumer",
         targetClass = "org.apache.activemq.artemis.core.client.impl.ClientSessionImpl",
         targetMethod = "handleFailover",
         targetLocation = "AFTER WRITE $consumerInternal 1",
         action = "org.apache.activemq.artemis.tests.extras.byteman.RaceOnClosingConsumerWhileReconnecting.closeConsumer();")})
   public void testClosingConsumerBeforeRecreatingOneConsumer() throws Exception {
      RemotingConnection conn = session.getConnection();

      ClientConsumer clientConsumer1 = session.createConsumer(queueName1);
      consumer = clientConsumer1;
      conn.fail(new ActiveMQNotConnectedException());

      Thread.sleep(500);
      Set<ServerConsumer> serverConsumers = server.getSessionByID(session.getName()).getServerConsumers();
      assertEquals(0, serverConsumers.size());
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "session.closeConsumer before recreating consumer",
         targetClass = "org.apache.activemq.artemis.core.client.impl.ClientSessionImpl",
         targetMethod = "handleFailover",
         targetLocation = "AFTER WRITE $consumerInternal 1",
         action = "org.apache.activemq.artemis.tests.extras.byteman.RaceOnClosingConsumerWhileReconnecting.closeConsumer();")})
   public void testClosingConsumerBeforeRecreatingTwoConsumers() throws Exception {
      RemotingConnection conn = session.getConnection();

      ClientConsumer clientConsumer1 = session.createConsumer(queueName1);
      ClientConsumer clientConsumer2 = session.createConsumer(queueName1);
      consumer = clientConsumer1;
      conn.fail(new ActiveMQNotConnectedException());

      Thread.sleep(500);
      ServerSession serverSession = server.getSessionByID(session.getName());
      assertNotNull(serverSession);
      Set<ServerConsumer> serverConsumers = serverSession.getServerConsumers();
      ServerConsumer serverConsumer = serverConsumers.iterator().next();
      assertEquals(1, serverConsumers.size());
      assertEquals(clientConsumer2.getConsumerContext().getId(), serverConsumer.getID());
   }

   public static void closeConsumer() {
      if (consumer != null) {
         try {
            consumer.close();
         } catch (Exception e) {
            e.printStackTrace();
         } finally {
            consumer = null;
         }
      }
   }

   public static void waitForReconnection() {
      if (conn != null) {
         try {
            conn.fail(new ActiveMQNotConnectedException());
         } catch (Exception e) {
            e.printStackTrace();
         } finally {
            conn = null;
         }
      }
   }
}
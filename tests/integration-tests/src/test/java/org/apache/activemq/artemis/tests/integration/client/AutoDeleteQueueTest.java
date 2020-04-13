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
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Before;
import org.junit.Test;

public class AutoDeleteQueueTest extends ActiveMQTestBase {

   public final SimpleString addressA = new SimpleString("addressA");
   public final SimpleString queueA = new SimpleString("queueA");

   private ServerLocator locator;
   private ActiveMQServer server;
   private ClientSessionFactory cf;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      locator = createInVMNonHALocator();
      server = createServer(false);
      server.getConfiguration().setAddressQueueScanPeriod(500);
      server.getConfiguration().setMessageExpiryScanPeriod(500);

      server.start();
      cf = createSessionFactory(locator);
   }

   @Test
   public void testAutoDeleteAutoCreatedQueueOnLastConsumerClose() throws Exception {
      // auto-delete-queues defaults to true
      server.createQueue(new QueueConfiguration(queueA).setAddress(addressA).setRoutingType(RoutingType.ANYCAST).setAutoCreated(true));
      assertNotNull(server.locateQueue(queueA));
      cf.createSession().createConsumer(queueA).close();
      Wait.assertTrue(() -> server.locateQueue(queueA) == null);
   }

   @Test
   public void testAutoDeleteAutoCreatedQueueOnLastMessageRemovedWithoutConsumer() throws Exception {
      // auto-delete-queues defaults to true
      server.createQueue(new QueueConfiguration(queueA).setAddress(addressA).setRoutingType(RoutingType.ANYCAST).setAutoCreated(true));
      assertNotNull(server.locateQueue(queueA));
      ClientSession session = cf.createSession();
      ClientProducer producer = session.createProducer(addressA);
      producer.send(session.createMessage(true));
      Wait.assertEquals(1, server.locateQueue(queueA)::getMessageCount);
      server.locateQueue(queueA).deleteAllReferences();
      Wait.assertTrue(() -> server.locateQueue(queueA) == null, 2000, 100);
   }

   @Test
   public void testAutoDeleteAutoCreatedQueueOnLastMessageExpired() throws Exception {
      // auto-delete-queues defaults to true
      server.createQueue(new QueueConfiguration(queueA).setAddress(addressA).setRoutingType(RoutingType.ANYCAST).setAutoCreated(true));
      assertNotNull(server.locateQueue(queueA));
      ClientSession session = cf.createSession();
      ClientProducer producer = session.createProducer(addressA);
      producer.send(session.createMessage(true).setExpiration(System.currentTimeMillis()));
      Wait.assertTrue(() -> server.locateQueue(queueA) == null, 2000, 100);
   }

   @Test
   public void testNegativeAutoDeleteAutoCreatedQueue() throws Exception {
      server.getAddressSettingsRepository().addMatch(addressA.toString(), new AddressSettings().setAutoDeleteQueues(false));
      server.createQueue(new QueueConfiguration(queueA).setAddress(addressA).setRoutingType(RoutingType.ANYCAST).setAutoCreated(true));
      assertNotNull(server.locateQueue(queueA));
      cf.createSession().createConsumer(queueA).close();
      assertNotNull(server.locateQueue(queueA));
   }

   @Test
   public void testNegativeAutoDeleteAutoCreatedQueue2() throws Exception {
      server.getAddressSettingsRepository().addMatch(addressA.toString(), new AddressSettings());
      server.createQueue(new QueueConfiguration(queueA).setAddress(addressA).setRoutingType(RoutingType.ANYCAST).setAutoCreated(true));
      assertNotNull(server.locateQueue(queueA));
      assertFalse(Wait.waitFor(() -> server.locateQueue(queueA) == null, 5000, 100));
   }
}

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
package org.apache.activemq.artemis.tests.integration.server;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.junit.Wait;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Before;
import org.junit.Test;

public class AddressQueueDeleteDelayTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   private ClientSession session;

   private ClientSessionFactory sf;

   private ServerLocator locator;

   @Test
   public void testAddressQueueDeleteDelay() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final long deleteQueuesDelay = 3000;
      final long deleteAddressesDelay = 5000;
      final long fudge = 200;

      AddressSettings addressSettings = new AddressSettings().setAutoDeleteQueuesDelay(deleteQueuesDelay).setAutoDeleteAddressesDelay(deleteAddressesDelay);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      session.createQueue(address, RoutingType.MULTICAST, queue, null, true, true);

      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) != null, 2000, 100));

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(true);
      producer.send(message);
      ClientConsumer consumer = session.createConsumer(queue);
      session.start();
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      session.commit();
      consumer.close();
      long start = System.currentTimeMillis();

      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) == null, deleteQueuesDelay + fudge, 50));
      long elapsedTime = System.currentTimeMillis() - start;
      IntegrationTestLogger.LOGGER.info("Elapsed time to delete queue: " + elapsedTime);
      assertTrue(elapsedTime >= (deleteQueuesDelay - fudge));
      start = System.currentTimeMillis();
      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) == null, deleteAddressesDelay + fudge, 50));
      elapsedTime = System.currentTimeMillis() - start;
      IntegrationTestLogger.LOGGER.info("Elapsed time to delete address: " + elapsedTime);
      assertTrue(elapsedTime >= (deleteAddressesDelay - fudge));
   }

   @Test
   public void testAddressQueueDeleteDelayWithAdditionalAddressQueue() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final long deleteQueuesDelay = 3000;
      final long deleteAddressesDelay = 5000;
      final long fudge = 200;

      AddressSettings addressSettings = new AddressSettings().setAutoDeleteQueuesDelay(deleteQueuesDelay).setAutoDeleteAddressesDelay(deleteAddressesDelay);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      session.createQueue(address, RoutingType.MULTICAST, queue, null, true, true);

      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) != null, 2000, 100));

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(true);
      producer.send(message);
      ClientConsumer consumer = session.createConsumer(queue);
      session.start();
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      session.commit();
      consumer.close();

      Thread.sleep(deleteQueuesDelay / 2);
      consumer = session.createConsumer(queue);
      Thread.sleep(deleteQueuesDelay / 2);
      consumer.close();

      long start = System.currentTimeMillis();
      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) == null, deleteQueuesDelay + fudge, 50));
      assertTrue(System.currentTimeMillis() - start >= (deleteQueuesDelay - fudge));

      Thread.sleep(deleteAddressesDelay / 2);
      session.createQueue(address, RoutingType.MULTICAST, queue, null, true, true);
      Thread.sleep(deleteAddressesDelay / 2);
      session.deleteQueue(queue);

      start = System.currentTimeMillis();
      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) == null, deleteAddressesDelay + fudge, 50));
      assertTrue(System.currentTimeMillis() - start >= (deleteAddressesDelay - fudge));
   }

   @Test
   public void testDefaultAddressQueueDeleteDelay() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final long fudge = 200;

      session.createQueue(address, RoutingType.MULTICAST, queue, null, true, true);

      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) != null, 2000, 100));

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(true);
      producer.send(message);
      ClientConsumer consumer = session.createConsumer(queue);
      session.start();
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      session.commit();
      consumer.close();
      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) == null, fudge, 50));
      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) == null, fudge, 50));
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false);

      server.getConfiguration().setAddressQueueScanPeriod(100);

      server.start();
      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));
   }
}

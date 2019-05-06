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

   public static final int DURATION_MILLIS = 30_000;
   public static final int SLEEP_MILLIS = 100;

   private ActiveMQServer server;

   private ClientSession session;

   private ClientSessionFactory sf;

   private ServerLocator locator;

   @Test
   public void testAddressQueueDeleteDelay() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final long deleteQueuesDelay = 300;
      final long deleteAddressesDelay = 500;

      AddressSettings addressSettings = new AddressSettings().setAutoDeleteQueuesDelay(deleteQueuesDelay).setAutoDeleteAddressesDelay(deleteAddressesDelay);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      session.createQueue(address, RoutingType.MULTICAST, queue, null, true, true);

      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) != null, DURATION_MILLIS, SLEEP_MILLIS));

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

      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) == null, DURATION_MILLIS, SLEEP_MILLIS));
      long elapsedTime = System.currentTimeMillis() - start;
      IntegrationTestLogger.LOGGER.info("Elapsed time to delete queue: " + elapsedTime);
      assertTrue(elapsedTime >= (deleteQueuesDelay));
      start = System.currentTimeMillis();
      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) == null, DURATION_MILLIS, SLEEP_MILLIS));
      elapsedTime = System.currentTimeMillis() - start;
      IntegrationTestLogger.LOGGER.info("Elapsed time to delete address: " + elapsedTime);
      assertTrue(elapsedTime >= (deleteAddressesDelay));
   }

   @Test
   public void testAddressQueueDeleteDelayWithAdditionalAddressQueue() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final long deleteQueuesDelay = 300;
      final long deleteAddressesDelay = 500;

      AddressSettings addressSettings = new AddressSettings().setAutoDeleteQueuesDelay(deleteQueuesDelay).setAutoDeleteAddressesDelay(deleteAddressesDelay);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      session.createQueue(address, RoutingType.MULTICAST, queue, null, true, true);

      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) != null, DURATION_MILLIS, SLEEP_MILLIS));

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

      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) != null, DURATION_MILLIS, SLEEP_MILLIS));
      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) == null, DURATION_MILLIS, SLEEP_MILLIS));

      session.createQueue(address, RoutingType.MULTICAST, queue, null, true, true);

      consumer = session.createConsumer(queue);
      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) != null, DURATION_MILLIS, SLEEP_MILLIS));
      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) != null, DURATION_MILLIS, SLEEP_MILLIS));
      consumer.close();

      long start = System.currentTimeMillis();
      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) == null, DURATION_MILLIS, SLEEP_MILLIS));
      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) == null, DURATION_MILLIS, SLEEP_MILLIS));
      assertTrue(System.currentTimeMillis() - start >= (deleteQueuesDelay));

      session.createQueue(address, RoutingType.MULTICAST, queue, null, true, true);
      session.deleteQueue(queue);

      start = System.currentTimeMillis();
      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) == null, DURATION_MILLIS, SLEEP_MILLIS));
      assertTrue(System.currentTimeMillis() - start >= (deleteAddressesDelay));
   }

   @Test
   public void testDefaultAddressQueueDeleteDelay() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, true, true);

      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) != null, DURATION_MILLIS, SLEEP_MILLIS));

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
      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) == null, DURATION_MILLIS, SLEEP_MILLIS));
      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) == null, DURATION_MILLIS, SLEEP_MILLIS));
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false);

      server.getConfiguration().setAddressQueueScanPeriod(SLEEP_MILLIS);

      server.start();
      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));
   }
}

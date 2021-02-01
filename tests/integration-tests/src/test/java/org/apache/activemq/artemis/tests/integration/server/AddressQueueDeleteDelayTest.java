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

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AddressQueueDeleteDelayTest extends ActiveMQTestBase {

   private static final Logger log = Logger.getLogger(AddressQueueDeleteDelayTest.class);

   public static final int DURATION_MILLIS = 30_000;
   public static final int NEGATIVE_DURATION_MILLIS = 1_000;
   public static final int SLEEP_MILLIS = 100;

   private ActiveMQServer server;

   private ClientSession session;

   private ClientSessionFactory sf;

   private ServerLocator locator;

   @Test
   public void testAddressQueueDeleteDelay() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final long deleteQueuesDelay = 150;
      final long deleteAddressesDelay = 500;

      AddressSettings addressSettings = new AddressSettings().setAutoDeleteQueuesDelay(deleteQueuesDelay).setAutoDeleteAddressesDelay(deleteAddressesDelay);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      session.createQueue(new QueueConfiguration(queue).setAddress(address).setAutoCreated(true));

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

      final AddressInfo info = server.getAddressInfo(address);
      Wait.assertTrue(() -> server.locateQueue(queue) == null, DURATION_MILLIS, 10);
      Assert.assertNotNull(info);
      Wait.assertTrue(() -> info.getBindingRemovedTimestamp() > 0, 5000, 10);


      long elapsedTime = System.currentTimeMillis() - start;
      log.debug("Elapsed time to delete queue: " + elapsedTime);
      assertTrue(elapsedTime >= (deleteQueuesDelay));

      start = info.getBindingRemovedTimestamp();

      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) == null, DURATION_MILLIS, SLEEP_MILLIS));
      elapsedTime = System.currentTimeMillis() - start;
      log.debug("Elapsed time to delete address: " + elapsedTime);
      assertTrue("ellapsedTime=" + elapsedTime + " while delay is " + deleteAddressesDelay, elapsedTime >= (deleteAddressesDelay));
   }

   @Test
   public void testAddressQueueDeleteDelayWithAdditionalAddressQueue() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final long deleteQueuesDelay = 300;
      final long deleteAddressesDelay = 500;

      AddressSettings addressSettings = new AddressSettings().setAutoDeleteQueuesDelay(deleteQueuesDelay).setAutoDeleteAddressesDelay(deleteAddressesDelay);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      session.createQueue(new QueueConfiguration(queue).setAddress(address).setAutoCreated(true));

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

      session.createQueue(new QueueConfiguration(queue).setAddress(address).setAutoCreated(true));

      consumer = session.createConsumer(queue);
      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) != null, DURATION_MILLIS, SLEEP_MILLIS));
      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) != null, DURATION_MILLIS, SLEEP_MILLIS));
      consumer.close();

      long start = System.currentTimeMillis();
      assertTrue(Wait.waitFor(() -> server.locateQueue(queue) == null, DURATION_MILLIS, SLEEP_MILLIS));
      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) == null, DURATION_MILLIS, SLEEP_MILLIS));
      assertTrue(System.currentTimeMillis() - start >= (deleteQueuesDelay));

      session.createQueue(new QueueConfiguration(queue).setAddress(address).setAutoCreated(true));
      session.deleteQueue(queue);

      start = System.currentTimeMillis();
      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) == null, DURATION_MILLIS, SLEEP_MILLIS));
      assertTrue(System.currentTimeMillis() - start >= (deleteAddressesDelay));
   }

   @Test
   public void testDefaultAddressQueueDeleteDelay() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(new QueueConfiguration(queue).setAddress(address).setAutoCreated(true));

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

   @Test
   public void testAddressDeleteDelay() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final long deleteAddressesDelay = 500;

      AddressSettings addressSettings = new AddressSettings().setAutoDeleteAddressesDelay(deleteAddressesDelay);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      session.createAddress(address, RoutingType.MULTICAST, true);
      session.createQueue(new QueueConfiguration(queue).setAddress(address));
      session.deleteQueue(queue);

      assertTrue(Wait.waitFor(() -> server.getAddressInfo(address) == null, DURATION_MILLIS, SLEEP_MILLIS));
   }

   @Test
   public void testAddressDeleteDelayNegative() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final long deleteAddressesDelay = 500;

      AddressSettings addressSettings = new AddressSettings().setAutoDeleteAddressesDelay(deleteAddressesDelay);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      // the address should not be deleted since it is not auto-created
      session.createAddress(address, RoutingType.MULTICAST, false);
      session.createQueue(new QueueConfiguration(queue).setAddress(address));
      session.deleteQueue(queue);

      Thread.sleep(1000); // waiting some time so the delay would kick in if misconfigured
      Assert.assertTrue(server.getAddressInfo(address) != null);
   }

   @Test
   public void testAddressDeleteDelayNegative2() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final long deleteAddressesDelay = 500;

      // the address should not be deleted since autoDeleteAddresses = false
      AddressSettings addressSettings = new AddressSettings().setAutoDeleteAddressesDelay(deleteAddressesDelay).setAutoDeleteAddresses(false);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      session.createAddress(address, RoutingType.MULTICAST, true);
      session.createQueue(new QueueConfiguration(queue).setAddress(address));
      session.deleteQueue(queue);

      Thread.sleep(1000); // waiting some time so the delay would kick in if misconfigured
      Assert.assertTrue(server.getAddressInfo(address) != null);
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

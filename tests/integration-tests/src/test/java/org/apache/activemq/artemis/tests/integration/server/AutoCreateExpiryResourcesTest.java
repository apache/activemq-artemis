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

package org.apache.activemq.artemis.tests.integration.server;

import javax.jms.JMSContext;

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
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.junit.Before;
import org.junit.Test;

public class AutoCreateExpiryResourcesTest extends ActiveMQTestBase {
   public final SimpleString addressA = new SimpleString("addressA");
   public final SimpleString queueA = new SimpleString("queueA");
   public final SimpleString expiryAddress = new SimpleString("myExpiry");

   private ActiveMQServer server;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(false);

      // set common address settings needed for all tests; make sure to use getMatch instead of addMatch in invidual tests or these will be overwritten
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setAutoCreateExpiryResources(true).setExpiryAddress(expiryAddress).setExpiryDelay(100L));
      server.getConfiguration().setMessageExpiryScanPeriod(50L);

      server.start();
   }

   @Test
   public void testAutoCreationOfExpiryResources() throws Exception {
      int before = server.getActiveMQServerControl().getQueueNames().length;
      triggerExpiration();
      Wait.assertTrue(() -> server.getAddressInfo(expiryAddress) != null, 2000, 100);
      assertNotNull(server.locateQueue(getDefaultExpiryQueueName(addressA)));
      assertEquals(2, server.getActiveMQServerControl().getQueueNames().length - before);
   }

   @Test
   public void testAutoCreationOfExpiryResourcesWithNullExpiry() throws Exception {
      testAutoCreationOfExpiryResourcesWithNoExpiry(null);
   }

   @Test
   public void testAutoCreationOfExpiryResourcesWithEmptyExpiry() throws Exception {
      testAutoCreationOfExpiryResourcesWithNoExpiry(SimpleString.toSimpleString(""));
   }

   private void testAutoCreationOfExpiryResourcesWithNoExpiry(SimpleString expiryAddress) throws Exception {
      server.getAddressSettingsRepository().getMatch("#").setExpiryAddress(expiryAddress);
      int before = server.getActiveMQServerControl().getQueueNames().length;
      triggerExpiration();
      if (expiryAddress != null) {
         assertNull(server.getAddressInfo(expiryAddress));
      }
      assertNull(server.locateQueue(getDefaultExpiryQueueName(addressA)));
      assertEquals(1, server.getActiveMQServerControl().getQueueNames().length - before);
   }

   @Test
   public void testAutoCreateExpiryQueuePrefix() throws Exception {
      SimpleString prefix = RandomUtil.randomSimpleString();
      server.getAddressSettingsRepository().getMatch("#").setExpiryQueuePrefix(prefix);
      triggerExpiration();
      Wait.assertTrue(() -> server.locateQueue(prefix.concat(addressA).concat(AddressSettings.DEFAULT_EXPIRY_QUEUE_SUFFIX)) != null, 2000, 100);
   }

   @Test
   public void testAutoCreateExpiryQueueSuffix() throws Exception {
      SimpleString suffix = RandomUtil.randomSimpleString();
      server.getAddressSettingsRepository().getMatch("#").setExpiryQueueSuffix(suffix);
      triggerExpiration();
      Wait.assertTrue(() -> server.locateQueue(AddressSettings.DEFAULT_EXPIRY_QUEUE_PREFIX.concat(addressA).concat(suffix)) != null, 2000, 100);
   }

   @Test
   public void testAutoCreateExpiryQueuePrefixAndSuffix() throws Exception {
      SimpleString prefix = RandomUtil.randomSimpleString();
      SimpleString suffix = RandomUtil.randomSimpleString();
      server.getAddressSettingsRepository().getMatch("#").setExpiryQueuePrefix(prefix).setExpiryQueueSuffix(suffix);
      triggerExpiration();
      Wait.assertTrue(() -> server.locateQueue(prefix.concat(addressA).concat(suffix)) != null, 2000, 100);
   }

   @Test
   public void testAutoCreatedExpiryFilterAnycast() throws Exception {
      testAutoCreatedExpiryFilter(RoutingType.ANYCAST);
   }

   @Test
   public void testAutoCreatedExpiryFilterMulticast() throws Exception {
      testAutoCreatedExpiryFilter(RoutingType.MULTICAST);
   }

   private void testAutoCreatedExpiryFilter(RoutingType routingType) throws Exception {
      final int ITERATIONS = 50;
      final int MESSAGE_COUNT = 5;

      for (int i = 0; i < ITERATIONS; i++) {
         SimpleString address = RandomUtil.randomSimpleString();
         SimpleString queue = RandomUtil.randomSimpleString();
         server.createQueue(new QueueConfiguration(queue).setAddress(address).setRoutingType(routingType));
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory cf = createSessionFactory(locator);
         ClientSession s = addClientSession(cf.createSession(true, false));
         ClientProducer p = s.createProducer(address);
         for (int j = 0; j < MESSAGE_COUNT; j++) {
            p.send(s.createMessage(true).setRoutingType(routingType));
         }
         p.close();
         s.close();
         cf.close();
         locator.close();
         Wait.assertTrue(() -> server.locateQueue(getDefaultExpiryQueueName(address)) != null, 2000, 10);
         Queue expiry = server.locateQueue(AddressSettings.DEFAULT_EXPIRY_QUEUE_PREFIX.concat(address).concat(AddressSettings.DEFAULT_EXPIRY_QUEUE_SUFFIX));
         Wait.assertEquals(MESSAGE_COUNT, expiry::getMessageCount);
      }

      assertEquals(ITERATIONS, server.getPostOffice().getBindingsForAddress(expiryAddress).getBindings().size());
   }

   @Test
   public void testAutoDeletionAndRecreationOfExpiryResources() throws Exception {
      SimpleString expiryQueueName = getDefaultExpiryQueueName(addressA);

      triggerExpiration();

      // consume the message from the DLQ so it will be auto-deleted
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sessionFactory = createSessionFactory(locator);
      ClientSession session = addClientSession(sessionFactory.createSession(true, true));
      Wait.assertTrue(() -> server.locateQueue(expiryQueueName) != null, 2000, 100);
      ClientConsumer consumer = session.createConsumer(expiryQueueName);
      session.start();
      ClientMessage message = consumer.receive();
      assertNotNull(message);
      message.acknowledge();
      consumer.close();
      session.close();
      sessionFactory.close();
      locator.close();

      Wait.assertTrue(() -> server.locateQueue(expiryQueueName) == null, 2000, 100);

      server.destroyQueue(queueA);

      triggerExpiration();
      Wait.assertTrue(() -> server.getAddressInfo(expiryAddress) != null, 2000, 100);
      Wait.assertTrue(() -> server.locateQueue(expiryQueueName) != null, 2000, 100);
   }

   @Test
   public void testWithJMSFQQN() throws Exception {
      SimpleString expiryQueueName = getDefaultExpiryQueueName(addressA);
      String fqqn = CompositeAddress.toFullyQualified(expiryAddress, expiryQueueName).toString();

      triggerExpiration();

      JMSContext context = new ActiveMQConnectionFactory("vm://0").createContext();
      context.start();
      assertNotNull(context.createConsumer(context.createQueue(fqqn)).receive(2000));
   }

   private SimpleString getDefaultExpiryQueueName(SimpleString address) {
      return AddressSettings.DEFAULT_EXPIRY_QUEUE_PREFIX.concat(address).concat(AddressSettings.DEFAULT_EXPIRY_QUEUE_SUFFIX);
   }

   private void triggerExpiration() throws Exception {
      server.createQueue(new QueueConfiguration(queueA).setAddress(addressA).setRoutingType(RoutingType.ANYCAST));
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sessionFactory = createSessionFactory(locator);
      ClientSession session = addClientSession(sessionFactory.createSession(true, false));
      ClientProducer producer = addClientProducer(session.createProducer(addressA));
      ClientMessage message = session.createMessage(true);
      producer.send(message);
      producer.close();
      session.close();
      sessionFactory.close();
      locator.close();
   }
}

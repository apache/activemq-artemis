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
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MessageExpirationTest extends ActiveMQTestBase {

   private static final int EXPIRATION = 1000;
   private static final int MIN_EXPIRATION = 500;
   private static final int MAX_EXPIRATION = 1500;

   private ActiveMQServer server;

   private ClientSession session;

   private ClientSessionFactory sf;

   private ServerLocator locator;

   @Test
   public void testMessagesExpiredNoBindings() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString expiryAddress = RandomUtil.randomSimpleString();

      AddressSettings addressSettings = new AddressSettings().setExpiryAddress(expiryAddress);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      session.createQueue(new QueueConfiguration(queue).setAddress(address));

      Assert.assertEquals(0, server.locateQueue(queue).getMessagesExpired());

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(true);
      message.setExpiration(System.currentTimeMillis() + EXPIRATION);
      producer.send(message);

      Assert.assertTrue(Wait.waitFor(() -> server.locateQueue(queue).getMessagesExpired() == 1, 2000, 100));

      assertEquals(0, server.locateQueue(queue).getMessageCount());
      assertEquals(0, server.locateQueue(queue).getDeliveringCount());

      session.deleteQueue(queue);
   }

   @Test
   public void testMessagesExpiredNoExpiryAddress() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(new QueueConfiguration(queue).setAddress(address));

      Assert.assertEquals(0, server.locateQueue(queue).getMessagesExpired());

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(true);
      message.setExpiration(System.currentTimeMillis() + EXPIRATION);
      producer.send(message);

      Assert.assertTrue(Wait.waitFor(() -> server.locateQueue(queue).getMessagesExpired() == 1, 2000, 100));

      assertEquals(0, server.locateQueue(queue).getMessageCount());
      assertEquals(0, server.locateQueue(queue).getDeliveringCount());

      session.deleteQueue(queue);
   }

   @Test
   public void testMessageWithNoExpirationMinExpiryDelayOverride() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.close();

      session = addClientSession(sf.createSession(false, true, false));
      session.createQueue(new QueueConfiguration(queue).setAddress(address).setDurable(false));

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);

      AddressSettings addressSettings = new AddressSettings().setMinExpiryDelay((long) MIN_EXPIRATION);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      producer.send(message);

      long start = System.currentTimeMillis();
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> server.locateQueue(queue).getMessagesExpired() == 1, MIN_EXPIRATION + 200, 50);
      assertTrue(System.currentTimeMillis() - start > MIN_EXPIRATION);

      session.deleteQueue(queue);
   }

   @Test
   public void testMessageWithTooSmallExpirationMinExpiryDelayOverride() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.close();

      session = addClientSession(sf.createSession(false, true, false));
      session.createQueue(new QueueConfiguration(queue).setAddress(address).setDurable(false));

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);

      AddressSettings addressSettings = new AddressSettings().setMinExpiryDelay((long) MIN_EXPIRATION);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      message.setExpiration(System.currentTimeMillis() + (MIN_EXPIRATION / 2));
      producer.send(message);

      long start = System.currentTimeMillis();
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> server.locateQueue(queue).getMessagesExpired() == 1, MIN_EXPIRATION + 200, 50);
      assertTrue(System.currentTimeMillis() - start > MIN_EXPIRATION);

      session.deleteQueue(queue);
   }

   @Test
   public void testMessageWithNoExpirationMaxExpiryDelayOverride() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.close();

      session = addClientSession(sf.createSession(false, true, false));
      session.createQueue(new QueueConfiguration(queue).setAddress(address).setDurable(false));

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);

      AddressSettings addressSettings = new AddressSettings().setMaxExpiryDelay((long) MAX_EXPIRATION);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      producer.send(message);

      long start = System.currentTimeMillis();
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> server.locateQueue(queue).getMessagesExpired() == 1, MAX_EXPIRATION + 200, 50);
      assertTrue(System.currentTimeMillis() - start <= (MAX_EXPIRATION + 200));

      session.deleteQueue(queue);
   }

   @Test
   public void testMessageWithTooLargeExpirationMaxExpiryDelayOverride() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.close();

      session = addClientSession(sf.createSession(false, true, false));
      session.createQueue(new QueueConfiguration(queue).setAddress(address).setDurable(false));

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);

      AddressSettings addressSettings = new AddressSettings().setMaxExpiryDelay((long) MAX_EXPIRATION);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      message.setExpiration(System.currentTimeMillis() + (3600 * 1000)); // The long expiration would be one hour from now
      producer.send(message);

      long start = System.currentTimeMillis();
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> server.locateQueue(queue).getMessagesExpired() == 1, 30_000, 50);

      session.deleteQueue(queue);
   }

   @Test
   public void testMessageWithAcceptableExpirationMinMaxExpiryDelayOverride() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.close();

      session = addClientSession(sf.createSession(false, true, false));
      session.createQueue(new QueueConfiguration(queue).setAddress(address).setDurable(false));

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);

      AddressSettings addressSettings = new AddressSettings().setMinExpiryDelay((long) MIN_EXPIRATION).setMaxExpiryDelay((long) MAX_EXPIRATION);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      message.setExpiration(System.currentTimeMillis() + EXPIRATION);
      producer.send(message);

      long start = System.currentTimeMillis();
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> server.locateQueue(queue).getMessagesExpired() == 1, EXPIRATION + 100, 50);
      assertTrue(System.currentTimeMillis() - start > MIN_EXPIRATION);
      assertTrue(System.currentTimeMillis() - start < MAX_EXPIRATION);

      session.deleteQueue(queue);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false);

      server.getConfiguration().setMessageExpiryScanPeriod(100);

      server.start();
      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));
   }
}

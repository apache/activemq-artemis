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

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExpiryAddressTest extends ActiveMQTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private ActiveMQServer server;

   private ClientSession clientSession;
   private ServerLocator locator;

   @Test
   public void testBasicSend() throws Exception {
      SimpleString ea = new SimpleString("EA");
      SimpleString adSend = new SimpleString("a1");
      SimpleString qName = new SimpleString("q1");
      SimpleString eq = new SimpleString("EA1");
      AddressSettings addressSettings = new AddressSettings().setExpiryAddress(ea);
      server.getAddressSettingsRepository().addMatch("#", addressSettings);
      clientSession.createQueue(ea, eq, null, false);
      clientSession.createQueue(adSend, qName, null, false);

      ClientProducer producer = clientSession.createProducer(adSend);
      ClientMessage clientMessage = createTextMessage(clientSession, "heyho!");
      clientMessage.setExpiration(System.currentTimeMillis());
      producer.send(clientMessage);

      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();
      clientConsumer = clientSession.createConsumer(eq);
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(qName.toString(), m.getStringProperty(Message.HDR_ORIGINAL_QUEUE));
      Assert.assertEquals(adSend.toString(), m.getStringProperty(Message.HDR_ORIGINAL_ADDRESS));
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      m.acknowledge();
   }

   @Test
   public void testBasicSendWithRetroActiveAddressSettings() throws Exception {
      // apply "original" address settings
      SimpleString expiryAddress1 = new SimpleString("expiryAddress1");
      SimpleString qName = new SimpleString("q1");
      SimpleString expiryQueue1 = new SimpleString("expiryQueue1");
      AddressSettings addressSettings = new AddressSettings().setExpiryAddress(expiryAddress1);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      clientSession.createQueue(expiryAddress1, expiryQueue1, null, false);
      clientSession.createQueue(qName, qName, null, false);

      // override "original" address settings
      SimpleString expiryAddress2 = new SimpleString("expiryAddress2");
      SimpleString expiryQueue2 = new SimpleString("expiryQueue2");
      addressSettings = new AddressSettings().setExpiryAddress(expiryAddress2);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      clientSession.createQueue(expiryAddress2, expiryQueue2, null, false);

      // send message that will expire ASAP
      ClientProducer producer = clientSession.createProducer(qName);
      ClientMessage clientMessage = createTextMessage(clientSession, "heyho!");
      clientMessage.setExpiration(System.currentTimeMillis());
      producer.send(clientMessage);

      clientSession.start();

      // make sure the message has expired from the original queue
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();

      // make sure the message wasn't sent to the original expiry address
      clientConsumer = clientSession.createConsumer(expiryQueue1);
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();

      // make sure the message was sent to the expected expected expiry address
      clientConsumer = clientSession.createConsumer(expiryQueue2);
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      m.acknowledge();
   }

   @Test
   public void testBasicSendToMultipleQueues() throws Exception {
      SimpleString ea = new SimpleString("EA");
      SimpleString qName = new SimpleString("q1");
      SimpleString eq = new SimpleString("EQ1");
      SimpleString eq2 = new SimpleString("EQ2");
      AddressSettings addressSettings = new AddressSettings().setExpiryAddress(ea);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      clientSession.createQueue(ea, eq, null, false);
      clientSession.createQueue(ea, eq2, null, false);
      clientSession.createQueue(qName, qName, null, false);
      ClientProducer producer = clientSession.createProducer(qName);
      ClientMessage clientMessage = createTextMessage(clientSession, "heyho!");
      clientMessage.setExpiration(System.currentTimeMillis());

      producer.send(clientMessage);

      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receiveImmediate();

      Assert.assertNull(m);

      clientConsumer.close();

      clientConsumer = clientSession.createConsumer(eq);

      m = clientConsumer.receive(500);

      Assert.assertNotNull(m);

      assertNotNull(m.getStringProperty(Message.HDR_ORIGINAL_ADDRESS));
      assertNotNull(m.getStringProperty(Message.HDR_ORIGINAL_QUEUE));

      ExpiryAddressTest.log.info("acking");
      m.acknowledge();

      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");

      clientConsumer.close();

      clientConsumer = clientSession.createConsumer(eq2);

      m = clientConsumer.receive(500);

      Assert.assertNotNull(m);

      assertNotNull(m.getStringProperty(Message.HDR_ORIGINAL_ADDRESS));
      assertNotNull(m.getStringProperty(Message.HDR_ORIGINAL_QUEUE));

      ExpiryAddressTest.log.info("acking");
      m.acknowledge();

      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");

      clientConsumer.close();

      clientSession.commit();
   }

   @Test
   public void testBasicSendToNoQueue() throws Exception {
      SimpleString ea = new SimpleString("EA");
      SimpleString qName = new SimpleString("q1");
      SimpleString eq = new SimpleString("EQ1");
      SimpleString eq2 = new SimpleString("EQ2");
      clientSession.createQueue(ea, eq, null, false);
      clientSession.createQueue(ea, eq2, null, false);
      clientSession.createQueue(qName, qName, null, false);
      ClientProducer producer = clientSession.createProducer(qName);
      ClientMessage clientMessage = createTextMessage(clientSession, "heyho!");
      clientMessage.setExpiration(System.currentTimeMillis());
      producer.send(clientMessage);
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);

      clientConsumer.close();
   }

   @Test
   public void testHeadersSet() throws Exception {
      final int NUM_MESSAGES = 5;
      SimpleString ea = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings().setExpiryAddress(ea);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      SimpleString eq = new SimpleString("EA1");
      clientSession.createQueue(ea, eq, null, false);
      clientSession.createQueue(qName, qName, null, false);
      ServerLocator locator1 = createInVMNonHALocator();

      ClientSessionFactory sessionFactory = createSessionFactory(locator1);

      ClientSession sendSession = sessionFactory.createSession(false, true, true);
      ClientProducer producer = sendSession.createProducer(qName);

      long expiration = System.currentTimeMillis();
      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage tm = createTextMessage(clientSession, "Message:" + i);
         tm.setExpiration(expiration);
         producer.send(tm);
      }

      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      clientSession.start();
      ClientMessage m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      // All the messages should now be in the EQ

      ClientConsumer cc3 = clientSession.createConsumer(eq);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage tm = cc3.receive(1000);

         Assert.assertNotNull(tm);

         String text = tm.getBodyBuffer().readString();
         Assert.assertEquals("Message:" + i, text);

         // Check the headers
         Long actualExpiryTime = (Long) tm.getObjectProperty(Message.HDR_ACTUAL_EXPIRY_TIME);
         Assert.assertTrue(actualExpiryTime >= expiration);
      }

      sendSession.close();

      locator1.close();

   }

   @Test
   public void testExpireWithDefaultAddressSettings() throws Exception {
      SimpleString ea = new SimpleString("EA");
      SimpleString qName = new SimpleString("q1");
      SimpleString eq = new SimpleString("EA1");
      AddressSettings addressSettings = new AddressSettings().setExpiryAddress(ea);
      server.getAddressSettingsRepository().setDefault(addressSettings);
      clientSession.createQueue(ea, eq, null, false);
      clientSession.createQueue(qName, qName, null, false);

      ClientProducer producer = clientSession.createProducer(qName);
      ClientMessage clientMessage = createTextMessage(clientSession, "heyho!");
      clientMessage.setExpiration(System.currentTimeMillis());
      producer.send(clientMessage);

      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();

      clientConsumer = clientSession.createConsumer(eq);
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      m.acknowledge();
   }

   @Test
   public void testExpireWithWildcardAddressSettings() throws Exception {
      SimpleString ea = new SimpleString("EA");
      SimpleString qName = new SimpleString("q1");
      SimpleString eq = new SimpleString("EA1");
      AddressSettings addressSettings = new AddressSettings().setExpiryAddress(ea);
      server.getAddressSettingsRepository().addMatch("*", addressSettings);
      clientSession.createQueue(ea, eq, null, false);
      clientSession.createQueue(qName, qName, null, false);

      ClientProducer producer = clientSession.createProducer(qName);
      ClientMessage clientMessage = createTextMessage(clientSession, "heyho!");
      clientMessage.setExpiration(System.currentTimeMillis());
      producer.send(clientMessage);

      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();

      clientConsumer = clientSession.createConsumer(eq);
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      m.acknowledge();
   }

   @Test
   public void testExpireWithOverridenSublevelAddressSettings() throws Exception {
      SimpleString address = new SimpleString("prefix.address");
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString defaultExpiryAddress = RandomUtil.randomSimpleString();
      SimpleString defaultExpiryQueue = RandomUtil.randomSimpleString();
      SimpleString specificExpiryAddress = RandomUtil.randomSimpleString();
      SimpleString specificExpiryQueue = RandomUtil.randomSimpleString();

      AddressSettings defaultAddressSettings = new AddressSettings().setExpiryAddress(defaultExpiryAddress);
      server.getAddressSettingsRepository().addMatch("prefix.*", defaultAddressSettings);
      AddressSettings specificAddressSettings = new AddressSettings().setExpiryAddress(specificExpiryAddress);
      server.getAddressSettingsRepository().addMatch("prefix.address", specificAddressSettings);

      clientSession.createQueue(address, queue, false);
      clientSession.createQueue(defaultExpiryAddress, defaultExpiryQueue, false);
      clientSession.createQueue(specificExpiryAddress, specificExpiryQueue, false);

      ClientProducer producer = clientSession.createProducer(address);
      ClientMessage clientMessage = createTextMessage(clientSession, "heyho!");
      clientMessage.setExpiration(System.currentTimeMillis());
      producer.send(clientMessage);

      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(queue);
      ClientMessage m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();

      clientConsumer = clientSession.createConsumer(defaultExpiryQueue);
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();

      clientConsumer = clientSession.createConsumer(specificExpiryQueue);
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      m.acknowledge();
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig(), false));
      server.start();
      // then we create a client as normal
      locator = createInVMNonHALocator().setBlockOnAcknowledge(true);
      ClientSessionFactory sessionFactory = createSessionFactory(locator);
      // There are assertions over sizes that needs to be done after the ACK
      // was received on server
      clientSession = addClientSession(sessionFactory.createSession(null, null, false, true, true, false, 0));
   }
}

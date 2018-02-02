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
package org.apache.activemq.artemis.tests.integration.divert;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.impl.DivertBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.Divert;
import org.apache.activemq.artemis.api.core.RoutingType;

import org.apache.activemq.artemis.core.server.transformer.Transformer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ServiceRegistryImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;

public class DivertTest extends ActiveMQTestBase {

   private static final int TIMEOUT = 3000;

   @Test
   public void testSingleNonExclusiveDivert() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress = "forwardAddress";

      DivertConfiguration divertConf = new DivertConfiguration().setName("divert1").setRoutingName("divert1").setAddress(testAddress).setForwardingAddress(forwardAddress);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      final SimpleString queueName1 = new SimpleString("queue1");

      final SimpleString queueName2 = new SimpleString("queue2");

      session.createQueue(new SimpleString(forwardAddress), RoutingType.MULTICAST, queueName1, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName2, null, false);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      final int numMessages = 1;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         Assert.assertEquals("forwardAddress", message.getAddress());

         Assert.assertEquals("testAddress", message.getStringProperty(Message.HDR_ORIGINAL_ADDRESS));

         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer2.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         Assert.assertEquals("testAddress", message.getAddress());

         message.acknowledge();
      }

      Assert.assertNull(consumer2.receiveImmediate());
   }

   @Test
   public void testSingleNonExclusiveDivertWithRoutingType() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress = "forwardAddress";

      DivertConfiguration divertConf = new DivertConfiguration().setName("divert1").setRoutingName("divert1").setAddress(testAddress).setForwardingAddress(forwardAddress);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      final SimpleString queueName1 = new SimpleString("queue1");

      final SimpleString queueName2 = new SimpleString("queue2");

      session.createQueue(new SimpleString(forwardAddress), RoutingType.ANYCAST, queueName1, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName2, null, false);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      final int numMessages = 1;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.setRoutingType(RoutingType.MULTICAST);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer2.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer2.receiveImmediate());
   }

   @Test
   public void testSingleExclusiveDivertWithRoutingType() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress = "forwardAddress";

      DivertConfiguration divertConf = new DivertConfiguration().setName("divert1").setRoutingName("divert1").setAddress(testAddress).setForwardingAddress(forwardAddress).setExclusive(true);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      final SimpleString queueName1 = new SimpleString("queue1");

      final SimpleString queueName2 = new SimpleString("queue2");

      session.createQueue(new SimpleString(forwardAddress), RoutingType.ANYCAST, queueName1, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName2, null, false);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      final int numMessages = 1;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.setRoutingType(RoutingType.MULTICAST);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());
   }

   @Test
   public void testSingleDivertWithExpiry() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress = "forwardAddress";

      final String expiryAddress = "expiryAddress";

      AddressSettings expirySettings = new AddressSettings().setExpiryAddress(new SimpleString(expiryAddress));

      DivertConfiguration divertConf = new DivertConfiguration().setName("divert1").setRoutingName("divert1").setAddress(testAddress).setForwardingAddress(forwardAddress);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf).clearAddressesSettings().addAddressesSetting("#", expirySettings);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, true));

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      final SimpleString queueName1 = new SimpleString("queue1");

      final SimpleString queueName2 = new SimpleString("queue2");

      session.createQueue(new SimpleString(forwardAddress), RoutingType.MULTICAST, queueName1, null, true);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName2, null, true);

      session.createQueue(new SimpleString(expiryAddress), RoutingType.MULTICAST, new SimpleString(expiryAddress), null, true);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      final int numMessages = 1;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(true);

         message.putIntProperty(propKey, i);

         message.setExpiration(System.currentTimeMillis() + 1000);

         producer.send(message);
      }
      session.commit();

      // this context is validating if these messages are routed correctly
      {
         int count1 = 0;
         ClientMessage message = null;
         while ((message = consumer1.receiveImmediate()) != null) {
            message.acknowledge();
            count1++;
         }

         int count2 = 0;
         while ((message = consumer2.receiveImmediate()) != null) {
            message.acknowledge();
            count2++;
         }

         assertEquals(1, count1);
         assertEquals(1, count2);
         session.rollback();
      }
      Thread.sleep(2000);

      // it must been expired by now
      assertNull(consumer1.receiveImmediate());
      // it must been expired by now
      assertNull(consumer2.receiveImmediate());

      int countOriginal1 = 0;
      int countOriginal2 = 0;
      ClientConsumer consumerExpiry = session.createConsumer(expiryAddress);

      for (int i = 0; i < numMessages * 2; i++) {
         ClientMessage message = consumerExpiry.receive(5000);
         System.out.println("Received message " + message);
         assertNotNull(message);

         if (message.getStringProperty(Message.HDR_ORIGINAL_QUEUE).equals("divert1")) {
            countOriginal1++;
         } else if (message.getStringProperty(Message.HDR_ORIGINAL_QUEUE).equals("queue2")) {
            countOriginal2++;
         } else {
            System.out.println("message not part of any expired queue" + message);
         }
      }

      assertEquals(numMessages, countOriginal1);
      assertEquals(numMessages, countOriginal2);
   }

   @Test
   public void testSingleNonExclusiveDivert2() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress = "forwardAddress";

      DivertConfiguration divertConf = new DivertConfiguration().setName("divert1").setRoutingName("divert1").setAddress(testAddress).setForwardingAddress(forwardAddress);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      final SimpleString queueName1 = new SimpleString("queue1");

      final SimpleString queueName2 = new SimpleString("queue2");

      final SimpleString queueName3 = new SimpleString("queue3");

      final SimpleString queueName4 = new SimpleString("queue4");

      session.createQueue(new SimpleString(forwardAddress), RoutingType.MULTICAST, queueName1, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName2, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName3, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName4, null, false);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      ClientConsumer consumer3 = session.createConsumer(queueName3);

      ClientConsumer consumer4 = session.createConsumer(queueName4);

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer2.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer2.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer3.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer3.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer4.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer4.receiveImmediate());
   }

   @Test
   public void testSingleNonExclusiveDivert3() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress = "forwardAddress";

      DivertConfiguration divertConf = new DivertConfiguration().setName("divert1").setRoutingName("divert1").setAddress(testAddress).setForwardingAddress(forwardAddress);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      final SimpleString queueName1 = new SimpleString("queue1");

      session.createQueue(new SimpleString(forwardAddress), RoutingType.MULTICAST, queueName1, null, false);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());
   }

   @Test
   public void testSingleExclusiveDivert() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress = "forwardAddress";

      DivertConfiguration divertConf = new DivertConfiguration().setName("divert1").setRoutingName("divert1").setAddress(testAddress).setForwardingAddress(forwardAddress).setExclusive(true);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      final SimpleString queueName1 = new SimpleString("queue1");

      final SimpleString queueName2 = new SimpleString("queue2");

      final SimpleString queueName3 = new SimpleString("queue3");

      final SimpleString queueName4 = new SimpleString("queue4");

      session.createQueue(new SimpleString(forwardAddress), RoutingType.MULTICAST, queueName1, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName2, null, false);
      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName3, null, false);
      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName4, null, false);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      ClientConsumer consumer3 = session.createConsumer(queueName3);

      ClientConsumer consumer4 = session.createConsumer(queueName4);

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         Assert.assertEquals("forwardAddress", message.getAddress());

         Assert.assertEquals("testAddress", message.getStringProperty(Message.HDR_ORIGINAL_ADDRESS));

         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());

      Assert.assertNull(consumer2.receiveImmediate());

      Assert.assertNull(consumer3.receiveImmediate());

      Assert.assertNull(consumer4.receiveImmediate());
   }

   @Test
   public void testMultipleNonExclusiveDivert() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress1 = "forwardAddress1";
      final String forwardAddress2 = "forwardAddress2";
      final String forwardAddress3 = "forwardAddress3";

      DivertConfiguration divertConf1 = new DivertConfiguration().setName("divert1").setRoutingName("divert1").setAddress(testAddress).setForwardingAddress(forwardAddress1);

      DivertConfiguration divertConf2 = new DivertConfiguration().setName("divert2").setRoutingName("divert2").setAddress(testAddress).setForwardingAddress(forwardAddress2);

      DivertConfiguration divertConf3 = new DivertConfiguration().setName("divert3").setRoutingName("divert3").setAddress(testAddress).setForwardingAddress(forwardAddress3);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf1).addDivertConfiguration(divertConf2).addDivertConfiguration(divertConf3);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      final SimpleString queueName1 = new SimpleString("queue1");

      final SimpleString queueName2 = new SimpleString("queue2");

      final SimpleString queueName3 = new SimpleString("queue3");

      final SimpleString queueName4 = new SimpleString("queue4");

      session.createQueue(new SimpleString(forwardAddress1), RoutingType.MULTICAST, queueName1, null, false);

      session.createQueue(new SimpleString(forwardAddress2), RoutingType.MULTICAST, queueName2, null, false);

      session.createQueue(new SimpleString(forwardAddress3), RoutingType.MULTICAST, queueName3, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName4, null, false);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      ClientConsumer consumer3 = session.createConsumer(queueName3);

      ClientConsumer consumer4 = session.createConsumer(queueName4);

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(propKey).intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer2.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(propKey).intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer2.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer3.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(propKey).intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer3.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer4.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(propKey).intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer4.receiveImmediate());
   }

   @Test
   public void testMultipleExclusiveDivert() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress1 = "forwardAddress1";
      final String forwardAddress2 = "forwardAddress2";
      final String forwardAddress3 = "forwardAddress3";

      DivertConfiguration divertConf1 = new DivertConfiguration().setName("divert1").setRoutingName("divert1").setAddress(testAddress).setForwardingAddress(forwardAddress1).setExclusive(true);

      DivertConfiguration divertConf2 = new DivertConfiguration().setName("divert2").setRoutingName("divert2").setAddress(testAddress).setForwardingAddress(forwardAddress2).setExclusive(true);

      DivertConfiguration divertConf3 = new DivertConfiguration().setName("divert3").setRoutingName("divert3").setAddress(testAddress).setForwardingAddress(forwardAddress3).setExclusive(true);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf1).addDivertConfiguration(divertConf2).addDivertConfiguration(divertConf3);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      final SimpleString queueName1 = new SimpleString("queue1");

      final SimpleString queueName2 = new SimpleString("queue2");

      final SimpleString queueName3 = new SimpleString("queue3");

      final SimpleString queueName4 = new SimpleString("queue4");

      session.createQueue(new SimpleString(forwardAddress1), RoutingType.MULTICAST, queueName1, null, false);

      session.createQueue(new SimpleString(forwardAddress2), RoutingType.MULTICAST, queueName2, null, false);

      session.createQueue(new SimpleString(forwardAddress3), RoutingType.MULTICAST, queueName3, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName4, null, false);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      ClientConsumer consumer3 = session.createConsumer(queueName3);

      ClientConsumer consumer4 = session.createConsumer(queueName4);

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(propKey).intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer2.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(propKey).intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer2.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer3.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(propKey).intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer3.receiveImmediate());

      Assert.assertNull(consumer4.receiveImmediate());
   }

   @Test
   public void testMixExclusiveAndNonExclusiveDiverts() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress1 = "forwardAddress1";
      final String forwardAddress2 = "forwardAddress2";
      final String forwardAddress3 = "forwardAddress3";

      DivertConfiguration divertConf1 = new DivertConfiguration().setName("divert1").setRoutingName("divert1").setAddress(testAddress).setForwardingAddress(forwardAddress1).setExclusive(true);

      DivertConfiguration divertConf2 = new DivertConfiguration().setName("divert2").setRoutingName("divert2").setAddress(testAddress).setForwardingAddress(forwardAddress2).setExclusive(true);

      DivertConfiguration divertConf3 = new DivertConfiguration().setName("divert3").setRoutingName("divert3").setAddress(testAddress).setForwardingAddress(forwardAddress3);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf1).addDivertConfiguration(divertConf2).addDivertConfiguration(divertConf3);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      final SimpleString queueName1 = new SimpleString("queue1");

      final SimpleString queueName2 = new SimpleString("queue2");

      final SimpleString queueName3 = new SimpleString("queue3");

      final SimpleString queueName4 = new SimpleString("queue4");

      session.createQueue(new SimpleString(forwardAddress1), RoutingType.MULTICAST, queueName1, null, false);

      session.createQueue(new SimpleString(forwardAddress2), RoutingType.MULTICAST, queueName2, null, false);

      session.createQueue(new SimpleString(forwardAddress3), RoutingType.MULTICAST, queueName3, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName4, null, false);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      ClientConsumer consumer3 = session.createConsumer(queueName3);

      ClientConsumer consumer4 = session.createConsumer(queueName4);

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(propKey).intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer2.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(propKey).intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer2.receiveImmediate());

      Assert.assertNull(consumer3.receiveImmediate());

      Assert.assertNull(consumer4.receiveImmediate());
   }

   // If no exclusive diverts match then non exclusive ones should be called
   @Test
   public void testSingleExclusiveNonMatchingAndNonExclusiveDiverts() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress1 = "forwardAddress1";
      final String forwardAddress2 = "forwardAddress2";
      final String forwardAddress3 = "forwardAddress3";

      final String filter = "animal='antelope'";

      DivertConfiguration divertConf1 = new DivertConfiguration().setName("divert1").setRoutingName("divert1").setAddress(testAddress).setForwardingAddress(forwardAddress1).setExclusive(true).setFilterString(filter);

      DivertConfiguration divertConf2 = new DivertConfiguration().setName("divert2").setRoutingName("divert2").setAddress(testAddress).setForwardingAddress(forwardAddress2);

      DivertConfiguration divertConf3 = new DivertConfiguration().setName("divert3").setRoutingName("divert3").setAddress(testAddress).setForwardingAddress(forwardAddress3);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf1).addDivertConfiguration(divertConf2).addDivertConfiguration(divertConf3);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      final SimpleString queueName1 = new SimpleString("queue1");

      final SimpleString queueName2 = new SimpleString("queue2");

      final SimpleString queueName3 = new SimpleString("queue3");

      final SimpleString queueName4 = new SimpleString("queue4");

      session.createQueue(new SimpleString(forwardAddress1), RoutingType.MULTICAST, queueName1, null, false);

      session.createQueue(new SimpleString(forwardAddress2), RoutingType.MULTICAST, queueName2, null, false);

      session.createQueue(new SimpleString(forwardAddress3), RoutingType.MULTICAST, queueName3, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName4, null, false);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      ClientConsumer consumer3 = session.createConsumer(queueName3);

      ClientConsumer consumer4 = session.createConsumer(queueName4);

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putStringProperty(new SimpleString("animal"), new SimpleString("giraffe"));

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      // for (int i = 0; i < numMessages; i++)
      // {
      // ClientMessage message = consumer1.receive(200);
      //
      // assertNotNull(message);
      //
      // assertEquals((Integer)i, (Integer)message.getProperty(propKey));
      //
      // message.acknowledge();
      // }

      Assert.assertNull(consumer1.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer2.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer2.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer3.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer3.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer4.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer4.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putStringProperty(new SimpleString("animal"), new SimpleString("antelope"));

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());

      Assert.assertNull(consumer2.receiveImmediate());

      Assert.assertNull(consumer3.receiveImmediate());

      Assert.assertNull(consumer4.receiveImmediate());
   }

   @Test
   public void testRoundRobinDiverts() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress1 = "forwardAddress1";
      final String forwardAddress2 = "forwardAddress2";
      final String forwardAddress3 = "forwardAddress3";

      DivertConfiguration divertConf1 = new DivertConfiguration().setName("divert1").setRoutingName("thename").setAddress(testAddress).setForwardingAddress(forwardAddress1);

      DivertConfiguration divertConf2 = new DivertConfiguration().setName("divert2").setRoutingName("thename").setAddress(testAddress).setForwardingAddress(forwardAddress2);

      DivertConfiguration divertConf3 = new DivertConfiguration().setName("divert3").setRoutingName("thename").setAddress(testAddress).setForwardingAddress(forwardAddress3);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf1).addDivertConfiguration(divertConf2).addDivertConfiguration(divertConf3);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      final SimpleString queueName1 = new SimpleString("queue1");

      final SimpleString queueName2 = new SimpleString("queue2");

      final SimpleString queueName3 = new SimpleString("queue3");

      final SimpleString queueName4 = new SimpleString("queue4");

      session.createQueue(new SimpleString(forwardAddress1), RoutingType.MULTICAST, queueName1, null, false);

      session.createQueue(new SimpleString(forwardAddress2), RoutingType.MULTICAST, queueName2, null, false);

      session.createQueue(new SimpleString(forwardAddress3), RoutingType.MULTICAST, queueName3, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName4, null, false);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      ClientConsumer consumer3 = session.createConsumer(queueName3);

      ClientConsumer consumer4 = session.createConsumer(queueName4);

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; ) {
         ClientMessage message = consumer1.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();

         i++;

         if (i == numMessages) {
            break;
         }

         message = consumer2.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();

         i++;

         if (i == numMessages) {
            break;
         }

         message = consumer3.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();

         i++;
      }

      Assert.assertNull(consumer1.receiveImmediate());
      Assert.assertNull(consumer2.receiveImmediate());
      Assert.assertNull(consumer3.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer4.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getObjectProperty(propKey));

         message.acknowledge();
      }

      Assert.assertNull(consumer4.receiveImmediate());
   }

   @Test
   public void testDeployDivertsSameUniqueName() throws Exception {
      final String testAddress = "testAddress";

      final String forwardAddress1 = "forwardAddress1";
      final String forwardAddress2 = "forwardAddress2";
      final String forwardAddress3 = "forwardAddress3";

      DivertConfiguration divertConf1 = new DivertConfiguration().setName("divert1").setRoutingName("thename1").setAddress(testAddress).setForwardingAddress(forwardAddress1);

      DivertConfiguration divertConf2 = new DivertConfiguration().setName("divert1").setRoutingName("thename2").setAddress(testAddress).setForwardingAddress(forwardAddress2);

      DivertConfiguration divertConf3 = new DivertConfiguration().setName("divert2").setRoutingName("thename3").setAddress(testAddress).setForwardingAddress(forwardAddress3);

      Configuration config = createDefaultInVMConfig().addDivertConfiguration(divertConf1).addDivertConfiguration(divertConf2).addDivertConfiguration(divertConf3);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, false));

      server.start();

      // Only the first and third should be deployed

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      final SimpleString queueName1 = new SimpleString("queue1");

      final SimpleString queueName2 = new SimpleString("queue2");

      final SimpleString queueName3 = new SimpleString("queue3");

      final SimpleString queueName4 = new SimpleString("queue4");

      session.createQueue(new SimpleString(forwardAddress1), RoutingType.MULTICAST, queueName1, null, false);

      session.createQueue(new SimpleString(forwardAddress2), RoutingType.MULTICAST, queueName2, null, false);

      session.createQueue(new SimpleString(forwardAddress3), RoutingType.MULTICAST, queueName3, null, false);

      session.createQueue(new SimpleString(testAddress), RoutingType.MULTICAST, queueName4, null, false);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      ClientConsumer consumer3 = session.createConsumer(queueName3);

      ClientConsumer consumer4 = session.createConsumer(queueName4);

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(propKey).intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());

      Assert.assertNull(consumer2.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer3.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(propKey).intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer3.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer4.receive(DivertTest.TIMEOUT);

         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(propKey).intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer4.receiveImmediate());
   }

   @Test
   public void testInjectedTransformer() throws Exception {
      final SimpleString ADDRESS = new SimpleString("myAddress");
      final String DIVERT = "myDivert";

      ServiceRegistryImpl serviceRegistry = new ServiceRegistryImpl();
      Transformer transformer = new Transformer() {
         @Override
         public Message transform(Message message) {
            return null;
         }
      };
      serviceRegistry.addDivertTransformer(DIVERT, transformer);

      ActiveMQServer server = addServer(new ActiveMQServerImpl(null, null, null, null, serviceRegistry));
      server.start();
      server.waitForActivation(100, TimeUnit.MILLISECONDS);
      server.createQueue(ADDRESS, RoutingType.MULTICAST, SimpleString.toSimpleString("myQueue"), null, false, false);
      server.deployDivert(new DivertConfiguration().setName(DIVERT).setAddress(ADDRESS.toString()).setForwardingAddress(ADDRESS.toString()));
      Collection<Binding> bindings = server.getPostOffice().getBindingsForAddress(ADDRESS).getBindings();
      Divert divert = null;
      for (Binding binding : bindings) {
         if (binding instanceof DivertBinding) {
            divert = ((DivertBinding) binding).getDivert();
         }
      }
      assertNotNull(divert);
      assertEquals(transformer, divert.getTransformer());
   }
}

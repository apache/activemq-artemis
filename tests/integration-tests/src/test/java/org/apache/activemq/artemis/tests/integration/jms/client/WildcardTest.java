/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.jms.client;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;
import javax.management.ObjectName;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class WildcardTest extends JMSTestBase {

   @Parameters(name = "a={0},b={1},c={2}")
   public static Iterable<Object[]> data() {
      return Arrays.asList(new Object[][] {{"test.topic.A", "test.topic.B", "test.topic.#"},
         {"test.topic.A", "test.topic.B", "test.#"}});
   }

   private String topicA;
   private String topicB;
   private String topicWildcard;

   @Override
   protected Configuration createDefaultConfig(boolean netty) throws Exception {
      Configuration configuration =  super.createDefaultConfig(netty).setJMXManagementEnabled(true);
      return configuration;
   }

   public WildcardTest(String topicA, String topicB, String topicWildcard) {
      super();

      this.topicA = topicA;
      this.topicB = topicB;
      this.topicWildcard = topicWildcard;
   }

   @Test
   public void testWildcard1Topic() throws Exception {
      Session         sessionA   = createSession();
      MessageProducer producerA  = createProducer(sessionA, topicA);

      MessageConsumer consumerA  = createConsumer(topicA);
      MessageConsumer consumerWC = createConsumer(topicWildcard);

      Message message = sessionA.createObjectMessage(1);
      producerA.send(message);

      ObjectMessage received1 = (ObjectMessage)consumerA.receive(500);
      Assert.assertNotNull(received1);
      Assert.assertNotNull(received1.getObject());

      ObjectMessage received2 = (ObjectMessage)consumerWC.receive(500);
      Assert.assertNotNull(received2);
      Assert.assertNotNull(received2.getObject());

      Assert.assertEquals(received1.getJMSMessageID(), received2.getJMSMessageID());
      Assert.assertEquals(received1.getObject(), received2.getObject());
   }

   @Test
   public void testWildcard2Topics() throws Exception {
      Session         sessionA   = createSession();
      MessageProducer producerA  = createProducer(sessionA, topicA);

      Session         sessionB   = createSession();
      MessageProducer producerB  = createProducer(sessionA, topicB);

      MessageConsumer consumerA  = createConsumer(topicA);
      MessageConsumer consumerB  = createConsumer(topicB);
      MessageConsumer consumerWC = createConsumer(topicWildcard);

      Message message1 = sessionA.createObjectMessage(1);
      producerA.send(message1);

      Message message2 = sessionB.createObjectMessage(2);
      producerB.send(message2);

      ObjectMessage received1 = (ObjectMessage)consumerA.receive(500);
      Assert.assertNotNull(received1);
      Assert.assertNotNull(received1.getObject());

      ObjectMessage received2 = (ObjectMessage)consumerB.receive(500);
      Assert.assertNotNull(received2);
      Assert.assertNotNull(received2.getObject());

      ObjectMessage received3 = (ObjectMessage)consumerWC.receive(500);
      Assert.assertNotNull(received3);
      Assert.assertNotNull(received3.getObject());

      ObjectMessage received4 = (ObjectMessage)consumerWC.receive(500);
      Assert.assertNotNull(received4);
      Assert.assertNotNull(received4.getObject());

      Assert.assertEquals(received1.getJMSMessageID(), received3.getJMSMessageID());
      Assert.assertEquals(received1.getObject(), received3.getObject());

      Assert.assertEquals(received2.getJMSMessageID(), received4.getJMSMessageID());
      Assert.assertEquals(received2.getObject(), received4.getObject());
   }

   @Test
   public void testNegativeAddressSizeOnWildcard1() throws Exception {
      testNegativeAddressSizeOnWildcard(1);
   }

   @Test
   public void testNegativeAddressSizeOnWildcard2() throws Exception {
      testNegativeAddressSizeOnWildcard(2);
   }

   @Test
   public void testNegativeAddressSizeOnWildcard10() throws Exception {
      testNegativeAddressSizeOnWildcard(10);
   }

   @Test
   public void testNegativeAddressSizeOnWildcard100() throws Exception {
      testNegativeAddressSizeOnWildcard(100);
   }

   @Test
   public void testNegativeAddressSizeOnWildcardAsync1() throws Exception {
      testNegativeAddressSizeOnWildcardAsync(1);
   }

   @Test
   public void testNegativeAddressSizeOnWildcardAsync2() throws Exception {
      testNegativeAddressSizeOnWildcardAsync(2);
   }

   @Test
   public void testNegativeAddressSizeOnWildcardAsync10() throws Exception {
      testNegativeAddressSizeOnWildcardAsync(10);
   }

   @Test
   public void testNegativeAddressSizeOnWildcardAsync100() throws Exception {
      testNegativeAddressSizeOnWildcardAsync(100);
   }

   private void testNegativeAddressSizeOnWildcard(int numMessages) throws Exception {
      Session         sessionA   = createSession();
      MessageProducer producerA  = createProducer(sessionA, topicA);

      MessageConsumer consumerA  = createConsumer(topicA);
      MessageConsumer consumerWC = createConsumer(topicWildcard);

      for (int i = 0; i < numMessages; i++) {
         Message message = sessionA.createObjectMessage(i);
         producerA.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ObjectMessage received1 = (ObjectMessage)consumerA.receive(500);
         Assert.assertNotNull("consumerA message - " + i + " is null", received1);
         Assert.assertNotNull("consumerA message - " + i + " is null", received1.getObject());

         ObjectMessage received2 = (ObjectMessage)consumerWC.receive(500);
         Assert.assertNotNull("consumerWC message - " + i + " is null", received2);
         Assert.assertNotNull("consumerWC message - " + i + " is null", received2.getObject());
      }

      long addressSizeA  = (Long)mbeanServer.getAttribute(new ObjectName("org.apache.activemq.artemis:broker=\"localhost\",component=addresses,address=\"" + topicA + "\""), "AddressSize");
      long addressSizeWC = (Long)mbeanServer.getAttribute(new ObjectName("org.apache.activemq.artemis:broker=\"localhost\",component=addresses,address=\"" + topicWildcard + "\""), "AddressSize");

      Assert.assertTrue(topicA + " AddressSize < 0", addressSizeA >= 0);
      Assert.assertTrue(topicWildcard + " AddressSize < 0", addressSizeWC >= 0);
   }

   private void testNegativeAddressSizeOnWildcardAsync(int numMessages) throws Exception {
      Session         sessionA   = createSession();
      MessageProducer producerA  = createProducer(sessionA, topicA);

      CountDownLatch  latchA    = new CountDownLatch(numMessages);
      MessageConsumer consumerA = createAsyncConsumer(topicA, latchA);

      CountDownLatch  latchWC    = new CountDownLatch(numMessages);
      MessageConsumer consumerWC = createAsyncConsumer(topicWildcard, latchWC);

      for (int i = 0; i < numMessages; i++) {
         Message message = sessionA.createObjectMessage(i);

         producerA.send(message);
      }

      if (!latchA.await(5, TimeUnit.SECONDS)) {
         Assert.fail("Waiting to receive " + latchA.getCount() + " messages on " + topicA);
      }

      if (!latchWC.await(5, TimeUnit.SECONDS)) {
         Assert.fail("Waiting to receive " + latchWC.getCount() + " messages on " + topicWildcard);
      }

      long addressSizeA  = (Long)mbeanServer.getAttribute(new ObjectName("org.apache.activemq.artemis:broker=\"localhost\",component=addresses,address=\"" + topicA + "\""), "AddressSize");
      long addressSizeWC = (Long)mbeanServer.getAttribute(new ObjectName("org.apache.activemq.artemis:broker=\"localhost\",component=addresses,address=\"" + topicWildcard + "\""), "AddressSize");

      Assert.assertTrue(topicA + " AddressSize < 0", addressSizeA >= 0);
      Assert.assertTrue(topicWildcard + " AddressSize < 0", addressSizeWC >= 0);
   }

   private Session createSession() throws Exception {
      Connection connection = createConnection();
      Session    session    = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      return session;
   }

   private MessageProducer createProducer(Session session, String topicName) throws Exception {
      Topic topic = session.createTopic(topicName);

      MessageProducer producer = session.createProducer(topic);
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      return producer;
   }

   private MessageConsumer createConsumer(String topicName) throws Exception {
      Connection connection = createConnection();
      connection.start();

      Session    session    = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic      topic      = session.createTopic(topicName);

      MessageConsumer consumer = session.createConsumer(topic, null, false);

      return consumer;
   }

   private MessageConsumer createAsyncConsumer(String topicName, CountDownLatch latch) throws Exception {
      MessageConsumer consumer = createConsumer(topicName);
      consumer.setMessageListener(m -> {
         try {
            latch.countDown();
         } catch (Throwable ex) {
            ex.printStackTrace();
         }
      });

      return consumer;
   }
}
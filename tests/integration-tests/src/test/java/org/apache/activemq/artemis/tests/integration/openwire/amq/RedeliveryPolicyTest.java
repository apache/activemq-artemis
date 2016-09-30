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
package org.apache.activemq.artemis.tests.integration.openwire.amq;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.artemis.tests.integration.openwire.BasicOpenWireTest;
import org.apache.activemq.broker.region.policy.RedeliveryPolicyMap;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Test;

/**
 * adapted from: org.apache.activemq.RedeliveryPolicyTest
 */
public class RedeliveryPolicyTest extends BasicOpenWireTest {

   @Test
   public void testGetNext() throws Exception {

      RedeliveryPolicy policy = new RedeliveryPolicy();
      policy.setInitialRedeliveryDelay(0);
      policy.setRedeliveryDelay(500);
      policy.setBackOffMultiplier((short) 2);
      policy.setUseExponentialBackOff(true);

      long delay = policy.getNextRedeliveryDelay(0);
      assertEquals(500, delay);
      delay = policy.getNextRedeliveryDelay(delay);
      assertEquals(500 * 2, delay);
      delay = policy.getNextRedeliveryDelay(delay);
      assertEquals(500 * 4, delay);

      policy.setUseExponentialBackOff(false);
      delay = policy.getNextRedeliveryDelay(delay);
      assertEquals(500, delay);
   }

   /**
    * @throws Exception
    */
   @Test
   public void testExponentialRedeliveryPolicyDelaysDeliveryOnRollback() throws Exception {

      // Receive a message with the JMS API
      RedeliveryPolicy policy = connection.getRedeliveryPolicy();
      policy.setInitialRedeliveryDelay(0);
      policy.setRedeliveryDelay(500);
      policy.setBackOffMultiplier((short) 2);
      policy.setUseExponentialBackOff(true);

      connection.start();
      Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = new ActiveMQQueue("testExponentialRedeliveryPolicyDelaysDeliveryOnRollback");
      this.makeSureCoreQueueExist("testExponentialRedeliveryPolicyDelaysDeliveryOnRollback");
      MessageProducer producer = session.createProducer(destination);

      MessageConsumer consumer = session.createConsumer(destination);

      // Send the messages
      producer.send(session.createTextMessage("1st"));
      producer.send(session.createTextMessage("2nd"));
      session.commit();

      TextMessage m;
      m = (TextMessage) consumer.receive(1000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      // No delay on first rollback..
      m = (TextMessage) consumer.receive(100);
      assertNotNull(m);
      session.rollback();

      // Show subsequent re-delivery delay is incrementing.
      m = (TextMessage) consumer.receive(100);
      assertNull(m);

      m = (TextMessage) consumer.receive(700);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      // Show re-delivery delay is incrementing exponentially
      m = (TextMessage) consumer.receive(100);
      assertNull(m);
      m = (TextMessage) consumer.receive(500);
      assertNull(m);
      m = (TextMessage) consumer.receive(700);
      assertNotNull(m);
      assertEquals("1st", m.getText());

   }

   /**
    * @throws Exception
    */
   @Test
   public void testNornalRedeliveryPolicyDelaysDeliveryOnRollback() throws Exception {

      // Receive a message with the JMS API
      RedeliveryPolicy policy = connection.getRedeliveryPolicy();
      policy.setInitialRedeliveryDelay(0);
      policy.setRedeliveryDelay(500);

      connection.start();
      Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = new ActiveMQQueue("testNornalRedeliveryPolicyDelaysDeliveryOnRollback");
      this.makeSureCoreQueueExist("testNornalRedeliveryPolicyDelaysDeliveryOnRollback");
      MessageProducer producer = session.createProducer(destination);

      MessageConsumer consumer = session.createConsumer(destination);

      // Send the messages
      producer.send(session.createTextMessage("1st"));
      producer.send(session.createTextMessage("2nd"));
      session.commit();

      TextMessage m;
      m = (TextMessage) consumer.receive(1000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      // No delay on first rollback..
      m = (TextMessage) consumer.receive(100);
      assertNotNull(m);
      session.rollback();

      // Show subsequent re-delivery delay is incrementing.
      m = (TextMessage) consumer.receive(100);
      assertNull(m);
      m = (TextMessage) consumer.receive(700);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      // The message gets redelivered after 500 ms every time since
      // we are not using exponential backoff.
      m = (TextMessage) consumer.receive(100);
      assertNull(m);
      m = (TextMessage) consumer.receive(700);
      assertNotNull(m);
      assertEquals("1st", m.getText());

   }

   /**
    * @throws Exception
    */
   @Test
   public void testDLQHandling() throws Exception {
      this.makeSureCoreQueueExist("ActiveMQ.DLQ");
      // Receive a message with the JMS API
      RedeliveryPolicy policy = connection.getRedeliveryPolicy();
      policy.setInitialRedeliveryDelay(100);
      policy.setUseExponentialBackOff(false);
      policy.setMaximumRedeliveries(2);

      connection.start();
      Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
      ActiveMQQueue destination = new ActiveMQQueue("TEST");
      this.makeSureCoreQueueExist("TEST");
      MessageProducer producer = session.createProducer(destination);

      MessageConsumer consumer = session.createConsumer(destination);
      MessageConsumer dlqConsumer = session.createConsumer(new ActiveMQQueue("ActiveMQ.DLQ"));

      // Send the messages
      producer.send(session.createTextMessage("1st"));
      producer.send(session.createTextMessage("2nd"));
      session.commit();

      TextMessage m;
      m = (TextMessage) consumer.receive(1000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      m = (TextMessage) consumer.receive(1000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      m = (TextMessage) consumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      // The last rollback should cause the 1st message to get sent to the DLQ
      m = (TextMessage) consumer.receive(1000);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      session.commit();

      // We should be able to get the message off the DLQ now.
      m = (TextMessage) dlqConsumer.receive(1000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      String cause = m.getStringProperty(ActiveMQMessage.DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY);
      assertTrue("cause exception has policy ref", cause.contains("RedeliveryPolicy"));
      session.commit();
   }

   /**
    * @throws Exception
    */
   @Test
   public void testInfiniteMaximumNumberOfRedeliveries() throws Exception {

      // Receive a message with the JMS API
      RedeliveryPolicy policy = connection.getRedeliveryPolicy();
      policy.setInitialRedeliveryDelay(100);
      policy.setUseExponentialBackOff(false);
      // let's set the maximum redeliveries to no maximum (ie. infinite)
      policy.setMaximumRedeliveries(-1);

      connection.start();
      Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = new ActiveMQQueue("TEST");
      this.makeSureCoreQueueExist("TEST");
      MessageProducer producer = session.createProducer(destination);

      MessageConsumer consumer = session.createConsumer(destination);

      // Send the messages
      producer.send(session.createTextMessage("1st"));
      producer.send(session.createTextMessage("2nd"));
      session.commit();

      TextMessage m;

      m = (TextMessage) consumer.receive(1000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      // we should be able to get the 1st message redelivered until a
      // session.commit is called
      m = (TextMessage) consumer.receive(1000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      m = (TextMessage) consumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      m = (TextMessage) consumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      m = (TextMessage) consumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      m = (TextMessage) consumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.commit();

      m = (TextMessage) consumer.receive(2000);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      session.commit();
   }

   /**
    * @throws Exception
    */
   @Test
   public void testMaximumRedeliveryDelay() throws Exception {

      // Receive a message with the JMS API
      RedeliveryPolicy policy = connection.getRedeliveryPolicy();
      policy.setInitialRedeliveryDelay(10);
      policy.setUseExponentialBackOff(true);
      policy.setMaximumRedeliveries(-1);
      policy.setRedeliveryDelay(50);
      policy.setMaximumRedeliveryDelay(1000);
      policy.setBackOffMultiplier((short) 2);
      policy.setUseExponentialBackOff(true);

      connection.start();
      Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = new ActiveMQQueue("TEST");
      this.makeSureCoreQueueExist("TEST");
      MessageProducer producer = session.createProducer(destination);

      MessageConsumer consumer = session.createConsumer(destination);

      // Send the messages
      producer.send(session.createTextMessage("1st"));
      producer.send(session.createTextMessage("2nd"));
      session.commit();

      TextMessage m;

      for (int i = 0; i < 10; ++i) {
         // we should be able to get the 1st message redelivered until a
         // session.commit is called
         m = (TextMessage) consumer.receive(2000);
         assertNotNull(m);
         assertEquals("1st", m.getText());
         session.rollback();
      }

      m = (TextMessage) consumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.commit();

      m = (TextMessage) consumer.receive(2000);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      session.commit();

      assertTrue(policy.getNextRedeliveryDelay(Long.MAX_VALUE) == 1000);
   }

   /**
    * @throws Exception
    */
   @Test
   public void testZeroMaximumNumberOfRedeliveries() throws Exception {

      // Receive a message with the JMS API
      RedeliveryPolicy policy = connection.getRedeliveryPolicy();
      policy.setInitialRedeliveryDelay(100);
      policy.setUseExponentialBackOff(false);
      // let's set the maximum redeliveries to 0
      policy.setMaximumRedeliveries(0);

      connection.start();
      Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = new ActiveMQQueue("TEST");
      this.makeSureCoreQueueExist("TEST");
      MessageProducer producer = session.createProducer(destination);

      MessageConsumer consumer = session.createConsumer(destination);

      // Send the messages
      producer.send(session.createTextMessage("1st"));
      producer.send(session.createTextMessage("2nd"));
      session.commit();

      TextMessage m;
      m = (TextMessage) consumer.receive(1000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      // the 1st message should not be redelivered since maximumRedeliveries is
      // set to 0
      m = (TextMessage) consumer.receive(1000);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      session.commit();

   }

   @Test
   public void testInitialRedeliveryDelayZero() throws Exception {
      // Receive a message with the JMS API
      RedeliveryPolicy policy = connection.getRedeliveryPolicy();
      policy.setInitialRedeliveryDelay(0);
      policy.setUseExponentialBackOff(false);
      policy.setMaximumRedeliveries(1);

      connection.start();
      Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = new ActiveMQQueue("TEST");
      this.makeSureCoreQueueExist("TEST");
      MessageProducer producer = session.createProducer(destination);

      MessageConsumer consumer = session.createConsumer(destination);

      // Send the messages
      producer.send(session.createTextMessage("1st"));
      producer.send(session.createTextMessage("2nd"));
      session.commit();

      TextMessage m;
      m = (TextMessage) consumer.receive(100);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      m = (TextMessage) consumer.receive(100);
      assertNotNull(m);
      assertEquals("1st", m.getText());

      m = (TextMessage) consumer.receive(100);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      session.commit();

      session.commit();
   }

   @Test
   public void testInitialRedeliveryDelayOne() throws Exception {
      // Receive a message with the JMS API
      RedeliveryPolicy policy = connection.getRedeliveryPolicy();
      policy.setInitialRedeliveryDelay(1000);
      policy.setUseExponentialBackOff(false);
      policy.setMaximumRedeliveries(1);

      connection.start();
      Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = new ActiveMQQueue("TEST");
      this.makeSureCoreQueueExist("TEST");
      MessageProducer producer = session.createProducer(destination);

      MessageConsumer consumer = session.createConsumer(destination);

      // Send the messages
      producer.send(session.createTextMessage("1st"));
      producer.send(session.createTextMessage("2nd"));
      session.commit();

      TextMessage m;
      m = (TextMessage) consumer.receive(100);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      m = (TextMessage) consumer.receive(100);
      assertNull(m);

      m = (TextMessage) consumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());

      m = (TextMessage) consumer.receive(100);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      session.commit();
   }

   @Test
   public void testRedeliveryDelayOne() throws Exception {
      // Receive a message with the JMS API
      RedeliveryPolicy policy = connection.getRedeliveryPolicy();
      policy.setInitialRedeliveryDelay(0);
      policy.setRedeliveryDelay(1000);
      policy.setUseExponentialBackOff(false);
      policy.setMaximumRedeliveries(2);

      connection.start();
      Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = new ActiveMQQueue("TEST");
      this.makeSureCoreQueueExist("TEST");
      MessageProducer producer = session.createProducer(destination);

      MessageConsumer consumer = session.createConsumer(destination);

      // Send the messages
      producer.send(session.createTextMessage("1st"));
      producer.send(session.createTextMessage("2nd"));
      session.commit();

      TextMessage m;
      m = (TextMessage) consumer.receive(100);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      session.rollback();

      m = (TextMessage) consumer.receive(100);
      assertNotNull("first immediate redelivery", m);
      session.rollback();

      m = (TextMessage) consumer.receive(100);
      assertNull("second delivery delayed: " + m, m);

      m = (TextMessage) consumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());

      m = (TextMessage) consumer.receive(100);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      session.commit();
   }

   @Test
   public void testRedeliveryPolicyPerDestination() throws Exception {
      RedeliveryPolicy queuePolicy = new RedeliveryPolicy();
      queuePolicy.setInitialRedeliveryDelay(0);
      queuePolicy.setRedeliveryDelay(1000);
      queuePolicy.setUseExponentialBackOff(false);
      queuePolicy.setMaximumRedeliveries(2);

      RedeliveryPolicy topicPolicy = new RedeliveryPolicy();
      topicPolicy.setInitialRedeliveryDelay(0);
      topicPolicy.setRedeliveryDelay(1000);
      topicPolicy.setUseExponentialBackOff(false);
      topicPolicy.setMaximumRedeliveries(3);

      // Receive a message with the JMS API
      RedeliveryPolicyMap map = connection.getRedeliveryPolicyMap();
      map.put(new ActiveMQTopic(">"), topicPolicy);
      map.put(new ActiveMQQueue(">"), queuePolicy);

      connection.start();
      Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue queue = new ActiveMQQueue("TEST");
      ActiveMQTopic topic = new ActiveMQTopic("TEST");
      this.makeSureCoreQueueExist("TEST");

      MessageProducer producer = session.createProducer(null);

      MessageConsumer queueConsumer = session.createConsumer(queue);
      MessageConsumer topicConsumer = session.createConsumer(topic);

      // Send the messages
      producer.send(queue, session.createTextMessage("1st"));
      producer.send(queue, session.createTextMessage("2nd"));
      producer.send(topic, session.createTextMessage("1st"));
      producer.send(topic, session.createTextMessage("2nd"));

      session.commit();

      TextMessage m;
      m = (TextMessage) queueConsumer.receive(100);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      m = (TextMessage) topicConsumer.receive(100);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      m = (TextMessage) queueConsumer.receive(100);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      m = (TextMessage) topicConsumer.receive(100);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      session.rollback();

      m = (TextMessage) queueConsumer.receive(100);
      assertNotNull("first immediate redelivery", m);
      m = (TextMessage) topicConsumer.receive(100);
      assertNotNull("first immediate redelivery", m);
      session.rollback();

      m = (TextMessage) queueConsumer.receive(100);
      assertNull("second delivery delayed: " + m, m);
      m = (TextMessage) topicConsumer.receive(100);
      assertNull("second delivery delayed: " + m, m);

      m = (TextMessage) queueConsumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      m = (TextMessage) topicConsumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());

      m = (TextMessage) queueConsumer.receive(100);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      m = (TextMessage) topicConsumer.receive(100);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      session.rollback();

      m = (TextMessage) queueConsumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());
      m = (TextMessage) topicConsumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());

      m = (TextMessage) queueConsumer.receive(100);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      m = (TextMessage) topicConsumer.receive(100);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      session.rollback();

      // No third attempt for the Queue consumer
      m = (TextMessage) queueConsumer.receive(2000);
      assertNull(m);
      m = (TextMessage) topicConsumer.receive(2000);
      assertNotNull(m);
      assertEquals("1st", m.getText());

      m = (TextMessage) queueConsumer.receive(100);
      assertNull(m);
      m = (TextMessage) topicConsumer.receive(100);
      assertNotNull(m);
      assertEquals("2nd", m.getText());
      session.commit();
   }

}

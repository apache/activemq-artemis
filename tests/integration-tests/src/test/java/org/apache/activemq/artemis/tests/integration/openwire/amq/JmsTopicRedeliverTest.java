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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.tests.integration.openwire.BasicOpenWireTest;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Before;
import org.junit.Test;

/**
 * adapted from: org.apache.activemq.JmsTopicRedeliverTest
 */
public class JmsTopicRedeliverTest extends BasicOpenWireTest {

   protected Session session;
   protected Session consumeSession;
   protected MessageConsumer consumer;
   protected MessageProducer producer;
   protected Destination consumerDestination;
   protected Destination producerDestination;
   protected boolean topic = true;
   protected boolean durable;
   protected boolean verbose;
   protected long initRedeliveryDelay;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      initRedeliveryDelay = connection.getRedeliveryPolicy().getInitialRedeliveryDelay();

      if (durable) {
         connection.setClientID(getClass().getName());
      }

      System.out.println("Created connection: " + connection);

      session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      consumeSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

      System.out.println("Created session: " + session);
      System.out.println("Created consumeSession: " + consumeSession);
      producer = session.createProducer(null);
      // producer.setDeliveryMode(deliveryMode);

      System.out.println("Created producer: " + producer);

      if (topic) {
         consumerDestination = this.createDestination(session, ActiveMQDestination.TOPIC_TYPE);
         producerDestination = this.createDestination(session, ActiveMQDestination.TOPIC_TYPE);
      } else {
         consumerDestination = this.createDestination(session, ActiveMQDestination.QUEUE_TYPE);
         producerDestination = this.createDestination(session, ActiveMQDestination.QUEUE_TYPE);
      }

      System.out.println("Created  consumer destination: " + consumerDestination + " of type: " + consumerDestination.getClass());
      System.out.println("Created  producer destination: " + producerDestination + " of type: " + producerDestination.getClass());
      consumer = createConsumer(getName());
      connection.start();

      System.out.println("Created connection: " + connection);
   }

   /**
    * Sends and consumes the messages.
    *
    * @throws Exception
    */
   @Test
   public void testRecover() throws Exception {
      String text = "TEST";
      Message sendMessage = session.createTextMessage(text);

      if (verbose) {
         System.out.println("About to send a message: " + sendMessage + " with text: " + text);
      }
      producer.send(producerDestination, sendMessage);

      // receive but don't acknowledge
      Message unackMessage = consumer.receive(initRedeliveryDelay + 1000);
      assertNotNull(unackMessage);
      String unackId = unackMessage.getJMSMessageID();
      assertEquals(((TextMessage) unackMessage).getText(), text);
      assertFalse(unackMessage.getJMSRedelivered());
      // assertEquals(unackMessage.getIntProperty("JMSXDeliveryCount"),1);

      // receive then acknowledge
      consumeSession.recover();
      Message ackMessage = consumer.receive(initRedeliveryDelay + 1000);
      assertNotNull(ackMessage);
      ackMessage.acknowledge();
      String ackId = ackMessage.getJMSMessageID();
      assertEquals(((TextMessage) ackMessage).getText(), text);
      assertTrue(ackMessage.getJMSRedelivered());
      // assertEquals(ackMessage.getIntProperty("JMSXDeliveryCount"),2);
      assertEquals(unackId, ackId);
      consumeSession.recover();
      assertNull(consumer.receiveNoWait());
   }

   protected MessageConsumer createConsumer(String durableName) throws JMSException {
      if (durable) {
         System.out.println("Creating durable consumer " + durableName);
         return consumeSession.createDurableSubscriber((Topic) consumerDestination, durableName);
      }
      return consumeSession.createConsumer(consumerDestination);
   }

}

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
package org.apache.activemq.artemis.tests.integration.jms.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Exclusive Test
 */
public class ConsumerDelayDispatchTest extends JMSTestBase {

   private SimpleString queueName = SimpleString.toSimpleString("jms.consumer.delay.queue");
   private SimpleString normalQueueName = SimpleString.toSimpleString("jms.noraml.queue");

   private static final long DELAY_BEFORE_DISPATCH = 10000L;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server.createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST).setExclusive(true).setConsumersBeforeDispatch(2).setDelayBeforeDispatch(DELAY_BEFORE_DISPATCH));
      server.createQueue(new QueueConfiguration(normalQueueName).setRoutingType(RoutingType.ANYCAST).setExclusive(true));
   }


   protected ConnectionFactory getCF() throws Exception {
      return cf;
   }

   @Test
   public void testNoDelayOnDefault() throws Exception {
      sendMessage(normalQueueName);

      ConnectionFactory fact = getCF();
      Connection connection = fact.createConnection();

      try {

         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         connection.start();

         Destination queue = session.createQueue(normalQueueName.toString());
         MessageConsumer consumer1 = session.createConsumer(queue);

         Assert.assertNotNull(receive(consumer1));
      } finally {
         connection.close();
      }
   }

   @Test
   public void testDelayBeforeDispatch() throws Exception {
      sendMessage(queueName);

      ConnectionFactory fact = getCF();
      Connection connection = fact.createConnection();

      try {

         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         connection.start();

         Destination queue = session.createQueue(queueName.toString());
         MessageConsumer consumer1 = session.createConsumer(queue);

         Assert.assertNull(receive(consumer1));
         Thread.sleep(DELAY_BEFORE_DISPATCH);

         Assert.assertNotNull(receive(consumer1));
      } finally {
         connection.close();
      }
   }

   @Test
   public void testConsumersBeforeDispatch() throws Exception {
      sendMessage(queueName);


      ConnectionFactory fact = getCF();
      Connection connection = fact.createConnection();

      try {
         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         connection.start();
         Destination queue = session.createQueue(queueName.toString());

         MessageConsumer consumer1 = session.createConsumer(queue);

         Assert.assertNull(receive(consumer1));

         MessageConsumer consumer2 = session.createConsumer(queue);

         Assert.assertNotNull(receive(consumer1, consumer2));
      } finally {
         connection.close();
      }
   }


   @Test
   public void testContinueAndResetConsumer() throws Exception {
      sendMessage(queueName);

      ConnectionFactory fact = getCF();
      Connection connection = fact.createConnection();

      try {
         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         connection.start();
         Destination queue = session.createQueue(queueName.toString());

         MessageConsumer consumer1 = session.createConsumer(queue);

         Assert.assertNull(receive(consumer1));

         MessageConsumer consumer2 = session.createConsumer(queue);

         Assert.assertNotNull(receive(consumer1, consumer2));

         consumer2.close();

         //Ensure that now dispatch is active, if we close a consumer, dispatching continues.
         sendMessage(queueName);

         Assert.assertNotNull(receive(consumer1));

         //Stop all consumers, which should reset dispatch rules.
         consumer1.close();

         //Ensure that once all consumers are stopped, that dispatch rules reset and wait for min consumers.
         sendMessage(queueName);

         MessageConsumer consumer3 = session.createConsumer(queue);

         Assert.assertNull(receive(consumer3));

         MessageConsumer consumer4 = session.createConsumer(queue);

         Assert.assertNotNull(receive(consumer3, consumer4));


         //Stop all consumers, which should reset dispatch rules.
         consumer3.close();
         consumer4.close();

         //Ensure that once all consumers are stopped, that dispatch rules reset and wait for delay.
         sendMessage(queueName);

         MessageConsumer consumer5 = session.createConsumer(queue);

         Assert.assertNull(receive(consumer5));

         Thread.sleep(DELAY_BEFORE_DISPATCH);

         Assert.assertNotNull(receive(consumer5));

      } finally {
         connection.close();
      }
   }

   private Message receive(MessageConsumer consumer1) throws JMSException {
      return consumer1.receive(1000);
   }

   private Message receive(MessageConsumer consumer1, MessageConsumer consumer2) throws JMSException {
      Message receivedMessage = receive(consumer1);
      if (receivedMessage == null) {
         receivedMessage = receive(consumer2);
      }
      return receivedMessage;
   }

   public void sendMessage(SimpleString queue) throws Exception {
      ConnectionFactory fact = getCF();
      Connection connection = fact.createConnection();
      try {

         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         connection.start();

         Destination destination = session.createQueue(queue.toString());
         MessageProducer producer = session.createProducer(destination);

         TextMessage message = session.createTextMessage();
         message.setText("Message");
         producer.send(message);
      } finally {
         connection.close();
      }
   }

}

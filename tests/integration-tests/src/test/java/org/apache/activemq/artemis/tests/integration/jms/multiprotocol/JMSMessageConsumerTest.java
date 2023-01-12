/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.jms.multiprotocol;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.utils.DestinationUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSMessageConsumerTest extends MultiprotocolJMSClientTestSupport {

   protected static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test(timeout = 30000)
   public void testDeliveryModeAMQPProducerCoreConsumer() throws Exception {
      Connection connection = createConnection(); //AMQP
      Connection connection2 = createCoreConnection(); //CORE
      testDeliveryMode(connection, connection2);
   }

   @Test(timeout = 30000)
   public void testDeliveryModeAMQPProducerAMQPConsumer() throws Exception {
      Connection connection = createConnection(); //AMQP
      Connection connection2 = createConnection(); //AMQP
      testDeliveryMode(connection, connection2);
   }

   @Test(timeout = 30000)
   public void testDeliveryModeCoreProducerAMQPConsumer() throws Exception {
      Connection connection = createCoreConnection(); //CORE
      Connection connection2 = createConnection(); //AMQP
      testDeliveryMode(connection, connection2);
   }

   @Test(timeout = 30000)
   public void testDeliveryModeCoreProducerCoreConsumer() throws Exception {
      Connection connection = createCoreConnection(); //CORE
      Connection connection2 = createCoreConnection(); //CORE
      testDeliveryMode(connection, connection2);
   }

   private void testDeliveryMode(Connection connection1, Connection connection2) throws JMSException {
      try {
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         javax.jms.Queue queue1 = session1.createQueue(getQueueName());
         javax.jms.Queue queue2 = session2.createQueue(getQueueName());

         final MessageConsumer consumer2 = session2.createConsumer(queue2);

         MessageProducer producer = session1.createProducer(queue1);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);
         connection1.start();

         TextMessage message = session1.createTextMessage();
         message.setText("hello");
         producer.send(message);

         Message received = consumer2.receive(100);

         assertNotNull("Should have received a message by now.", received);
         assertTrue("Should be an instance of TextMessage", received instanceof TextMessage);
         assertEquals(DeliveryMode.PERSISTENT, received.getJMSDeliveryMode());
      } finally {
         connection1.close();
         connection2.close();
      }
   }

   @Test(timeout = 30000)
   public void testPriorityAMQPProducerCoreConsumer() throws Exception {
      Connection connection = createConnection(); //AMQP
      Connection connection2 = createCoreConnection(); //CORE
      testPriority(connection, connection2);
   }

   @Test(timeout = 30000)
   public void testPriorityAMQPProducerAMQPConsumer() throws Exception {
      Connection connection = createConnection(); //AMQP
      Connection connection2 = createConnection(); //AMQP
      testPriority(connection, connection2);
   }

   @Test(timeout = 30000)
   public void testPriorityModeCoreProducerAMQPConsumer() throws Exception {
      Connection connection = createCoreConnection(); //CORE
      Connection connection2 = createConnection(); //AMQP
      testPriority(connection, connection2);
   }

   @Test(timeout = 30000)
   public void testPriorityCoreProducerCoreConsumer() throws Exception {
      Connection connection = createCoreConnection(); //CORE
      Connection connection2 = createCoreConnection(); //CORE
      testPriority(connection, connection2);
   }

   private void testPriority(Connection connection1, Connection connection2) throws JMSException {
      try {
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         javax.jms.Queue queue1 = session1.createQueue(getQueueName());
         javax.jms.Queue queue2 = session2.createQueue(getQueueName());

         final MessageConsumer consumer2 = session2.createConsumer(queue2);

         MessageProducer producer = session1.createProducer(queue1);
         producer.setPriority(2);
         connection1.start();

         TextMessage message = session1.createTextMessage();
         message.setText("hello");
         producer.send(message);

         Message received = consumer2.receive(100);

         assertNotNull("Should have received a message by now.", received);
         assertTrue("Should be an instance of TextMessage", received instanceof TextMessage);
         assertEquals(2, received.getJMSPriority());
      } finally {
         connection1.close();
         connection2.close();
      }
   }

   @Test(timeout = 60000)
   public void testDurableSubscriptionWithConfigurationManagedQueueWithCore() throws Exception {
      testDurableSubscriptionWithConfigurationManagedQueue(() -> createCoreConnection(false));

   }

   @Test(timeout = 60000)
   public void testDurableSubscriptionWithConfigurationManagedQueueWithOpenWire() throws Exception {
      testDurableSubscriptionWithConfigurationManagedQueue(() -> createOpenWireConnection(false));

   }

   @Test(timeout = 60000)
   public void testDurableSubscriptionWithConfigurationManagedQueueWithAMQP() throws Exception {
      testDurableSubscriptionWithConfigurationManagedQueue(() -> JMSMessageConsumerTest.super.createConnection(false));
   }

   private void testDurableSubscriptionWithConfigurationManagedQueue(ConnectionSupplier connectionSupplier) throws Exception {
      final String clientId = "bar";
      final String subName = "foo";
      final String queueName = DestinationUtil.createQueueNameForSubscription(true, clientId, subName).toString();
      server.stop();
      server.getConfiguration().addQueueConfiguration(new QueueConfiguration(queueName).setAddress("myTopic").setFilterString("color = 'BLUE'").setRoutingType(RoutingType.MULTICAST));
      server.getConfiguration().setAmqpUseCoreSubscriptionNaming(true);
      server.start();

      try (Connection connection = connectionSupplier.createConnection()) {
         connection.setClientID(clientId);
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic destination = session.createTopic("myTopic");

         MessageConsumer messageConsumer = session.createDurableSubscriber(destination, subName);
         messageConsumer.close();

         Queue queue = server.locateQueue(queueName);
         assertNotNull(queue);
         assertNotNull(queue.getFilter());
         assertEquals("color = 'BLUE'", queue.getFilter().getFilterString().toString());
      }
   }
}

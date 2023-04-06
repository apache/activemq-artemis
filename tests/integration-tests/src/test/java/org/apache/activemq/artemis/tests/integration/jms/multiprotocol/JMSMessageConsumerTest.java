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
import javax.jms.MapMessage;
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
import org.apache.activemq.artemis.utils.RandomUtil;
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

   @Test(timeout = 30000)
   public void testEmptyMapMessageConversionBetweenOpenWireAndAMQP() throws Exception {
      testEmptyMapMessageConversion(createOpenWireConnection(), createConnection());
   }

   @Test(timeout = 30000)
   public void testEmptyMapMessageConversionBetweenAMQPAndOpenWire() throws Exception {
      testEmptyMapMessageConversion(createConnection(), createOpenWireConnection());
   }

   @Test(timeout = 30000)
   public void testEmptyMapMessageConversionBetweenCoreAndAMQP() throws Exception {
      testEmptyMapMessageConversion(createCoreConnection(), createConnection());
   }

   @Test(timeout = 30000)
   public void testEmptyMapMessageConversionBetweenAMQPAndCore() throws Exception {
      testEmptyMapMessageConversion(createConnection(), createCoreConnection());
   }

   @Test(timeout = 30000)
   public void testEmptyMapMessageConversionBetweenCoreAndOpenWire() throws Exception {
      testEmptyMapMessageConversion(createCoreConnection(), createOpenWireConnection());
   }

   @Test(timeout = 30000)
   public void testEmptyMapMessageConversionBetweenOpenWireAndCore() throws Exception {
      testEmptyMapMessageConversion(createOpenWireConnection(), createCoreConnection());
   }

   private void testEmptyMapMessageConversion(Connection senderConnection, Connection consumerConnection) throws Exception {
      try {
         Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSession.createConsumer(consumerSession.createQueue(getQueueName()));

         Session senderSession = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = senderSession.createProducer(senderSession.createQueue(getQueueName()));
         MapMessage message = senderSession.createMapMessage();
         producer.send(message);

         Message received = consumer.receive(1000);

         assertNotNull("Should have received a message by now.", received);
         assertTrue("Should be an instance of MapMessage", received instanceof MapMessage);
      } finally {
         senderConnection.close();
         consumerConnection.close();
      }
   }

   @Test(timeout = 30000)
   public void testMapMessageConversionBetweenAMQPAndOpenWire() throws Exception {
      testMapMessageConversion(createConnection(), createOpenWireConnection());
   }

   @Test(timeout = 30000)
   public void testMapMessageConversionBetweenCoreAndAMQP() throws Exception {
      testMapMessageConversion(createCoreConnection(), createConnection());
   }

   @Test(timeout = 30000)
   public void testMapMessageConversionBetweenAMQPAndCore() throws Exception {
      testMapMessageConversion(createConnection(), createCoreConnection());
   }

   @Test(timeout = 30000)
   public void testMapMessageConversionBetweenCoreAndOpenWire() throws Exception {
      testMapMessageConversion(createCoreConnection(), createOpenWireConnection());
   }

   @Test(timeout = 30000)
   public void testMapMessageConversionBetweenOpenWireAndCore() throws Exception {
      testMapMessageConversion(createOpenWireConnection(), createCoreConnection());
   }

   private void testMapMessageConversion(Connection senderConnection, Connection consumerConnection) throws Exception {
      final boolean BOOLEAN_VALUE = RandomUtil.randomBoolean();
      final String BOOLEAN_KEY = "myBoolean";
      final byte BYTE_VALUE = RandomUtil.randomByte();
      final String BYTE_KEY = "myByte";
      final byte[] BYTES_VALUE = RandomUtil.randomBytes();
      final String BYTES_KEY = "myBytes";
      final char CHAR_VALUE = RandomUtil.randomChar();
      final String CHAR_KEY = "myChar";
      final double DOUBLE_VALUE = RandomUtil.randomDouble();
      final String DOUBLE_KEY = "myDouble";
      final float FLOAT_VALUE = RandomUtil.randomFloat();
      final String FLOAT_KEY = "myFloat";
      final int INT_VALUE = RandomUtil.randomInt();
      final String INT_KEY = "myInt";
      final long LONG_VALUE = RandomUtil.randomLong();
      final String LONG_KEY = "myLong";
      final Boolean OBJECT_VALUE = RandomUtil.randomBoolean();
      final String OBJECT_KEY = "myObject";
      final short SHORT_VALUE = RandomUtil.randomShort();
      final String SHORT_KEY = "myShort";
      final String STRING_VALUE = RandomUtil.randomString();
      final String STRING_KEY = "myString";

      try {
         Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSession.createConsumer(consumerSession.createQueue(getQueueName()));

         Session senderSession = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = senderSession.createProducer(senderSession.createQueue(getQueueName()));
         MapMessage message = senderSession.createMapMessage();message.setBoolean(BOOLEAN_KEY, BOOLEAN_VALUE);
         message.setByte(BYTE_KEY, BYTE_VALUE);
         message.setBytes(BYTES_KEY, BYTES_VALUE);
         message.setChar(CHAR_KEY, CHAR_VALUE);
         message.setDouble(DOUBLE_KEY, DOUBLE_VALUE);
         message.setFloat(FLOAT_KEY, FLOAT_VALUE);
         message.setInt(INT_KEY, INT_VALUE);
         message.setLong(LONG_KEY, LONG_VALUE);
         message.setObject(OBJECT_KEY, OBJECT_VALUE);
         message.setShort(SHORT_KEY, SHORT_VALUE);
         message.setString(STRING_KEY, STRING_VALUE);
         producer.send(message);

         Message received = consumer.receive(1000);

         assertNotNull("Should have received a message by now.", received);
         assertTrue("Should be an instance of MapMessage", received instanceof MapMessage);
         MapMessage receivedMapMessage = (MapMessage) received;

         assertEquals(BOOLEAN_VALUE, receivedMapMessage.getBoolean(BOOLEAN_KEY));
         assertEquals(BYTE_VALUE, receivedMapMessage.getByte(BYTE_KEY));
         assertEqualsByteArrays(BYTES_VALUE, receivedMapMessage.getBytes(BYTES_KEY));
         assertEquals(CHAR_VALUE, receivedMapMessage.getChar(CHAR_KEY));
         assertEquals(DOUBLE_VALUE, receivedMapMessage.getDouble(DOUBLE_KEY), 0);
         assertEquals(FLOAT_VALUE, receivedMapMessage.getFloat(FLOAT_KEY), 0);
         assertEquals(INT_VALUE, receivedMapMessage.getInt(INT_KEY));
         assertEquals(LONG_VALUE, receivedMapMessage.getLong(LONG_KEY));
         assertTrue(receivedMapMessage.getObject(OBJECT_KEY) instanceof Boolean);
         assertEquals(OBJECT_VALUE, receivedMapMessage.getObject(OBJECT_KEY));
         assertEquals(SHORT_VALUE, receivedMapMessage.getShort(SHORT_KEY));
         assertEquals(STRING_VALUE, receivedMapMessage.getString(STRING_KEY));
      } finally {
         senderConnection.close();
         consumerConnection.close();
      }
   }
}

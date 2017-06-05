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
package org.apache.activemq.artemis.tests.integration.amqp;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.junit.Test;

public class JMSSharedConsumerTest extends JMSClientTestSupport {

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   private void testSharedConsumer(Connection connection1, Connection connection2) throws JMSException {
      try {
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Topic topic = session1.createTopic(getTopicName());
         Topic topic2 = session2.createTopic(getTopicName());

         final MessageConsumer consumer1 = session1.createSharedConsumer(topic, "SharedConsumer");
         final MessageConsumer consumer2 = session2.createSharedConsumer(topic2, "SharedConsumer");

         MessageProducer producer = session1.createProducer(topic);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);
         connection1.start();

         TextMessage message = session1.createTextMessage();
         message.setText("hello");
         producer.send(message);

         Message message1 = consumer1.receive(100);
         Message message2 = consumer2.receive(100);

         Message received = null;
         if (message1 != null) {
            assertNull("Message should only be delivered once per subscribtion but see twice", message2);
            received = message1;
         } else {
            received = message2;
         }
         assertNotNull("Should have received a message by now.", received);
         assertTrue("Should be an instance of TextMessage", received instanceof TextMessage);
      } finally {
         connection1.close();
         connection2.close();
      }
   }

   @Test(timeout = 30000)
   public void testSharedConsumer() throws Exception {
      Connection connection = createConnection(); //AMQP
      Connection connection2 = createConnection(); //AMQP

      testSharedConsumer(connection, connection2);
   }

   @Test(timeout = 30000)
   public void testSharedConsumerWithArtemisClient() throws Exception {

      Connection connection = createCoreConnection(); //CORE
      Connection connection2 = createCoreConnection(); //CORE

      testSharedConsumer(connection, connection2);

   }

   @Test(timeout = 30000)
   public void testSharedConsumerWithAMQPClientAndArtemisClient() throws Exception {

      Connection connection = createConnection(); //AMQP
      Connection connection2 = createCoreConnection(); //CORE

      testSharedConsumer(connection, connection2);

   }

   @Test(timeout = 30000)
   public void testSharedConsumerWithArtemisClientAndAMQPClient() throws Exception {

      Connection connection = createCoreConnection(); //CORE
      Connection connection2 = createConnection(); //AMQP

      testSharedConsumer(connection, connection2);

   }


   protected String getBrokerCoreJMSConnectionString() {

      try {
         int port = AMQP_PORT;

         String uri = null;

         if (isUseSSL()) {
            uri = "tcp://127.0.0.1:" + port;
         } else {
            uri = "tcp://127.0.0.1:" + port;
         }

         if (!getJmsConnectionURIOptions().isEmpty()) {
            uri = uri + "?" + getJmsConnectionURIOptions();
         }

         return uri;
      } catch (Exception e) {
         throw new RuntimeException();
      }
   }

   protected Connection createCoreConnection() throws JMSException {
      return createCoreConnection(getBrokerCoreJMSConnectionString(), null, null, null, true);
   }

   private Connection createCoreConnection(String connectionString, String username, String password, String clientId, boolean start) throws JMSException {
      ActiveMQJMSConnectionFactory factory = new ActiveMQJMSConnectionFactory(connectionString);

      Connection connection = trackJMSConnection(factory.createConnection(username, password));

      connection.setExceptionListener(new ExceptionListener() {
         @Override
         public void onException(JMSException exception) {
            exception.printStackTrace();
         }
      });

      if (clientId != null && !clientId.isEmpty()) {
         connection.setClientID(clientId);
      }

      if (start) {
         connection.start();
      }

      return connection;
   }

}

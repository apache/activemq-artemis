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
package org.apache.activemq.artemis.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * This example demonstrates the use of ActiveMQ Artemis "pre-acknowledge" functionality where
 * messages are acknowledged before they are delivered to the consumer.
 *
 * Please see the readme for more details.
 */
public class PreacknowledgeExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      try {

         // Step 2. instantiate the queue object
         Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

         // new connection factory
         ConnectionFactory cf = new ActiveMQConnectionFactory();

         // Step 3. Create a the JMS objects
         connection = cf.createConnection();

         Session session = connection.createSession(false, ActiveMQJMSConstants.PRE_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(queue);

         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 4. Create and send a message
         TextMessage message1 = session.createTextMessage("This is a text message 1");

         producer.send(message1);

         System.out.println("Sent message: " + message1.getText());

         // Step 5. Print out the message count of the queue. The queue contains one message as expected
         // delivery has not yet started on the queue
         int count = getMessageCount(connection);

         System.out.println("Queue message count is " + count);

         // Step 6. Start the Connection, delivery will now start. Give a little time for delivery to occur.
         connection.start();

         Thread.sleep(1000);

         // Step 7. Print out the message countof the queue. It should now be zero, since the message has
         // already been acknowledged even before the consumer has received it.
         count = getMessageCount(connection);

         System.out.println("Queue message count is now " + count);

         if (count != 0) {
            throw new IllegalStateException("Queue message count is not 0.");
         }

         // Step 8. Finally, receive the message
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

         System.out.println("Received message: " + messageReceived.getText());
      } finally {
         // Step 9. Be sure to close our resources!
         if (connection != null) {
            connection.close();
         }
      }
   }

   // To do this we send a management message to get the message count.
   // In real life you wouldn't create a new session every time you send a management message
   private static int getMessageCount(final Connection connection) throws Exception {
      QueueSession session = ((QueueConnection) connection).createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");

      QueueRequestor requestor = new QueueRequestor(session, managementQueue);

      connection.start();

      Message m = session.createMessage();

      JMSManagementHelper.putAttribute(m, ResourceNames.QUEUE + "exampleQueue", "messageCount");

      Message response = requestor.request(m);

      int messageCount = (Integer) JMSManagementHelper.getResult(response, Integer.class);

      return messageCount;
   }

}

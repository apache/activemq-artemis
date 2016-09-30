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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Enumeration;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * This example shows how to configure and use a <em>Last-Value</em> queues.
 * Only the last message with a well-defined property is hold by the queue.
 */
public class LastValueQueueExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      try {
         // Step 2. new Queue
         Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

         // Step 3. new Connection Factory
         ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory();

         // Step 4.Create a JMS Connection, session and producer on the queue
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);

         // Step 5. Create and send a text message with the Last-Value header set
         TextMessage message = session.createTextMessage("1st message with Last-Value property set");
         message.setStringProperty("_AMQ_LVQ_NAME", "STOCK_NAME");
         producer.send(message);
         System.out.format("Sent message: %s%n", message.getText());

         // Step 6. Create and send a second text message with the Last-Value header set
         message = session.createTextMessage("2nd message with Last-Value property set");
         message.setStringProperty("_AMQ_LVQ_NAME", "STOCK_NAME");
         producer.send(message);
         System.out.format("Sent message: %s%n", message.getText());

         // Step 7. Create and send a third text message with the Last-Value header set
         message = session.createTextMessage("3rd message with Last-Value property set");
         message.setStringProperty("_AMQ_LVQ_NAME", "STOCK_NAME");
         producer.send(message);
         System.out.format("Sent message: %s%n", message.getText());

         // Step 8. Browse the queue. There is only 1 message in it, the last sent
         try (QueueBrowser browser = session.createBrowser(queue)) {
            Enumeration enumeration = browser.getEnumeration();
            while (enumeration.hasMoreElements()) {
               TextMessage messageInTheQueue = (TextMessage) enumeration.nextElement();
               System.out.format("Message in the queue: %s%n", messageInTheQueue.getText());
            }
         }

         // Step 9. Create a JMS Message Consumer for the queue
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 10. Start the Connection
         connection.start();

         // Step 11. Trying to receive a message. Since the queue is configured to keep only the
         // last message with the Last-Value header set, the message received is the last sent
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);
         System.out.format("Received message: %s%n", messageReceived.getText());

         // Step 12. Trying to receive another message but there will be none.
         // The 1st message was discarded when the 2nd was sent to the queue.
         // The 2nd message was in turn discarded when the 3trd was sent to the queue
         messageReceived = (TextMessage) messageConsumer.receive(5000);
         System.out.format("Received message: %s%n", messageReceived);

         cf.close();
      } finally {
         // Step 13. Be sure to close our JMS resources!
         if (connection != null) {
            connection.close();
         }
      }
   }
}

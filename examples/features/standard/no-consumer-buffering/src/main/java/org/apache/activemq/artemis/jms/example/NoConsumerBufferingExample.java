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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * This example demonstrates how ActiveMQ Artemis consumers can be configured to not buffer any messages from
 * the server.
 */
public class NoConsumerBufferingExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      try {
         // Step 2. Perfom a lookup on the queue
         Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

         // Step 3. new Connection factory with consumerWindowsize=0
         ConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616?consumerWindowSize=0");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a JMS MessageConsumer

         MessageConsumer consumer1 = session.createConsumer(queue);

         // Step 8. Start the connection

         connection.start();

         // Step 9. Send 10 messages to the queue

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session.createTextMessage("This is text message: " + i);

            producer.send(message);
         }

         System.out.println("Sent messages");

         // Step 10. Create another consumer on the same queue

         MessageConsumer consumer2 = session.createConsumer(queue);

         // Step 11. Consume three messages from consumer2

         for (int i = 0; i < 3; i++) {
            TextMessage message = (TextMessage) consumer2.receive(2000);

            System.out.println("Consumed message from consumer2: " + message.getText());
         }

         // Step 12. Consume five messages from consumer1

         for (int i = 0; i < 5; i++) {
            TextMessage message = (TextMessage) consumer1.receive(2000);

            System.out.println("Consumed message from consumer1: " + message.getText());
         }

         // Step 13. Consume another two messages from consumer2

         for (int i = 0; i < 2; i++) {
            TextMessage message = (TextMessage) consumer2.receive(2000);

            System.out.println("Consumed message from consumer1: " + message.getText());
         }
      } finally {
         // Step 9. Be sure to close our resources!

         if (connection != null) {
            connection.close();
         }
      }
   }
}

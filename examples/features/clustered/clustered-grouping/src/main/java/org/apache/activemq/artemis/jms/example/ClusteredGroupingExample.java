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
 * A simple example that demonstrates server side load-balancing of messages between the queue instances on different
 * nodes of the cluster.
 */
public class ClusteredGroupingExample {

   public static void main(String[] args) throws Exception {
      Connection connection0 = null;

      Connection connection1 = null;

      Connection connection2 = null;

      try {
         // Step 1. We will instantiate the queue object directly on this example
         //         This could be done through JNDI or JMSession.createQueue
         Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

         // Step 2. create a connection factory towards server 0.
         ConnectionFactory cf0 = new ActiveMQConnectionFactory("tcp://localhost:61616");

         // Step 3. create a connection factory towards server 1.
         ConnectionFactory cf1 = new ActiveMQConnectionFactory("tcp://localhost:61617");

         // Step 4.  create a connection factory towards server 2.
         ConnectionFactory cf2 = new ActiveMQConnectionFactory("tcp://localhost:61618");

         // Step 5. We create a JMS Connection connection0 which is a connection to server 0
         connection0 = cf0.createConnection();

         // Step 6. We create a JMS Connection connection1 which is a connection to server 1
         connection1 = cf1.createConnection();

         // Step 7. We create a JMS Connection connection2 which is a connection to server 2
         connection2 = cf2.createConnection();

         // Step 8. We create a JMS Session on server 0
         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 9. We create a JMS Session on server 1
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 10. We create a JMS Session on server 2
         Session session2 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 11. We start the connections to ensure delivery occurs on them
         connection0.start();

         connection1.start();

         connection2.start();

         // Step 12. We create JMS MessageConsumer objects on server 0
         MessageConsumer consumer = session0.createConsumer(queue);

         // Step 13. We create a JMS MessageProducer object on server 0, 1 and 2
         MessageProducer producer0 = session0.createProducer(queue);

         MessageProducer producer1 = session1.createProducer(queue);

         MessageProducer producer2 = session2.createProducer(queue);

         // Step 14. We send some messages to server 0, 1 and 2 with the same groupid set

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session0.createTextMessage("This is text message " + i);

            message.setStringProperty("JMSXGroupID", "Group-0");

            producer0.send(message);

            System.out.println("Sent messages: " + message.getText() + " to node 0");
         }

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session1.createTextMessage("This is text message " + (i + 10));

            message.setStringProperty("JMSXGroupID", "Group-0");

            producer1.send(message);

            System.out.println("Sent messages: " + message.getText() + " to node 1");

         }

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session2.createTextMessage("This is text message " + (i + 20));

            message.setStringProperty("JMSXGroupID", "Group-0");

            producer2.send(message);

            System.out.println("Sent messages: " + message.getText() + " to node 2");
         }

         // Step 15. We now consume those messages from server 0
         // We note the messages have all been sent to the same consumer on the same node

         for (int i = 0; i < numMessages * 3; i++) {
            TextMessage message0 = (TextMessage) consumer.receive(5000);

            System.out.println("Got message: " + message0.getText() + " from node 0");

         }
      } finally {
         // Step 17. Be sure to close our resources!

         if (connection0 != null) {
            connection0.close();
         }

         if (connection1 != null) {
            connection1.close();
         }

         if (connection2 != null) {
            connection2.close();
         }
      }
   }
}

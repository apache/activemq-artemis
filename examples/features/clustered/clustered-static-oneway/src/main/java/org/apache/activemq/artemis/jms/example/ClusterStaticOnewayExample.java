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
import org.apache.activemq.artemis.util.ServerUtil;

/**
 * A simple example that demonstrates server side load-balancing of messages between the queue instances on different
 * nodes of the cluster. The cluster is created from a static list of nodes.
 */
public class ClusterStaticOnewayExample {

   public static void main(final String[] args) throws Exception {
      Connection initialConnection = null;

      Connection connection0 = null;

      Connection connection1 = null;

      Connection connection2 = null;

      try {
         // Step 2. Instantiate Queue
         Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

         // Step 3. Look-up a JMS Connection Factory object from JNDI on server 0
         ConnectionFactory cf0 = new ActiveMQConnectionFactory("tcp://localhost:61616");

         //step 4. grab an initial connection and wait, in reality you wouldn't do it this way but since we want to ensure an
         // equal load balance we do this and then create 4 connections round robined
         initialConnection = cf0.createConnection();

         Thread.sleep(2000);
         // Step 5. We create a JMS Connection connection0 which is a connection to server 0
         connection0 = cf0.createConnection();

         // Step 6. We create a JMS Connection connection1 which is a connection to server 1
         connection1 = cf0.createConnection();

         // Step 7. We create a JMS Connection connection0 which is a connection to server 2
         connection2 = cf0.createConnection();

         // Step 8. We create a JMS Session on server 0
         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 9. We create a JMS Session on server 1
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 10. We create a JMS Session on server 2
         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 11. We start the connections to ensure delivery occurs on them
         connection0.start();

         connection1.start();

         connection2.start();

         // Step 12. We create JMS MessageConsumer objects on server 0,server 1 and server 2
         MessageConsumer consumer0 = session0.createConsumer(queue);

         MessageConsumer consumer1 = session1.createConsumer(queue);

         MessageConsumer consumer2 = session2.createConsumer(queue);

         Thread.sleep(4000);

         int con0Node = ServerUtil.getServer(connection0);
         int con1Node = ServerUtil.getServer(connection1);
         int con2Node = ServerUtil.getServer(connection2);

         System.out.println("con0Node = " + con0Node);
         System.out.println("con1Node = " + con1Node);
         System.out.println("con2Node = " + con2Node);

         if (con0Node + con1Node + con2Node != 3) {
            throw new IllegalStateException("connections not load balanced");
         }
         // Step 13. We create a JMS MessageProducer object on server 0
         Session sendSession = ServerUtil.getServerConnection(0, connection0, connection1, connection2).createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = sendSession.createProducer(queue);

         // Step 14. We send some messages to server 0

         final int numMessages = 18;

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session0.createTextMessage("This is text message " + i);

            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }
         Thread.sleep(2000);
         // Step 15. We now consume those messages on *both* server 0,server 1 and 2.
         // We note the messages have been distributed between servers in a round robin fashion
         // JMS Queues implement point-to-point message where each message is only ever consumed by a
         // maximum of one consumer

         for (int i = 0; i < numMessages; i += 3) {
            TextMessage message0 = (TextMessage) consumer0.receive(5000);

            System.out.println("Got message: " + message0.getText() + " from node " + con0Node);

            TextMessage message1 = (TextMessage) consumer1.receive(5000);

            System.out.println("Got message: " + message1.getText() + " from node " + con1Node);

            TextMessage message2 = (TextMessage) consumer2.receive(5000);

            System.out.println("Got message: " + message2.getText() + " from node " + con2Node);
         }
      } finally {
         // Step 15. Be sure to close our resources!

         if (initialConnection != null) {
            initialConnection.close();
         }

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

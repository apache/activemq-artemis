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
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq.artemis.util.ServerUtil;

/**
 * Example of live and replicating backup pair using Zookeeper as the quorum provider.
 * <p>
 * After both servers are started, the live server is killed and the backup becomes active ("fails-over").
 * <p>
 * Later the live server is restarted and takes back its position by asking the backup to stop ("fail-back").
 */
public class ZookeeperSinglePairFailback {

   private static Process server0;

   private static Process server1;

   public static void main(final String[] args) throws Exception {
      // Step 0. Prepare Zookeeper Evironment as shown on readme.md

      final int numMessages = 30;

      Connection connection = null;

      InitialContext initialContext = null;

      try {
         server0 = ServerUtil.startServer(args[0], ZookeeperSinglePairFailback.class.getSimpleName() + "-primary", 0, 30000);
         server1 = ServerUtil.startServer(args[1], ZookeeperSinglePairFailback.class.getSimpleName() + "-backup", 1, 10000);

         // Step 2. Get an initial context for looking up JNDI from the server #1
         initialContext = new InitialContext();

         // Step 3. Look up the JMS resources from JNDI
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");
         ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = connectionFactory.createConnection();

         // Step 5. Create a *non-transacted* JMS Session with client acknowledgement
         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         // Step 6. Start the connection to ensure delivery occurs
         connection.start();

         // Step 7. Create a JMS MessageProducer and a MessageConsumer
         MessageProducer producer = session.createProducer(queue);
         MessageConsumer consumer = session.createConsumer(queue);

         // Step 8. Send some messages to server #1, the live server
         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session.createTextMessage("This is text message " + i);
            producer.send(message);
            System.out.println("Sent message: " + message.getText());
         }

         // Step 9. Receive and acknowledge a third of the sent messages
         TextMessage message0 = null;
         for (int i = 0; i < numMessages / 3; i++) {
            message0 = (TextMessage) consumer.receive(5000);
            System.out.println("Got message: " + message0.getText());
         }
         message0.acknowledge();
         System.out.println("Received and acknowledged a third of the sent messages");

         // Step 10. Receive the rest third of the sent messages but *do not* acknowledge them yet
         for (int i = numMessages / 3; i < numMessages; i++) {
            message0 = (TextMessage) consumer.receive(5000);
            System.out.println("Got message: " + message0.getText());
         }
         System.out.println("Received without acknowledged the rest of the sent messages");

         Thread.sleep(2000);
         // Step 11. Crash server #0, the live server, and wait a little while to make sure
         // it has really crashed
         ServerUtil.killServer(server0);
         System.out.println("Killed primary");

         Thread.sleep(2000);

         // Step 12. Acknowledging the received messages will fail as failover to the backup server has occurred
         try {
            message0.acknowledge();
         } catch (JMSException e) {
            System.out.println("Got (the expected) exception while acknowledging message: " + e.getMessage());
         }

         // Step 13. Consume again the 2nd third of the messages again. Note that they are not considered as redelivered.
         for (int i = numMessages / 3; i < (numMessages / 3) * 2; i++) {
            message0 = (TextMessage) consumer.receive(5000);
            System.out.printf("Got message: %s (redelivered?: %s)\n", message0.getText(), message0.getJMSRedelivered());
         }

         // Step 14. Acknowledging them on the failed-over broker works fine
         message0.acknowledge();
         System.out.println("Acknowledged 2n third of messages");

         // Step 15. Restarting primary
         server0 = ServerUtil.startServer(args[0], ZookeeperSinglePairFailback.class.getSimpleName() + "-primary", 0, 10000);
         System.out.println("Started primary");

         // await fail-back to complete
         Thread.sleep(4000);

         // Step 16. Consuming the 3rd third of the messages. Note that they are not considered as redelivered.
         for (int i = (numMessages / 3) * 2; i < numMessages; i++) {
            message0 = (TextMessage) consumer.receive(5000);
            System.out.printf("Got message: %s (redelivered?: %s)\n", message0.getText(), message0.getJMSRedelivered());
         }
         message0.acknowledge();
         System.out.println("Acknowledged 3d third of messages");

      } finally {
         // Step 17. Be sure to close our resources!

         if (connection != null) {
            connection.close();
         }

         if (initialContext != null) {
            initialContext.close();
         }

         ServerUtil.killServer(server0);
         ServerUtil.killServer(server1);

         // Step 18. stop the ZK server
      }
   }
}

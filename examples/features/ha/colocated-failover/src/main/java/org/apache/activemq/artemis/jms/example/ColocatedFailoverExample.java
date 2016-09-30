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
import javax.naming.InitialContext;
import java.util.Hashtable;

import org.apache.activemq.artemis.util.ServerUtil;

/**
 * A simple example that demonstrates a colocated server
 */
public class ColocatedFailoverExample {

   private static Process server0;

   private static Process server1;

   public static void main(final String[] args) throws Exception {
      final int numMessages = 30;

      Connection connection = null;
      Connection connection1 = null;

      InitialContext initialContext = null;
      InitialContext initialContext1 = null;

      try {
         server0 = ServerUtil.startServer(args[0], ColocatedFailoverExample.class.getSimpleName() + "0", 0, 5000);
         server1 = ServerUtil.startServer(args[1], ColocatedFailoverExample.class.getSimpleName() + "1", 1, 5000);

         Thread.sleep(3000);

         // Step 1. Get an initial context for looking up JNDI for both servers
         Hashtable<String, Object> properties = new Hashtable<>();
         properties.put("java.naming.factory.initial", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
         properties.put("connectionFactory.ConnectionFactory", "tcp://localhost:61616?ha=true&retryInterval=1000&retryIntervalMultiplier=1.0&reconnectAttempts=-1");
         properties.put("queue.queue/exampleQueue", "exampleQueue");
         initialContext = new InitialContext(properties);

         properties = new Hashtable<>();
         properties.put("java.naming.factory.initial", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
         properties.put("connectionFactory.ConnectionFactory", "tcp://localhost:61617");
         initialContext1 = new InitialContext(properties);

         // Step 2. Look up the JMS resources from JNDI
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");
         ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup("ConnectionFactory");
         ConnectionFactory connectionFactory1 = (ConnectionFactory) initialContext1.lookup("ConnectionFactory");

         // Step 3. Create a JMS Connections
         connection = connectionFactory.createConnection();
         connection1 = connectionFactory1.createConnection();

         // Step 4. Create a *non-transacted* JMS Session with client acknowledgement
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 5. Create a JMS MessageProducers
         MessageProducer producer = session.createProducer(queue);
         MessageProducer producer1 = session1.createProducer(queue);

         // Step 6. Send some messages to both servers
         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session.createTextMessage("This is text message " + i);
            producer.send(message);
            System.out.println("Sent message: " + message.getText());
            message = session1.createTextMessage("This is another text message " + i);
            producer1.send(message);
            System.out.println("Sent message: " + message.getText());
         }

         // Step 7. Crash server #0, the live server, and wait a little while to make sure
         // it has really crashed
         ServerUtil.killServer(server0);
         Thread.sleep(10000);

         // Step 8. start the connection ready to receive messages
         connection.start();
         connection1.start();

         // Step 9.create a consumer
         MessageConsumer consumer = session1.createConsumer(queue);

         // Step 10. Receive and acknowledge all of the sent messages, the backup server that is colocated with server 1
         // will have become live and is now handling messages for server 0.
         TextMessage message0 = null;
         for (int i = 0; i < numMessages; i++) {
            message0 = (TextMessage) consumer.receive(5000);
            if (message0 == null) {
               throw new IllegalStateException("Message not received!");
            }
            System.out.println("Got message: " + message0.getText());
         }
         message0.acknowledge();

         MessageConsumer consumer1 = session.createConsumer(queue);

         // Step 11. Receive and acknowledge the rest of the sent messages from server 1.
         for (int i = 0; i < numMessages; i++) {
            message0 = (TextMessage) consumer1.receive(5000);
            System.out.println("Got message: " + message0.getText());
         }
         message0.acknowledge();
      } finally {
         // Step 11. Be sure to close our resources!

         if (connection != null) {
            connection.close();
         }

         if (initialContext != null) {
            initialContext.close();
         }
         if (connection1 != null) {
            connection1.close();
         }

         if (initialContext1 != null) {
            initialContext1.close();
         }

         ServerUtil.killServer(server0);
         ServerUtil.killServer(server1);
      }
   }
}

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
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.util.ServerUtil;

/**
 * A simple example that demonstrates application-layer failover of the JMS connection from one node to another
 * when the live server crashes
 */
public class ApplicationLayerFailoverExample {

   private static InitialContext initialContext;

   private static Connection connection;

   private static Session session;

   private static MessageConsumer consumer;

   private static MessageProducer producer;

   private static final CountDownLatch failoverLatch = new CountDownLatch(1);

   private static Process server0;

   private static Process server1;

   public static void main(final String[] args) throws Exception {
      try {
         server0 = ServerUtil.startServer(args[0], ApplicationLayerFailoverExample.class.getSimpleName() + "0", 0, 5000);
         server1 = ServerUtil.startServer(args[1], ApplicationLayerFailoverExample.class.getSimpleName() + "1", 1, 5000);

         // Step 1. We create our JMS Connection, Session, MessageProducer and MessageConsumer on server 1.
         createJMSObjects(0);

         // Step 2. We set a JMS ExceptionListener on the connection. On failure this will be called and the connection,
         // session, etc. will be then recreated on the backup node.
         connection.setExceptionListener(new ExampleListener());

         System.out.println("The initial JMS objects have been created, and the ExceptionListener set");

         // Step 3. We send some messages to server 1, the live server

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session.createTextMessage("This is text message " + i);

            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }

         // Step 4. We consume those messages on server 1.

         for (int i = 0; i < numMessages; i++) {
            TextMessage message0 = (TextMessage) consumer.receive(5000);

            System.out.println("Got message: " + message0.getText());
         }

         // Step 5. We now cause server 1, the live server to crash. After a little while the connection's
         // ExceptionListener will register the failure and reconnection will occur.

         System.out.println("Killing the server");

         ServerUtil.killServer(server0);

         // Step 6. Wait for the client side to register the failure and reconnect

         boolean ok = failoverLatch.await(5000, TimeUnit.MILLISECONDS);

         System.out.println("Reconnection has occurred. Now sending more messages.");

         // Step 8. We now send some more messages

         for (int i = numMessages; i < numMessages * 2; i++) {
            TextMessage message = session.createTextMessage("This is text message " + i);

            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }

         // Step 9. And consume them.

         for (int i = 0; i < numMessages; i++) {
            TextMessage message0 = (TextMessage) consumer.receive(5000);

            System.out.println("Got message: " + message0.getText());
         }
      } catch (Throwable t) {
         t.printStackTrace();
      } finally {
         // Step 14. Be sure to close our resources!
         closeResources();
         ServerUtil.killServer(server0);
         ServerUtil.killServer(server1);
      }
   }

   private static void createJMSObjects(final int server) throws Exception {
      // Step 1. Instantiate a JMS Connection Factory object from JNDI on server 1
      ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://127.0.0.1:" + (61616 + server));

      // Step 2. We create a JMS Connection connection
      connection = connectionFactory.createConnection();

      // Step 3. We start the connection to ensure delivery occurs
      connection.start();

      // Step 4. We create a JMS Session
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // Step 5. Look-up the JMS Queue object from JNDI
      Queue queue = session.createQueue("exampleQueue");

      // Step 6. We create a JMS MessageConsumer object
      consumer = session.createConsumer(queue);

      // Step 7. We create a JMS MessageProducer object
      producer = session.createProducer(queue);
   }

   private static void closeResources() {
      if (initialContext != null) {
         try {
            initialContext.close();
         } catch (NamingException e) {
            e.printStackTrace();
         }
      }

      if (connection != null) {
         try {
            connection.close();
         } catch (JMSException e) {
            e.printStackTrace();
         }
      }
   }

   private static class ExampleListener implements ExceptionListener {

      @Override
      public void onException(final JMSException exception) {
         try {
            connection.close();
         } catch (JMSException e) {
            //ignore
         }
         for (int i = 0; i < 10; i++) {
            try {
               // Step 7. The ExceptionListener gets called and we recreate the JMS objects on the new node

               System.out.println("Connection failure has been detected on a the client.");

               // Close the old resources

               // closeResources();

               System.out.println("The old resources have been closed.");

               // Create new JMS objects on the backup server

               createJMSObjects(1);

               System.out.println("The new resources have been created.");

               failoverLatch.countDown();

               return;
            } catch (Exception e) {
               System.out.println("Failed to handle failover, trying again.");
               try {
                  Thread.sleep(500);
               } catch (InterruptedException e1) {
                  //ignored
               }
            }
         }
         System.out.println("tried 10 times to reconnect, giving up");
      }
   }
}

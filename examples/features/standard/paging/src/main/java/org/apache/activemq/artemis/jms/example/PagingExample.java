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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;

/**
 * A simple JMS Queue example that creates a producer and consumer on a queue and sends then receives a message.
 */
public class PagingExample {

   public static void main(final String[] args) throws Exception {
      // simple routing showing how paging should work
      simplePaging();
      // simple routing showing what happens when paging enters into page-full
      pageFullLimit();
   }


   public static void pageFullLimit() throws Exception {
      InitialContext initialContext = null;
      try {
         // Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Create a JMS Connection
         try (Connection connection = cf.createConnection()) {

            // Create a JMS Session
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            // lookup the queue
            Queue queue = session.createQueue("pagingQueueLimited");

            // Create a JMS Message Producer for pageQueueAddress
            MessageProducer pageMessageProducer = session.createProducer(queue);

            // We don't need persistent messages in order to use paging. (This step is optional)
            pageMessageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

            // Create a Binary Bytes Message with 10K arbitrary bytes
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(new byte[10 * 1024]);

            try {
               // Send messages to the queue until the address is full
               for (int i = 0; i < 2000; i++) {
                  pageMessageProducer.send(message);
                  if (i > 0 && i % 100 == 0) {
                     // batch commit on the sends
                     session.commit();
                  }
               }

               throw new RuntimeException("Example was supposed to get a page full exception. Check your example configuration or report a bug");
            } catch (JMSException e) {
               System.out.println("The producer has thrown an expected exception " + e);
            }
            session.commit();
         }
      } finally {
         // And finally, always remember to close your JMS connections after use, in a finally block. Closing a JMS
         // connection will automatically close all of its sessions, consumers, producer and browser objects

         if (initialContext != null) {
            initialContext.close();
         }

      }
   }


   public static void simplePaging() throws Exception {

      InitialContext initialContext = null;
      try {
         // Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // We look-up the JMS queue object from JNDI. pagingQueue is configured to hold a very limited number
         // of bytes in memory
         Queue pageQueue = (Queue) initialContext.lookup("queue/pagingQueue");

         // Lookup for a JMS Queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Create a JMS Connection
         try (Connection connection = cf.createConnection()) {

            // Create a JMS Session
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            // Create a JMS Message Producer for pageQueueAddress
            MessageProducer pageMessageProducer = session.createProducer(pageQueue);

            // We don't need persistent messages in order to use paging. (This step is optional)
            pageMessageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            // Create a Binary Bytes Message with 10K arbitrary bytes
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(new byte[10 * 1024]);

            // Send only 20 messages to the Queue. This will be already enough for pagingQueue. Look at
            // ./paging/config/activemq-queues.xml for the config.
            for (int i = 0; i < 20; i++) {
               pageMessageProducer.send(message);
            }

            // Create a JMS Message Producer
            MessageProducer messageProducer = session.createProducer(queue);

            // We don't need persistent messages in order to use paging. (This step is optional)
            messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            // Send the message for about 1K, which should be over the memory limit imposed by the server
            for (int i = 0; i < 1000; i++) {
               messageProducer.send(message);
            }

            // if you pause this example here, you will see several files under ./build/data/paging
            // Thread.sleep(30000); // if you want to just our of curiosity, you can sleep here and inspect the created
            // files just for

            // Create a JMS Message Consumer
            MessageConsumer messageConsumer = session.createConsumer(queue);

            // Start the JMS Connection. This step will activate the subscribers to receive messages.
            connection.start();

            // Receive the messages. It's important to ACK for messages as ActiveMQ Artemis will not read messages from
            // paging
            // until messages are ACKed

            for (int i = 0; i < 1000; i++) {
               message = (BytesMessage) messageConsumer.receive(3000);

               if (i % 100 == 0) {
                  System.out.println("Received " + i + " messages");
                  message.acknowledge();
               }
            }

            message.acknowledge();

            // Receive the messages from the Queue names pageQueue. Create the proper consumer for that
            messageConsumer.close();
            messageConsumer = session.createConsumer(pageQueue);

            for (int i = 0; i < 20; i++) {
               message = (BytesMessage) messageConsumer.receive(1000);

               System.out.println("Received message " + i + " from pageQueue");

               message.acknowledge();
            }
         }

      } finally {
         // And finally, always remember to close your JMS connections after use, in a finally block. Closing a JMS
         // connection will automatically close all of its sessions, consumers, producer and browser objects

         if (initialContext != null) {
            initialContext.close();
         }

      }
   }
}

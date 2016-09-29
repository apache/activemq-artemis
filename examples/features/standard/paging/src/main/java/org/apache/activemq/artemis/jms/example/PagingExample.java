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
      Connection connection = null;

      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 3. We look-up the JMS queue object from JNDI. pagingQueue is configured to hold a very limited number
         // of bytes in memory
         Queue pageQueue = (Queue) initialContext.lookup("queue/pagingQueue");

         // Step 4. Lookup for a JMS Queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 5. Create a JMS Connection
         connection = cf.createConnection();

         // Step 6. Create a JMS Session
         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         // Step 7. Create a JMS Message Producer for pageQueueAddress
         MessageProducer pageMessageProducer = session.createProducer(pageQueue);

         // Step 8. We don't need persistent messages in order to use paging. (This step is optional)
         pageMessageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         // Step 9. Create a Binary Bytes Message with 10K arbitrary bytes
         BytesMessage message = session.createBytesMessage();
         message.writeBytes(new byte[10 * 1024]);

         // Step 10. Send only 20 messages to the Queue. This will be already enough for pagingQueue. Look at
         // ./paging/config/activemq-queues.xml for the config.
         for (int i = 0; i < 20; i++) {
            pageMessageProducer.send(message);
         }

         // Step 11. Create a JMS Message Producer
         MessageProducer messageProducer = session.createProducer(queue);

         // Step 12. We don't need persistent messages in order to use paging. (This step is optional)
         messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         // Step 13. Send the message for about 1K, which should be over the memory limit imposed by the server
         for (int i = 0; i < 1000; i++) {
            messageProducer.send(message);
         }

         // Step 14. if you pause this example here, you will see several files under ./build/data/paging
         // Thread.sleep(30000); // if you want to just our of curiosity, you can sleep here and inspect the created
         // files just for

         // Step 15. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 16. Start the JMS Connection. This step will activate the subscribers to receive messages.
         connection.start();

         // Step 17. Receive the messages. It's important to ACK for messages as ActiveMQ Artemis will not read messages from
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

         // Step 18. Receive the messages from the Queue names pageQueue. Create the proper consumer for that
         messageConsumer.close();
         messageConsumer = session.createConsumer(pageQueue);

         for (int i = 0; i < 20; i++) {
            message = (BytesMessage) messageConsumer.receive(1000);

            System.out.println("Received message " + i + " from pageQueue");

            message.acknowledge();
         }
      } finally {
         // And finally, always remember to close your JMS connections after use, in a finally block. Closing a JMS
         // connection will automatically close all of its sessions, consumers, producer and browser objects

         if (initialContext != null) {
            initialContext.close();
         }

         if (connection != null) {
            connection.close();
         }
      }
   }
}

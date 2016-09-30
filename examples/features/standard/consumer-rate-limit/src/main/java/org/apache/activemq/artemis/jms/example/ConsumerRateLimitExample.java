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

/**
 * This example demonstrates how a message consumer can be limited to consumer messages at a maximum rate
 * specified in messages per sec.
 */
public class ConsumerRateLimitExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a JMS Message Consumer

         MessageConsumer consumer = session.createConsumer(queue);

         // Step 8. Start the connection

         connection.start();

         // Step 9. Send a bunch of messages

         final int numMessages = 150;

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session.createTextMessage("This is text message: " + i);

            producer.send(message);
         }

         System.out.println("Sent messages");

         System.out.println("Will now try and consume as many as we can in 10 seconds ...");

         // Step 10. Consume as many messages as we can in 10 seconds

         final long duration = 10000;

         int i = 0;

         long start = System.currentTimeMillis();

         while (System.currentTimeMillis() - start <= duration) {
            TextMessage message = (TextMessage) consumer.receive(2000);

            if (message == null) {
               throw new RuntimeException("Message was null");
            }

            i++;
         }

         long end = System.currentTimeMillis();

         double rate = 1000 * (double) i / (end - start);

         System.out.println("We consumed " + i + " messages in " + (end - start) + " milliseconds");

         System.out.println("Actual consume rate was " + rate + " messages per second");
      } finally {
         // Step 9. Be sure to close our resources!
         if (initialContext != null) {
            initialContext.close();
         }

         if (connection != null) {
            connection.close();
         }
      }
   }
}

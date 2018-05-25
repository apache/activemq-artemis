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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * example showing Exclusive Queue, broker is configured with "default-exclusive-queue" to true for the matching
 * address setting. All messages from that queue are routed to the same consumer
 */

public class ExclusiveQueueExample {

   public static void main(final String[] args) throws Exception {

      // Step 1. Create a JMS Connection factory
      ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

      // Step 2. Create a JMS Connection
      try (Connection connection = connectionFactory.createConnection()) {

         //Step 3. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 4. Create a Queue Object
         Queue queue = session.createQueue("my.exclusive.queue");

         //Step 5. Create a JMS producer
         MessageProducer producer = session.createProducer(queue);

         //Step 6. Create 2 consumers on the queue
         MessageConsumer consumer1 = session.createConsumer(queue);
         MessageConsumer consumer2 = session.createConsumer(queue);

         //Step 7. Start the connection
         connection.start();

         //Step 8. send 10 text messages
         Message message = session.createTextMessage("My Message");
         for (int i = 0; i < 10; i++) {
            producer.send(message);
         }

         //Step 9. ensure consumer1 gets all 10 messages
         for (int i = 0; i < 10; i++) {
            Message consumer1Message = consumer1.receive(1000);
            if (consumer1Message == null) {
               throw new RuntimeException("Example FAILED - 'consumer1' should have received all 10 messages");
            }
         }

         System.out.println(ExclusiveQueueExample.class.getName() + " 'consumer1' received 10 messages as expected");

         //Step10. ensure consumer2 gets no messages
         Message consumer2Message = consumer2.receive(1000);
         if (consumer2Message != null) {
            throw new RuntimeException("Example FAILED - 'consumer2' should have not received any Messages");
         }

         System.out.println(ExclusiveQueueExample.class.getName() + " 'consumer2' received 0 messages as expected");

      }
   }
}

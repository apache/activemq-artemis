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
 * This example demonstrates how ActiveMQ Artemis can be configured with a redelivery delay in the event a message
 * is redelivered.
 *
 * Please see the readme for more information
 */
public class DelayedRedeliveryExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      InitialContext initialContext = null;

      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perform a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a transacted JMS Session
         Session session = connection.createSession(true, 0);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("this is a text message");

         // Step 8. Send the Message
         producer.send(message);

         System.out.println("Sent message to " + queue.getQueueName() + ": " + message.getText());

         // Step 9. Commit the session to effectively send the message
         session.commit();

         // Step 10. Create a JMS Message Consumer for the queue
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 11. Start the Connection
         connection.start();

         // Step 12. We receive a message...
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);
         System.out.println("1st delivery from " + queue.getQueueName() + ": " + messageReceived.getText());

         // Step 13. ... but we roll back the session. the message returns to the queue, but only after a
         // 5 second delay
         session.rollback();

         // Step 14. We try to receive the message but it's being delayed
         messageReceived = (TextMessage) messageConsumer.receive(3000);

         if (messageReceived != null) {
            throw new IllegalStateException("Expected to receive message.");
         }

         System.out.println("Redelivery has been delayed so received message is " + messageReceived);

         // Step 15. We try and receive the message again, this time we should get it

         messageReceived = (TextMessage) messageConsumer.receive(3000);

         System.out.println("2nd delivery from " + queue.getQueueName() + ": " + messageReceived.getText());

         // Step 16. We rollback the session again to cause another redelivery, and we time how long this one takes

         long start = System.currentTimeMillis();

         session.rollback();

         messageReceived = (TextMessage) messageConsumer.receive(8000);

         long end = System.currentTimeMillis();

         System.out.println("3nd delivery from " + queue.getQueueName() +
                               ": " +
                               messageReceived.getText() +
                               " after " +
                               (end - start) +
                               " milliseconds.");
      } finally {
         // Step 17. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}

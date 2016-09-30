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
 * An example showing how messages are moved to an expiry queue when they expire.
 */
public class ExpiryExample {

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

         // Step 4.Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Messages sent by this producer will be retained for 1s (1000ms) before expiration
         producer.setTimeToLive(1000);

         // Step 8. Create a Text Message
         TextMessage message = session.createTextMessage("this is a text message");

         // Step 9. Send the Message
         producer.send(message);
         System.out.println("Sent message to " + queue.getQueueName() + ": " + message.getText());

         // Step 10. Sleep for 5s. Once we wake up, the message will have been expired
         System.out.println("Sleep a little bit to let the message expire...");
         Thread.sleep(5000);

         // Step 11. Create a JMS Message Consumer for the queue
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 12. Start the Connection
         connection.start();

         // Step 13. Trying to receive a message. Since there is none on the queue, the call will timeout after 5000ms
         // and messageReceived will be null
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);
         System.out.println("Received message from " + queue.getQueueName() + ": " + messageReceived);

         // Step 14. Perfom a lookup on the expiry queue
         Queue expiryQueue = (Queue) initialContext.lookup("queue/expiryQueue");

         // Step 15. Create a JMS Message Consumer for the expiry queue
         MessageConsumer expiryConsumer = session.createConsumer(expiryQueue);

         // Step 16. Receive the message from the expiry queue
         messageReceived = (TextMessage) expiryConsumer.receive(5000);

         // Step 17. The message sent to the queue was moved to the expiry queue when it expired.
         System.out.println("Received message from " + expiryQueue.getQueueName() + ": " + messageReceived.getText());

         // The message received from the expiry queue has the same content than the expired message but its JMS headers
         // differ
         // (from JMS point of view, it's not the same message).
         // ActiveMQ Artemis defines additional properties to correlate the message received from the expiry queue with the
         // message expired from the queue

         System.out.println();
         // Step 18. the messageReceived's destination is now the expiry queue.
         System.out.println("Destination of the expired message: " + ((Queue) messageReceived.getJMSDestination()).getQueueName());
         // Step 19. and its own expiration (the time to live in the *expiry* queue)
         System.out.println("Expiration time of the expired message (relative to the expiry queue): " + messageReceived.getJMSExpiration());

         System.out.println();
         // Step 20. the *origin* destination is stored in the _AMQ_ORIG_ADDRESS property
         System.out.println("*Origin destination* of the expired message: " + messageReceived.getStringProperty("_AMQ_ORIG_ADDRESS"));
         // Step 21. the actual expiration time is stored in the _AMQ_ACTUAL_EXPIRY property
         System.out.println("*Actual expiration time* of the expired message: " + messageReceived.getLongProperty("_AMQ_ACTUAL_EXPIRY"));
      } finally {
         // Step 22. Be sure to close the resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}

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
 * An example showing how messages are moved to dead letter destination when they are unsuccessfully delivered multiple times
 */
public class DeadLetterExample {

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

         // Step 5. Create a * transacted* JMS Session
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

         // Step 13. ... but we roll back the session. the message returns to the queue ready to be redelivered
         session.rollback();

         // Step 14. We receive a message and roll back the session a second time
         messageReceived = (TextMessage) messageConsumer.receive(5000);
         System.out.println("2nd delivery from " + queue.getQueueName() + ": " + messageReceived.getText());
         session.rollback();

         // Step 15. We receive a message and roll back the session a third time
         messageReceived = (TextMessage) messageConsumer.receive(5000);
         System.out.println("3rd delivery from " + queue.getQueueName() + ": " + messageReceived.getText());
         session.rollback();

         // The message has been delivered unsuccessfully 3 times -> it is moved to the dead letter queue.

         // Step 16. The 4th time, call will timeout after 5000ms and messageReceived will be null
         messageReceived = (TextMessage) messageConsumer.receive(5000);
         System.out.println("4th delivery from " + queue.getQueueName() + ": " + messageReceived);

         // We will now consume the message from the dead letter queue

         // Step 17. Perform a lookup on the dead letter queue
         Queue deadLetterQueue = (Queue) initialContext.lookup("queue/deadLetterQueue");

         // Step 18. Create a JMS Message Consumer for the dead letter queue
         MessageConsumer deadLetterConsumer = session.createConsumer(deadLetterQueue);

         // Step 19. Receive the message from the dead letter queue
         messageReceived = (TextMessage) deadLetterConsumer.receive(5000);

         // Step 20. The message sent to the queue was moved to the dead letter queue after 3 unsuccessful deliveries
         System.out.println("Received message from " + deadLetterQueue.getQueueName() +
                               ": " +
                               messageReceived.getText());

         // The message received from the dead letter queue has the same content than the undelivered message but its
         // JMS headers
         // differ (from JMS point of view, it's not the same message).
         // ActiveMQ Artemis defines additional properties for messages received from the dead letter queue

         System.out.println();
         // Step 21. the messageReceived's destination is now the dead letter queue.
         System.out.println("Destination of the message: " + ((Queue) messageReceived.getJMSDestination()).getQueueName());

         // Step 22. the *origin* destination is stored in the _AMQ_ORIG_ADDRESS property
         System.out.println("*Origin destination* of the message: " + messageReceived.getStringProperty("_AMQ_ORIG_ADDRESS"));

         // Step 23. This time, we commit the session, the delivery from the dead letter queue is successful!
         session.commit();
      } finally {
         // Step 24. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}

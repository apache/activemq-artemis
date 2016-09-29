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
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * A simple JMS example that shows how to use temporary queues.
 */
public class TemporaryQueueExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Look-up the JMS connection factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 3. Create a JMS Connection
         connection = cf.createConnection();

         // Step 4. Start the connection
         connection.start();

         // Step 5. Create a JMS session with AUTO_ACKNOWLEDGE mode
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a Temporary Queue
         TemporaryQueue tempQueue = session.createTemporaryQueue();

         System.out.println("Temporary queue is created: " + tempQueue);

         // Step 7. Create a JMS message producer
         MessageProducer messageProducer = session.createProducer(tempQueue);

         // Step 8. Create a text message
         TextMessage message = session.createTextMessage("This is a text message");

         // Step 9. Send the text message to the queue
         messageProducer.send(message);

         System.out.println("Sent message: " + message.getText());

         // Step 11. Create a message consumer
         MessageConsumer messageConsumer = session.createConsumer(tempQueue);

         // Step 12. Receive the message from the queue
         message = (TextMessage) messageConsumer.receive(5000);

         System.out.println("Received message: " + message.getText());

         // Step 13. Close the consumer and producer
         messageConsumer.close();
         messageProducer.close();

         // Step 14. Delete the temporary queue
         tempQueue.delete();

         // Step 15. Create another temporary queue.
         TemporaryQueue tempQueue2 = session.createTemporaryQueue();

         System.out.println("Another temporary queue is created: " + tempQueue2);

         // Step 16. Close the connection.
         connection.close();

         // Step 17. Create a new connection.
         connection = cf.createConnection();

         // Step 18. Create a new session.
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 19. Try to access the tempQueue2 outside its lifetime
         try {
            messageConsumer = session.createConsumer(tempQueue2);
            throw new Exception("Temporary queue cannot be accessed outside its lifecycle!");
         } catch (JMSException e) {
            System.out.println("Exception got when trying to access a temp queue outside its scope: " + e);
         }
      } finally {
         if (connection != null) {
            // Step 20. Be sure to close our JMS resources!
            connection.close();
         }
         if (initialContext != null) {
            // Step 21. Also close the initialContext!
            initialContext.close();
         }
      }
   }
}

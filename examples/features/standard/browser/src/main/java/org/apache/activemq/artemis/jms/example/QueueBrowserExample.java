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
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import java.util.Enumeration;

/**
 * A simple example which shows how to use a QueueBrowser to look at messages of a queue without removing them from the queue
 */
public class QueueBrowserExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         //          you could alternatively instantiate the connection directly
         //          ConnectionFactory cf = new ActiveMQConnectionFactory(); // this would accept the broker URI as well
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create 2 Text Messages
         TextMessage message_1 = session.createTextMessage("this is the 1st message");
         TextMessage message_2 = session.createTextMessage("this is the 2nd message");

         // Step 8. Send the Message
         producer.send(message_1);
         producer.send(message_2);

         // Step 9. Create the JMS QueueBrowser
         QueueBrowser browser = session.createBrowser(queue);

         // Step 10. Browse the messages on the queue
         // Browsing a queue does not remove the messages from the queue
         Enumeration messageEnum = browser.getEnumeration();
         while (messageEnum.hasMoreElements()) {
            TextMessage message = (TextMessage) messageEnum.nextElement();
            System.out.println("Browsing: " + message.getText());
         }

         // Step 11. Close the browser
         browser.close();

         // Step 12. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 13. Start the Connection
         connection.start();

         // Step 14. Receive the 2 messages
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);
         System.out.println("Received message: " + messageReceived.getText());
         messageReceived = (TextMessage) messageConsumer.receive(5000);
         System.out.println("Received message: " + messageReceived.getText());
      } finally {
         // Step 15. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}

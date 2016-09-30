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

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * A simple JMS example that shows how to use queues requestors.
 */
public class QueueRequestorExample {

   public static void main(final String[] args) throws Exception {
      QueueConnection connection = null;
      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. Look-up the JMS queue connection factory
         QueueConnectionFactory cf = (QueueConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4. Create a TextReverserService which consumes messages from the queue and sends message with reversed
         // text
         TextReverserService reverserService = new TextReverserService(cf, queue);

         // Step 5. Create a JMS QueueConnection
         connection = cf.createQueueConnection();

         // Step 6. Start the connection
         connection.start();

         // Step 7. Create a JMS queue session with AUTO_ACKNOWLEDGE mode
         QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 8. Create a JMS queue requestor to send requests to the queue
         QueueRequestor queueRequestor = new QueueRequestor(session, queue);

         // Step 9. Create a JMS message to send as a request
         TextMessage request = session.createTextMessage("Hello, World!");

         // Step 10. Use the requestor to send the request and wait to receive a reply
         TextMessage reply = (TextMessage) queueRequestor.request(request);

         // Step 11. The reply's text contains the reversed request's text
         System.out.println("Send request: " + request.getText());
         System.out.println("Received reply:" + reply.getText());

         // Step.12 close the queue requestor
         queueRequestor.close();

         // Step 13. close the text reverser service
         reverserService.close();
      } finally {
         if (connection != null) {
            try {
               // Step 14. Be sure to close the JMS resources!
               connection.close();
            } catch (JMSException e) {
               e.printStackTrace();
            }
         }

         if (initialContext != null) {
            // Also the InitialContext
            initialContext.close();
         }
      }
   }
}

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
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple JMS example that shows how to use Request/Replay style messaging.
 *
 * Of course, in a real world example you would re-use the session, producer, consumer and temporary queue
 * and not create a new one for each message!
 *
 * Or better still use the correlation id, and just store the requests in a map, then you don't need a temporary queue at all
 */
public class RequestReplyExample {

   public static void main(final String[] args) throws Exception {
      final Map<String, TextMessage> requestMap = new HashMap<>();
      Connection connection = null;
      InitialContext initialContext = null;

      try {
         // Step 1. Start the request server
         SimpleRequestServer server = new SimpleRequestServer();
         server.start();

         // Step 2. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 3. Lookup the queue for sending the request message
         Queue requestQueue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 4. Lookup for the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 5. Create a JMS Connection
         connection = cf.createConnection();

         // Step 6. Start the connection.
         connection.start();

         // Step 7. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 8. Create a JMS Message Producer to send request message
         MessageProducer producer = session.createProducer(requestQueue);

         // Step 9. Create a temporary queue used to send reply message
         TemporaryQueue replyQueue = session.createTemporaryQueue();

         // Step 10. Create consumer to receive reply message
         MessageConsumer replyConsumer = session.createConsumer(replyQueue);

         // Step 11. Create a request Text Message
         TextMessage requestMsg = session.createTextMessage("A request message");

         // Step 12. Set the ReplyTo header so that the request receiver knows where to send the reply.
         requestMsg.setJMSReplyTo(replyQueue);

         // Step 13. Sent the request message
         producer.send(requestMsg);

         System.out.println("Request message sent.");

         // Step 14. Put the request message to the map. Later we can use it to
         // check out which request message a reply message is for. Here we use the MessageID as the
         // correlation id (JMSCorrelationID). You don't have to use it though. You can use some arbitrary string for
         // example.
         requestMap.put(requestMsg.getJMSMessageID(), requestMsg);

         // Step 15. Receive the reply message.
         TextMessage replyMessageReceived = (TextMessage) replyConsumer.receive();

         System.out.println("Received reply: " + replyMessageReceived.getText());
         System.out.println("CorrelatedId: " + replyMessageReceived.getJMSCorrelationID());

         // Step 16. Check out which request message is this reply message sent for.
         // Here we just have one request message for illustrative purpose. In real world there may be many requests and
         // many replies.
         TextMessage matchedMessage = requestMap.get(replyMessageReceived.getJMSCorrelationID());

         System.out.println("We found matched request: " + matchedMessage.getText());

         // Step 17. close the consumer.
         replyConsumer.close();

         // Step 18. Delete the temporary queue
         replyQueue.delete();

         // Step 19. Shutdown the request server
         server.shutdown();
      } finally {
         // Step 20. Be sure to close our JMS resources!
         if (connection != null) {
            connection.close();
         }
         // Step 21. Also close the initialContext!
         if (initialContext != null) {
            initialContext.close();
         }
      }
   }
}

class SimpleRequestServer implements MessageListener {

   private Connection connection;

   private Session session;

   MessageProducer replyProducer;

   MessageConsumer requestConsumer;

   public void start() throws Exception {
      // Get an initial context to perform the JNDI lookup.
      InitialContext initialContext = new InitialContext();

      // Lookup the queue to receive the request message
      Queue requestQueue = (Queue) initialContext.lookup("queue/exampleQueue");

      // Lookup for the Connection Factory
      ConnectionFactory cfact = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

      // Create a connection
      connection = cfact.createConnection();

      // Start the connection;
      connection.start();

      // Create a session
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // Create a producer to send the reply message
      replyProducer = session.createProducer(null);

      // Create the request comsumer
      requestConsumer = session.createConsumer(requestQueue);

      // register the listener
      requestConsumer.setMessageListener(this);
   }

   @Override
   public void onMessage(final Message request) {
      try {
         System.out.println("Received request message: " + ((TextMessage) request).getText());

         // Extract the ReplyTo destination
         Destination replyDestination = request.getJMSReplyTo();

         System.out.println("Reply to queue: " + replyDestination);

         // Create the reply message
         TextMessage replyMessage = session.createTextMessage("A reply message");

         // Set the CorrelationID, using message id.
         replyMessage.setJMSCorrelationID(request.getJMSMessageID());

         // Send out the reply message
         replyProducer.send(replyDestination, replyMessage);

         System.out.println("Reply sent");
      } catch (JMSException e) {
         e.printStackTrace();
      }
   }

   public void shutdown() throws JMSException {
      connection.close();
   }
}

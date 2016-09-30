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
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * This examples demonstrates a connection created to a server. Failure of the network connection is then simulated
 *
 * The network is brought back up and the client reconnects and resumes transparently.
 */
public class ReattachExample {

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

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         System.out.println("Sent message: " + message.getText());

         // Step 8. Send the Message
         producer.send(message);

         // Step 9. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 10. Start the Connection
         connection.start();

         // Step 11. To simulate a temporary problem on the network, we stop the remoting acceptor on the
         // server which will close all connections
         stopAcceptor();

         System.out.println("Acceptor now stopped, will wait for 10 seconds. This simulates the network connection failing for a while");

         // Step 12. Wait a while then restart the acceptor
         Thread.sleep(10000);

         System.out.println("Re-starting acceptor");

         startAcceptor();

         System.out.println("Restarted acceptor. The client will now reconnect.");

         // Step 13. We receive the message
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

         System.out.println("Received message: " + messageReceived.getText());
      } finally {
         // Step 14. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }

         if (connection != null) {
            connection.close();
         }
      }
   }

   private static void stopAcceptor() throws Exception {
      stopStartAcceptor(true);
   }

   private static void startAcceptor() throws Exception {
      stopStartAcceptor(false);
   }

   // To do this we send a management message to close the acceptor, we do this on a different
   // connection factory which uses a different remoting connection so we can still send messages
   // when the main connection has been stopped
   private static void stopStartAcceptor(final boolean stop) throws Exception {
      ConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61617");

      Connection connection = null;
      try {
         connection = cf.createConnection();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");

         MessageProducer producer = session.createProducer(managementQueue);

         connection.start();

         Message m = session.createMessage();

         String oper = stop ? "stop" : "start";

         JMSManagementHelper.putOperationInvocation(m, "core.acceptor.netty-acceptor", oper);

         producer.send(m);
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

}

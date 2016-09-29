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
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;

/**
 * This example demonstrates how sessions created from a single connection can be load
 * balanced across the different nodes of the cluster.
 *
 * In this example there are three nodes and we use a round-robin client side load-balancing
 * policy.
 */
public class ClientSideLoadBalancingExample {

   public static void main(final String[] args) throws Exception {
      InitialContext initialContext = null;

      Connection connectionA = null;
      Connection connectionB = null;
      Connection connectionC = null;

      try {
         // Step 1. Get an initial context for looking up JNDI from server 0
         initialContext = new InitialContext();

         // Step 2. Look-up the JMS Queue object from JNDI
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. Look-up a JMS Connection Factory object from JNDI on server 0
         ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4. We create 3 JMS connections from the same connection factory. Since we are using round-robin
         // load-balancing this should result in each sessions being connected to a different node of the cluster
         try (Connection conn = connectionFactory.createConnection()) {
            // Wait a little while to make sure broadcasts from all nodes have reached the client
            Thread.sleep(5000);
            connectionA = connectionFactory.createConnection();
            connectionB = connectionFactory.createConnection();
            connectionC = connectionFactory.createConnection();
         }

         // Step 5. We create JMS Sessions
         Session sessionA = connectionA.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sessionB = connectionB.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sessionC = connectionC.createSession(false, Session.AUTO_ACKNOWLEDGE);

         System.out.println("Session A - " + ((ClientSessionInternal) ((org.apache.activemq.artemis.jms.client.ActiveMQSession) sessionA).getCoreSession()).getConnection().getRemoteAddress());
         System.out.println("Session B - " + ((ClientSessionInternal) ((org.apache.activemq.artemis.jms.client.ActiveMQSession) sessionB).getCoreSession()).getConnection().getRemoteAddress());
         System.out.println("Session C - " + ((ClientSessionInternal) ((org.apache.activemq.artemis.jms.client.ActiveMQSession) sessionC).getCoreSession()).getConnection().getRemoteAddress());

         // Step 6. We create JMS MessageProducer objects on the sessions
         MessageProducer producerA = sessionA.createProducer(queue);
         MessageProducer producerB = sessionB.createProducer(queue);
         MessageProducer producerC = sessionC.createProducer(queue);

         // Step 7. We send some messages on each producer
         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++) {
            TextMessage messageA = sessionA.createTextMessage("A:This is text message " + i);
            producerA.send(messageA);
            System.out.println("Sent message: " + messageA.getText());

            TextMessage messageB = sessionB.createTextMessage("B:This is text message " + i);
            producerB.send(messageB);
            System.out.println("Sent message: " + messageB.getText());

            TextMessage messageC = sessionC.createTextMessage("C:This is text message " + i);
            producerC.send(messageC);
            System.out.println("Sent message: " + messageC.getText());
         }

         // Step 8. We start the connection to consume messages
         connectionA.start();
         connectionB.start();
         connectionC.start();

         // Step 9. We consume messages from the 3 session, one at a time.
         // We try to consume one more message than expected from each session. If
         // the session were not properly load-balanced, we would be missing a
         // message from one of the sessions at the end.
         consume(sessionA, queue, numMessages, "A");
         consume(sessionB, queue, numMessages, "B");
         consume(sessionC, queue, numMessages, "C");
      } finally {
         // Step 10. Be sure to close our resources!

         if (connectionA != null) {
            connectionA.close();
         }
         if (connectionB != null) {
            connectionB.close();
         }
         if (connectionC != null) {
            connectionC.close();
         }

         if (initialContext != null) {
            initialContext.close();
         }
      }
   }

   private static void consume(Session session, Queue queue, int numMessages, String node) throws JMSException {
      MessageConsumer consumer = session.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         TextMessage message = (TextMessage) consumer.receive(2000);
         System.out.println("Got message: " + message.getText() + " from node " + node);
      }

      System.out.println("receive other message from node " + node + ": " + consumer.receive(2000));
   }
}

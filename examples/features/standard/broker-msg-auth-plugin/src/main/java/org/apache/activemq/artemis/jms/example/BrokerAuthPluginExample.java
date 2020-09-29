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

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.qpid.jms.JmsConnectionFactory;

/**
 * A simple example which shows how to use the BrokerMessageAuthorizationPlugin to filter messages given to user based on there role
 */
public class BrokerAuthPluginExample {

   public static void main(final String[] args) throws Exception {

      // This example will send and receive an AMQP message
      sendConsumeAMQP();

      // And it will also send and receive a Core message
      sendConsumeCore();
   }

   private static void sendConsumeAMQP() throws JMSException {
      Connection adminConn = null;
      Connection guestConn = null;
      ConnectionFactory connectionFactory = new JmsConnectionFactory("amqp://localhost:5672");

      try {

         // Create an amqp qpid 1.0 connection
         adminConn = connectionFactory.createConnection("admin", "admin");
         guestConn = connectionFactory.createConnection();

         // Create a session
         Session adminSession = adminConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session guestSession = guestConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Create a sender
         //         Topic destination = adminSession.createTopic("exampleTopic");
         Queue destination = adminSession.createQueue("exampleQueue");
         MessageProducer sender = adminSession.createProducer(destination);

         TextMessage textMessage = adminSession.createTextMessage("Hello world ");
         textMessage.setStringProperty("requiredRole", "admin");

         // create a moving receiver, this means the message will be removed from the queue
         MessageConsumer guestConsumer = guestSession.createConsumer(destination);
         MessageConsumer adminConsumer = adminSession.createConsumer(destination);

         // send a simple message
         sender.send(textMessage);

         guestConn.start();
         adminConn.start();

         // receive the simple message
         TextMessage guestMessage = (TextMessage) guestConsumer.receive(5000);
         TextMessage adminMessage = (TextMessage) adminConsumer.receive(5000);

         if (adminMessage == null) {
            throw new RuntimeException(("admin did not receive message"));
         }
         if (guestMessage != null) {
            throw new RuntimeException(("guest received a message that should have been filtered."));
         }

      } finally {
         if (adminConn != null) {
            // close the connection
            adminConn.close();
         }
         if (guestConn != null) {
            // close the connection
            guestConn.close();
         }
      }
   }

   private static void sendConsumeCore() throws JMSException {
      Connection adminConn = null;
      Connection guestConn = null;

      try {
         // Perform a lookup on the Connection Factory
         ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

         //         Topic destination = new ActiveMQTopic("exampleTopic");
         Queue destination = new ActiveMQQueue("exampleQueue");

         // Create a JMS Connection
         adminConn = connectionFactory.createConnection("admin", "admin");
         guestConn = connectionFactory.createConnection();

         // Create a JMS Session
         Session adminSession = adminConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session guestSession = guestConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Create a JMS Message Producer
         MessageProducer sender = adminSession.createProducer(destination);

         // Create a Text Message
         TextMessage textMessage = adminSession.createTextMessage("Hello world ");
         textMessage.setStringProperty("requiredRole", "admin");

         // create a moving receiver, this means the message will be removed from the queue
         MessageConsumer guestConsumer = guestSession.createConsumer(destination);
         MessageConsumer adminConsumer = adminSession.createConsumer(destination);

         // send a simple message
         sender.send(textMessage);

         guestConn.start();
         adminConn.start();

         // receive the simple message
         TextMessage guestMessage = (TextMessage) guestConsumer.receive(5000);
         TextMessage adminMessage = (TextMessage) adminConsumer.receive(5000);

         if (adminMessage == null) {
            throw new RuntimeException(("admin did not receive message"));
         }
         if (guestMessage != null) {
            throw new RuntimeException(("guest received a message that should have been filtered."));
         }

      } finally {
         if (adminConn != null) {
            // close the connection
            adminConn.close();
         }
         if (guestConn != null) {
            // close the connection
            guestConn.close();
         }
      }

   }
}

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
 * A simple example which shows how to use a QueueBrowser to look at messages of a queue without removing them from the queue
 */
public class OpenTracingPluginExample {

   public static void main(final String[] args) throws Exception {

      // This example will send and receive an AMQP message
      sendConsumeAMQP();

      // And it will also send and receive a Core message
      sendConsumeCore();
   }

   private static void sendConsumeAMQP() throws JMSException {
      Connection connection = null;
      ConnectionFactory connectionFactory = new JmsConnectionFactory("amqp://localhost:5672");

      try {

         // Create an amqp qpid 1.0 connection
         connection = connectionFactory.createConnection();

         // Create a session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Create a sender
         Queue queue = session.createQueue("exampleQueue");
         MessageProducer sender = session.createProducer(queue);

         // send a few simple message
         sender.send(session.createTextMessage("Hello world "));

         connection.start();

         // create a moving receiver, this means the message will be removed from the queue
         MessageConsumer consumer = session.createConsumer(queue);

         // receive the simple message
         consumer.receive(5000);

      } finally {
         if (connection != null) {
            // close the connection
            connection.close();
         }
      }
   }


   private static void sendConsumeCore() throws JMSException {
      Connection connection = null;
      try {
         // Perform a lookup on the Connection Factory
         ConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");

         Queue queue = new ActiveMQQueue("exampleQueue");

         // Create a JMS Connection
         connection = cf.createConnection();

         // Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         // Send the Message
         producer.send(message);

         // Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Start the Connection
         connection.start();

         // Receive the message
         messageConsumer.receive(5000);

      } finally {
         if (connection != null) {
            connection.close();
         }
      }

   }
}

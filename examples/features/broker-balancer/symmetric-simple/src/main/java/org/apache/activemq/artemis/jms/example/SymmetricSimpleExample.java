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

import org.apache.qpid.jms.JmsConnectionFactory;


/**
 * This example demonstrates how incoming client connections are partitioned across two brokers
 * it uses AMQP to take advantage of the simple round-robin retry logic of the failover url scheme
 */
public class SymmetricSimpleExample {

   public static void main(final String[] args) throws Exception {

      /**
       * Step 1. Create a connection for producer0 and producer1, and send a few messages with different key values
       */
      ConnectionFactory connectionFactory = new JmsConnectionFactory("failover:(amqp://localhost:61616,amqp://localhost:61617)");


      try (Connection connectionProducer0 = connectionFactory.createConnection();
           Connection connectionProducer1 = connectionFactory.createConnection()) {

         // using first 3 characters of clientID as key for data gravity
         connectionProducer0.setClientID("BAR_PRODUCER");
         connectionProducer1.setClientID("FOO_PRODUCER");


         for (Connection connectionProducer : new Connection[] {connectionProducer0, connectionProducer1}) {
            Session session = connectionProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Queue queue = session.createQueue("exampleQueue" + connectionProducer.getClientID().substring(0, 3));
            MessageProducer sender = session.createProducer(queue);
            for (int i = 0; i < 10; i++) {
               TextMessage message = session.createTextMessage("Hello world n" + i + " - " + connectionProducer.getClientID().substring(0, 3));
               System.out.println("Sending message " + message.getText() + "/" + connectionProducer.getClientID());
               sender.send(message);
            }
         }
      }

      /**
       * Step 2. create a connection for consumer0 and consumer1, and receive a few messages.
       * the consumers will find data that matches their key
       */

      try (Connection connectionConsumer0 = connectionFactory.createConnection();
           Connection connectionConsumer1 = connectionFactory.createConnection()) {

         // using first 3 characters of clientID as key for data gravity
         connectionConsumer0.setClientID("FOO_CONSUMER");
         connectionConsumer1.setClientID("BAR_CONSUMER");

         for (Connection connectionConsumer : new Connection[]{connectionConsumer0, connectionConsumer1}) {
            connectionConsumer.start();
            Session session = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("exampleQueue" + connectionConsumer.getClientID().substring(0, 3));
            MessageConsumer consumer = session.createConsumer(queue);
            for (int i = 0; i < 10; i++) {
               TextMessage message = (TextMessage) consumer.receive(5000);
               System.out.println("Received message " + message.getText() + "/" + connectionConsumer.getClientID());
            }
         }
      }
   }
}

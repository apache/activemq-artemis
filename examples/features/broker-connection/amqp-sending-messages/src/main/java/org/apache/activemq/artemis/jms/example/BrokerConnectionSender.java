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
 * This example is demonstrating how messages are transferred from one broker towards another broker
 * through the sender operation on a AMQP Broker Connection.
 */
public class BrokerConnectionSender {

   public static void main(final String[] args) throws Exception {
      Connection connectionOnServer0 = null;
      ConnectionFactory connectionFactoryServer0 = new JmsConnectionFactory("amqp://localhost:5660");

      // Step 1. Create a connection on server0, and send a few messages
      try {

         connectionOnServer0 = connectionFactoryServer0.createConnection();

         Session session = connectionOnServer0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue queue = session.createQueue("exampleQueue");
         MessageProducer sender = session.createProducer(queue);
         for (int i = 0; i < 100; i++) {
            sender.send(session.createTextMessage("Hello world n" + i));
         }
      } finally {
         if (connectionFactoryServer0 != null) {
            connectionOnServer0.close();
         }
      }

      // Step 2. create a connection on server1, and receive a few messages.
      //          the sender on the broker conneciton will take care of the transfer.
      Connection connectionOnServer1 = null;
      ConnectionFactory connectionFactoryServer1 = new JmsConnectionFactory("amqp://localhost:5771");

      try {
         connectionOnServer1 = connectionFactoryServer1.createConnection();
         connectionOnServer1.start();
         Session session = connectionOnServer1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue("exampleQueue");
         MessageConsumer consumer = session.createConsumer(queue);
         for (int i = 0; i < 100; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            System.out.println("Received message " + message.getText());
         }
      } finally {
         if (connectionOnServer1 != null) {
            connectionOnServer1.close();
         }
      }
   }
}

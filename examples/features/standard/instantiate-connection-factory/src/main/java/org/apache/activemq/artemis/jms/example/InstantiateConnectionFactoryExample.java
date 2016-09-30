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

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * This example demonstrates how a JMS client can directly instantiate it's JMS Objects like
 * Queue, ConnectionFactory, etc. without having to use JNDI at all.
 *
 * For more information please see the readme.html file.
 */
public class InstantiateConnectionFactoryExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      try {
         // Step 1. Directly instantiate the JMS Queue object.
         Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

         // Starting with Artemis 1.0.1 you can just use the URI to instantiate the object directly
         ConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");

         // Step 4.Create a JMS Connection
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

         // Step 11. Receive the message
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

         System.out.println("Received message: " + messageReceived.getText());
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }
}

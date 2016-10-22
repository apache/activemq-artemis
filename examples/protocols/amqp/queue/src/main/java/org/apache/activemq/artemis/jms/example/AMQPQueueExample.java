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

public class AMQPQueueExample {

   public static void main(String[] args) throws Exception {
      Connection connection = null;
      ConnectionFactory connectionFactory = new JmsConnectionFactory("amqp://localhost:5672");

      try {

         // Step 1. Create an amqp qpid 1.0 connection
         connection = connectionFactory.createConnection();

         // Step 2. Create a session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 3. Create a sender
         Queue queue = session.createQueue("exampleQueue");
         MessageProducer sender = session.createProducer(queue);

         // Step 4. send a few simple message
         sender.send(session.createTextMessage("Hello world "));

         connection.start();

         // Step 5. create a moving receiver, this means the message will be removed from the queue
         MessageConsumer consumer = session.createConsumer(queue);

         // Step 7. receive the simple message
         TextMessage m = (TextMessage) consumer.receive(5000);
         System.out.println("message = " + m.getText());

      } finally {
         if (connection != null) {
            // Step 9. close the connection
            connection.close();
         }
      }
   }
}

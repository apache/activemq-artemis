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
import javax.naming.InitialContext;

import org.apache.qpid.jms.JmsConnectionFactory;

/**
 * This example demonstrates the use of ActiveMQ Artemis "pre-acknowledge" functionality where
 * messages are acknowledged before they are delivered to the consumer.
 * <p>
 * Please see the readme for more details.
 */
public class ProtonCPPExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;

      InitialContext initialContext = null;
      try {
         // Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // if you wanted to use Core JMS, use this line instead.
         // ConnectionFactory cf = new ActiveMQConnectionFactory();
         ConnectionFactory cf = new JmsConnectionFactory("amqp://localhost:61616");

         // Create a the JMS objects
         connection = cf.createConnection();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Perform the look-ups
         Queue queue = session.createQueue("exampleQueue");

         MessageConsumer messageConsumer = session.createConsumer(queue);

         MessageProducer producerAnswer = session.createProducer(queue);

         // Start the connection
         connection.start();

         System.out.println("On a shell script, execute the following:");

         System.out.println("./compile.sh");

         System.out.println("./hello");

         for (int i = 0; i < 10; i++) {
            try {
               // Step 5. Finally, receive the message
               TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

               if (messageReceived == null) {
                  System.out.println("No messages");
                  // We are not going to issue this as an error because
                  // we also use this example as part of our tests on artemis
                  // this is not considered an error, just that no messages arrived (i.e. hello wasn't called)
               } else {
                  System.out.println("message received: " + messageReceived.getText());

                  // Sending message back to client
                  producerAnswer.send(session.createTextMessage("HELLO from Apache ActiveMQ Artemis " + i + "!!"));
               }
            } catch (Throwable e) {
               e.printStackTrace();
            }
         }
      } finally {
         // Step 9. Be sure to close our resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}

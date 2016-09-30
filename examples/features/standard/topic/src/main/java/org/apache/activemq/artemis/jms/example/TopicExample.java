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
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

/**
 * A simple JMS Topic example that creates a producer and consumer on a queue and sends and receives a message.
 */
public class TopicExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      InitialContext initialContext = null;
      try {
         // /Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. perform a lookup on the topic
         Topic topic = (Topic) initialContext.lookup("topic/exampleTopic");

         // Step 3. perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a Message Producer
         MessageProducer producer = session.createProducer(topic);

         // Step 7. Create a JMS Message Consumer
         MessageConsumer messageConsumer1 = session.createConsumer(topic);

         // Step 8. Create a JMS Message Consumer
         MessageConsumer messageConsumer2 = session.createConsumer(topic);

         // Step 9. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         System.out.println("Sent message: " + message.getText());

         // Step 10. Send the Message
         producer.send(message);

         // Step 11. Start the Connection
         connection.start();

         // Step 12. Receive the message
         TextMessage messageReceived = (TextMessage) messageConsumer1.receive();

         System.out.println("Consumer 1 Received message: " + messageReceived.getText());

         // Step 13. Receive the message
         messageReceived = (TextMessage) messageConsumer2.receive();

         System.out.println("Consumer 2 Received message: " + messageReceived.getText());
      } finally {
         // Step 14. Be sure to close our JMS resources!
         if (connection != null) {
            connection.close();
         }

         // Also the initialContext
         if (initialContext != null) {
            initialContext.close();
         }
      }
   }
}

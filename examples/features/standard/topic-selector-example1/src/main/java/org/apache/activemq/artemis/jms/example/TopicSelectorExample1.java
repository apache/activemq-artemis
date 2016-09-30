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
public class TopicSelectorExample1 {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;

      InitialContext initialContext = null;
      try {

         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Look-up the JMS topic
         Topic topic = (Topic) initialContext.lookup("topic/exampleTopic");

         // Step 3. Look-up the JMS connection factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4. Create a JMS connection
         connection = cf.createConnection();

         // Step 5. Create a JMS session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS message producer
         MessageProducer producer = session.createProducer(topic);

         // Step 7. Create one subscription with a specific Filter for someID=1
         MessageConsumer messageConsumer1 = session.createConsumer(topic, "someID=1", false);

         // Step 8. Create another subscription with a specific Filter for someID=2
         MessageConsumer messageConsumer2 = session.createConsumer(topic, "someID=2", false);

         // Step 9. Create another subscription with no filters, which will receive every message sent to the topic
         MessageConsumer messageConsumer3 = session.createConsumer(topic);

         // Step 10. Send 20 messages, 10 with someID=1, 10 with someID=2

         for (int i = 1; i < 10; i++) {
            for (int someID = 1; someID <= 2; someID++) {
               // Step 10.1 Create a text message
               TextMessage message1 = session.createTextMessage("This is a text message " + i +
                                                                   " sent for someID=" +
                                                                   someID);

               // Step 10.1 Set a property
               message1.setIntProperty("someID", someID);

               // Step 10.2 Send the message
               producer.send(message1);

               System.out.println("Sent message: " + message1.getText());
            }
         }

         // Step 11. Start the JMS Connection. This step will activate the subscribers to receive messages.
         connection.start();

         // Step 12. Consume the messages from MessageConsumer1, filtering out someID=2

         System.out.println("*************************************************************");
         System.out.println("MessageConsumer1 will only receive messages where someID=1:");
         for (;;) {
            TextMessage messageReceivedA = (TextMessage) messageConsumer1.receive(1000);
            if (messageReceivedA == null) {
               break;
            }

            System.out.println("messageConsumer1 received " + messageReceivedA.getText() +
                                  " someID = " +
                                  messageReceivedA.getIntProperty("someID"));
         }

         // Step 13. Consume the messages from MessageConsumer2, filtering out someID=2
         System.out.println("*************************************************************");
         System.out.println("MessageConsumer2 will only receive messages where someID=2:");
         for (;;) {
            TextMessage messageReceivedB = (TextMessage) messageConsumer2.receive(1000);
            if (messageReceivedB == null) {
               break;
            }

            System.out.println("messageConsumer2 received " + messageReceivedB.getText() +
                                  " someID = " +
                                  messageReceivedB.getIntProperty("someID"));
         }

         // Step 14. Consume the messages from MessageConsumer3, receiving the complete set of messages
         System.out.println("*************************************************************");
         System.out.println("MessageConsumer3 will receive every message:");
         for (;;) {
            TextMessage messageReceivedC = (TextMessage) messageConsumer3.receive(1000);
            if (messageReceivedC == null) {
               break;
            }
            System.out.println("messageConsumer3 received " + messageReceivedC.getText() +
                                  " someID = " +
                                  messageReceivedC.getIntProperty("someID"));
         }

         // Step 15. Close the subscribers
         messageConsumer1.close();
         messageConsumer2.close();
         messageConsumer3.close();
      } finally {
         // Step 15. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}

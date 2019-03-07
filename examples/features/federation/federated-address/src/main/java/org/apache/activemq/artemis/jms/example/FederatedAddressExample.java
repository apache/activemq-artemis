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
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * A simple example that demonstrates multicast address replication between remote servers,
 * using Address Federation feature.
 */
public class FederatedAddressExample {

   public static void main(final String[] args) throws Exception {
      Connection connectionEUWest = null;

      Connection connectionEUEast = null;

      Connection connectionUSCentral = null;


      try {
         // Step 1. Instantiate the Topic (multicast)
         Topic topic = ActiveMQJMSClient.createTopic("exampleTopic");

         // Step 2. Instantiate connection towards server EU West
         ConnectionFactory cfEUWest = new ActiveMQConnectionFactory("tcp://localhost:61616");

         // Step 3. Instantiate connection towards server EU East
         ConnectionFactory cfEUEast = new ActiveMQConnectionFactory("tcp://localhost:61617");

         // Step 4. Instantiate connection towards server US Central
         ConnectionFactory cfUSCentral = new ActiveMQConnectionFactory("tcp://localhost:61618");


         // Step 5. We create a JMS Connection connectionEUWest which is a connection to server EU West
         connectionEUWest = cfEUWest.createConnection();

         // Step 6. We create a JMS Connection connectionEUEast which is a connection to server EU East
         connectionEUEast = cfEUEast.createConnection();

         // Step 7. We create a JMS Connection connectionUSCentral which is a connection to server US Central
         connectionUSCentral = cfUSCentral.createConnection();

         // Step 8. We create a JMS Session on server EU West
         Session sessionEUWest = connectionEUWest.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 9. We create a JMS Session on server EU East
         Session sessionEUEast = connectionEUEast.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 10. We create a JMS Session on server US Central
         Session sessionUSCentral = connectionUSCentral.createSession(false, Session.AUTO_ACKNOWLEDGE);


         // Step 11. We start the connections to ensure delivery occurs on them
         connectionEUWest.start();

         connectionEUEast.start();

         connectionUSCentral.start();

         // Step 12. We create a JMS MessageProducer object on each server
         MessageProducer producerEUWest = sessionEUWest.createProducer(topic);

         MessageProducer producerEUEast = sessionEUEast.createProducer(topic);

         MessageProducer producerUSCentral = sessionUSCentral.createProducer(topic);

         // Step 13. We create JMS MessageConsumer objects on each server
         MessageConsumer consumerEUWest = sessionEUWest.createSharedDurableConsumer(topic, "exampleSubscription");

         MessageConsumer consumerEUEast = sessionEUEast.createSharedDurableConsumer(topic, "exampleSubscription");

         MessageConsumer consumerUSCentral = sessionUSCentral.createSharedDurableConsumer(topic, "exampleSubscription");


         // Step 14. Let a little time for everything to start and form.

         Thread.sleep(5000);


         // Step 13. We send some messages to server EU West
         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = sessionEUWest.createTextMessage("This is text sent from EU West, message " + i);
            message.setStringProperty("text", "This is text sent from EU West, message " + i);
            producerEUWest.send(message);

            System.out.println("EU West   :: Sent message: " + message.getText());
         }

         // Step 14. We now consume those messages on *all* servers .
         // We note that every consumer, receives a message even so on seperate servers

         for (int i = 0; i < numMessages; i++) {
            TextMessage messageEUWest = (TextMessage) consumerEUWest.receive(5000);

            System.out.println("EU West   :: Got message: " + messageEUWest.getText());

            TextMessage messageUSCentral = (TextMessage) consumerUSCentral.receive(5000);

            System.out.println("US Central:: Got message: " + messageUSCentral.getText());

            TextMessage messageEUEast = (TextMessage) consumerEUEast.receive(5000);

            System.out.println("EU East   :: Got message: " + messageEUEast.getText());

         }





         // Step 15. Repeat same test this time sending on US Central


         for (int i = 0; i < numMessages; i++) {
            TextMessage message = sessionUSCentral.createTextMessage("This is text sent from US Central, message " + i);

            producerUSCentral.send(message);

            System.out.println("US Central:: Sent message: " + message.getText());
         }

         for (int i = 0; i < numMessages; i++) {
            TextMessage messageEUWest = (TextMessage) consumerEUWest.receive(5000);

            System.out.println("EU West   :: Got message: " + messageEUWest.getText());

            TextMessage messageUSCentral = (TextMessage) consumerUSCentral.receive(5000);

            System.out.println("US Central:: Got message: " + messageUSCentral.getText());

            TextMessage messageEUEast = (TextMessage) consumerEUEast.receive(5000);

            System.out.println("EU East   :: Got message: " + messageEUEast.getText());

         }





         // Step 15. Repeat same test one last time, this time sending on EU East

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = sessionEUEast.createTextMessage("This is text sent from EU East, message " + i);

            producerEUEast.send(message);

            System.out.println("EU East   :: Sent message: " + message.getText());
         }


         for (int i = 0; i < numMessages; i++) {
            TextMessage messageEUWest = (TextMessage) consumerEUWest.receive(5000);

            System.out.println("EU West   :: Got message: " + messageEUWest.getText());

            TextMessage messageUSCentral = (TextMessage) consumerUSCentral.receive(5000);

            System.out.println("US Central:: Got message: " + messageUSCentral.getText());

            TextMessage messageEUEast = (TextMessage) consumerEUEast.receive(5000);

            System.out.println("EU East   :: Got message: " + messageEUEast.getText());

         }

      } finally {
         // Step 16. Be sure to close our resources!

         if (connectionEUWest != null) {
            connectionEUWest.stop();
            connectionEUWest.close();
         }

         if (connectionEUEast != null) {
            connectionEUEast.stop();
            connectionEUEast.close();
         }

         if (connectionUSCentral != null) {
            connectionUSCentral.stop();
            connectionUSCentral.close();
         }
      }
   }
}

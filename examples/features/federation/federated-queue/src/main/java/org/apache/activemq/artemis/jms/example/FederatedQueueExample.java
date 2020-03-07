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
 * A simple example that demonstrates dynamic queue messaging routing between remote servers,
 * as consumers come and go, routing based on priorities.
 * using Queue Federation feature.
 */
public class FederatedQueueExample {

   public static void main(final String[] args) throws Exception {
      Connection connectionEUWest = null;

      Connection connectionEUEast = null;

      Connection connectionUSCentral = null;


      try {
         // Step 1. Instantiate the Queue
         Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

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
         MessageProducer producerEUWest = sessionEUWest.createProducer(queue);

         MessageProducer producerEUEast = sessionEUEast.createProducer(queue);

         MessageProducer producerUSCentral = sessionUSCentral.createProducer(queue);

         // Step 13. We create JMS MessageConsumer objects on each server
         MessageConsumer consumerEUWest = sessionEUWest.createConsumer(queue);

         MessageConsumer consumerEUEast = sessionEUEast.createConsumer(queue);

         MessageConsumer consumerUSCentral = sessionUSCentral.createConsumer(queue);


         // Step 14. Let a little time for everything to start and form.

         Thread.sleep(5000);


         // Step 15. We send some messages to server EU West

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = sessionEUWest.createTextMessage("This is text sent from EU West, message " + i);

            producerEUWest.send(message);

            System.out.println("EU West   :: Sent message: " + message.getText());
         }

         // Step 16. we now consume those messages on EU West demonstrating that messages will deliver to local consumer by priority.

         for (int i = 0; i < numMessages; i++) {
            TextMessage messageEUWest = (TextMessage) consumerEUWest.receive(5000);

            System.out.println("EU West   :: Got message: " + messageEUWest.getText());

         }



         // Step 17. We repeat the same local consumer priority check on US Central


         for (int i = 0; i < numMessages; i++) {
            TextMessage message = sessionEUWest.createTextMessage("This is text sent from US Central, message " + i);

            producerUSCentral.send(message);

            System.out.println("US Central:: Sent message: " + message.getText());
         }

         for (int i = 0; i < numMessages; i++) {
            TextMessage messageUSCentral = (TextMessage) consumerUSCentral.receive(5000);

            System.out.println("US Central:: Got message: " + messageUSCentral.getText());

         }


         // Step 18. We now close the consumer on EU West leaving it no local consumers.

         consumerEUWest.close();
         System.out.println("Consumer EU West now closed");


         // Step 19. We now send some more messages to server EU West


         for (int i = 0; i < numMessages; i++) {
            TextMessage message = sessionEUWest.createTextMessage("This is text sent from EU West, message " + i);

            producerEUWest.send(message);

            System.out.println("EU West   :: Sent message: " + message.getText());
         }

         // Step 20. we now consume those messages on EU East demonstrating that messages will re-route to the another broker based on upstream priority.
         // As US Central is configured to be -1 priority compared to EU East, messages should re-route to EU East for its consumers to consume.
         // If US Central and EU East were even priority then the re-direct would be loaded between the two.

         for (int i = 0; i < numMessages; i++) {
            TextMessage messageEUEast = (TextMessage) consumerEUEast.receive(5000);

            System.out.println("EU East   :: Got message: " + messageEUEast.getText());

         }

         // Step 21. We now close the consumer on US Central leaving it no local consumers.
         consumerUSCentral.close();
         System.out.println("Consumer US Central now closed");


         // Step 19. We now send some more messages to server US Central


         for (int i = 0; i < numMessages; i++) {
            TextMessage message = sessionUSCentral.createTextMessage("This is text sent from US Central, message " + i);

            producerUSCentral.send(message);

            System.out.println("US Central:: Sent message: " + message.getText());
         }


         // Step 20. we now consume those messages on EU East demonstrating that its messages also can re-route, if no local-consumer.
         for (int i = 0; i < numMessages; i++) {
            TextMessage messageEUEast = (TextMessage) consumerEUEast.receive(5000);

            System.out.println("EU East   :: Got message: " + messageEUEast.getText());

         }


         // Step 21. Restart local consumers on EU West and US Central
         consumerEUWest = sessionEUWest.createConsumer(queue);
         System.out.println("Consumer EU West re-created");

         consumerUSCentral = sessionUSCentral.createConsumer(queue);
         System.out.println("Consumer US Central re-created");


         // Step 22. we produce and consume on US Central, showing that dynamically re-adjusts now local consumers exist and messages delivery by priority to local consumer.
         for (int i = 0; i < numMessages; i++) {
            TextMessage message = sessionUSCentral.createTextMessage("This is text sent from US Central, message " + i);

            producerUSCentral.send(message);

            System.out.println("US Central:: Sent message: " + message.getText());
         }

         for (int i = 0; i < numMessages; i++) {
            TextMessage messageUSCentral = (TextMessage) consumerUSCentral.receive(5000);

            System.out.println("US Central:: Got message: " + messageUSCentral.getText());

         }


         // Step 23. we repeat the same test on EU West.
         for (int i = 0; i < numMessages; i++) {
            TextMessage message = sessionEUWest.createTextMessage("This is text sent from EU West, message " + i);

            producerEUWest.send(message);

            System.out.println("EU West   :: Sent message: " + message.getText());
         }

         for (int i = 0; i < numMessages; i++) {
            TextMessage messageEUWest = (TextMessage) consumerEUWest.receive(5000);

            System.out.println("EU West   :: Got message: " + messageEUWest.getText());

         }



      } finally {
         // Step 24. Be sure to close our resources!

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

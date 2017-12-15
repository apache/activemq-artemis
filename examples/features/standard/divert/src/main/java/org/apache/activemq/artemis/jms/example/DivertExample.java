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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * This examples demonstrates the use of ActiveMQ Artemis "Diverts" to transparently divert or copy messages
 * from one address to another.
 *
 * Please see the readme for more information.
 */
public class DivertExample {

   public static void main(final String[] args) throws Exception {
      Connection connectionLondon = null;

      Connection connectionNewYork = null;
      try {
         // Step 2. Look-up the queue orderQueue on the London server - this is the queue any orders are sent to
         Queue orderQueue = ActiveMQJMSClient.createQueue("orders");

         // Step 3. Look-up the topic priceUpdates on the London server- this is the topic that any price updates are
         // sent to
         Topic priceUpdates = ActiveMQJMSClient.createTopic("priceUpdates");

         // Step 4. Look-up the spy topic on the London server- this is what we will use to snoop on any orders
         Topic spyTopic = ActiveMQJMSClient.createTopic("spyTopic");

         // Step 7. Look-up the topic newYorkPriceUpdates on the New York server - any price updates sent to
         // priceUpdates on the London server will
         // be diverted to the queue priceForward on the London server, and a bridge will consume from that queue and
         // forward
         // them to the address newYorkPriceUpdates on the New York server where they will be distributed to the topic
         // subscribers on
         // the New York server
         Topic newYorkPriceUpdates = ActiveMQJMSClient.createTopic("newYorkPriceUpdates");

         // Step 8. Perform a lookup on the Connection Factory on the London server
         ConnectionFactory cfLondon = new ActiveMQConnectionFactory("tcp://localhost:61616");

         // Step 9. Perform a lookup on the Connection Factory on the New York server
         ConnectionFactory cfNewYork = new ActiveMQConnectionFactory("tcp://localhost:61617");

         // Step 10. Create a JMS Connection on the London server
         connectionLondon = cfLondon.createConnection();

         // Step 11. Create a JMS Connection on the New York server
         connectionNewYork = cfNewYork.createConnection();

         // Step 12. Create a JMS Session on the London server
         Session sessionLondon = connectionLondon.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 13. Create a JMS Session on the New York server
         Session sessionNewYork = connectionNewYork.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 14. Create a JMS MessageProducer orderProducer that sends to the queue orderQueue on the London server
         MessageProducer orderProducer = sessionLondon.createProducer(orderQueue);

         // Step 15. Create a JMS MessageProducer priceProducer that sends to the topic priceUpdates on the London
         // server
         MessageProducer priceProducer = sessionLondon.createProducer(priceUpdates);

         // Step 15. Create a JMS subscriber which subscribes to the spyTopic on the London server
         MessageConsumer spySubscriberA = sessionLondon.createConsumer(spyTopic);

         // Step 16. Create another JMS subscriber which also subscribes to the spyTopic on the London server
         MessageConsumer spySubscriberB = sessionLondon.createConsumer(spyTopic);

         // Step 17. Create a JMS MessageConsumer which consumes orders from the order queue on the London server
         MessageConsumer orderConsumer = sessionLondon.createConsumer(orderQueue);

         // Step 18. Create a JMS subscriber which subscribes to the priceUpdates topic on the London server
         MessageConsumer priceUpdatesSubscriberLondon = sessionLondon.createConsumer(priceUpdates);

         // Step 19. Create a JMS subscriber which subscribes to the newYorkPriceUpdates topic on the New York server
         MessageConsumer newYorkPriceUpdatesSubscriberA = sessionNewYork.createConsumer(newYorkPriceUpdates);

         // Step 20. Create another JMS subscriber which also subscribes to the newYorkPriceUpdates topic on the New
         // York server
         MessageConsumer newYorkPriceUpdatesSubscriberB = sessionNewYork.createConsumer(newYorkPriceUpdates);

         // Step 21. Start the connections

         connectionLondon.start();

         connectionNewYork.start();

         // Step 22. Create an order message
         TextMessage orderMessage = sessionLondon.createTextMessage("This is an order");

         // Step 23. Send the order message to the order queue on the London server
         orderProducer.send(orderMessage);

         System.out.println("Sent message: " + orderMessage.getText());

         // Step 24. The order message is consumed by the orderConsumer on the London server
         TextMessage receivedOrder = (TextMessage) orderConsumer.receive(5000);

         System.out.println("Received order: " + receivedOrder.getText());

         // Step 25. A copy of the order is also received by the spyTopic subscribers on the London server
         TextMessage spiedOrder1 = (TextMessage) spySubscriberA.receive(5000);

         System.out.println("Snooped on order: " + spiedOrder1.getText());

         TextMessage spiedOrder2 = (TextMessage) spySubscriberB.receive(5000);

         System.out.println("Snooped on order: " + spiedOrder2.getText());

         // Step 26. Create and send a price update message, destined for London
         TextMessage priceUpdateMessageLondon = sessionLondon.createTextMessage("This is a price update for London");

         priceUpdateMessageLondon.setStringProperty("office", "London");

         priceProducer.send(priceUpdateMessageLondon);

         // Step 27. The price update *should* be received by the local subscriber since we only divert messages
         // where office = New York
         TextMessage receivedUpdate = (TextMessage) priceUpdatesSubscriberLondon.receive(2000);

         System.out.println("Received price update locally: " + receivedUpdate.getText());

         // Step 28. The price update *should not* be received in New York

         TextMessage priceUpdate1 = (TextMessage) newYorkPriceUpdatesSubscriberA.receive(1000);

         if (priceUpdate1 != null) {
            throw new IllegalStateException("Message is not null");
         }

         System.out.println("Did not received price update in New York, look it's: " + priceUpdate1);

         TextMessage priceUpdate2 = (TextMessage) newYorkPriceUpdatesSubscriberB.receive(1000);

         if (priceUpdate2 != null) {
            throw new IllegalStateException("Message is not null");
         }

         System.out.println("Did not received price update in New York, look it's: " + priceUpdate2);

         // Step 29. Create a price update message, destined for New York

         TextMessage priceUpdateMessageNewYork = sessionLondon.createTextMessage("This is a price update for New York");

         priceUpdateMessageNewYork.setStringProperty("office", "New York");

         // Step 30. Send the price update message to the priceUpdates topic on the London server
         priceProducer.send(priceUpdateMessageNewYork);

         // Step 31. The price update *should not* be received by the local subscriber to the priceUpdates topic
         // since it has been *exclusively* diverted to the priceForward queue, because it has a header saying
         // it is destined for the New York office
         Message message = priceUpdatesSubscriberLondon.receive(1000);

         if (message != null) {
            throw new IllegalStateException("Message is not null");
         }

         System.out.println("Didn't receive local price update, look, it's: " + message);

         // Step 32. The remote subscribers on server 1 *should* receive a copy of the price update since
         // it has been diverted to a local priceForward queue which has a bridge consuming from it and which
         // forwards it to the same address on server 1.
         // We notice how the forwarded messages have had a special header added by our custom transformer that
         // we told the divert to use

         priceUpdate1 = (TextMessage) newYorkPriceUpdatesSubscriberA.receive(5000);

         System.out.println("Received forwarded price update on server 1: " + priceUpdate1.getText());
         System.out.println("Time of forward: " + priceUpdate1.getLongProperty("time_of_forward"));

         priceUpdate2 = (TextMessage) newYorkPriceUpdatesSubscriberB.receive(5000);

         System.out.println("Received forwarded price update on server 2: " + priceUpdate2.getText());
         System.out.println("Time of forward: " + priceUpdate2.getLongProperty("time_of_forward"));
      } finally {
         if (connectionLondon != null) {
            connectionLondon.close();
         }
         if (connectionNewYork != null) {
            connectionNewYork.close();
         }
      }
   }
}

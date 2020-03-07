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
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * This example demonstrates how a JMS TopicSubscriber can be created to subscribe to a wild-card Topic.
 *
 * For more information please see the readme
 */
public class TopicHierarchyExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      try {
         ConnectionFactory cf = new ActiveMQConnectionFactory();

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Instantiate a topic representing the wildcard we're going to subscribe to
         Topic topicSubscribe = ActiveMQJMSClient.createTopic("news.europe.#");

         // Step 7. Create a consumer (topic subscriber) that will consume using that wildcard
         // The consumer will receive any messages sent to any topic that starts with news.europe
         MessageConsumer messageConsumer = session.createConsumer(topicSubscribe);

         // Step 8. Create an anonymous producer
         MessageProducer producer = session.createProducer(null);

         // Step 9. Instantiate some more topic objects corresponding to the individual topics
         // we're going to send messages to
         Topic topicNewsUsaWrestling = ActiveMQJMSClient.createTopic("news.usa.wrestling");

         Topic topicNewsEuropeSport = ActiveMQJMSClient.createTopic("news.europe.sport");

         Topic topicNewsEuropeEntertainment = ActiveMQJMSClient.createTopic("news.europe.entertainment");

         // Step 10. Send a message destined for the usa wrestling topic
         TextMessage messageWrestlingNews = session.createTextMessage("Hulk Hogan starts ballet classes");

         producer.send(topicNewsUsaWrestling, messageWrestlingNews);

         // Step 11. Send a message destined for the europe sport topic
         TextMessage messageEuropeSport = session.createTextMessage("Lewis Hamilton joins European synchronized swimming team");

         producer.send(topicNewsEuropeSport, messageEuropeSport);

         // Step 12. Send a message destined for the europe entertainment topic
         TextMessage messageEuropeEntertainment = session.createTextMessage("John Lennon resurrected from dead");

         producer.send(topicNewsEuropeEntertainment, messageEuropeEntertainment);

         // Step 9. Start the connection

         connection.start();

         // Step 10. We don't receive the usa wrestling message since we subscribed to news.europe.# and
         // that doesn't match news.usa.wrestling. However we do receive the Europe sport message, and the
         // europe entertainment message, since these match the wildcard.

         TextMessage messageReceived1 = (TextMessage) messageConsumer.receive(5000);

         System.out.println("Received message: " + messageReceived1.getText());

         TextMessage messageReceived2 = (TextMessage) messageConsumer.receive(5000);

         System.out.println("Received message: " + messageReceived2.getText());

         Message message = messageConsumer.receive(1000);

         if (message != null) {
            throw new IllegalStateException("Message was not null.");
         }

         System.out.println("Didn't received any more message: " + message);
      } finally {
         // Step 12. Be sure to close our resources!
         if (connection != null) {
            connection.close();
         }
      }
   }
}

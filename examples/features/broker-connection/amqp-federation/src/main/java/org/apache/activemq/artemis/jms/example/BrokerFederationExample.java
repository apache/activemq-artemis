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
import javax.jms.Topic;

import org.apache.qpid.jms.JmsConnectionFactory;

/**
 * This example is demonstrating how messages are federated between two brokers with the
 * federation configuration located on only one broker (server0) and only a single outbound
 * connection is configured from server0 to server1
 */
public class BrokerFederationExample {

   public static void main(final String[] args) throws Exception {
      final ConnectionFactory connectionFactoryServer0 = new JmsConnectionFactory("amqp://localhost:5660");
      final ConnectionFactory connectionFactoryServer1 = new JmsConnectionFactory("amqp://localhost:5771");

      final Connection connectionOnServer0 = connectionFactoryServer0.createConnection();
      final Connection connectionOnServer1 = connectionFactoryServer1.createConnection();

      connectionOnServer0.start();
      connectionOnServer1.start();

      final Session sessionOnServer0 = connectionOnServer0.createSession(Session.AUTO_ACKNOWLEDGE);
      final Session sessionOnServer1 = connectionOnServer1.createSession(Session.AUTO_ACKNOWLEDGE);

      final Topic ordersTopic = sessionOnServer0.createTopic("orders");
      final Queue trackingQueue = sessionOnServer1.createQueue("tracking");

      // Federation from server1 back to server0 on the orders address
      final MessageProducer ordersProducerOn1 = sessionOnServer1.createProducer(ordersTopic);
      final MessageConsumer ordersConsumerOn0 = sessionOnServer0.createConsumer(ordersTopic);

      final TextMessage orderMessageSent = sessionOnServer1.createTextMessage("new-order");

      ordersProducerOn1.send(orderMessageSent);

      final TextMessage orderMessageReceived = (TextMessage) ordersConsumerOn0.receive(5_000);

      System.out.println("Consumer on server 0 received order message from producer on server 1 " + orderMessageReceived.getText());

      // Federation from server0 to server1 on the tracking queue
      final MessageProducer trackingProducerOn0 = sessionOnServer0.createProducer(trackingQueue);
      final MessageConsumer trackingConsumerOn1 = sessionOnServer1.createConsumer(trackingQueue);

      final TextMessage trackingMessageSent = sessionOnServer0.createTextMessage("new-tracking-data");

      trackingProducerOn0.send(trackingMessageSent);

      final TextMessage trackingMessageReceived = (TextMessage) trackingConsumerOn1.receive(5_000);

      System.out.println("Consumer on server 1 received tracking data from producer on server 0 " + trackingMessageReceived.getText());
   }
}

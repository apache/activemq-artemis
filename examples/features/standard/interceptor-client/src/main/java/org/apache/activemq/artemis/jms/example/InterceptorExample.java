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

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * A simple JMS example that shows how to implement and use client-side interceptors with ActiveMQ Artemis.
 */
public class InterceptorExample {

   public static void main(final String[] args) throws Exception {
      ConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616?incomingInterceptorList=" + SimpleInterceptor.class.getName());
      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue queue = session.createQueue("exampleQueue");

         MessageProducer producer = session.createProducer(queue);

         TextMessage message = session.createTextMessage("This is a text message");

         System.out.println("Sending message [" + message.getText() +
                               "] with String property: " +
                               message.getStringProperty("newproperty"));

         producer.send(message);

         MessageConsumer messageConsumer = session.createConsumer(queue);

         connection.start();

         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

         System.out.println("Received message [" + messageReceived.getText() +
                               "] with String property: " +
                               messageReceived.getStringProperty("newproperty"));

         if (messageReceived.getStringProperty("newproperty") == null) {
            throw new IllegalStateException("Check your configuration as the example interceptor wasn't actually called!");
         }
      }
   }
}

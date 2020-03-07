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

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * An example showing how messages are moved to an expiry queue when they expire.
 */
public class DBExample {

   public static void main(final String[] args) throws Exception {
      InitialContext initialContext = null;
      ConnectionFactory cf = new ActiveMQConnectionFactory();
      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue queue = session.createQueue("queue1");

         MessageProducer producer = session.createProducer(queue);

         TextMessage message = session.createTextMessage("this is a text message");

         producer.send(message);
         System.out.println("Sent message to " + queue.getQueueName() + ": " + message.getText());

         MessageConsumer messageConsumer = session.createConsumer(queue);

         connection.start();

         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);
         System.out.println("Received message from " + queue.getQueueName() + ": " + messageReceived);

      }
   }
}

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

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * The example sends a message to a topic (using openwire protocol) and an openwire consumer listens on the backing queue
 * using the ActiveMQ 5.x virtual topic naming convention. Due to the acceptor parameter virtualTopicConsumerWildcards
 * Artemis maps the consumer consuming from "Consumer.A.VirtualTopic.Orders" to actually consume from
 * FQQN  "VirtualTopic.Orders::Consumer.A.VirtualTopic.Orders"
 */
public class VirtualTopicMappingExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      try {

         ConnectionFactory cf = new ActiveMQConnectionFactory();

         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //create consumer on queue that is used by the Virtual Topic
         Queue queue = session.createQueue("Consumer.A.VirtualTopic.Orders");
         MessageConsumer messageConsumer = session.createConsumer(queue);
         connection.start();

         //send message to virtual topic
         Topic topic = session.createTopic("VirtualTopic.Orders");
         MessageProducer producer = session.createProducer(topic);
         TextMessage message = session.createTextMessage("This is a text message");
         producer.send(message);

         System.out.println("Sent message with ID: " + message.getJMSMessageID() + " to Topic: " + topic.getTopicName());

         //consume the message from the backing queue
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

         if (messageReceived != null) {
            System.out.println("Received message with ID: " + messageReceived.getJMSMessageID() + " from Queue: " + queue.getQueueName());
         } else {
            //unexpected outcome
            throw new RuntimeException("EXAMPLE FAILED - No message received from Queue: " + queue.getQueueName());
         }
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }
}

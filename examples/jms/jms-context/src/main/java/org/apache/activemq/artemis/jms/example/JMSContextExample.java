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

import javax.jms.DeliveryMode;
import javax.jms.JMSContext;
import javax.jms.Queue;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * A simple JMS Queue example that creates a producer and consumer on a queue and sends then receives a message.
 */
public class JMSContextExample {

   public static void main(final String[] args) throws Exception {
      // Instantiate the queue
      Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

      // Instantiate the ConnectionFactory (Using the default URI on this case)
      // Also instantiate the jmsContext
      // Using closeable interface
      try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory();
           JMSContext jmsContext = cf.createContext()) {
         // Create a message producer, note that we can chain all this into one statement
         jmsContext.createProducer().setDeliveryMode(DeliveryMode.PERSISTENT).send(queue, "this is a string");

         // Create a Consumer and receive the payload of the message direct.
         String payLoad = jmsContext.createConsumer(queue).receiveBody(String.class);

         System.out.println("payLoad = " + payLoad);

      }

   }
}

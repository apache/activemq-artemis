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

import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Queue;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * A simple JMS example that shows how AutoCloseable is used by JMS 2 resources.
 */
public class JMSAutoCloseableExample {

   public static void main(final String[] args) throws Exception {
      // Step 2. Perfom a lookup on the queue
      Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

      // Step 4.Create a JMS Context using the try-with-resources statement
      try
         (
            // Even though ConnectionFactory is not closeable it would be nice to close an ActiveMQConnectionFactory
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory();
            JMSContext jmsContext = cf.createContext()
         ) {
         // Step 5. create a jms producer
         JMSProducer jmsProducer = jmsContext.createProducer();

         // Step 6. Try sending a message, we don't have the appropriate privileges to do this so this will throw an exception
         jmsProducer.send(queue, "A Message from JMS2!");

         System.out.println("Received:" + jmsContext.createConsumer(queue).receiveBody(String.class));
      }
   }
}

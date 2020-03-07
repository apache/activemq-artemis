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

import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Topic;
import javax.naming.InitialContext;

/**
 * A JMS Example that uses shared consumers.
 */
public class JMSSharedConsumerExample {

   public static void main(final String[] args) throws Exception {
      InitialContext initialContext = null;
      JMSContext jmsContext = null;
      JMSContext jmsContext2 = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perfom a lookup on the queue
         Topic topic = (Topic) initialContext.lookup("topic/exampleTopic");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4.Create a JMS Context
         jmsContext = cf.createContext();

         // Step 5. Create a message producer.
         JMSProducer producer = jmsContext.createProducer();

         // Step 6. Create a shared consumer
         JMSConsumer jmsConsumer = jmsContext.createSharedConsumer(topic, "sc1");

         // Step 7. Create a second JMS Context for a second shared consumer
         jmsContext2 = cf.createContext();

         // Step 8. Create the second shared consumer
         JMSConsumer jmsConsumer2 = jmsContext2.createSharedConsumer(topic, "sc1");

         // Step 9. send 2 messages
         producer.send(topic, "this is a String!");

         producer.send(topic, "this is a second String!");

         // Step 10. receive the messages shared by both consumers
         String body = jmsConsumer.receiveBody(String.class, 5000);

         System.out.println("body = " + body);

         body = jmsConsumer2.receiveBody(String.class, 5000);

         System.out.println("body = " + body);
      } finally {
         // Step 11. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (jmsContext != null) {
            jmsContext.close();
         }
         if (jmsContext2 != null) {
            jmsContext2.close();
         }
      }
   }
}

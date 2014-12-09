/**
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
package org.apache.activemq.jms.example;

import javax.jms.CompletionListener;
import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.apache.activemq.common.example.ActiveMQExample;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A JMS Example that uses shared consumers.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JMSSharedConsumerExample extends ActiveMQExample
{
   public static void main(final String[] args)
   {
      new JMSSharedConsumerExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      InitialContext initialContext = null;
      JMSContext jmsContext = null;
      JMSContext jmsContext2 = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perfom a lookup on the queue
         Topic topic = (Topic) initialContext.lookup("topic/exampleTopic");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("ConnectionFactory");

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
         producer.send(topic, "this is a String!") ;

         producer.send(topic, "this is a second String!") ;

         // Step 10. receive the messages shared by both consumers
         String body = jmsConsumer.receiveBody(String.class, 5000);

         System.out.println("body = " + body);

         body = jmsConsumer2.receiveBody(String.class, 5000);

         System.out.println("body = " + body);

         return true;
      }
      finally
      {
         // Step 11. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if (jmsContext != null)
         {
            jmsContext.close();
         }
         if (jmsContext2 != null)
         {
            jmsContext2.close();
         }
      }
   }
}

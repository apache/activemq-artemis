/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.jms.example;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSContext;
import javax.jms.Queue;
import javax.naming.InitialContext;

import org.apache.activemq6.common.example.HornetQExample;

/**
 * A simple JMS Queue example that creates a producer and consumer on a queue and sends then receives a message.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JMSContextExample extends HornetQExample
{
   public static void main(final String[] args)
   {
      new JMSContextExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      InitialContext initialContext = null;
      JMSContext jmsContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4.Create a JMS Context
         jmsContext = cf.createContext();

         // Step 5. Create a message producer, note that we can chain all this into one statement
         jmsContext.createProducer().setDeliveryMode(DeliveryMode.PERSISTENT).send(queue, "this is a string");

         // Step 6. Create a Consumer and receive the payload of the message direct.
         String payLoad = jmsContext.createConsumer(queue).receiveBody(String.class);

         System.out.println("payLoad = " + payLoad);

         return true;
      }
      finally
      {
         // Step 7. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if (jmsContext != null)
         {
            jmsContext.close();
         }
      }
   }
}

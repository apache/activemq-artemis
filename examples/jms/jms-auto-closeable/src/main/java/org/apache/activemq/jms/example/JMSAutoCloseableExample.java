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
package org.apache.activemq.jms.example;

import javax.jms.*;
import javax.naming.InitialContext;

import org.apache.activemq.common.example.ActiveMQExample;

/**
 * A simple JMS example that shows how AutoCloseable is used by JMS 2 resources.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JMSAutoCloseableExample extends ActiveMQExample
{
   public static void main(final String[] args)
   {
      new JMSAutoCloseableExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4.Create a JMS Context using the try-with-resources statement
         try
         (
            JMSContext jmsContext = cf.createContext()
         )
         {
            // Step 5. create a jms producer
            JMSProducer jmsProducer = jmsContext.createProducer();

            // Step 6. Try sending a message, we don't have the appropriate privileges to do this so this will throw an exception
            jmsProducer.send(queue, "this message will fail security!");
         }
         catch(JMSRuntimeException e)
         {
            //Step 7. we can handle the new JMSRuntimeException if we want or let the exception get handled elsewhere, the
            //JMSCcontext will have been closed by the time we get to this point
            System.out.println("expected exception from jmsProducer.send: " + e.getMessage());
         }

         return true;
      }
      finally
      {
         // Step 8. Be sure to close our Initial Context, note that we don't have to close the JMSContext as it is auto closeable
         //and closed by the vm
         if (initialContext != null)
         {
            initialContext.close();
         }
      }
   }
}

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
package org.apache.activemq6.javaee.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class JmsContextInjectionClientExample
{
   public static void main(final String[] args) throws Exception
   {
      Thread.sleep(5000);
      InitialContext initialContext = null;
      try
      {
         // Step 3. Create an initial context to perform the JNDI lookup.
         final Properties env = new Properties();

         env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");

         env.put(Context.PROVIDER_URL, "http-remoting://localhost:8080");

         initialContext = new InitialContext(env);

         // Step 4. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("jms/queues/testQueue");

         // Step 5. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("jms/RemoteConnectionFactory");

         try
         (
            // Step 6.Create a JMS Connection inside the try-with-resource block so it will auto close
            JMSContext context = cf.createContext("guest", "password")
         )
         {
            // Step 6. create a JMSProducer and send the message
            context.createProducer().send(queue, "This is a text message");

            // Step 7 start the context
            context.start();

            // Step 8. look up the reply queue
            Queue replyQueue = (Queue)initialContext.lookup("jms/queues/replyQueue");

            // Step 9. receive the body of the message as a String
            String text = context.createConsumer(replyQueue).receiveBody(String.class);
         }
      }
      finally
      {
         // Step 10. Be sure to close our Initial Context!
         if (initialContext != null)
         {
            initialContext.close();
         }
      }
   }
}

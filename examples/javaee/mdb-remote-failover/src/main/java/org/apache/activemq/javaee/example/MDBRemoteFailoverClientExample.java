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

import org.apache.activemq6.javaee.example.server.ServerKiller;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 * @author Justin Bertram
 */
public class MDBRemoteFailoverClientExample
{
   private static ServerKiller killer;

   public static void main(String[] args) throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         final Properties env = new Properties();

         env.put(Context.URL_PKG_PREFIXES, "org.jboss.ejb.client.naming");

         env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");

         env.put(Context.PROVIDER_URL, "http-remoting://localhost:8180");

         initialContext = new InitialContext(env);
         //Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("/queues/inQueue");

         //Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("jms/RemoteConnectionFactory");

         //Step 4.Create a JMS Connection
         connection = cf.createConnection("guest", "password");

         //Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         //Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         System.out.println("Sent message: " + message.getText());

         //Step 8. Send the Message
         producer.send(message);

         //Step 15. We lookup the reply queue
         queue = (Queue) initialContext.lookup("/queues/outQueue");

         //Step 16. We create a JMS message consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         //Step 17. We start the connection so we can receive messages
         connection.start();

         //Step 18. We receive the message and print it out
         message = (TextMessage) messageConsumer.receive(20000);

         System.out.println("message.getText() = " + message.getText());

         //Step 19. Kill the live server
         System.out.println("Killing Live Server");
         killer.kill();

         //Step 20. Create a Text Message
         message = session.createTextMessage("This is another text message");

         System.out.println("Sent message: " + message.getText());

         //Step 21. Send the Message
         producer.send(message);

         //Step 22. We receive the message and print it out
         message = (TextMessage) messageConsumer.receive(20000);

         System.out.println("message.getText() = " + message.getText());
      }
      finally
      {
         //Step 23. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if(connection != null)
         {
            connection.close();
         }
      }
   }

   public static void setKiller(ServerKiller killer)
   {
      MDBRemoteFailoverClientExample.killer = killer;
   }
}

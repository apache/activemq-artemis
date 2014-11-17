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
package org.apache.activemq.javaee.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

import org.apache.activemq.javaee.example.server.SendMessageService;

/**
 * An example showing how to invoke a EJB which sends a JMS message and update a JDBC table in the same transaction.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author Justin Bertram
 */
public class EJBClientExample
{
   public static void main(final String[] args) throws Exception
   {
      InitialContext initialContext = null;
      Connection connection = null;
      try
      {
         // Step 1. Create an initial context to perform the EJB lookup.
         Properties env = new Properties();
         env.put(Context.URL_PKG_PREFIXES, "org.jboss.ejb.client.naming");
         initialContext = new InitialContext(env);

         // Step 2. Lookup the EJB
         SendMessageService service = (SendMessageService) initialContext.lookup("ejb:/test//SendMessageBean!org.apache.activemq.javaee.example.server.SendMessageService");

         // Step 3. Create the DB table which will be updated
         service.createTable();

         // Step 4. Invoke the sendAndUpdate method
         service.sendAndUpdate("This is a text message");
         System.out.println("invoked the EJB service");

         // Step 5. Create a new initial context for the JMS JNDI look-ups.
         env = new Properties();
         env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");
         env.put(Context.PROVIDER_URL, "http-remoting://localhost:8080");
         initialContext = new InitialContext(env);

         // Step 6. Lookup the JMS connection factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("jms/RemoteConnectionFactory");

         // Step 7. Lookup the queue
         Queue queue = (Queue) initialContext.lookup("jms/queues/testQueue");

         // Step 8. Create a connection, a session and a message consumer for the queue
         connection = cf.createConnection("guest", "password");
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(queue);

         // Step 9. Start the connection
         connection.start();

         // Step 10. Receive the message sent by the EJB
         TextMessage messageReceived = (TextMessage) consumer.receive(5000);
         System.out.println("Received message: " + messageReceived.getText() + " (" + messageReceived.getJMSMessageID() + ")");
      }
      finally
      {
         // Step 11. Be sure to close the resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if (connection != null)
         {
            connection.close();
         }
      }
   }
}

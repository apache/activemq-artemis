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
package org.hornetq.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.hornetq.common.example.HornetQExample;

/**
 * A simple example that demonstrates server side load-balancing of messages between the queue instances on different
 * nodes of the cluster.
 *
 * @author <a href="tim.fox@jboss.com>Tim Fox</a>
 */
public class ColocatedFailoverScaleDownExample extends HornetQExample
{
   public static void main(final String[] args)
   {
      new ColocatedFailoverScaleDownExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      final int numMessages = 30;

      Connection connection = null;
      Connection connection1 = null;

      InitialContext initialContext = null;
      InitialContext initialContext1 = null;

      try
      {
         // Step 1. Get an initial context for looking up JNDI for both servers
         initialContext1 = getContext(1);
         initialContext = getContext(0);

         // Step 2. Look up the JMS resources from JNDI
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");
         ConnectionFactory connectionFactory = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
         ConnectionFactory connectionFactory1 = (ConnectionFactory)initialContext1.lookup("/ConnectionFactory");

         // Step 3. Create a JMS Connections
         connection = connectionFactory.createConnection();
         connection1 = connectionFactory1.createConnection();

         // Step 4. Create a *non-transacted* JMS Session with client acknowledgement
         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         Session session1 = connection1.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         // Step 5. Create a JMS MessageProducers
         MessageProducer producer = session.createProducer(queue);
         MessageProducer producer1 = session1.createProducer(queue);

         // Step 6. Send some messages to both servers
         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session.createTextMessage("This is text message " + i);
            producer.send(message);
            System.out.println("Sent message: " + message.getText());
            message = session1.createTextMessage("This is another text message " + i);
            producer1.send(message);
            System.out.println("Sent message: " + message.getText());
         }

         // Step 7. Crash server #0, the live server, and wait a little while to make sure
         // it has really crashed
         Thread.sleep(2000);
         killServer(0);


         // Step 8. start the connection ready to receive messages
         connection1.start();

         // Step 9.create a consumer
         MessageConsumer consumer = session1.createConsumer(queue);

         // Step 10. Receive and acknowledge all of the sent messages, notice that they will be out of order, this is
         // because they were initially round robined to both nodes then when the server failed were reloaded into the
         // live server.
         TextMessage message0 = null;
         for (int i = 0; i < numMessages * 2; i++)
         {
            message0 = (TextMessage)consumer.receive(5000);
            System.out.println("Got message: " + message0.getText());
         }
         message0.acknowledge();

         return true;
      }
      finally
      {
         // Step 11. Be sure to close our resources!

         if (connection != null)
         {
            connection.close();
         }

         if (initialContext != null)
         {
            initialContext.close();
         }
         if (connection1 != null)
         {
            connection1.close();
         }

         if (initialContext1 != null)
         {
            initialContext1.close();
         }
      }
   }

}

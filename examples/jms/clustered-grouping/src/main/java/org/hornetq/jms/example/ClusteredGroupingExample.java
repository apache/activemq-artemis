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
public class ClusteredGroupingExample extends HornetQExample
{
   public static void main(String[] args)
   {
      new ClusteredGroupingExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      Connection connection0 = null;

      Connection connection1 = null;

      Connection connection2 = null;

      InitialContext ic0 = null;

      InitialContext ic1 = null;

      InitialContext ic2 = null;

      try
      {
         // Step 1. Get an initial context for looking up JNDI from server 0
         ic0 = getContext(0);

         // Step 2. Look-up the JMS Queue object from JNDI
         Queue queue = (Queue)ic0.lookup("/queue/exampleQueue");

         // Step 3. Look-up a JMS Connection Factory object from JNDI on server 0
         ConnectionFactory cf0 = (ConnectionFactory)ic0.lookup("/ConnectionFactory");

         // Step 4. Get an initial context for looking up JNDI from server 1
         ic1 = getContext(1);

         // Step 5. Look-up a JMS Connection Factory object from JNDI on server 1
         ConnectionFactory cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");

         // Step 4. Get an initial context for looking up JNDI from server 1
         ic2 = getContext(2);

         // Step 5. Look-up a JMS Connection Factory object from JNDI on server 1
         ConnectionFactory cf2 = (ConnectionFactory)ic2.lookup("/ConnectionFactory");

         // Step 6. We create a JMS Connection connection0 which is a connection to server 0
         connection0 = cf0.createConnection();

         // Step 7. We create a JMS Connection connection1 which is a connection to server 1
         connection1 = cf1.createConnection();

         // Step 8. We create a JMS Connection connection2 which is a connection to server 2
         connection2 = cf2.createConnection();

         // Step 9. We create a JMS Session on server 0
         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 10. We create a JMS Session on server 1
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 11. We create a JMS Session on server 1
         Session session2 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 12. We start the connections to ensure delivery occurs on them
         connection0.start();

         connection1.start();

         connection2.start();

         // Step 13. We create JMS MessageConsumer objects on server 0
         MessageConsumer consumer = session0.createConsumer(queue);


         // Step 14. We create a JMS MessageProducer object on server 0, 1 and 2
         MessageProducer producer0 = session0.createProducer(queue);

         MessageProducer producer1 = session1.createProducer(queue);

         MessageProducer producer2 = session2.createProducer(queue);

         // Step 15. We send some messages to server 0, 1 and 2 with the same groupid set

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session0.createTextMessage("This is text message " + i);

            message.setStringProperty("JMSXGroupID", "Group-0");

            producer0.send(message);

            System.out.println("Sent messages: " + message.getText() + " to node 0");
         }

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session1.createTextMessage("This is text message " + (i + 10));

            message.setStringProperty("JMSXGroupID", "Group-0");

            producer1.send(message);

            System.out.println("Sent messages: " + message.getText() + " to node 1");

         }

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session2.createTextMessage("This is text message " + (i + 20));

            message.setStringProperty("JMSXGroupID", "Group-0");

            producer2.send(message);

            System.out.println("Sent messages: " + message.getText() + " to node 2");
         }

         // Step 16. We now consume those messages from server 0
         // We note the messages have all been sent to the same consumer on the same node

         for (int i = 0; i < numMessages * 3; i++)
         {
            TextMessage message0 = (TextMessage)consumer.receive(5000);

            System.out.println("Got message: " + message0.getText() + " from node 0");

         }

         return true;
      }
      finally
      {
         // Step 17. Be sure to close our resources!

         if (connection0 != null)
         {
            connection0.close();
         }

         if (connection1 != null)
         {
            connection1.close();
         }

         if (connection2 != null)
         {
            connection2.close();
         }

         if (ic0 != null)
         {
            ic0.close();
         }

         if (ic1 != null)
         {
            ic1.close();
         }

         if (ic2 != null)
         {
            ic2.close();
         }
      }
   }

}

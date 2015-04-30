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
package org.apache.activemq.artemis.jms.example;

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq.artemis.common.example.ActiveMQExample;

/**
 * A simple example that demonstrates server side load-balancing of messages between the queue instances on different
 * nodes of the cluster. The cluster is created from a static list of nodes.
 */
public class StaticClusteredQueueExample extends ActiveMQExample
{
   public static void main(final String[] args)
   {
      new StaticClusteredQueueExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      Connection initialConnection = null;

      Connection connection0 = null;

      Connection connection1 = null;

      Connection connection2 = null;

      Connection connection3 = null;

      InitialContext ic0 = null;

      try
      {
         // Step 1. Get an initial context for looking up JNDI from server 3
         Hashtable<String, Object> properties = new Hashtable<String, Object>();
         properties.put("java.naming.factory.initial", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
         properties.put("connectionFactory.ConnectionFactory", args[3]);
         properties.put("queue.queue/exampleQueue", "exampleQueue");
         ic0 = new InitialContext(properties);

         // Step 2. Look-up the JMS Queue object from JNDI
         Queue queue = (Queue)ic0.lookup("queue/exampleQueue");

         // Step 3. Look-up a JMS Connection Factory object from JNDI on server 0
         ConnectionFactory cf0 = (ConnectionFactory)ic0.lookup("ConnectionFactory");

         //grab an initial connection and wait, in reality you wouldn't do it this way but since we want to ensure an
         // equal load balance we do this and then create 4 connections round robined
         initialConnection = cf0.createConnection();

         Thread.sleep(2000);
         // Step 6. We create a JMS Connection connection0 which is a connection to server 0
         connection0 = cf0.createConnection();

         // Step 7. We create a JMS Connection connection1 which is a connection to server 1
         connection1 = cf0.createConnection();

         // Step 6. We create a JMS Connection connection0 which is a connection to server 0
         connection2 = cf0.createConnection();

         // Step 7. We create a JMS Connection connection1 which is a connection to server 1
         connection3 = cf0.createConnection();

         // Step 8. We create a JMS Session on server 0
         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 9. We create a JMS Session on server 1
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);


         // Step 8. We create a JMS Session on server 0
         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 9. We create a JMS Session on server 1
         Session session3 = connection3.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 10. We start the connections to ensure delivery occurs on them
         connection0.start();

         connection1.start();

         connection2.start();

         connection3.start();

         // Step 11. We create JMS MessageConsumer objects on server 0 and server 1
         MessageConsumer consumer0 = session0.createConsumer(queue);

         MessageConsumer consumer1 = session1.createConsumer(queue);

         MessageConsumer consumer2 = session2.createConsumer(queue);

         MessageConsumer consumer3 = session3.createConsumer(queue);

         Thread.sleep(2000);

         // Step 12. We create a JMS MessageProducer object on server 3
         MessageProducer producer = session3.createProducer(queue);

         // Step 13. We send some messages to server 0

         final int numMessages = 20;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session0.createTextMessage("This is text message " + i);

            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }
         Thread.sleep(2000);
         // Step 14. We now consume those messages on *both* server 0 and server 1.
         // We note the messages have been distributed between servers in a round robin fashion
         // JMS Queues implement point-to-point message where each message is only ever consumed by a
         // maximum of one consumer
         int con0Node = getServer(connection0);
         int con1Node = getServer(connection1);
         int con2Node = getServer(connection2);
         int con3Node = getServer(connection3);

         if(con0Node + con1Node + con2Node + con3Node != 6)
         {
            return false;
         }
         for (int i = 0; i < numMessages; i += 4)
         {
            TextMessage message0 = (TextMessage)consumer0.receive(5000);

            System.out.println("Got message: " + message0.getText() + " from node " + con0Node);

            TextMessage message1 = (TextMessage)consumer1.receive(5000);

            System.out.println("Got message: " + message1.getText() + " from node " + con1Node);

            TextMessage message2 = (TextMessage)consumer2.receive(5000);

            System.out.println("Got message: " + message2.getText() + " from node " + con2Node);

            TextMessage message3 = (TextMessage)consumer3.receive(5000);

            System.out.println("Got message: " + message3.getText() + " from node " + con3Node);
         }

         return true;
      }
      finally
      {
         // Step 15. Be sure to close our resources!

         if (initialConnection != null)
         {
            initialConnection.close();
         }

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

         if (connection3 != null)
         {
            connection3.close();
         }

         if (ic0 != null)
         {
            ic0.close();
         }
      }
   }

}

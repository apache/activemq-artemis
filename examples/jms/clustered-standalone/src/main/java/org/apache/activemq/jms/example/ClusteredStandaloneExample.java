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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.apache.activemq6.common.example.HornetQExample;

/**
 *
 * This example demonstrates a distributed topic, and needs three servers to be started before the example is run.
 *
 * The example will not spawn the servers itself.
 *
 * The servers should be started using ./run.sh ../config/stand-alone/clustered
 *
 * If running on the same physical box, make sure that each server:
 *
 * a) uses a different data directory
 * b) uses different ports for the netty acceptor
 * c) uses different ports for JNDI
 *
 * Update server[0|1|2]/client-jndi.properties to the correct ports and hosts for the 3 servers
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ClusteredStandaloneExample extends HornetQExample
{
   public static void main(final String[] args)
   {
      new ClusteredStandaloneExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      Connection connection0 = null;

      Connection connection1 = null;

      Connection connection2 = null;

      InitialContext initialContext0 = null;
      InitialContext initialContext1 = null;
      InitialContext initialContext2 = null;

      try
      {
         initialContext0 = getContext(0);

         initialContext1 = getContext(1);

         initialContext2 = getContext(2);

         // First we demonstrate a distributed topic.
         // We create a connection on each node, create a consumer on each connection and send some
         // messages at a node and verify they are all received by all consumers

         ConnectionFactory cf0 = (ConnectionFactory)initialContext0.lookup("/ConnectionFactory");

         System.out.println("Got cf " + cf0);

         ConnectionFactory cf1 = (ConnectionFactory)initialContext1.lookup("/ConnectionFactory");

         System.out.println("Got cf " + cf1);

         ConnectionFactory cf2 = (ConnectionFactory)initialContext2.lookup("/ConnectionFactory");

         System.out.println("Got cf " + cf2);

         Topic topic = (Topic)initialContext0.lookup("/topic/ExampleTopic");

         connection0 = cf0.createConnection();

         connection1 = cf1.createConnection();

         connection2 = cf2.createConnection();

         connection0.start();

         connection1.start();

         connection2.start();

         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer messageConsumer0 = session0.createConsumer(topic);

         MessageConsumer messageConsumer1 = session1.createConsumer(topic);

         MessageConsumer messageConsumer2 = session2.createConsumer(topic);

         MessageProducer producer = session0.createProducer(topic);

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session0.createTextMessage("Message " + i);

            producer.send(message);
         }

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message0 = (TextMessage)messageConsumer0.receive(2000);

            if (message0 == null)
            {
               return false;
            }

            // System.out.println("Received message " + message0.getText());

            TextMessage message1 = (TextMessage)messageConsumer1.receive(2000);

            if (message1 == null)
            {
               return false;
            }

            // System.out.println("Received message " + message1.getText());

            TextMessage message2 = (TextMessage)messageConsumer2.receive(2000);

            if (message2 == null)
            {
               return false;
            }

           // System.out.println("Received message " + message2.getText());
         }

         producer.close();

         messageConsumer0.close();

         messageConsumer1.close();

         messageConsumer2.close();

         return true;
      }
      finally
      {
         // Step 12. Be sure to close our JMS resources!
         if (initialContext0 != null)
         {
            initialContext0.close();
         }
         if (initialContext1 != null)
         {
            initialContext1.close();
         }
         if (initialContext2 != null)
         {
            initialContext2.close();
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
      }
   }

}

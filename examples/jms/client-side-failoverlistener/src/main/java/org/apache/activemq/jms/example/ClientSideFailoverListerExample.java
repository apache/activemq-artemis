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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq.api.core.client.FailoverEventListener;
import org.apache.activemq.api.core.client.FailoverEventType;
import org.apache.activemq.common.example.ActiveMQExample;
import org.apache.activemq.jms.client.ActiveMQConnection;

/**
 * This example demonstrates how you can listen on failover event on the client side
 *
 * In this example there are two nodes running in a cluster, both server will be running for start,
 * but after a while the first server will crash. This will trigger an fail oever event
 *
 * @author <a href="flemming.harms@gmail.com>Flemming Harms</a>
 */
public class ClientSideFailoverListerExample extends ActiveMQExample
{
   public static void main(final String[] args)
   {
      new ClientSideFailoverListerExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      InitialContext initialContext = null;

      Connection connectionA = null;

      try
      {
         // Step 1. Get an initial context for looking up JNDI from server 0
         initialContext = getContext(0);

         // Step 2. Look-up the JMS Queue object from JNDI
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Look-up a JMS Connection Factory object from JNDI on server 0
         ConnectionFactory connectionFactory = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. We create 1 JMS connections from the same connection factory.
         // Wait a little while to make sure broadcasts from all nodes have reached the client
         Thread.sleep(5000);
         connectionA = connectionFactory.createConnection();
         ((ActiveMQConnection)connectionA).setFailoverListener(new FailoverListenerImpl());

         // Step 5. We create JMS Sessions
         Session sessionA = connectionA.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. We create JMS MessageProducer objects on the sessions
         MessageProducer producerA = sessionA.createProducer(queue);

         // Step 7. We send some messages on each producer
         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage messageA = sessionA.createTextMessage("A:This is text message " + i);
            producerA.send(messageA);
            System.out.println("Sent message: " + messageA.getText());

         }

         // Step 8. We start the connection to consume messages
         connectionA.start();

         // Step 9. We consume messages from the session A, one at a time.
         // We reached message no 5 the first server will crash
         consume(sessionA, queue, numMessages, "A");

         return true;
      }
      finally
      {
         // Step 10. Be sure to close our resources!

         if (connectionA != null)
         {
            connectionA.close();
         }

         if (initialContext != null)
         {
            initialContext.close();
         }
      }
   }

   private void consume(Session session, Queue queue, int numMessages, String node) throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage message = (TextMessage)consumer.receive(2000);
         System.out.println("Got message: " + message.getText() + " from node " + node);
         if (i == 5)
         {
            killServer(0);
         }
      }

      System.out.println("receive other message from node " + node + ": " + consumer.receive(2000));

   }

   private static class FailoverListenerImpl implements FailoverEventListener
   {
      public void failoverEvent(FailoverEventType eventType)
      {
         System.out.println("Failover event triggered :" + eventType.toString());
      }
   }
}

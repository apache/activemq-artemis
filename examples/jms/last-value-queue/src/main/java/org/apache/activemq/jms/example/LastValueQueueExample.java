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

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq6.common.example.HornetQExample;

/**
 * This example shows how to configure and use a <em>Last-Value</em> queues.
 * Only the last message with a well-defined property is hold by the queue.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class LastValueQueueExample extends HornetQExample
{
   public static void main(final String[] args)
   {
      new LastValueQueueExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/lastValueQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4.Create a JMS Connection, session and producer on the queue
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);

         // Step 5. Create and send a text message with the Last-Value header set
         TextMessage message = session.createTextMessage("1st message with Last-Value property set");
         message.setStringProperty("_HQ_LVQ_NAME", "STOCK_NAME");
         producer.send(message);
         System.out.format("Sent message: %s%n", message.getText());

         // Step 6. Create and send a second text message with the Last-Value header set
         message = session.createTextMessage("2nd message with Last-Value property set");
         message.setStringProperty("_HQ_LVQ_NAME", "STOCK_NAME");
         producer.send(message);
         System.out.format("Sent message: %s%n", message.getText());

         // Step 7. Create and send a third text message with the Last-Value header set
         message = session.createTextMessage("3rd message with Last-Value property set");
         message.setStringProperty("_HQ_LVQ_NAME", "STOCK_NAME");
         producer.send(message);
         System.out.format("Sent message: %s%n", message.getText());

         // Step 8. Browse the queue. There is only 1 message in it, the last sent
         QueueBrowser browser = session.createBrowser(queue);
         Enumeration enumeration = browser.getEnumeration();
         while (enumeration.hasMoreElements())
         {
            TextMessage messageInTheQueue = (TextMessage)enumeration.nextElement();
            System.out.format("Message in the queue: %s%n", messageInTheQueue.getText());
         }
         browser.close();

         // Step 9. Create a JMS Message Consumer for the queue
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 10. Start the Connection
         connection.start();

         // Step 11. Trying to receive a message. Since the queue is configured to keep only the
         // last message with the Last-Value header set, the message received is the last sent
         TextMessage messageReceived = (TextMessage)messageConsumer.receive(5000);
         System.out.format("Received message: %s%n", messageReceived.getText());

         // Step 12. Trying to receive another message but there will be none.
         // The 1st message was discarded when the 2nd was sent to the queue.
         // The 2nd message was in turn discarded when the 3trd was sent to the queue
         messageReceived = (TextMessage)messageConsumer.receive(5000);
         System.out.format("Received message: %s%n", messageReceived);

         initialContext.close();

         return true;
      }
      finally
      {
         // Step 13. Be sure to close our JMS resources!
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

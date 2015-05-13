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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;

import org.apache.activemq.artemis.common.example.ActiveMQExample;

/**
 * This example demonstrates the use of ActiveMQ Artemis "pre-acknowledge" functionality where
 * messages are acknowledged before they are delivered to the consumer.
 *
 * Please see the readme.html for more details.
 */
public class ProtonCPPExample extends ActiveMQExample
{
   public static void main(final String[] args)
   {
      new ProtonCPPExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      Connection connection = null;

      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perform the look-ups
         Queue queue = (Queue)initialContext.lookup("queue/exampleQueue");

         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("ConnectionFactory");

         // Step 3. Create a the JMS objects
         connection = cf.createConnection();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer messageConsumer = session.createConsumer(queue);

         MessageProducer producerAnswer = session.createProducer(queue);

         // Step 4. Start the connection
         connection.start();

         System.out.println("On a shell script, execute the following:");

         System.out.println("./compile.sh");

         System.out.println("./hello");

         // Step 5. Finally, receive the message
         Message messageReceived = messageConsumer.receive(5000);

         if (messageReceived == null)
         {
            // We are not going to issue this as an error because
            // we also use this example as part of our tests on artemis
            // this is not considered an error, just that no messages arrived (i.e. hello wasn't called)
         }
         else
         {
            System.out.println("message received: " + messageReceived);

            // Sending message back to client
            producerAnswer.send(session.createTextMessage("HELLO from Apache ActiveMQ Artemis"));
         }

         return true;
      }
      finally
      {
         // Step 9. Be sure to close our resources!
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

   // To do this we send a management message to get the message count.
   // In real life you wouldn't create a new session every time you send a management message
   private int getMessageCount(final Connection connection) throws Exception
   {
      QueueSession session = ((QueueConnection)connection).createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");

      QueueRequestor requestor = new QueueRequestor(session, managementQueue);

      connection.start();

      Message m = session.createMessage();

      JMSManagementHelper.putAttribute(m, "jms.queue.exampleQueue", "messageCount");

      Message response = requestor.request(m);

      int messageCount = (Integer)JMSManagementHelper.getResult(response);

      return messageCount;
   }

}

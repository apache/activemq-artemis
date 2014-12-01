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
package org.apache.activemq.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq.common.example.ActiveMQExample;

/**
 * This example demonstrates how ActiveMQ consumers can be configured to not buffer any messages from
 * the server.
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class NoConsumerBufferingExample extends ActiveMQExample
{
   public static void main(final String[] args)
   {
      new NoConsumerBufferingExample().run(args);
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
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a JMS MessageConsumer

         MessageConsumer consumer1 = session.createConsumer(queue);

         // Step 8. Start the connection

         connection.start();

         // Step 9. Send 10 messages to the queue

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session.createTextMessage("This is text message: " + i);

            producer.send(message);
         }

         System.out.println("Sent messages");

         // Step 10. Create another consumer on the same queue

         MessageConsumer consumer2 = session.createConsumer(queue);

         // Step 11. Consume three messages from consumer2

         for (int i = 0; i < 3; i++)
         {
            TextMessage message = (TextMessage)consumer2.receive(2000);

            System.out.println("Consumed message from consumer2: " + message.getText());
         }

         // Step 12. Consume five messages from consumer1

         for (int i = 0; i < 5; i++)
         {
            TextMessage message = (TextMessage)consumer1.receive(2000);

            System.out.println("Consumed message from consumer1: " + message.getText());
         }

         // Step 13. Consume another two messages from consumer2

         for (int i = 0; i < 2; i++)
         {
            TextMessage message = (TextMessage)consumer2.receive(2000);

            System.out.println("Consumed message from consumer1: " + message.getText());
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

}

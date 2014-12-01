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
package org.apache.activemq.javaee.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

/**
 * @author Justin Bertram
 */
public class MDB_CMT_SetRollbackOnlyWithDLQClientExample
{
   public static void main(final String[] args) throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         final Properties env = new Properties();

         env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");

         env.put(Context.PROVIDER_URL, "http-remoting://localhost:8080");

         initialContext = new InitialContext(env);

         // Step 2. Perfom a lookup on the destination
         Destination destination = (Destination) initialContext.lookup("jms/topics/testTopic");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/jms/RemoteConnectionFactory");

         // Step 4.Create a JMS Connection
         connection = cf.createConnection("guest", "password");

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(destination);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         // Step 8. Send the Message
         producer.send(message);

         System.out.println("Sent message: " + message.getText());

         // Step 9, 10, and 11 in MDB_CMT_SetRollbackOnlyWithDLQExample

         // Step 12. Perform a lookup on the DLQ
         destination = (Destination) initialContext.lookup("jms/queues/dlq");

         // Step 13. Create the consumer and start the connection
         MessageConsumer consumer = session.createConsumer(destination);
         connection.start();

         // Step 14. Receive the message.
         message = (TextMessage) consumer.receive(3000);

         // Step 15. Print the special DLQ properties
         System.out.println("Original address: " + message.getStringProperty("_HQ_ORIG_ADDRESS"));
         System.out.println("Original queue: " + message.getStringProperty("_HQ_ORIG_QUEUE"));
      }
      finally
      {
         // Step 16. Be sure to close our JMS resources!
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

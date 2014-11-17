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
package org.apache.activemq6.javaee.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

/**
 * An example which sends a message to a source queue and consume from a target queue.
 * The source and target queues are bridged by a JMS Bridge configured and running in WildFly.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSBridgeExample
{
   public static void main(final String[] args) throws Exception
   {
      InitialContext initialContext = null;
      Connection sourceConnection = null;
      Connection targetConnection = null;
      try
      {
         // Step 1. Obtain an Initial Context
         final Properties env = new Properties();

         env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");

         env.put(Context.PROVIDER_URL, "http-remoting://localhost:8080");

         initialContext = new InitialContext(env);

         // Step 2. Lookup the JMS connection factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/jms/RemoteConnectionFactory");

         // Step 3. Lookup the source queue
         Queue sourceQueue = (Queue)initialContext.lookup("jms/queues/sourceQueue");

         // Step 4. Create a connection, a session and a message producer for the *source* queue
         sourceConnection = cf.createConnection("guest", "password");
         Session sourceSession = sourceConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer sourceProducer = sourceSession.createProducer(sourceQueue);

         // Step 5. Create and send a text message to the *source* queue
         TextMessage message = sourceSession.createTextMessage("this is a text message");
         sourceProducer.send(message);
         System.out.format("Sent message to %s: %s\n",
                           ((Queue)message.getJMSDestination()).getQueueName(),
                           message.getText());
         System.out.format("Message ID : %s\n", message.getJMSMessageID());

         // Step 6. Close the *source* connection
         sourceConnection.close();

         // Step 7. Lookup the *target* queue
         Queue targetQueue = (Queue)initialContext.lookup("jms/queues/targetQueue");

         // Step 8. Create a connection, a session and a message consumer for the *target* queue
         targetConnection = cf.createConnection("guest", "password");
         Session targetSession = targetConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer targetConsumer = targetSession.createConsumer(targetQueue);

         // Step 9. Start the connection to receive messages from the *targe* queue
         targetConnection.start();

         // Step 10. Receive a message from the *target* queue
         TextMessage messageReceived = (TextMessage)targetConsumer.receive(15000);
         System.out.format("\nReceived from %s: %s\n",
                           ((Queue)messageReceived.getJMSDestination()).getQueueName(),
                           messageReceived.getText());

         // Step 11. Display the received message's ID
         System.out.format("Message ID         : %s\n", messageReceived.getJMSMessageID());

         // Step 12. Display the message ID of the message received by the *bridge*
         System.out.format("Bridged Message ID : %s\n", messageReceived.getStringProperty("HQ_BRIDGE_MSG_ID_LIST"));
      }
      finally
      {
         // Step 13. Be sure to close the resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if (sourceConnection != null)
         {
            sourceConnection.close();
         }
         if (targetConnection != null)
         {
            targetConnection.close();
         }
      }
   }
}

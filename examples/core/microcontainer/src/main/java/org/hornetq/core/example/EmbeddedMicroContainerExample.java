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
package org.hornetq.core.example;

import java.util.Date;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.integration.bootstrap.HornetQBootstrapServer;

/**
 *
 * This example shows how to run a HornetQ core client and server embedded in your
 * own application
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class EmbeddedMicroContainerExample
{

   public static void main(final String[] args) throws Exception
   {

      HornetQBootstrapServer hornetQ = null;
      try
      {

         // Step 1. Start the server
         hornetQ = new HornetQBootstrapServer("hornetq-beans.xml");
         hornetQ.run();

         // Step 2. As we are not using a JNDI environment we instantiate the objects directly
         ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName()));
         ClientSessionFactory sf = serverLocator.createSessionFactory();

         // Step 3. Create a core queue
         ClientSession coreSession = sf.createSession(false, false, false);

         final String queueName = "queue.exampleQueue";

         coreSession.createQueue(queueName, queueName, true);

         coreSession.close();

         ClientSession session = null;

         try
         {

            // Step 4. Create the session, and producer
            session = sf.createSession();

            ClientProducer producer = session.createProducer(queueName);

            // Step 5. Create and send a message
            ClientMessage message = session.createMessage(false);

            final String propName = "myprop";

            message.putStringProperty(propName, "Hello sent at " + new Date());

            System.out.println("Sending the message.");

            producer.send(message);

            // Step 6. Create the message consumer and start the connection
            ClientConsumer messageConsumer = session.createConsumer(queueName);
            session.start();

            // Step 7. Receive the message.
            ClientMessage messageReceived = messageConsumer.receive(1000);

            System.out.println("Received TextMessage:" + messageReceived.getStringProperty(propName));
         }
         finally
         {
            // Step 8. Be sure to close our resources!
            if (sf != null)
            {
               sf.close();
            }

            // Step 9. Shutdown the container
            if (hornetQ != null)
            {
               hornetQ.shutDown();
            }
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
         throw e;
      }
   }
}

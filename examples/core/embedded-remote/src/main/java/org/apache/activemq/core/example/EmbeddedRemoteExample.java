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
package org.apache.activemq.core.example;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory;

/**
 *
 * This example shows how to run a ActiveMQ core client and server embedded in your
 * own application
 */
public class EmbeddedRemoteExample
{

   public static void main(final String[] args)
   {
      try
      {
         // Step 3. As we are not using a JNDI environment we instantiate the objects directly

         /**
          * this map with configuration values is not necessary (it configures the default values).
          * If you modify it to run the example in two different hosts, remember to also modify the
          * server's Acceptor at {@link EmbeddedServer}
          */
         Map<String,Object> map = new HashMap<String,Object>();
         map.put("host", "localhost");
         map.put("port", 61616);
         // -------------------------------------------------------

         ServerLocator serverLocator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName(), map));
         ClientSessionFactory sf = serverLocator.createSessionFactory();

         // Step 4. Create a core queue
         ClientSession coreSession = sf.createSession(false, false, false);

         final String queueName = "queue.exampleQueue";

         coreSession.createQueue(queueName, queueName, true);

         coreSession.close();

         ClientSession session = null;

         try
         {

            // Step 5. Create the session, and producer
            session = sf.createSession();

            ClientProducer producer = session.createProducer(queueName);

            // Step 6. Create and send a message
            ClientMessage message = session.createMessage(false);

            final String propName = "myprop";

            message.putStringProperty(propName, "Hello sent at " + new Date());

            System.out.println("Sending the message.");

            producer.send(message);

            // Step 7. Create the message consumer and start the connection
            ClientConsumer messageConsumer = session.createConsumer(queueName);
            session.start();

            // Step 8. Receive the message.
            ClientMessage messageReceived = messageConsumer.receive(1000);
            System.out.println("Received TextMessage:" + messageReceived.getStringProperty(propName));
         }
         finally
         {
            // Step 9. Be sure to close our resources!
            if (sf != null)
            {
               sf.close();
            }
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}

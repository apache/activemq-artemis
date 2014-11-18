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
package org.apache.activemq.tests.integration.clientcrash;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.jms.client.ActiveMQTextMessage;
import org.apache.activemq.tests.integration.IntegrationTestLogger;

/**
 * Code to be run in an external VM, via main()
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class GracefulClient
{
   // Constants ------------------------------------------------------------------------------------

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Static ---------------------------------------------------------------------------------------

   public static void main(final String[] args) throws Exception
   {
      if (args.length != 2)
      {
         throw new Exception("require 2 arguments: queue name + message text");
      }
      String queueName = args[0];
      String messageText = args[1];

      try
      {
         ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName()));
         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(false, true, true);
         ClientProducer producer = session.createProducer(queueName);
         ClientConsumer consumer = session.createConsumer(queueName);

         ClientMessage message = session.createMessage(ActiveMQTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte) 1);
         message.getBodyBuffer().writeString(messageText);
         producer.send(message);

         session.start();

         // block in receiving for 5 secs, we won't receive anything
         consumer.receive(5000);

         // this should silence any non-daemon thread and allow for graceful exit
         session.close();
         System.exit(0);
      }
      catch (Throwable t)
      {
         GracefulClient.log.error(t.getMessage(), t);
         System.exit(1);
      }
   }

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // Command implementation -----------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}

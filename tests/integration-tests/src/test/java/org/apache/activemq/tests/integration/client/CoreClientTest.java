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
package org.apache.activemq.tests.integration.client;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.junit.Test;

import org.junit.Assert;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.HornetQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.HornetQServers;
import org.apache.activemq.jms.client.HornetQTextMessage;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.util.ServiceTestBase;

public class CoreClientTest extends ServiceTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testCoreClientNetty() throws Exception
   {
      testCoreClient(NETTY_ACCEPTOR_FACTORY, NETTY_CONNECTOR_FACTORY);
   }

   @Test
   public void testCoreClientInVM() throws Exception
   {
      testCoreClient(INVM_ACCEPTOR_FACTORY, INVM_CONNECTOR_FACTORY);
   }

   private void testCoreClient(final String acceptorFactoryClassName, final String connectorFactoryClassName) throws Exception
   {
      final SimpleString QUEUE = new SimpleString("CoreClientTestQueue");

      Configuration conf = createDefaultConfig()
         .setSecurityEnabled(false)
         .addAcceptorConfiguration(new TransportConfiguration(acceptorFactoryClassName));

      HornetQServer server = addServer(HornetQServers.newHornetQServer(conf, false));

      server.start();
      ServerLocator locator =
               addServerLocator(HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(
                                                                                                      connectorFactoryClassName)));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);

         message.putStringProperty("foo", "bar");

         // One way around the setting destination problem is as follows -
         // Remove destination as an attribute from client producer.
         // The destination always has to be set explicity before sending a message

         message.setAddress(QUEUE);

         message.getBodyBuffer().writeString("testINVMCoreClient");

         producer.send(message);
      }

      CoreClientTest.log.info("sent messages");

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         ActiveMQBuffer buffer = message2.getBodyBuffer();

         Assert.assertEquals("testINVMCoreClient", buffer.readString());

         message2.acknowledge();
      }
   }
}

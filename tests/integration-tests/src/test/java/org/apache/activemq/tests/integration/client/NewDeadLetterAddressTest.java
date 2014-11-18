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
import org.apache.activemq.api.core.ActiveMQException;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import org.junit.Assert;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.core.settings.impl.AddressSettings;
import org.apache.activemq.tests.util.UnitTestCase;

/**
 *
 * A NewDeadLetterAddressTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class NewDeadLetterAddressTest extends UnitTestCase
{
   private ActiveMQServer server;

   private ClientSession clientSession;
   private ServerLocator locator;

   @Test
   public void testSendToDLAWhenNoRoute() throws Exception
   {
      SimpleString dla = new SimpleString("DLA");
      SimpleString address = new SimpleString("empty_address");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setDeadLetterAddress(dla);
      addressSettings.setSendToDLAOnNoRoute(true);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      clientSession.createQueue(dla, dlq, null, false);
      ClientProducer producer = clientSession.createProducer(address);
      producer.send(createTextMessage(clientSession, "heyho!"));
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(dlq);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      TransportConfiguration transportConfig = new TransportConfiguration(UnitTestCase.INVM_ACCEPTOR_FACTORY);

      Configuration configuration = createDefaultConfig()
         .setSecurityEnabled(false)
         .addAcceptorConfiguration(transportConfig);
      server = addServer(ActiveMQServers.newActiveMQServer(configuration, false));
      // start the server
      server.start();
      // then we create a client as normal
      locator =
               addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(
                  UnitTestCase.INVM_CONNECTOR_FACTORY)));
      ClientSessionFactory sessionFactory = createSessionFactory(locator);
      clientSession = sessionFactory.createSession(false, true, false);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (clientSession != null)
      {
         try
         {
            clientSession.close();
         }
         catch (ActiveMQException e1)
         {
            //
         }
      }
      clientSession = null;
      super.tearDown();
   }

}

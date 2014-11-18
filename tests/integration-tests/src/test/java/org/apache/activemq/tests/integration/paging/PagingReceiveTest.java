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
package org.apache.activemq.tests.integration.paging;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.core.settings.impl.AddressSettings;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.junit.Before;
import org.junit.Test;

public class PagingReceiveTest extends ServiceTestBase
{

   private static final SimpleString ADDRESS = new SimpleString("jms.queue.catalog-service.price.change.bm");

   private ActiveMQServer server;

   private ServerLocator locator;

   protected boolean isNetty()
   {
      return false;
   }


   @Test
   public void testReceive() throws Exception
   {
      ClientMessage message = receiveMessage();
      System.out.println("message received:" + message);

      assertNotNull("Message not found.", message);
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server = internalCreateServer();

      Queue queue = server.createQueue(ADDRESS, ADDRESS, null, true, false);
      queue.getPageSubscription().getPagingStore().startPaging();

      for (int i = 0; i < 10; i++)
      {
         queue.getPageSubscription().getPagingStore().forceAnotherPage();
      }

      final ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);
      ClientProducer prod = session.createProducer(ADDRESS);

      for (int i = 0; i < 500; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("key", i);
         prod.send(msg);
         if (i > 0 && i % 10 == 0)
         {
            session.commit();
         }
      }

      session.close();
      locator.close();

      server.stop();

      internalCreateServer();


   }

   private ActiveMQServer internalCreateServer() throws Exception
   {
      final ActiveMQServer server = newActiveMQServer();

      server.start();

      waitForServer(server);

      locator = createFactory(isNetty());
      return server;
   }

   private ClientMessage receiveMessage() throws Exception
   {
      final ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

      session.start();
      ClientConsumer consumer = session.createConsumer(ADDRESS);

      ClientMessage message = consumer.receive(1000);

      session.commit();

      if (message != null)
      {
         message.acknowledge();
      }

      consumer.close();

      session.close();

      return message;
   }

   private ActiveMQServer newActiveMQServer() throws Exception
   {
      final ActiveMQServer server = createServer(true, isNetty());

      final AddressSettings settings = new AddressSettings();
      settings.setMaxSizeBytes(67108864);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      settings.setMaxRedeliveryDelay(3600000);
      settings.setRedeliveryMultiplier(2.0);
      settings.setRedeliveryDelay(500);

      server.getAddressSettingsRepository().addMatch("#", settings);

      return server;
   }


}

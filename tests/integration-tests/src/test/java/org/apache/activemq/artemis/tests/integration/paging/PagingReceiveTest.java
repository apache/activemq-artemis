/*
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
package org.apache.activemq.artemis.tests.integration.paging;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class PagingReceiveTest extends ActiveMQTestBase {

   private static final SimpleString ADDRESS = new SimpleString("catalog-service.price.change.bm");

   private ActiveMQServer server;

   private ServerLocator locator;

   private int numMsgs = 500;

   protected boolean isNetty() {
      return false;
   }

   @Test
   public void testReceive() throws Exception {
      ClientMessage message = receiveMessage();
      assertNotNull("Message not found.", message);
   }

   @Test
   public void testReceiveThenCheckCounter() throws Exception {

      Queue queue = server.locateQueue(ADDRESS);
      assertEquals(numMsgs, queue.getMessagesAdded());
      receiveAllMessages();
      queue.getPageSubscription().cleanupEntries(true);
      assertEquals(numMsgs, queue.getMessagesAdded());
   }


   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = internalCreateServer();

      server.addAddressInfo(new AddressInfo(ADDRESS, RoutingType.ANYCAST));
      Queue queue = server.createQueue(ADDRESS, RoutingType.ANYCAST, ADDRESS, null, true, false);
      queue.getPageSubscription().getPagingStore().startPaging();

      for (int i = 0; i < 10; i++) {
         queue.getPageSubscription().getPagingStore().forceAnotherPage();
      }

      final ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);
      ClientProducer prod = session.createProducer(ADDRESS);

      for (int i = 0; i < 500; i++) {
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("key", i);
         prod.send(msg);
         if (i > 0 && i % 10 == 0) {
            session.commit();
         }
      }

      session.close();
      locator.close();

      server.stop();

      server = internalCreateServer();

   }

   private ActiveMQServer internalCreateServer() throws Exception {
      final ActiveMQServer server = newActiveMQServer();

      server.start();

      waitForServerToStart(server);

      locator = createFactory(isNetty());
      return server;
   }

   private void receiveAllMessages() throws Exception {
      final ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

      session.start();
      ClientConsumer consumer = session.createConsumer(ADDRESS);
      for (int i = 0; i < numMsgs; i++) {
         ClientMessage message = consumer.receive(2000);
         assertNotNull(message);
         message.acknowledge();
      }

      session.commit();
      session.close();
   }

   private ClientMessage receiveMessage() throws Exception {
      final ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

      session.start();
      ClientConsumer consumer = session.createConsumer(ADDRESS);

      ClientMessage message = consumer.receive(1000);

      session.commit();

      if (message != null) {
         message.acknowledge();
      }

      consumer.close();

      session.close();

      return message;
   }

   private ActiveMQServer newActiveMQServer() throws Exception {
      final ActiveMQServer server = createServer(true, isNetty());

      final AddressSettings settings = new AddressSettings().setMaxSizeBytes(67108864).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE).setMaxRedeliveryDelay(3600000).setRedeliveryMultiplier(2.0).setRedeliveryDelay(500);

      server.getAddressSettingsRepository().addMatch("#", settings);

      return server;
   }

}

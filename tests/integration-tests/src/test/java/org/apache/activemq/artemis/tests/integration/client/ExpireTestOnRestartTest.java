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
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Before;
import org.junit.Test;

public class ExpireTestOnRestartTest extends ActiveMQTestBase {

   ActiveMQServer server;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(true);
      AddressSettings setting = new AddressSettings().setExpiryAddress(SimpleString.toSimpleString("exp")).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE).setPageSizeBytes(100 * 1024).setMaxSizeBytes(200 * 1024).setMaxReadPageBytes(-1).setMaxReadPageMessages(-1);
      server.getConfiguration().setJournalSyncNonTransactional(false);
      server.getConfiguration().setMessageExpiryScanPeriod(-1);
      server.getConfiguration().setJournalSyncTransactional(false);
      server.getAddressSettingsRepository().addMatch("#", setting);
      server.start();
   }

   @Test
   public void testRestartWithExpireAndPaging() throws Exception {
      int NUMBER_OF_EXPIRED_MESSAGES = 1000;
      ServerLocator locator = createInVMNonHALocator();
      locator.setBlockOnDurableSend(false);
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(true, true);

      session.createQueue(new QueueConfiguration("test"));
      session.createQueue(new QueueConfiguration("exp"));
      ClientProducer prod = session.createProducer("test");

      for (int i = 0; i < 10; i++) {
         ClientMessage message = session.createMessage(true);
         message.getBodyBuffer().writeBytes(new byte[1024 * 10]);
         message.putStringProperty("expiryStatus", "not Expiring");
         prod.send(message);
      }

      for (int i = 0; i < NUMBER_OF_EXPIRED_MESSAGES; i++) {
         ClientMessage message = session.createMessage(true);
         message.putIntProperty("i", i);
         message.putStringProperty("expiryStatus", "Will Expire");
         message.getBodyBuffer().writeBytes(new byte[1024 * 10]);
         message.setExpiration(System.currentTimeMillis() + 1000);
         prod.send(message);
      }

      session.commit();

      session.close();

      server.stop();
      server.getConfiguration().setMessageExpiryScanPeriod(1);

      Thread.sleep(1500); // enough time for expiration of the messages

      server.start();

      Queue queue = server.locateQueue(SimpleString.toSimpleString("test"));

      factory = locator.createSessionFactory();
      session = factory.createSession(false, false);

      ClientConsumer cons = session.createConsumer("test");
      session.start();
      for (int i = 0; i < 10; i++) {
         ClientMessage msg = cons.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
      }
      session.commit();

      assertNull(cons.receiveImmediate());
      cons.close();

      cons = session.createConsumer("exp");
      for (int i = 0; i < NUMBER_OF_EXPIRED_MESSAGES; i++) {
         ClientMessage msg = cons.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      session.commit();

      int extras = 0;
      ClientMessage msg;
      while ((msg = cons.receiveImmediate()) != null) {
         instanceLog.debug("{}", msg);
         extras++;
      }

      assertEquals("Received extra messages on expire address", 0, extras);

      session.commit();

      session.close();

      locator.close();

      Wait.assertFalse(queue.getPagingStore()::isPaging, 5000, 100);
   }

}

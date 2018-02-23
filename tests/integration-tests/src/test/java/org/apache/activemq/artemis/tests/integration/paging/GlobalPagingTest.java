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

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GlobalPagingTest extends PagingTest {

   public GlobalPagingTest(StoreConfiguration.StoreType storeType, boolean mapped) {
      super(storeType, mapped);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }

   @Override
   protected ActiveMQServer createServer(final boolean realFiles,
                                         final Configuration configuration,
                                         final long pageSize,
                                         final long maxAddressSize,
                                         final Map<String, AddressSettings> settings) {
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(configuration, realFiles));

      if (settings != null) {
         for (Map.Entry<String, AddressSettings> setting : settings.entrySet()) {
            server.getAddressSettingsRepository().addMatch(setting.getKey(), setting.getValue());
         }
      }

      server.getConfiguration().setGlobalMaxSize(maxAddressSize);
      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(pageSize).setMaxSizeBytes(-1).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   // test doesn't make sense on GlobalPaging due to configuration issues
   @Test @Ignore @Override
   public void testPurge() throws Exception {
   }

   @Test
   public void testPagingOverFullDisk() throws Exception {
      if (storeType == StoreConfiguration.StoreType.DATABASE) return;

      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
      server.getConfiguration().setGlobalMaxSize(-1);

      server.start();

      ActiveMQServerImpl serverImpl = (ActiveMQServerImpl) server;
      serverImpl.getMonitor().stop(); // stop the scheduled executor, we will do it manually only
      serverImpl.getMonitor().tick();

      final int numberOfMessages = 500;

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, false, false);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      final byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      Queue queue = server.locateQueue(ADDRESS);
      queue.getPageSubscription().getPagingStore().forceAnotherPage();

      sendFewMessages(numberOfMessages, session, producer, body);

      serverImpl.getMonitor().setMaxUsage(0); // forcing disk full (faking it)

      serverImpl.getMonitor().tick();

      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               sendFewMessages(numberOfMessages, session, producer, body);
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      };

      t.start();

      t.join(1000);
      Assert.assertTrue(t.isAlive());

      // releasing the disk
      serverImpl.getMonitor().setMaxUsage(1).tick();
      t.join(5000);
      Assert.assertFalse(t.isAlive());

      session.start();

      assertEquals(numberOfMessages * 2, getMessageCount(queue));

      // The consumer has to be created after the getMessageCount(queue) assertion
      // otherwise delivery could alter the messagecount and give us a false failure
      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);
      ClientMessage msg = null;

      for (int i = 0; i < numberOfMessages * 2; i++) {
         msg = consumer.receive(1000);
         assertNotNull(msg);
         msg.acknowledge();
         if (i % 500 == 0) {
            session.commit();
         }
      }
      session.commit();

      assertEquals(0, getMessageCount(queue));
   }

   protected void sendFewMessages(int numberOfMessages,
                                  ClientSession session,
                                  ClientProducer producer,
                                  byte[] body) throws ActiveMQException {
      ClientMessage message;
      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }
      session.commit();
   }

}

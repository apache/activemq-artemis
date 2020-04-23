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
package org.apache.activemq.artemis.tests.integration.cluster.failover;

import java.util.HashMap;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.RetryMethod;
import org.apache.activemq.artemis.utils.RetryRule;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class ReplicatedPagedFailoverTest extends ReplicatedFailoverTest {

   @Rule
   public RetryRule retryRule = new RetryRule(0);

   @Override
   protected ActiveMQServer createInVMFailoverServer(final boolean realFiles,
                                                     final Configuration configuration,
                                                     final NodeManager nodeManager,
                                                     int id) {
      return createInVMFailoverServer(realFiles, configuration, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>(), nodeManager, id);
   }

   @Override
   @Test
   public void testFailWithBrowser() throws Exception {
      internalBrowser(0);
   }

   @Test
   public void testFailWithBrowserWithClose() throws Exception {
      internalBrowser(1);
   }

   @Test
   public void testFailWithBrowserWithDelete() throws Exception {
      internalBrowser(2);
   }

   @Override
   @RetryMethod(retries = 2)
   @Test(timeout = 120000)
   public void testReplicatedFailback() throws Exception {
      super.testReplicatedFailback();
   }

   @Override
   @RetryMethod(retries = 2)
   @Test(timeout = 120000)
   public void testFailoverOnInitialConnection() throws Exception {
      super.testFailoverOnInitialConnection();
   }

   //
   // 0 - no tamper
   // 1 - close files
   // 2 - remove files
   private void internalBrowser(int temperMode) throws Exception {
      int numMessages = 50;
      int messagesPerPage = 10;
      int iterations = 10;
      createSessionFactory();
      ClientSession session = createSession(sf, true, true);

      session.createQueue(new QueueConfiguration(FailoverTestBase.ADDRESS).setDurable(false));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      Queue queue = liveServer.getServer().locateQueue(FailoverTest.ADDRESS);

      for (int j = 0; j < iterations; j++) {
         System.err.println("#iteration " + j);
         queue.getPageSubscription().getPagingStore().startPaging();
         Assert.assertNotNull(queue);

         for (int i = 0; i < numMessages; i++) {
            // some are durable, some are not!
            producer.send(createMessage(session, i, i % 2 == 0));
            if (i > 0 && i % messagesPerPage == 0) {
               queue.getPageSubscription().getPagingStore().forceAnotherPage();
            }
         }

         ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS, true);

         session.start();

         while (true) {
            ClientMessage msg = consumer.receive(500);
            if (msg == null) {
               break;
            }
         }
         consumer.close();

         PagingStore store = queue.getPageSubscription().getPagingStore();

         if (temperMode == 1) {
            // this is tampering with the system causing an artifical issue. The system should still heal itself.
            for (long pageID = store.getFirstPage(); pageID <= store.getCurrentPage().getPageId() + 10; pageID++) {
               liveServer.getServer().getStorageManager().pageClosed(store.getStoreName(), (int) pageID);
            }
         }  else if (temperMode == 2) {
            // this is tampering with the system causing an artifical issue. The system should still heal itself.
            for (long pageID = store.getFirstPage(); pageID <= store.getCurrentPage().getPageId() + 10; pageID++) {
               liveServer.getServer().getStorageManager().pageDeleted(store.getStoreName(), (int) pageID);
            }
         }
         store.getFirstPage();
         store.getCurrentPage().getPageId();

         consumer = session.createConsumer(FailoverTestBase.ADDRESS, false);
         session.start();

         while (true) {
            ClientMessage msg = consumer.receive(500);
            if (msg == null) {
               break;
            }
            msg.acknowledge();
         }
         consumer.close();

         Wait.assertFalse(queue.getPageSubscription().getPagingStore()::isPaging);
      }

   }
}

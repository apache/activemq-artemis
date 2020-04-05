/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.extras.byteman;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PagedReference;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class RaceOnCursorIteratorTest extends ActiveMQTestBase {

   private static ServerLocator locator;

   private static ActiveMQServer server;

   private static ClientSessionFactory sf;

   private static ClientSession session;

   private static Queue queue;

   private static final ReentrantReadWriteLock.ReadLock lock = new ReentrantReadWriteLock().readLock();

   private static final int PAGE_MAX = 100 * 1024;

   private static final int PAGE_SIZE = 10 * 1024;

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   static boolean skipLivePageCache = false;

   static boolean skipNullPageCache = false;

   static boolean moveNextPageCalled = false;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      skipLivePageCache = false;
      skipNullPageCache = false;
      moveNextPageCalled = false;
      locator = createInVMNonHALocator();

      clearDataRecreateServerDirs();

      Configuration config = createDefaultConfig(false);

      config.setJournalSyncNonTransactional(false);

      HashMap<String, AddressSettings> map = new HashMap<>();
      AddressSettings value = new AddressSettings();
      map.put(ADDRESS.toString(), value);
      server = createServer(true, config, PAGE_SIZE, PAGE_MAX, map);

      server.start();

      locator = createInVMNonHALocator();

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(false);
      locator.setConsumerWindowSize(0);

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.createQueue(new QueueConfiguration(ADDRESS));

      queue = server.locateQueue(ADDRESS);
      queue.getPageSubscription().getPagingStore().startPaging();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      session.close();
      sf.close();
      locator.close();
      server.stop();
      super.tearDown();
   }

   public static void raceAddLivePageCache() throws Exception {
      if (skipLivePageCache) {
         createMessage(1);

         queue.getPageSubscription().getPagingStore().forceAnotherPage();

         createMessage(2);
      }
      moveNextPageCalled = true;
   }

   public static void raceAddTwoCaches() throws Exception {
      if (skipNullPageCache && moveNextPageCalled) {
         createMessage(1);

         queue.getPageSubscription().getPagingStore().forceAnotherPage();

         createMessage(2);
      }
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "raceLiveCache",
         targetClass = "org.apache.activemq.artemis.core.paging.cursor.impl.PageSubscriptionImpl",
         targetMethod = "moveNextPage",
         targetLocation = "EXIT",
         action = "org.apache.activemq.artemis.tests.extras.byteman.RaceOnCursorIteratorTest.raceAddLivePageCache()")})
   public void testSkipLivePageCache() {
      skipLivePageCache = true;
      // Simulate scenario #1 depicted in https://issues.apache.org/jira/browse/ARTEMIS-2418
      PagedReference ref = queue.getPageSubscription().iterator().next();
      assertTrue("first msg should not be " + (ref == null ? "null" : ref.getPagedMessage().getMessage().getMessageID()),
                 ref == null || ref.getPagedMessage().getMessage().getMessageID() == 1);
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "raceLiveCache",
         targetClass = "org.apache.activemq.artemis.core.paging.cursor.impl.PageSubscriptionImpl",
         targetMethod = "moveNextPage",
         targetLocation = "EXIT",
         action = "org.apache.activemq.artemis.tests.extras.byteman.RaceOnCursorIteratorTest.raceAddLivePageCache()"),
         @BMRule(
            name = "raceNullCache",
            targetClass = "org.apache.activemq.artemis.core.paging.cursor.impl.PageCursorProviderImpl",
            targetMethod = "getPageCache",
            targetLocation = "EXIT",
            action = "org.apache.activemq.artemis.tests.extras.byteman.RaceOnCursorIteratorTest.raceAddTwoCaches()")})
   public void testSkipNullPageCache() throws Exception {
      skipNullPageCache = true;
      // Simulate scenario #2 depicted in https://issues.apache.org/jira/browse/ARTEMIS-2418
      queue.getPageSubscription().getPagingStore().getCurrentPage().close(false);

      PagedReference ref = queue.getPageSubscription().iterator().next();
      assertTrue("first msg should not be " + (ref == null ? "null" : ref.getPagedMessage().getMessage().getMessageID()),
         ref == null || ref.getPagedMessage().getMessage().getMessageID() == 1);
   }

   private static CoreMessage createMessage(final long id) throws Exception {
      ActiveMQBuffer buffer = createRandomBuffer(0, 10);
      PagingStore pagingStore = queue.getPageSubscription().getPagingStore();

      CoreMessage msg = new CoreMessage(id, 50 + buffer.capacity());

      msg.setAddress(ADDRESS);

      msg.getBodyBuffer().resetReaderIndex();
      msg.getBodyBuffer().resetWriterIndex();

      msg.getBodyBuffer().writeBytes(buffer, buffer.capacity());

      final RoutingContextImpl ctx = new RoutingContextImpl(null);
      ctx.addQueue(ADDRESS, queue);
      pagingStore.page(msg, ctx.getTransaction(), ctx.getContextListing(ADDRESS), lock);

      return msg;
   }

   protected static ActiveMQBuffer createRandomBuffer(final long id, final int size) {
      return RandomUtil.randomBuffer(size, id);
   }
}

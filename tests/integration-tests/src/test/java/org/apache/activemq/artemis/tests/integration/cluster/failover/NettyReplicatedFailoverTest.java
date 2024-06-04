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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.server.impl.SharedNothingBackupActivation;
import org.apache.activemq.artemis.tests.integration.cluster.util.SameProcessActiveMQServer;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyReplicatedFailoverTest extends NettyFailoverInVMTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Override
   protected TestableServer createTestableServer(Configuration config) {
      return new SameProcessActiveMQServer(createServer(true, config));
   }

   @Override
   protected void createConfigs() throws Exception {
      createReplicatedConfigs();
   }

   @Override
   protected final void crash(boolean waitFailure, ClientSession... sessions) throws Exception {
      if (sessions.length > 0) {
         for (ClientSession session : sessions) {
            waitForRemoteBackup(session.getSessionFactory(), 5, true, backupServer.getServer());
         }
      } else {
         waitForRemoteBackup(null, 5, true, backupServer.getServer());
      }
      super.crash(waitFailure, sessions);
   }

   @Override
   protected final void crash(ClientSession... sessions) throws Exception {
      crash(true, sessions);
   }

   @Test
   public void testPagedInSync() throws Exception {

      String queueName = "testPagedInSync";

      ConnectionFactory factory = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
      try (Connection conn = factory.createConnection()) {
         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue(queueName);
         MessageProducer producer = session.createProducer(queue);
         producer.send(session.createTextMessage("hello"));
         session.commit();

         org.apache.activemq.artemis.core.server.Queue serverQueue = primaryServer.getServer().locateQueue(queueName);
         Assertions.assertNotNull(serverQueue);

         serverQueue.getPagingStore().startPaging();

         for (int i = 0; i < 50; i++) {
            producer.send(session.createTextMessage("hello"));
            session.commit();
            serverQueue.getPagingStore().forceAnotherPage();
         }
         backupServer.stop();
         backupServer.start();
         Wait.assertTrue(backupServer.getServer()::isReplicaSync);

         SharedNothingBackupActivation activation = (SharedNothingBackupActivation) backupServer.getServer().getActivation();
         Map<Long, Page> currentPages = activation.getReplicationEndpoint().getPageIndex().get(SimpleString.of(queueName));

         logger.info("There are {} page files open", currentPages.size());
         Wait.assertTrue(() -> currentPages.size() <= 1, 10_000);

         producer.send(session.createTextMessage("on currentPage"));
         session.commit();

         PagingStore store = primaryServer.getServer().getPagingManager().getPageStore(SimpleString.of(queueName));
         Page currentPage = store.getCurrentPage();
         logger.info("Page {}", currentPage.getPageId());

         Page depaged = null;
         for (; ; ) {
            depaged = store.depage();
            if (depaged == null || currentPage.getPageId() == depaged.getPageId()) {
               break;
            }
            logger.info("depage :: {} and currentPageID={}", depaged.getPageId(), currentPage.getPageId());
         }

         Assertions.assertNotNull(depaged);

         logger.info("Depaged:: {}", depaged.getPageId());

         for (int i = 0;  i < 10; i++) {
            producer.send(session.createTextMessage("on current page"));
            session.commit();
            store.depage();
         }

         logger.info("Size:: {}", currentPages.size());

         Wait.assertTrue(() -> currentPages.size() <= 1, 10_000);

      }
   }

}

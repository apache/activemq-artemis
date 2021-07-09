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
package org.apache.activemq.artemis.tests.integration.cluster.failover.quorum;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.component.WebServerComponent;
import org.apache.activemq.artemis.core.config.ha.DistributedPrimitiveManagerConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationPrimaryPolicyConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.ServiceComponent;
import org.apache.activemq.artemis.dto.AppDTO;
import org.apache.activemq.artemis.dto.WebServerDTO;
import org.apache.activemq.artemis.quorum.MutableLong;
import org.apache.activemq.artemis.quorum.file.FileBasedPrimitiveManager;
import org.apache.activemq.artemis.tests.integration.cluster.failover.FailoverTest;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.artemis.tests.util.Wait;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

public class PluggableQuorumNettyNoGroupNameReplicatedFailoverTest extends FailoverTest {
   private static final Logger log = Logger.getLogger(PluggableQuorumReplicatedLargeMessageFailoverTest.class);

   protected void beforeWaitForRemoteBackupSynchronization() {
   }

   private void waitForSync(ActiveMQServer server) throws Exception {
      Wait.waitFor(server::isReplicaSync);
   }

   /**
    * Default maxSavedReplicatedJournalsSize is 2, this means the backup will fall back to replicated only twice, after this
    * it is stopped permanently.
    */
   @Test(timeout = 120000)
   public void testReplicatedFailback() throws Exception {
      try {
         beforeWaitForRemoteBackupSynchronization();

         waitForSync(backupServer.getServer());

         createSessionFactory();

         ClientSession session = createSession(sf, true, true);

         session.createQueue(new QueueConfiguration(ADDRESS));

         crash(session);

         liveServer.start();

         waitForSync(liveServer.getServer());

         waitForSync(backupServer.getServer());

         waitForServerToStart(liveServer.getServer());

         session = createSession(sf, true, true);

         crash(session);

         liveServer.start();

         waitForSync(liveServer.getServer());

         waitForSync(backupServer.getServer());

         waitForServerToStart(liveServer.getServer());

         session = createSession(sf, true, true);

         crash(session);

         liveServer.start();

         waitForSync(liveServer.getServer());

         liveServer.getServer().waitForActivation(5, TimeUnit.SECONDS);

         waitForSync(liveServer.getServer());

         waitForServerToStart(backupServer.getServer());

         assertTrue(backupServer.getServer().isStarted());

      } finally {
         if (sf != null) {
            sf.close();
         }
         try {
            liveServer.getServer().stop();
         } catch (Throwable ignored) {
         }
         try {
            backupServer.getServer().stop();
         } catch (Throwable ignored) {
         }
      }
   }

   @Test
   public void testReplicatedFailbackBackupFromLiveBackToBackup() throws Exception {

      InetSocketAddress address = new InetSocketAddress("127.0.0.1", 8787);
      HttpServer httpServer = HttpServer.create(address, 100);
      httpServer.start();

      try {
         httpServer.createContext("/", new HttpHandler() {
            @Override
            public void handle(HttpExchange t) throws IOException {
               String response = "<html><body><b>This is a unit test</b></body></html>";
               t.sendResponseHeaders(200, response.length());
               OutputStream os = t.getResponseBody();
               os.write(response.getBytes());
               os.close();
            }
         });
         WebServerDTO wdto = new WebServerDTO();
         AppDTO appDTO = new AppDTO();
         appDTO.war = "console.war";
         appDTO.url = "console";
         wdto.apps = new ArrayList<AppDTO>();
         wdto.apps.add(appDTO);
         wdto.bind = "http://localhost:0";
         wdto.path = "console";
         WebServerComponent webServerComponent = new WebServerComponent();
         webServerComponent.configure(wdto, ".", ".");
         webServerComponent.start();

         backupServer.getServer().getNetworkHealthCheck().parseURIList("http://localhost:8787");
         Assert.assertTrue(backupServer.getServer().getNetworkHealthCheck().isStarted());
         backupServer.getServer().addExternalComponent(webServerComponent, false);
         // this is called when backup servers go from live back to backup
         backupServer.getServer().fail(true);
         Assert.assertTrue(backupServer.getServer().getNetworkHealthCheck().isStarted());
         Assert.assertTrue(backupServer.getServer().getExternalComponents().get(0).isStarted());
         ((ServiceComponent) (backupServer.getServer().getExternalComponents().get(0))).stop(true);
      } finally {
         httpServer.stop(0);
      }

   }

   @Override
   protected void createConfigs() throws Exception {
      createPluggableReplicatedConfigs();
   }

   @Override
   protected void setupHAPolicyConfiguration() {
      ((ReplicationPrimaryPolicyConfiguration) liveConfig.getHAPolicyConfiguration())
         .setCheckForLiveServer(true);
      ((ReplicationBackupPolicyConfiguration) backupConfig.getHAPolicyConfiguration())
         .setMaxSavedReplicatedJournalsSize(2)
         .setAllowFailBack(true);
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return getNettyAcceptorTransportConfiguration(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return getNettyConnectorTransportConfiguration(live);
   }

   @Override
   protected void crash(boolean waitFailure, ClientSession... sessions) throws Exception {
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
   protected void crash(ClientSession... sessions) throws Exception {
      if (sessions.length > 0) {
         for (ClientSession session : sessions) {
            waitForRemoteBackup(session.getSessionFactory(), 5, true, backupServer.getServer());
         }
      } else {
         waitForRemoteBackup(null, 5, true, backupServer.getServer());
      }
      super.crash(sessions);
   }

   @Override
   protected void decrementActivationSequenceForForceRestartOf(TestableServer testableServer) throws Exception {
      doDecrementActivationSequenceForForceRestartOf(log, nodeManager, managerConfiguration);
   }

   public static void doDecrementActivationSequenceForForceRestartOf(Logger log, NodeManager nodeManager, DistributedPrimitiveManagerConfiguration distributedPrimitiveManagerConfiguration) throws Exception {
      nodeManager.start();
      long localActivation = nodeManager.readNodeActivationSequence();
      // file based
      FileBasedPrimitiveManager fileBasedPrimitiveManager = new FileBasedPrimitiveManager(distributedPrimitiveManagerConfiguration.getProperties());
      fileBasedPrimitiveManager.start();
      try {
         MutableLong mutableLong = fileBasedPrimitiveManager.getMutableLong(nodeManager.getNodeId().toString());

         if (!mutableLong.compareAndSet(localActivation + 1, localActivation)) {
            throw new Exception("Failed to decrement coordinated activation sequence to:" + localActivation + ", not +1 : " + mutableLong.get());
         }
         log.warn("Intentionally decrementing coordinated activation sequence for test, may result is lost data");

      } finally {
         fileBasedPrimitiveManager.stop();
         nodeManager.stop();
      }
   }

}

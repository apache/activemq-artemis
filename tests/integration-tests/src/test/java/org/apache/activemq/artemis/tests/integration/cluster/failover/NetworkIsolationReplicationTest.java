/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.cluster.failover;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

public class NetworkIsolationReplicationTest extends FailoverTestBase {

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getNettyAcceptor(live, 1);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getNettyConnector(live, 1);
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean xa,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return addClientSession(sf1.createSession(xa, autoCommitSends, autoCommitAcks));
   }

   @Test
   public void testDoNotActivateOnIsolation() throws Exception {
      ServerLocator locator = getServerLocator();

      backupServer.getServer().getNetworkHealthCheck().addAddress(InetAddress.getByName("203.0.113.1"));

      ClientSessionFactory sf = addSessionFactory(locator.createSessionFactory());

      ClientSession session = createSession(sf, false, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      Assert.assertFalse(backupServer.getServer().getNetworkHealthCheck().check());

      crash(false, true, session);

      for (int i = 0; i < 1000 && !backupServer.isStarted(); i++) {
         Thread.sleep(10);
      }

      Assert.assertTrue(backupServer.isStarted());
      Assert.assertFalse(backupServer.isActive());

      liveServer.start();

      for (int i = 0; i < 1000 && backupServer.getServer().getReplicationEndpoint() != null && !backupServer.getServer().getReplicationEndpoint().isStarted(); i++) {
         Thread.sleep(10);
      }

      backupServer.getServer().getNetworkHealthCheck().clearAddresses();

      // This will make sure the backup got synchronized after the network was activated again
      Assert.assertTrue(backupServer.getServer().getReplicationEndpoint().isStarted());
   }

   @Test
   public void testLiveIsolated() throws Exception {
      backupServer.stop();

      liveServer.stop();
      liveServer.getServer().getConfiguration().setNetworkCheckList("203.0.113.1").
         setNetworkCheckPeriod(100).setNetworkCheckTimeout(100);

      liveServer.start();

      Assert.assertEquals(100L, liveServer.getServer().getNetworkHealthCheck().getPeriod());

      liveServer.getServer().getNetworkHealthCheck().setTimeUnit(TimeUnit.MILLISECONDS);

      Assert.assertFalse(liveServer.getServer().getNetworkHealthCheck().check());

      long timeout = System.currentTimeMillis() + 30000;
      while (liveServer.isStarted() && System.currentTimeMillis() < timeout) {
         Thread.sleep(100);
      }

      Assert.assertFalse(liveServer.isStarted());

      liveServer.getServer().getNetworkHealthCheck().addAddress(InetAddress.getByName("127.0.0.1"));

      timeout = System.currentTimeMillis() + 30000;
      while (!liveServer.isStarted() && System.currentTimeMillis() < timeout) {
         Thread.sleep(100);
      }

      Assert.assertTrue(liveServer.isStarted());
   }

   @Override
   protected void createConfigs() throws Exception {
      createReplicatedConfigs();
   }

   @Override
   protected void crash(boolean failover, boolean waitFailure, ClientSession... sessions) throws Exception {
      if (sessions.length > 0) {
         for (ClientSession session : sessions) {
            waitForRemoteBackup(session.getSessionFactory(), 5, true, backupServer.getServer());
         }
      } else {
         waitForRemoteBackup(null, 5, true, backupServer.getServer());
      }
      super.crash(failover, waitFailure, sessions);
   }
}

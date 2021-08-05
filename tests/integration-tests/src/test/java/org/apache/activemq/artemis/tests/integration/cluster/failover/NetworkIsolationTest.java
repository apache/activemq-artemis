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

import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.apache.activemq.artemis.tests.util.Wait;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NetworkIsolationTest extends FailoverTestBase {

   private static final Logger logger = Logger.getLogger(NetworkIsolationTest.class);

   // This address is guaranteed to fail... reserved for documentation https://tools.ietf.org/html/rfc5737
   private static final String badAddress = "203.0.113.1";

   @Before
   @Override
   public void setUp() throws Exception {
      this.startBackupServer = false;
      super.setUp();
   }

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
   public void testReactivate() throws Exception {
      liveServer.getServer().getConfiguration().setNetworkCheckPeriod(100).setNetworkCheckTimeout(200);
      liveServer.start();

      Assert.assertTrue(Wait.waitFor(liveServer::isActive));

      liveServer.getServer().getNetworkHealthCheck().addAddress(badAddress);

      Wait.assertFalse(liveServer::isStarted);

      liveServer.getServer().getNetworkHealthCheck().clearAddresses();

      Assert.assertTrue(Wait.waitFor(liveServer::isStarted));
   }

   @Test
   public void testDoNotActivateOnIsolation() throws Exception {
      AssertionLoggerHandler.startCapture();

      try {
         ServerLocator locator = getServerLocator();

         // this block here is just to validate if ignoring loopback addresses logic is in place
         {
            backupServer.getServer().getNetworkHealthCheck().addAddress("127.0.0.1");

            Assert.assertTrue(AssertionLoggerHandler.findText("AMQ202001"));

            AssertionLoggerHandler.clear();

            backupServer.getServer().getNetworkHealthCheck().setIgnoreLoopback(true).addAddress("127.0.0.1");

            Assert.assertFalse(AssertionLoggerHandler.findText("AMQ202001"));

            backupServer.getServer().getNetworkHealthCheck().clearAddresses();
         }

         backupServer.getServer().getNetworkHealthCheck().addAddress(badAddress);
         backupServer.getServer().start();

         ClientSessionFactory sf = addSessionFactory(locator.createSessionFactory());

         ClientSession session = createSession(sf, false, true, true);

         session.createQueue(new QueueConfiguration(FailoverTestBase.ADDRESS));

         Assert.assertFalse(backupServer.getServer().getNetworkHealthCheck().check());

         crash(false, true, session);

         for (int i = 0; i < 1000 && !backupServer.isStarted(); i++) {
            Thread.sleep(10);
         }

         Assert.assertTrue(backupServer.isStarted());
         Assert.assertFalse(backupServer.isActive());

         liveServer.start();

         for (int i = 0; i < 1000 && getReplicationEndpoint(backupServer.getServer()) != null && !getReplicationEndpoint(backupServer.getServer()).isStarted(); i++) {
            Thread.sleep(10);
         }

         backupServer.getServer().getNetworkHealthCheck().clearAddresses();

         // This will make sure the backup got synchronized after the network was activated again
         Assert.assertTrue(getReplicationEndpoint(backupServer.getServer()).isStarted());
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

   @Test
   public void testLiveIsolated() throws Exception {
      backupServer.stop();

      FakeServiceComponent component = new FakeServiceComponent("Component for " + getName());

      liveServer.getServer().addExternalComponent(component, true);
      liveServer.getServer().getConfiguration().setNetworkCheckList(badAddress).
         setNetworkCheckPeriod(100).setNetworkCheckTimeout(100);
      ((ActiveMQServerImpl)liveServer.getServer()).reloadNetworkHealthCheck();

      try {

         Assert.assertEquals(100L, liveServer.getServer().getNetworkHealthCheck().getPeriod());

         liveServer.getServer().getNetworkHealthCheck().setTimeUnit(TimeUnit.MILLISECONDS);

         Assert.assertFalse(liveServer.getServer().getNetworkHealthCheck().check());

         Wait.assertFalse(liveServer::isStarted);

         liveServer.getServer().getNetworkHealthCheck().setIgnoreLoopback(true).addAddress("127.0.0.1");

         Wait.assertTrue(liveServer::isStarted);

         Assert.assertTrue(component.isStarted());
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
         throw e;
      } finally {
         liveServer.getServer().stop();
         backupServer.getServer().stop();
      }

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

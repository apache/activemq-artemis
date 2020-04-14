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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.FailoverEventType;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.artemis.utils.RetryMethod;
import org.apache.activemq.artemis.utils.RetryRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class ReplicatedMultipleServerFailoverExtraBackupsTest extends ReplicatedMultipleServerFailoverTest {

   @Rule
   public RetryRule retryRule = new RetryRule();

   private void waitForSync(ActiveMQServer server) throws Exception {
      Wait.waitFor(server::isReplicaSync);
   }

   @RetryMethod(retries = 1)
   @Override
   @Test
   public void testStartLiveFirst() throws Exception {
      switch (haType()) {
         case SharedNothingReplication:
            ((ReplicaPolicyConfiguration) backupServers.get(2).getServer().getConfiguration().getHAPolicyConfiguration()).setGroupName(getNodeGroupName() + "-0");
            ((ReplicaPolicyConfiguration) backupServers.get(3).getServer().getConfiguration().getHAPolicyConfiguration()).setGroupName(getNodeGroupName() + "-1");
            break;
         case PluggableQuorumReplication:
            ((ReplicationBackupPolicyConfiguration) backupServers.get(2).getServer().getConfiguration().getHAPolicyConfiguration()).setGroupName(getNodeGroupName() + "-0");
            ((ReplicationBackupPolicyConfiguration) backupServers.get(3).getServer().getConfiguration().getHAPolicyConfiguration()).setGroupName(getNodeGroupName() + "-1");
            break;
      }

      startServers(liveServers);
      backupServers.get(0).start();
      backupServers.get(1).start();
      waitForSync(backupServers.get(0).getServer());
      waitForSync(backupServers.get(1).getServer());

      // wait to start the other 2 backups so the first 2 can sync with the 2 live servers
      backupServers.get(2).start();
      backupServers.get(3).start();

      sendCrashReceive();
      Wait.assertTrue(backupServers.get(0)::isActive, 5000, 10);
      waitForTopology(backupServers.get(0).getServer(), liveServers.size(), 2);
      sendCrashBackupReceive();
   }

   private void waitForBackups() throws InterruptedException {
      for (TestableServer backupServer : backupServers) {
         waitForComponent(backupServer.getServer(), 5);
      }
   }

   private void startServers(List<TestableServer> servers) throws Exception {
      for (TestableServer testableServer : servers) {
         testableServer.start();
      }
   }

   @Override
   @Test
   public void testStartBackupFirst() throws Exception {
      switch (haType()) {
         case SharedNothingReplication:
            ((ReplicaPolicyConfiguration) backupServers.get(2).getServer().getConfiguration().getHAPolicyConfiguration()).setGroupName(getNodeGroupName() + "-0");
            ((ReplicaPolicyConfiguration) backupServers.get(3).getServer().getConfiguration().getHAPolicyConfiguration()).setGroupName(getNodeGroupName() + "-1");
            break;
         case PluggableQuorumReplication:
            ((ReplicationBackupPolicyConfiguration) backupServers.get(2).getServer().getConfiguration().getHAPolicyConfiguration()).setGroupName(getNodeGroupName() + "-0");
            ((ReplicationBackupPolicyConfiguration) backupServers.get(3).getServer().getConfiguration().getHAPolicyConfiguration()).setGroupName(getNodeGroupName() + "-1");
            break;
      }


      startServers(backupServers);
      startServers(liveServers);
      waitForBackups();

      waitForTopology(liveServers.get(0).getServer(), liveServers.size(), 2);
      sendCrashReceive();
   }

   protected void sendCrashBackupReceive() throws Exception {
      ServerLocator locator0 = getBackupServerLocator(0);
      ServerLocator locator1 = getBackupServerLocator(1);

      ClientSessionFactory factory0 = createSessionFactory(locator0);
      ClientSessionFactory factory1 = createSessionFactory(locator1);

      ClientSession session0 = factory0.createSession(false, true, true);
      ClientSession session1 = factory1.createSession(false, true, true);

      ClientProducer producer = session0.createProducer(ADDRESS);

      for (int i = 0; i < 200; i++) {
         ClientMessage message = session0.createMessage(true);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      producer.close();

      waitForDistribution(ADDRESS, backupServers.get(0).getServer(), 100);
      waitForDistribution(ADDRESS, backupServers.get(1).getServer(), 100);

      List<TestableServer> toCrash = new ArrayList<>();
      for (TestableServer backupServer : backupServers) {
         if (!backupServer.getServer().getHAPolicy().isBackup()) {
            toCrash.add(backupServer);
         }
      }

      CountDownLatch failoverHappened = new CountDownLatch(1);

      session0.addFailoverListener((FailoverEventType type) -> failoverHappened.countDown());

      for (TestableServer testableServer : toCrash) {
         testableServer.crash().await(10, TimeUnit.SECONDS);
         //if we dont stop the server it tries to replicate again and the test becomes non deterministic
         testableServer.stop();
      }

      Assert.assertTrue(failoverHappened.await(10, TimeUnit.SECONDS));

      ClientConsumer consumer0 = session0.createConsumer(ADDRESS);
      ClientConsumer consumer1 = session1.createConsumer(ADDRESS);
      session0.start();
      session1.start();

      for (int i = 0; i < 100; i++) {
         ClientMessage message = consumer0.receive(1000);
         assertNotNull("expecting durable msg " + i, message);
         message.acknowledge();
         message = consumer1.receive(1000);
         assertNotNull("expecting durable msg " + i, message);
         message.acknowledge();

      }
   }

   @Override
   public int getBackupServerCount() {
      return 4;
   }
}

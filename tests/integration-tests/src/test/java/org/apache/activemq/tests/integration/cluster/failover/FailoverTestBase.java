/**
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
package org.apache.activemq.tests.integration.cluster.failover;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClusterTopologyListener;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.api.core.client.TopologyMember;
import org.apache.activemq.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.core.remoting.impl.invm.InVMConnector;
import org.apache.activemq.core.remoting.impl.invm.InVMRegistry;
import org.apache.activemq.core.server.NodeManager;
import org.apache.activemq.core.server.cluster.ha.ReplicatedPolicy;
import org.apache.activemq.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.core.server.impl.InVMNodeManager;
import org.apache.activemq.tests.integration.cluster.util.SameProcessActiveMQServer;
import org.apache.activemq.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.tests.util.ReplicatedBackupUtils;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * A FailoverTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public abstract class FailoverTestBase extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   protected static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   /*
    * Used only by tests of large messages.
    */
   protected static final int MIN_LARGE_MESSAGE = 1024;
   private static final int LARGE_MESSAGE_SIZE = MIN_LARGE_MESSAGE * 3;

   protected static final int PAGE_MAX = 2 * 1024;
   protected static final int PAGE_SIZE = 1024;

   // Attributes ----------------------------------------------------

   protected TestableServer liveServer;

   protected TestableServer backupServer;

   protected Configuration backupConfig;

   protected Configuration liveConfig;

   protected NodeManager nodeManager;

   protected boolean startBackupServer = true;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      createConfigs();

      setLiveIdentity();
      liveServer.start();
      waitForServer(liveServer.getServer());

      if (backupServer != null)
      {
         setBackupIdentity();
         if (startBackupServer)
         {
            backupServer.start();
            waitForBackup();
         }
      }
   }

   protected void waitForBackup()
   {
      waitForRemoteBackupSynchronization(backupServer.getServer());
   }

   protected void setBackupIdentity()
   {
      backupServer.setIdentity(this.getClass()
                                  .getSimpleName() + "/backupServers");
   }

   protected void setLiveIdentity()
   {
      liveServer.setIdentity(this.getClass().getSimpleName() + "/liveServer");
   }

   protected TestableServer createTestableServer(Configuration config)
   {
      boolean isBackup = config.getHAPolicyConfiguration() instanceof ReplicaPolicyConfiguration ||
         config.getHAPolicyConfiguration() instanceof SharedStoreSlavePolicyConfiguration;
      return new SameProcessActiveMQServer(createInVMFailoverServer(true, config, nodeManager, isBackup ? 2 : 1));
   }

   protected TestableServer createColocatedTestableServer(Configuration config, NodeManager liveNodeManager,NodeManager backupNodeManager, int id)
   {
      return new SameProcessActiveMQServer(createColocatedInVMFailoverServer(true, config, liveNodeManager, backupNodeManager, id));
   }

   /**
    * Large message version of {@link #setBody(int, ClientMessage)}.
    *
    * @param i
    * @param message
    */
   protected static void setLargeMessageBody(final int i, final ClientMessage message)
   {
      try
      {
         message.setBodyInputStream(UnitTestCase.createFakeLargeStream(LARGE_MESSAGE_SIZE));
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   /**
    * Large message version of {@link #assertMessageBody(int, ClientMessage)}.
    *
    * @param i
    * @param message
    */
   protected static void assertLargeMessageBody(final int i, final ClientMessage message)
   {
      ActiveMQBuffer buffer = message.getBodyBuffer();

      for (int j = 0; j < LARGE_MESSAGE_SIZE; j++)
      {
         Assert.assertTrue("msg " + i + ", expecting " + LARGE_MESSAGE_SIZE + " bytes, got " + j, buffer.readable());
         Assert.assertEquals("equal at " + j, UnitTestCase.getSamplebyte(j), buffer.readByte());
      }
   }

   protected void createConfigs() throws Exception
   {
      nodeManager = new InVMNodeManager(false);
      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);

      backupConfig = super.createDefaultConfig()
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(getAcceptorTransportConfiguration(false))
         .setHAPolicyConfiguration(new SharedStoreSlavePolicyConfiguration()
                                      .setFailbackDelay(1000))
         .addConnectorConfiguration(liveConnector.getName(), liveConnector)
         .addConnectorConfiguration(backupConnector.getName(), backupConnector)
         .addClusterConfiguration(basicClusterConnectionConfig(backupConnector.getName(), liveConnector.getName()));

      backupServer = createTestableServer(backupConfig);

      liveConfig = super.createDefaultConfig()
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(getAcceptorTransportConfiguration(true))
         .setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration()
                                      .setFailbackDelay(1000))
         .addClusterConfiguration(basicClusterConnectionConfig(liveConnector.getName()))
         .addConnectorConfiguration(liveConnector.getName(), liveConnector);

      liveServer = createTestableServer(liveConfig);
   }

   protected void createReplicatedConfigs() throws Exception
   {
      final TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      final TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      final TransportConfiguration backupAcceptor = getAcceptorTransportConfiguration(false);

      backupConfig = createDefaultConfig();
      liveConfig = createDefaultConfig();

      ReplicatedBackupUtils.configureReplicationPair(backupConfig, backupConnector, backupAcceptor, liveConfig, liveConnector, null);

      final String suffix = "_backup";
      backupConfig.setBindingsDirectory(backupConfig.getBindingsDirectory() + suffix)
         .setJournalDirectory(backupConfig.getJournalDirectory() + suffix)
         .setPagingDirectory(backupConfig.getPagingDirectory() + suffix)
         .setLargeMessagesDirectory(backupConfig.getLargeMessagesDirectory() + suffix)
         .setSecurityEnabled(false);

      setupHAPolicyConfiguration();
      nodeManager = new InVMNodeManager(true, backupConfig.getJournalDirectory());

      backupServer = createTestableServer(backupConfig);

      liveConfig.clearAcceptorConfigurations()
         .addAcceptorConfiguration(getAcceptorTransportConfiguration(true));

      liveServer = createTestableServer(liveConfig);
   }

   protected void setupHAPolicyConfiguration()
   {
      ((ReplicaPolicyConfiguration) backupConfig.getHAPolicyConfiguration())
         .setMaxSavedReplicatedJournalsSize(0)
         .setAllowFailBack(true)
         .setFailbackDelay(5000);
   }

   protected final void adaptLiveConfigForReplicatedFailBack(TestableServer server)
   {
      Configuration configuration = server.getServer().getConfiguration();
      final TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      if (server.getServer().getHAPolicy().isSharedStore())
      {
         ClusterConnectionConfiguration cc = configuration.getClusterConfigurations().get(0);
         assertNotNull("cluster connection configuration", cc);
         assertNotNull("static connectors", cc.getStaticConnectors());
         cc.getStaticConnectors().add(backupConnector.getName());
         // backupConnector is only necessary for fail-back tests
         configuration.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);
         return;
      }
      ReplicatedPolicy haPolicy = (ReplicatedPolicy) server.getServer().getHAPolicy();
      haPolicy.setCheckForLiveServer(true);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      logAndSystemOut("#test tearDown");

      InVMConnector.failOnCreateConnection = false;

      super.tearDown();
      Assert.assertEquals(0, InVMRegistry.instance.size());

      backupServer = null;

      liveServer = null;

      nodeManager = null;

      try
      {
         ServerSocket serverSocket = new ServerSocket(5445);
         serverSocket.close();
      }
      catch (IOException e)
      {
         throw e;
      }
      try
      {
         ServerSocket serverSocket = new ServerSocket(5446);
         serverSocket.close();
      }
      catch (IOException e)
      {
         throw e;
      }
   }

   protected ClientSessionFactoryInternal
   createSessionFactoryAndWaitForTopology(ServerLocator locator, int topologyMembers) throws Exception
   {
      CountDownLatch countDownLatch = new CountDownLatch(topologyMembers);

      locator.addClusterTopologyListener(new LatchClusterTopologyListener(countDownLatch));

      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();
      addSessionFactory(sf);

      assertTrue("topology members expected " + topologyMembers, countDownLatch.await(5, TimeUnit.SECONDS));
      return sf;
   }

   /**
    * Waits for backup to be in the "started" state and to finish synchronization with its live.
    *
    * @param sessionFactory
    * @param seconds
    * @throws Exception
    */
   protected void waitForBackup(ClientSessionFactoryInternal sessionFactory, int seconds) throws Exception
   {
      final ActiveMQServerImpl actualServer = (ActiveMQServerImpl) backupServer.getServer();
      if (actualServer.getHAPolicy().isSharedStore())
      {
         waitForServer(actualServer);
      }
      else
      {
         waitForRemoteBackup(sessionFactory, seconds, true, actualServer);
      }
   }

   protected abstract TransportConfiguration getAcceptorTransportConfiguration(boolean live);

   protected abstract TransportConfiguration getConnectorTransportConfiguration(final boolean live);

   protected ServerLocatorInternal getServerLocator() throws Exception
   {
      ServerLocator locator = ActiveMQClient.createServerLocatorWithHA(getConnectorTransportConfiguration(true), getConnectorTransportConfiguration(false));
      locator.setRetryInterval(50);
      addServerLocator(locator);
      return (ServerLocatorInternal) locator;
   }


   protected void crash(final ClientSession... sessions) throws Exception
   {
      liveServer.crash(sessions);
   }

   protected void crash(final boolean waitFailure, final ClientSession... sessions) throws Exception
   {
      liveServer.crash(waitFailure, sessions);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   public static final class LatchClusterTopologyListener implements ClusterTopologyListener
   {
      final CountDownLatch latch;
      List<String> liveNode = new ArrayList<String>();
      List<String> backupNode = new ArrayList<String>();

      public LatchClusterTopologyListener(CountDownLatch latch)
      {
         this.latch = latch;
      }

      @Override
      public void nodeUP(TopologyMember topologyMember, boolean last)
      {
         if (topologyMember.getLive() != null && !liveNode.contains(topologyMember.getLive().getName()))
         {
            liveNode.add(topologyMember.getLive().getName());
            latch.countDown();
         }
         if (topologyMember.getBackup() != null && !backupNode.contains(topologyMember.getBackup().getName()))
         {
            backupNode.add(topologyMember.getBackup().getName());
            latch.countDown();
         }
      }

      @Override
      public void nodeDown(final long uniqueEventID, String nodeID)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }
   }
}

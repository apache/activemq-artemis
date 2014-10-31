/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.cluster.failover;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.cluster.ha.HAPolicy;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.ReplicatedBackupUtils;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;
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
      backupServer.setIdentity(this.getClass().getSimpleName() + "/backupServers");
   }

   protected void setLiveIdentity()
   {
      liveServer.setIdentity(this.getClass().getSimpleName() + "/liveServer");
   }

   protected TestableServer createTestableServer(Configuration config)
   {
      return new SameProcessHornetQServer(createInVMFailoverServer(true, config, nodeManager, config.getHAPolicy().isBackup() ? 2 : 1));
   }

   protected TestableServer createColocatedTestableServer(Configuration config, NodeManager liveNodeManager,NodeManager backupNodeManager, int id)
   {
      return new SameProcessHornetQServer(createColocatedInVMFailoverServer(true, config, liveNodeManager, backupNodeManager, id));
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
      HornetQBuffer buffer = message.getBodyBuffer();

      for (int j = 0; j < LARGE_MESSAGE_SIZE; j++)
      {
         Assert.assertTrue("msg " + i + ", expecting " + LARGE_MESSAGE_SIZE + " bytes, got " + j, buffer.readable());
         Assert.assertEquals("equal at " + j, UnitTestCase.getSamplebyte(j), buffer.readByte());
      }
   }

   protected void createConfigs() throws Exception
   {
      nodeManager = new InVMNodeManager(false);

      backupConfig = super.createDefaultConfig();
      backupConfig.getAcceptorConfigurations().clear();
      backupConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false));
      backupConfig.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_SHARED_STORE);
      backupConfig.getHAPolicy().setFailbackDelay(1000);

      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      backupConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      backupConfig.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);
      basicClusterConnectionConfig(backupConfig, backupConnector.getName(), liveConnector.getName());
      backupServer = createTestableServer(backupConfig);

      liveConfig = super.createDefaultConfig();
      liveConfig.getAcceptorConfigurations().clear();
      liveConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));
      liveConfig.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.SHARED_STORE);
      liveConfig.getHAPolicy().setFailbackDelay(1000);

      basicClusterConnectionConfig(liveConfig, liveConnector.getName());
      liveConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      liveServer = createTestableServer(liveConfig);
   }

   protected void createReplicatedConfigs() throws Exception
   {

      final TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      final TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      final TransportConfiguration backupAcceptor = getAcceptorTransportConfiguration(false);

      backupConfig = createDefaultConfig();
      liveConfig = createDefaultConfig();

      ReplicatedBackupUtils.configureReplicationPair(backupConfig, backupConnector, backupAcceptor, liveConfig,
                                                     liveConnector);

      final String suffix = "_backup";
      backupConfig.setBindingsDirectory(backupConfig.getBindingsDirectory() + suffix);
      backupConfig.setJournalDirectory(backupConfig.getJournalDirectory() + suffix);
      backupConfig.setPagingDirectory(backupConfig.getPagingDirectory() + suffix);
      backupConfig.setLargeMessagesDirectory(backupConfig.getLargeMessagesDirectory() + suffix);
      backupConfig.setSecurityEnabled(false);
      backupConfig.setMaxSavedReplicatedJournalSize(0);
      nodeManager = new InVMNodeManager(true, backupConfig.getJournalDirectory());

      backupServer = createTestableServer(backupConfig);
      liveConfig.getAcceptorConfigurations().clear();
      liveConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));

      liveServer = createTestableServer(liveConfig);
   }

   protected final void adaptLiveConfigForReplicatedFailBack(Configuration configuration)
   {
      final TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      if (configuration.getHAPolicy().isSharedStore())
      {
         ClusterConnectionConfiguration cc = configuration.getClusterConfigurations().get(0);
         assertNotNull("cluster connection configuration", cc);
         assertNotNull("static connectors", cc.getStaticConnectors());
         cc.getStaticConnectors().add(backupConnector.getName());
         // backupConnector is only necessary for fail-back tests
         configuration.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);
         return;
      }
      configuration.setCheckForLiveServer(true);
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
      final HornetQServerImpl actualServer = (HornetQServerImpl) backupServer.getServer();
      if (actualServer.getConfiguration().getHAPolicy().isSharedStore())
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
      ServerLocator locator = HornetQClient.createServerLocatorWithHA(getConnectorTransportConfiguration(true), getConnectorTransportConfiguration(false));
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

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

import java.util.ArrayList;
import java.util.List;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.cluster.ha.HAPolicy;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.TransportConfigurationUtils;
import org.junit.After;
import org.junit.Before;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         8/1/12
 */
public abstract class MultipleServerFailoverTestBase extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   protected static final SimpleString ADDRESS = new SimpleString("jms.queues.FailoverTestAddress");

   // Attributes ----------------------------------------------------

   protected List<TestableServer> liveServers = new ArrayList<TestableServer>();

   protected List<TestableServer> backupServers = new ArrayList<TestableServer>();

   protected List<Configuration> backupConfigs = new ArrayList<Configuration>();

   protected List<Configuration> liveConfigs = new ArrayList<Configuration>();

   protected List<NodeManager> nodeManagers;

   public abstract int getLiveServerCount();

   public abstract int getBackupServerCount();

   public abstract boolean useNetty();

   public abstract boolean isSharedStore();

   public abstract void createLiveClusterConfiguration(int server, Configuration configuration, int servers);

   public abstract void createBackupClusterConfiguration(int server, Configuration configuration, int servers);

   public abstract String getNodeGroupName();

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      liveServers = new ArrayList<TestableServer>();
      backupServers = new ArrayList<TestableServer>();
      backupConfigs = new ArrayList<Configuration>();
      liveConfigs = new ArrayList<Configuration>();
      for (int i = 0; i < getLiveServerCount(); i++)
      {
         Configuration configuration = createDefaultConfig(useNetty());
         configuration.getAcceptorConfigurations().clear();
         configuration.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true, i));

         if (isSharedStore())
            configuration.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.SHARED_STORE);
         else
            configuration.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.REPLICATED);

         if (!isSharedStore())
         {
            configuration.setBindingsDirectory(getBindingsDir(i, false));
            configuration.setJournalDirectory(getJournalDir(i, false));
            configuration.setPagingDirectory(getPageDir(i, false));
            configuration.setLargeMessagesDirectory(getLargeMessagesDir(i, false));
         }
         else
         {
            //todo
         }
         configuration.getHAPolicy().setFailbackDelay(1000);
         if (getNodeGroupName() != null)
         {
            configuration.getHAPolicy().setBackupGroupName(getNodeGroupName() + "-" + i);
         }
         createLiveClusterConfiguration(i, configuration, getLiveServerCount());
         liveConfigs.add(configuration);
         HornetQServer server = createServer(true, configuration);
         TestableServer hornetQServer = new SameProcessHornetQServer(server);
         hornetQServer.setIdentity("Live-" + i);
         liveServers.add(hornetQServer);
      }
      for (int i = 0; i < getBackupServerCount(); i++)
      {
         Configuration configuration = createDefaultConfig(useNetty());
         configuration.getAcceptorConfigurations().clear();
         configuration.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false, i));

         if (isSharedStore())
            configuration.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_SHARED_STORE);
         else
            configuration.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_REPLICATED);

         if (!isSharedStore())
         {
            configuration.setBindingsDirectory(getBindingsDir(i, true));
            configuration.setJournalDirectory(getJournalDir(i, true));
            configuration.setPagingDirectory(getPageDir(i, true));
            configuration.setLargeMessagesDirectory(getLargeMessagesDir(i, true));
         }
         else
         {
            //todo
         }

         configuration.getHAPolicy().setFailbackDelay(1000);
         if (getNodeGroupName() != null)
         {
            configuration.getHAPolicy().setBackupGroupName(getNodeGroupName() + "-" + i);
         }
         createBackupClusterConfiguration(i, configuration, getBackupServerCount());
         backupConfigs.add(configuration);
         HornetQServer server = createServer(true, configuration);
         TestableServer testableServer = new SameProcessHornetQServer(server);
         testableServer.setIdentity("Backup-" + i);
         backupServers.add(testableServer);
      }
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      for (TestableServer backupServer : backupServers)
      {
         try
         {
            backupServer.stop();
         }
         catch (Exception e)
         {
            logAndSystemOut("unable to stop server", e);
         }
      }
      backupServers.clear();
      backupServers = null;
      backupConfigs.clear();
      backupConfigs = null;
      for (TestableServer liveServer : liveServers)
      {
         try
         {
            liveServer.stop();
         }
         catch (Exception e)
         {
            logAndSystemOut("unable to stop server", e);
         }
      }
      liveServers.clear();
      liveServers = null;
      liveConfigs.clear();
      liveConfigs = null;
      super.tearDown();
   }

   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live, int node)
   {
      TransportConfiguration transportConfiguration;
      if (useNetty())
      {
         transportConfiguration = TransportConfigurationUtils.getNettyAcceptor(live, node, (live ? "live-" : "backup-") + node);
      }
      else
      {
         transportConfiguration = TransportConfigurationUtils.getInVMAcceptor(live, node, (live ? "live-" : "backup-") + node);
      }
      return transportConfiguration;
   }

   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live, int node)
   {
      TransportConfiguration transportConfiguration;
      if (useNetty())
      {
         transportConfiguration = TransportConfigurationUtils.getNettyConnector(live, node, (live ? "live-" : "backup-") + node);
      }
      else
      {
         transportConfiguration = TransportConfigurationUtils.getInVMConnector(live, node, (live ? "live-" : "backup-") + node);
      }
      return transportConfiguration;
   }

   protected ServerLocatorInternal getServerLocator(int node) throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithHA(getConnectorTransportConfiguration(true, node));
      locator.setRetryInterval(50);
      locator.setReconnectAttempts(-1);
      locator.setInitialConnectAttempts(-1);
      addServerLocator(locator);
      return (ServerLocatorInternal) locator;
   }

   protected ServerLocatorInternal getBackupServerLocator(int node) throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithHA(getConnectorTransportConfiguration(false, node));
      locator.setRetryInterval(50);
      locator.setReconnectAttempts(-1);
      locator.setInitialConnectAttempts(-1);
      addServerLocator(locator);
      return (ServerLocatorInternal) locator;
   }

   protected ClientSession createSession(ClientSessionFactory sf,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         int ackBatchSize) throws Exception
   {
      return addClientSession(sf.createSession(autoCommitSends, autoCommitAcks, ackBatchSize));
   }

   protected ClientSession createSession(ClientSessionFactory sf, boolean autoCommitSends, boolean autoCommitAcks) throws Exception
   {
      return addClientSession(sf.createSession(autoCommitSends, autoCommitAcks));
   }

   protected ClientSession createSession(ClientSessionFactory sf) throws Exception
   {
      return addClientSession(sf.createSession());
   }

   protected ClientSession createSession(ClientSessionFactory sf,
                                         boolean xa,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception
   {
      return addClientSession(sf.createSession(xa, autoCommitSends, autoCommitAcks));
   }

   protected void waitForDistribution(SimpleString address, HornetQServer server, int messageCount) throws Exception
   {
      HornetQServerLogger.LOGGER.debug("waiting for distribution of messages on server " + server);

      long start = System.currentTimeMillis();

      long timeout = 5000;

      Queue q = (Queue) server.getPostOffice().getBinding(address).getBindable();

      do
      {

         if (q.getMessageCount() >= messageCount)
         {
            return;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < timeout);

      throw new Exception();
   }
}

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
package org.hornetq.core.server.cluster;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.NodeManager;
import org.hornetq.utils.ExecutorFactory;

/*
* takes care of updating the cluster with a backups transport configuration which is based on each cluster connection.
* */
public class BackupManager implements HornetQComponent
{
   private HornetQServer server;
   private Executor executor;
   private ScheduledExecutorService scheduledExecutor;
   private NodeManager nodeManager;
   private Configuration configuration;
   private ClusterManager clusterManager;

   List<BackupConnector> backupConnectors = new ArrayList<>();

   private boolean started;

   public BackupManager(HornetQServer server, ExecutorFactory executorFactory, ScheduledExecutorService scheduledExecutor, NodeManager nodeManager,
                        Configuration configuration, ClusterManager clusterManager)
   {
      this.server = server;
      this.executor = executorFactory.getExecutor();
      this.scheduledExecutor = scheduledExecutor;
      this.nodeManager = nodeManager;
      this.configuration = configuration;
      this.clusterManager = clusterManager;
   }

   /*
   * Start the backup manager if not already started. This entails deploying a backup connector based on a cluster
   * configuration, informing the cluster manager so that it can add it to its topology and announce itself to the cluster.
   * */
   public synchronized void start()
   {
      if (started) return;
      //deploy the backup connectors using the cluster configuration
      for (ClusterConnectionConfiguration config : configuration.getClusterConfigurations())
      {
         deployBackupConnector(config);
      }
      //start each connector and if we are backup and shared store announce ourselves. NB with replication we dont do this
      //as we wait for replication to start and be notififed by the replication manager.
      for (BackupConnector conn : backupConnectors)
      {
         conn.start();
         if (server.getHAPolicy().isBackup() && server.getHAPolicy().isSharedStore())
         {
            conn.informTopology();
            conn.announceBackup();
         }
      }
      started = true;
   }

   /*
   * stop all the connectors
   * */
   public synchronized void stop()
   {
      if (!started) return;
      for (BackupConnector backupConnector : backupConnectors)
      {
         backupConnector.close();
      }
      started = false;
   }

   /*
   * announce the fact that we are a backup server ready to fail over if required.
   */
   public void announceBackup()
   {
      for (BackupConnector backupConnector : backupConnectors)
      {
         backupConnector.announceBackup();
      }
   }

   /*
   * create the connectors using the cluster configurations
   * */
   private void deployBackupConnector(final ClusterConnectionConfiguration config)
   {
      TransportConfiguration connector = ClusterConfigurationUtil.getTransportConfiguration(config, configuration);

      if (connector == null) return;

      if (config.getDiscoveryGroupName() != null)
      {
         DiscoveryGroupConfiguration dg = ClusterConfigurationUtil.getDiscoveryGroupConfiguration(config, configuration);

         if (dg == null) return;


         DiscoveryBackupConnector backupConnector = new DiscoveryBackupConnector(dg, config.getName(), connector,
                                                                                 config.getRetryInterval(), clusterManager);

         backupConnectors.add(backupConnector);
      }
      else
      {
         TransportConfiguration[] tcConfigs = ClusterConfigurationUtil.getTransportConfigurations(config, configuration);

         StaticBackupConnector backupConnector = new StaticBackupConnector(tcConfigs, config.getName(), connector,
                                                                           config.getRetryInterval(), clusterManager);

         backupConnectors.add(backupConnector);
      }
   }

   /*
   * called to notify us that we have been activated as a live server so the connectors are no longer needed.
   * */
   public void activated()
   {
      for (BackupConnector backupConnector : backupConnectors)
      {
         backupConnector.close();
      }
   }

   @Override
   public boolean isStarted()
   {
      return started;
   }

   public boolean isBackupAnnounced()
   {
      for (BackupConnector backupConnector : backupConnectors)
      {
         if (!backupConnector.isBackupAnnounced())
         {
            return false;
         }
      }
      return true;
   }

   /*
   * A backup connector will connect to the cluster and announce that we are a backup server ready to fail over.
   * */
   private abstract class BackupConnector
   {
      private volatile ServerLocatorInternal backupServerLocator;
      private String name;
      private TransportConfiguration connector;
      private long retryInterval;
      private ClusterManager clusterManager;
      private boolean stopping = false;
      private boolean announcingBackup;
      private boolean backupAnnounced = false;

      public BackupConnector(String name, TransportConfiguration connector, long retryInterval,
                             ClusterManager clusterManager)
      {
         this.name = name;
         this.connector = connector;
         this.retryInterval = retryInterval;
         this.clusterManager = clusterManager;
      }

      /*
      * used to create the server locator needed, will be connectors or discovery
      * */
      abstract ServerLocatorInternal createServerLocator(Topology topology);

      /*
      * start the connector by creating the server locator to use.
      * */
      void start()
      {
         stopping = false;
         backupAnnounced = false;

         ClusterConnection clusterConnection = clusterManager.getClusterConnection(name);
         //NB we use the same topology as the sister cluster connection so it knows when started about all the nodes to bridge to
         backupServerLocator = createServerLocator(clusterConnection.getTopology());

         if (backupServerLocator != null)
         {
            backupServerLocator.setIdentity("backupLocatorFor='" + server + "'");
            backupServerLocator.setReconnectAttempts(-1);
            backupServerLocator.setInitialConnectAttempts(-1);
            backupServerLocator.setProtocolManagerFactory(HornetQServerSideProtocolManagerFactory.getInstance());
         }
      }

      /*
      * this connects to the cluster and announces that we are a backup
      * */
      public void announceBackup()
      {
         //this has to be done in a separate thread
         executor.execute(new Runnable()
         {

            public void run()
            {
               if (stopping)
                  return;
               try
               {
                  //make a copy to avoid npe if we are nulled on close
                  ServerLocatorInternal localBackupLocator = backupServerLocator;
                  if (localBackupLocator == null)
                  {
                     if (!stopping)
                        HornetQServerLogger.LOGGER.error("Error announcing backup: backupServerLocator is null. " + this);
                     return;
                  }
                  if (HornetQServerLogger.LOGGER.isDebugEnabled())
                  {
                     HornetQServerLogger.LOGGER.debug(BackupConnector.this + ":: announcing " + connector + " to " + backupServerLocator);
                  }
                  announcingBackup = true;
                  //connect to the cluster
                  ClientSessionFactoryInternal backupSessionFactory = localBackupLocator.connect();
                  //send the announce message
                  if (backupSessionFactory != null)
                  {
                     ClusterControl clusterControl = clusterManager.getClusterController().connectToNodeInCluster(backupSessionFactory);
                     clusterControl.authorize();
                     clusterControl.sendNodeAnnounce(System.currentTimeMillis(),
                                                      nodeManager.getNodeId().toString(),
                                                      server.getHAPolicy().getBackupGroupName(),
                                                      server.getHAPolicy().getScaleDownClustername(),
                                                      true,
                                                      connector,
                                                      null);
                     HornetQServerLogger.LOGGER.backupAnnounced();
                     backupAnnounced = true;
                  }
               }
               catch (RejectedExecutionException e)
               {
                  // assumption is that the whole server is being stopped. So the exception is ignored.
               }
               catch (Exception e)
               {
                  if (scheduledExecutor.isShutdown())
                     return;
                  if (stopping)
                     return;
                  HornetQServerLogger.LOGGER.errorAnnouncingBackup();

                  scheduledExecutor.schedule(new Runnable()
                  {
                     public void run()
                     {
                        announceBackup();
                     }

                  }, retryInterval, TimeUnit.MILLISECONDS);
               }
               finally
               {
                  announcingBackup = false;
               }
            }
         });
      }

      /*
      * called to notify the cluster manager about the backup
      * */
      public void informTopology()
      {
         clusterManager.informClusterOfBackup(name);
      }

      /*
      * close everything
      * */
      public void close()
      {
         stopping = true;
         if (announcingBackup)
         {
           /*
           * The executor used is ordered so if we are announcing the backup, scheduling the following
           * Runnable would never execute.
           */
            closeLocator(backupServerLocator);
         }
         executor.execute(new Runnable()
         {
            public void run()
            {
               synchronized (BackupConnector.this)
               {
                  closeLocator(backupServerLocator);
                  backupServerLocator = null;
               }

            }
         });
      }

      public boolean isBackupAnnounced()
      {
         return backupAnnounced;
      }

      private void closeLocator(ServerLocatorInternal backupServerLocator)
      {
         if (backupServerLocator != null)
         {
            backupServerLocator.close();
         }
      }
   }

   /*
   * backup connector using static connectors
   * */
   private final class StaticBackupConnector extends BackupConnector
   {
      private final TransportConfiguration[] tcConfigs;

      public StaticBackupConnector(TransportConfiguration[] tcConfigs, String name, TransportConfiguration connector, long retryInterval,
                                   ClusterManager clusterManager)
      {
         super(name, connector, retryInterval, clusterManager);
         this.tcConfigs = tcConfigs;
      }

      public ServerLocatorInternal createServerLocator(Topology topology)
      {
         if (tcConfigs != null && tcConfigs.length > 0)
         {
            if (HornetQServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQServerLogger.LOGGER.debug(BackupManager.this + "Creating a serverLocator for " + Arrays.toString(tcConfigs));
            }
            ServerLocatorImpl locator = new ServerLocatorImpl(topology, true, tcConfigs);
            locator.setClusterConnection(true);
            locator.setProtocolManagerFactory(HornetQServerSideProtocolManagerFactory.getInstance());
            return locator;
         }
         return null;
      }

      @Override
      public String toString()
      {
         return "StaticBackupConnector [tcConfigs=" + Arrays.toString(tcConfigs) + "]";
      }

   }

   /*
   * backup connector using discovery
   * */
   private final class DiscoveryBackupConnector extends BackupConnector
   {

      private final DiscoveryGroupConfiguration discoveryGroupConfiguration;

      public DiscoveryBackupConnector(DiscoveryGroupConfiguration discoveryGroupConfiguration, String name, TransportConfiguration connector, long retryInterval,
                                      ClusterManager clusterManager)
      {
         super(name, connector, retryInterval, clusterManager);
         this.discoveryGroupConfiguration = discoveryGroupConfiguration;
      }

      public ServerLocatorInternal createServerLocator(Topology topology)
      {
         return new ServerLocatorImpl(topology, true, discoveryGroupConfiguration);
      }

      @Override
      public String toString()
      {
         return "DiscoveryBackupConnector [group=" + discoveryGroupConfiguration + "]";
      }

   }
}

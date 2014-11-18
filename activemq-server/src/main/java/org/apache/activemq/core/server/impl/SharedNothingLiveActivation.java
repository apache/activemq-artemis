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
package org.apache.activemq.core.server.impl;

import org.apache.activemq.api.core.ActiveMQAlreadyReplicatingException;
import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.Pair;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClusterTopologyListener;
import org.apache.activemq.api.core.client.HornetQClient;
import org.apache.activemq.api.core.client.TopologyMember;
import org.apache.activemq.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.core.config.ConfigurationUtils;
import org.apache.activemq.core.protocol.core.Channel;
import org.apache.activemq.core.protocol.core.ChannelHandler;
import org.apache.activemq.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.core.protocol.core.Packet;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.core.protocol.core.impl.wireformat.BackupRegistrationMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.BackupReplicationStartFailedMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage;
import org.apache.activemq.core.remoting.CloseListener;
import org.apache.activemq.core.remoting.FailureListener;
import org.apache.activemq.core.remoting.server.RemotingService;
import org.apache.activemq.core.replication.ReplicationManager;
import org.apache.activemq.core.server.HornetQMessageBundle;
import org.apache.activemq.core.server.HornetQServerLogger;
import org.apache.activemq.core.server.NodeManager;
import org.apache.activemq.core.server.cluster.ClusterConnection;
import org.apache.activemq.core.server.cluster.ha.ReplicatedPolicy;
import org.apache.activemq.spi.core.remoting.Acceptor;

import java.lang.reflect.Array;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SharedNothingLiveActivation extends LiveActivation
{
   //this is how we act when we initially start as a live
   private ReplicatedPolicy replicatedPolicy;

   private HornetQServerImpl hornetQServer;

   private ReplicationManager replicationManager;

   private final Object replicationLock = new Object();

   public SharedNothingLiveActivation(HornetQServerImpl hornetQServer,
                                      ReplicatedPolicy replicatedPolicy)
   {
      this.hornetQServer = hornetQServer;
      this.replicatedPolicy = replicatedPolicy;
   }

   @Override
   public void freezeConnections(RemotingService remotingService)
   {
      ReplicationManager localReplicationManager = replicationManager;

      if (remotingService != null && localReplicationManager != null)
      {
         remotingService.freeze(null, localReplicationManager.getBackupTransportConnection());
      }
      else if (remotingService != null)
      {
         remotingService.freeze(null, null);
      }
   }

   public void run()
   {
      try
      {
         if (replicatedPolicy.isCheckForLiveServer() && isNodeIdUsed())
         {
            //set for when we failback
            replicatedPolicy.getReplicaPolicy().setReplicatedPolicy(replicatedPolicy);
            hornetQServer.setHAPolicy(replicatedPolicy.getReplicaPolicy());
            return;
         }

         hornetQServer.initialisePart1(false);

         hornetQServer.initialisePart2(false);

         if (hornetQServer.getIdentity() != null)
         {
            HornetQServerLogger.LOGGER.serverIsLive(hornetQServer.getIdentity());
         }
         else
         {
            HornetQServerLogger.LOGGER.serverIsLive();
         }
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.initializationError(e);
      }
   }

   @Override
   public ChannelHandler getActivationChannelHandler(final Channel channel, final Acceptor acceptorUsed)
   {
      return new ChannelHandler()
      {
         @Override
         public void handlePacket(Packet packet)
         {
            if (packet.getType() == PacketImpl.BACKUP_REGISTRATION)
            {
               BackupRegistrationMessage msg = (BackupRegistrationMessage)packet;
               ClusterConnection clusterConnection = acceptorUsed.getClusterConnection();
               try
               {
                  startReplication(channel.getConnection(), clusterConnection, getPair(msg.getConnector(), true),
                        msg.isFailBackRequest());
               }
               catch (ActiveMQAlreadyReplicatingException are)
               {
                  channel.send(new BackupReplicationStartFailedMessage(BackupReplicationStartFailedMessage.BackupRegistrationProblem.ALREADY_REPLICATING));
               }
               catch (ActiveMQException e)
               {
                  channel.send(new BackupReplicationStartFailedMessage(BackupReplicationStartFailedMessage.BackupRegistrationProblem.EXCEPTION));
               }
            }
         }
      };
   }

   public void startReplication(CoreRemotingConnection rc, final ClusterConnection clusterConnection,
                                final Pair<TransportConfiguration, TransportConfiguration> pair, final boolean isFailBackRequest) throws ActiveMQException
   {
      if (replicationManager != null)
      {
         throw new ActiveMQAlreadyReplicatingException();
      }

      if (!hornetQServer.isStarted())
      {
         throw new ActiveMQIllegalStateException();
      }

      synchronized (replicationLock)
      {

         if (replicationManager != null)
         {
            throw new ActiveMQAlreadyReplicatingException();
         }
         ReplicationFailureListener listener = new ReplicationFailureListener();
         rc.addCloseListener(listener);
         rc.addFailureListener(listener);
         replicationManager = new ReplicationManager(rc, hornetQServer.getExecutorFactory());
         replicationManager.start();
         Thread t = new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  hornetQServer.getStorageManager().startReplication(replicationManager, hornetQServer.getPagingManager(), hornetQServer.getNodeID().toString(),
                        isFailBackRequest && replicatedPolicy.isAllowAutoFailBack());

                  clusterConnection.nodeAnnounced(System.currentTimeMillis(), hornetQServer.getNodeID().toString(), replicatedPolicy.getGroupName(), replicatedPolicy.getScaleDownGroupName(), pair, true);

                  //todo, check why this was set here
                  //backupUpToDate = false;

                  if (isFailBackRequest && replicatedPolicy.isAllowAutoFailBack())
                  {
                     BackupTopologyListener listener1 = new BackupTopologyListener(hornetQServer.getNodeID().toString());
                     clusterConnection.addClusterTopologyListener(listener1);
                     if (listener1.waitForBackup())
                     {
                        try
                        {
                           Thread.sleep(replicatedPolicy.getFailbackDelay());
                        }
                        catch (InterruptedException e)
                        {
                           //
                        }
                        //if we have to many backups kept or arent configured to restart just stop, otherwise restart as a backup
                        if (!replicatedPolicy.getReplicaPolicy().isRestartBackup() && hornetQServer.countNumberOfCopiedJournals() >= replicatedPolicy.getReplicaPolicy().getMaxSavedReplicatedJournalsSize() && replicatedPolicy.getReplicaPolicy().getMaxSavedReplicatedJournalsSize() >= 0)
                        {
                           hornetQServer.stop(true);
                           HornetQServerLogger.LOGGER.stopReplicatedBackupAfterFailback();
                        }
                        else
                        {
                           hornetQServer.stop(true);
                           HornetQServerLogger.LOGGER.restartingReplicatedBackupAfterFailback();
                           hornetQServer.setHAPolicy(replicatedPolicy.getReplicaPolicy());
                           hornetQServer.start();
                        }
                     }
                     else
                     {
                        HornetQServerLogger.LOGGER.failbackMissedBackupAnnouncement();
                     }
                  }
               }
               catch (Exception e)
               {
                  if (hornetQServer.getState() == HornetQServerImpl.SERVER_STATE.STARTED)
                  {
                  /*
                   * The reasoning here is that the exception was either caused by (1) the
                   * (interaction with) the backup, or (2) by an IO Error at the storage. If (1), we
                   * can swallow the exception and ignore the replication request. If (2) the live
                   * will crash shortly.
                   */
                     HornetQServerLogger.LOGGER.errorStartingReplication(e);
                  }
                  try
                  {
                     HornetQServerImpl.stopComponent(replicationManager);
                  }
                  catch (Exception hqe)
                  {
                     HornetQServerLogger.LOGGER.errorStoppingReplication(hqe);
                  }
                  finally
                  {
                     synchronized (replicationLock)
                     {
                        replicationManager = null;
                     }
                  }
               }
            }
         });

         t.start();
      }
   }

   private final class ReplicationFailureListener implements FailureListener, CloseListener
   {

      @Override
      public void connectionFailed(ActiveMQException exception, boolean failedOver)
      {
         connectionClosed();
      }

      @Override
      public void connectionFailed(final ActiveMQException me, boolean failedOver, String scaleDownTargetNodeID)
      {
         connectionFailed(me, failedOver);
      }

      @Override
      public void connectionClosed()
      {
         hornetQServer.getThreadPool().execute(new Runnable()
         {
            public void run()
            {
               synchronized (replicationLock)
               {
                  if (replicationManager != null)
                  {
                     hornetQServer.getStorageManager().stopReplication();
                     replicationManager = null;
                  }
               }
            }
         });
      }
   }

   private Pair<TransportConfiguration, TransportConfiguration> getPair(TransportConfiguration conn,
                                                                        boolean isBackup)
   {
      if (isBackup)
      {
         return new Pair<>(null, conn);
      }
      return new Pair<>(conn, null);
   }
   /**
    * Determines whether there is another server already running with this server's nodeID.
    * <p/>
    * This can happen in case of a successful fail-over followed by the live's restart
    * (attempting a fail-back).
    *
    * @throws Exception
    */
   private boolean isNodeIdUsed() throws Exception
   {
      if (hornetQServer.getConfiguration().getClusterConfigurations().isEmpty())
         return false;
      SimpleString nodeId0;
      try
      {
         nodeId0 = hornetQServer.getNodeManager().readNodeId();
      }
      catch (ActiveMQIllegalStateException e)
      {
         nodeId0 = null;
      }

      ServerLocatorInternal locator;

      ClusterConnectionConfiguration config = ConfigurationUtils.getReplicationClusterConfiguration(hornetQServer.getConfiguration(), replicatedPolicy.getClusterName());

      locator = getLocator(config);

      ClientSessionFactoryInternal factory = null;

      NodeIdListener listener = new NodeIdListener(nodeId0);

      locator.addClusterTopologyListener(listener);
      try
      {
         locator.setReconnectAttempts(0);
         try
         {
            locator.addClusterTopologyListener(listener);
            factory = locator.connectNoWarnings();
         }
         catch (Exception notConnected)
         {
            return false;
         }

         listener.latch.await(5, TimeUnit.SECONDS);

         return listener.isNodePresent;
      }
      finally
      {
         if (factory != null)
            factory.close();
         if (locator != null)
            locator.close();
      }
   }

   public void close(boolean permanently, boolean restarting) throws Exception
   {

      replicationManager = null;
      // To avoid a NPE cause by the stop
      NodeManager nodeManagerInUse = hornetQServer.getNodeManager();

      if (nodeManagerInUse != null)
      {
         //todo does this actually make any difference, we only set a different flag in the lock file which replication doesnt use
         if (permanently)
         {
            nodeManagerInUse.crashLiveServer();
         }
         else
         {
            nodeManagerInUse.pauseLiveServer();
         }
      }
   }

   @Override
   public void sendLiveIsStopping()
   {
      final ReplicationManager localReplicationManager = replicationManager;

      if (localReplicationManager != null)
      {
         localReplicationManager.sendLiveIsStopping(ReplicationLiveIsStoppingMessage.LiveStopping.STOP_CALLED);
         // Schedule for 10 seconds
         // this pool gets a 'hard' shutdown, no need to manage the Future of this Runnable.
         hornetQServer.getScheduledPool().schedule(new Runnable()
         {
            @Override
            public void run()
            {
               localReplicationManager.clearReplicationTokens();
            }
         }, 30, TimeUnit.SECONDS);
      }
   }

   public ReplicationManager getReplicationManager()
   {
      synchronized (replicationLock)
      {
         return replicationManager;
      }
   }

   private ServerLocatorInternal getLocator(ClusterConnectionConfiguration config) throws ActiveMQException
   {
      ServerLocatorInternal locator;
      if (config.getDiscoveryGroupName() != null)
      {
         DiscoveryGroupConfiguration dg = hornetQServer.getConfiguration().getDiscoveryGroupConfigurations().get(config.getDiscoveryGroupName());

         if (dg == null)
         {
            throw HornetQMessageBundle.BUNDLE.noDiscoveryGroupFound(dg);
         }
         locator = (ServerLocatorInternal) HornetQClient.createServerLocatorWithHA(dg);
      }
      else
      {
         TransportConfiguration[] tcConfigs = config.getStaticConnectors() != null ? connectorNameListToArray(config.getStaticConnectors())
               : null;

         locator = (ServerLocatorInternal) HornetQClient.createServerLocatorWithHA(tcConfigs);
      }
      return locator;
   }


   static final class NodeIdListener implements ClusterTopologyListener
   {
      volatile boolean isNodePresent = false;

      private final SimpleString nodeId;
      private final CountDownLatch latch = new CountDownLatch(1);

      public NodeIdListener(SimpleString nodeId)
      {
         this.nodeId = nodeId;
      }

      @Override
      public void nodeUP(TopologyMember topologyMember, boolean last)
      {
         boolean isOurNodeId = nodeId != null && nodeId.toString().equals(topologyMember.getNodeId());
         if (isOurNodeId)
         {
            isNodePresent = true;
         }
         if (isOurNodeId || last)
         {
            latch.countDown();
         }
      }

      @Override
      public void nodeDown(long eventUID, String nodeID)
      {
         // no-op
      }
   }

   private TransportConfiguration[] connectorNameListToArray(final List<String> connectorNames)
   {
      TransportConfiguration[] tcConfigs = (TransportConfiguration[]) Array.newInstance(TransportConfiguration.class,
            connectorNames.size());
      int count = 0;
      for (String connectorName : connectorNames)
      {
         TransportConfiguration connector = hornetQServer.getConfiguration().getConnectorConfigurations().get(connectorName);

         if (connector == null)
         {
            HornetQServerLogger.LOGGER.bridgeNoConnector(connectorName);

            return null;
         }

         tcConfigs[count++] = connector;
      }

      return tcConfigs;
   }
}

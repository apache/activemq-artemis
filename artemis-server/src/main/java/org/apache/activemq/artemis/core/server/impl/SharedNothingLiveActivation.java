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
package org.apache.activemq.artemis.core.server.impl;

import org.apache.activemq.artemis.api.core.ActiveMQAlreadyReplicatingException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.ConfigurationUtils;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.ChannelHandler;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.BackupRegistrationMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.BackupReplicationStartFailedMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicatedPolicy;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;

import java.lang.reflect.Array;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SharedNothingLiveActivation extends LiveActivation
{
   //this is how we act when we initially start as a live
   private ReplicatedPolicy replicatedPolicy;

   private ActiveMQServerImpl activeMQServer;

   private ReplicationManager replicationManager;

   private final Object replicationLock = new Object();

   public SharedNothingLiveActivation(ActiveMQServerImpl activeMQServer,
                                      ReplicatedPolicy replicatedPolicy)
   {
      this.activeMQServer = activeMQServer;
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
            activeMQServer.setHAPolicy(replicatedPolicy.getReplicaPolicy());
            return;
         }

         activeMQServer.initialisePart1(false);

         activeMQServer.initialisePart2(false);

         activeMQServer.completeActivation();

         if (activeMQServer.getIdentity() != null)
         {
            ActiveMQServerLogger.LOGGER.serverIsLive(activeMQServer.getIdentity());
         }
         else
         {
            ActiveMQServerLogger.LOGGER.serverIsLive();
         }
      }
      catch (Exception e)
      {
         ActiveMQServerLogger.LOGGER.initializationError(e);
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
                  ActiveMQServerLogger.LOGGER.debug("Failed to process backup registration packet", e);
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

      if (!activeMQServer.isStarted())
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
         replicationManager = new ReplicationManager(rc, activeMQServer.getExecutorFactory());
         replicationManager.start();
         Thread t = new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  activeMQServer.getStorageManager().startReplication(replicationManager, activeMQServer.getPagingManager(), activeMQServer.getNodeID().toString(),
                        isFailBackRequest && replicatedPolicy.isAllowAutoFailBack());

                  clusterConnection.nodeAnnounced(System.currentTimeMillis(), activeMQServer.getNodeID().toString(), replicatedPolicy.getGroupName(), replicatedPolicy.getScaleDownGroupName(), pair, true);

                  //todo, check why this was set here
                  //backupUpToDate = false;

                  if (isFailBackRequest && replicatedPolicy.isAllowAutoFailBack())
                  {
                     BackupTopologyListener listener1 = new BackupTopologyListener(activeMQServer.getNodeID().toString());
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
                        if (!replicatedPolicy.getReplicaPolicy().isRestartBackup() && activeMQServer.countNumberOfCopiedJournals() >= replicatedPolicy.getReplicaPolicy().getMaxSavedReplicatedJournalsSize() && replicatedPolicy.getReplicaPolicy().getMaxSavedReplicatedJournalsSize() >= 0)
                        {
                           activeMQServer.stop(true);
                           ActiveMQServerLogger.LOGGER.stopReplicatedBackupAfterFailback();
                        }
                        else
                        {
                           activeMQServer.stop(true);
                           ActiveMQServerLogger.LOGGER.restartingReplicatedBackupAfterFailback();
                           activeMQServer.setHAPolicy(replicatedPolicy.getReplicaPolicy());
                           activeMQServer.start();
                        }
                     }
                     else
                     {
                        ActiveMQServerLogger.LOGGER.failbackMissedBackupAnnouncement();
                     }
                  }
               }
               catch (Exception e)
               {
                  if (activeMQServer.getState() == ActiveMQServerImpl.SERVER_STATE.STARTED)
                  {
                  /*
                   * The reasoning here is that the exception was either caused by (1) the
                   * (interaction with) the backup, or (2) by an IO Error at the storage. If (1), we
                   * can swallow the exception and ignore the replication request. If (2) the live
                   * will crash shortly.
                   */
                     ActiveMQServerLogger.LOGGER.errorStartingReplication(e);
                  }
                  try
                  {
                     ActiveMQServerImpl.stopComponent(replicationManager);
                  }
                  catch (Exception amqe)
                  {
                     ActiveMQServerLogger.LOGGER.errorStoppingReplication(amqe);
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
         activeMQServer.getThreadPool().execute(new Runnable()
         {
            public void run()
            {
               synchronized (replicationLock)
               {
                  if (replicationManager != null)
                  {
                     activeMQServer.getStorageManager().stopReplication();
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
    * <p>
    * This can happen in case of a successful fail-over followed by the live's restart
    * (attempting a fail-back).
    *
    * @throws Exception
    */
   private boolean isNodeIdUsed() throws Exception
   {
      if (activeMQServer.getConfiguration().getClusterConfigurations().isEmpty())
         return false;
      SimpleString nodeId0;
      try
      {
         nodeId0 = activeMQServer.getNodeManager().readNodeId();
      }
      catch (ActiveMQIllegalStateException e)
      {
         nodeId0 = null;
      }

      ServerLocatorInternal locator;

      ClusterConnectionConfiguration config = ConfigurationUtils.getReplicationClusterConfiguration(activeMQServer.getConfiguration(), replicatedPolicy.getClusterName());

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
      NodeManager nodeManagerInUse = activeMQServer.getNodeManager();

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
         activeMQServer.getScheduledPool().schedule(new Runnable()
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
         DiscoveryGroupConfiguration dg = activeMQServer.getConfiguration().getDiscoveryGroupConfigurations().get(config.getDiscoveryGroupName());

         if (dg == null)
         {
            throw ActiveMQMessageBundle.BUNDLE.noDiscoveryGroupFound(dg);
         }
         locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithHA(dg);
      }
      else
      {
         TransportConfiguration[] tcConfigs = config.getStaticConnectors() != null ? connectorNameListToArray(config.getStaticConnectors())
               : null;

         locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithHA(tcConfigs);
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
         TransportConfiguration connector = activeMQServer.getConfiguration().getConnectorConfigurations().get(connectorName);

         if (connector == null)
         {
            ActiveMQServerLogger.LOGGER.bridgeNoConnector(connectorName);

            return null;
         }

         tcConfigs[count++] = connector;
      }

      return tcConfigs;
   }
}

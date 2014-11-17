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
package org.apache.activemq6.core.server.impl;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.HornetQInternalErrorException;
import org.apache.activemq6.api.core.Pair;
import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.core.client.TopologyMember;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.paging.PagingManager;
import org.apache.activemq6.core.persistence.StorageManager;
import org.apache.activemq6.core.postoffice.PostOffice;
import org.apache.activemq6.core.protocol.core.Channel;
import org.apache.activemq6.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage;
import org.apache.activemq6.core.replication.ReplicationEndpoint;
import org.apache.activemq6.core.server.ActivationParams;
import org.apache.activemq6.core.server.HornetQMessageBundle;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.core.server.HornetQServerLogger;
import org.apache.activemq6.core.server.LiveNodeLocator;
import org.apache.activemq6.core.server.NodeManager;
import org.apache.activemq6.core.server.QueueFactory;
import org.apache.activemq6.core.server.cluster.ClusterControl;
import org.apache.activemq6.core.server.cluster.ClusterController;
import org.apache.activemq6.core.server.cluster.ha.ReplicaPolicy;
import org.apache.activemq6.core.server.cluster.ha.ScaleDownPolicy;
import org.apache.activemq6.core.server.cluster.qourum.SharedNothingBackupQuorum;
import org.apache.activemq6.core.server.group.GroupingHandler;
import org.apache.activemq6.core.server.management.ManagementService;
import org.apache.activemq6.utils.ReusableLatch;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.activemq6.core.server.cluster.qourum.SharedNothingBackupQuorum.BACKUP_ACTIVATION.FAILURE_REPLICATING;
import static org.apache.activemq6.core.server.cluster.qourum.SharedNothingBackupQuorum.BACKUP_ACTIVATION.FAIL_OVER;
import static org.apache.activemq6.core.server.cluster.qourum.SharedNothingBackupQuorum.BACKUP_ACTIVATION.STOP;

public final class SharedNothingBackupActivation extends Activation
{
   //this is how we act when we start as a backup
   private ReplicaPolicy replicaPolicy;

   //this is the endpoint where we replicate too
   private ReplicationEndpoint replicationEndpoint;

   private final HornetQServerImpl hornetQServer;
   private SharedNothingBackupQuorum backupQuorum;
   private final boolean attemptFailBack;
   private final Map<String, Object> activationParams;
   private final HornetQServerImpl.ShutdownOnCriticalErrorListener shutdownOnCriticalIO;
   private String nodeID;
   ClusterControl clusterControl;
   private boolean closed;
   private volatile boolean backupUpToDate = true;

   private final ReusableLatch backupSyncLatch = new ReusableLatch(0);

   public SharedNothingBackupActivation(HornetQServerImpl hornetQServer,
                                        boolean attemptFailBack,
                                        Map<String, Object> activationParams,
                                        HornetQServerImpl.ShutdownOnCriticalErrorListener shutdownOnCriticalIO,
                                        ReplicaPolicy replicaPolicy)
   {
      this.hornetQServer = hornetQServer;
      this.attemptFailBack = attemptFailBack;
      this.activationParams = activationParams;
      this.shutdownOnCriticalIO = shutdownOnCriticalIO;
      this.replicaPolicy = replicaPolicy;
      backupSyncLatch.setCount(1);
   }

   public void init() throws Exception
   {
      assert replicationEndpoint == null;
      hornetQServer.resetNodeManager();
      backupUpToDate = false;
      replicationEndpoint = new ReplicationEndpoint(hornetQServer, shutdownOnCriticalIO, attemptFailBack, this);
   }

   public void run()
   {
      try
      {
         synchronized (hornetQServer)
         {
            hornetQServer.setState(HornetQServerImpl.SERVER_STATE.STARTED);
         }
         // move all data away:
         hornetQServer.getNodeManager().stop();
         hornetQServer.moveServerData();
         hornetQServer.getNodeManager().start();
         synchronized (this)
         {
            if (closed)
               return;
         }

         boolean scalingDown = replicaPolicy.getScaleDownPolicy() != null && replicaPolicy.getScaleDownPolicy().isEnabled();

         if (!hornetQServer.initialisePart1(scalingDown))
            return;

         synchronized (this)
         {
            if (closed)
               return;
            backupQuorum = new SharedNothingBackupQuorum(hornetQServer.getStorageManager(), hornetQServer.getNodeManager(), hornetQServer.getScheduledPool());
            hornetQServer.getClusterManager().getQuorumManager().registerQuorum(backupQuorum);
         }

         //use a Node Locator to connect to the cluster
         LiveNodeLocator nodeLocator;
         if (activationParams.get(ActivationParams.REPLICATION_ENDPOINT) != null)
         {
            TopologyMember member = (TopologyMember) activationParams.get(ActivationParams.REPLICATION_ENDPOINT);
            nodeLocator = new NamedNodeIdNodeLocator(member.getNodeId(), new Pair<>(member.getLive(), member.getBackup()));
         }
         else
         {
            nodeLocator = replicaPolicy.getGroupName() == null ?
                  new AnyLiveNodeLocatorForReplication(backupQuorum, hornetQServer) :
                  new NamedLiveNodeLocatorForReplication(replicaPolicy.getGroupName(), backupQuorum);
         }
         ClusterController clusterController = hornetQServer.getClusterManager().getClusterController();
         clusterController.addClusterTopologyListenerForReplication(nodeLocator);
         //todo do we actually need to wait?
         clusterController.awaitConnectionToReplicationCluster();

         clusterController.addIncomingInterceptorForReplication(new ReplicationError(hornetQServer, nodeLocator));

         // nodeManager.startBackup();

         hornetQServer.getBackupManager().start();

         replicationEndpoint.setBackupQuorum(backupQuorum);
         replicationEndpoint.setExecutor(hornetQServer.getExecutorFactory().getExecutor());
         EndpointConnector endpointConnector = new EndpointConnector();

         HornetQServerLogger.LOGGER.backupServerStarted(hornetQServer.getVersion().getFullVersion(), hornetQServer.getNodeManager().getNodeId());
         hornetQServer.setState(HornetQServerImpl.SERVER_STATE.STARTED);

         SharedNothingBackupQuorum.BACKUP_ACTIVATION signal;
         do
         {
            //locate the first live server to try to replicate
            nodeLocator.locateNode();
            if (closed)
            {
               return;
            }
            Pair<TransportConfiguration, TransportConfiguration> possibleLive = nodeLocator.getLiveConfiguration();
            nodeID = nodeLocator.getNodeID();
            //in a normal (non failback) scenario if we couldn't find our live server we should fail
            if (!attemptFailBack)
            {
               //this shouldn't happen
               if (nodeID == null)
                  throw new RuntimeException("Could not establish the connection");
               hornetQServer.getNodeManager().setNodeID(nodeID);
            }

            try
            {
               clusterControl =  clusterController.connectToNodeInReplicatedCluster(possibleLive.getA());
            }
            catch (Exception e)
            {
               if (possibleLive.getB() != null)
               {
                  try
                  {
                     clusterControl = clusterController.connectToNodeInReplicatedCluster(possibleLive.getB());
                  }
                  catch (Exception e1)
                  {
                     clusterControl = null;
                  }
               }
            }
            if (clusterControl == null)
            {
               //its ok to retry here since we haven't started replication yet
               //it may just be the server has gone since discovery
               Thread.sleep(clusterController.getRetryIntervalForReplicatedCluster());
               signal = SharedNothingBackupQuorum.BACKUP_ACTIVATION.ALREADY_REPLICATING;
               continue;
            }

            hornetQServer.getThreadPool().execute(endpointConnector);
            /**
             * Wait for a signal from the the quorum manager, at this point if replication has been successful we can
             * fail over or if there is an error trying to replicate (such as already replicating) we try the
             * process again on the next live server.  All the action happens inside {@link BackupQuorum}
             */
            signal = backupQuorum.waitForStatusChange();
            /**
             * replicationEndpoint will be holding lots of open files. Make sure they get
             * closed/sync'ed.
             */
            HornetQServerImpl.stopComponent(replicationEndpoint);
            // time to give up
            if (!hornetQServer.isStarted() || signal == STOP)
               return;
               // time to fail over
            else if (signal == FAIL_OVER)
               break;
               // something has gone badly run restart from scratch
            else if (signal == SharedNothingBackupQuorum.BACKUP_ACTIVATION.FAILURE_REPLICATING)
            {
               Thread startThread = new Thread(new Runnable()
               {
                  @Override
                  public void run()
                  {
                     try
                     {
                        hornetQServer.stop();
                     }
                     catch (Exception e)
                     {
                        HornetQServerLogger.LOGGER.errorRestartingBackupServer(e, hornetQServer);
                     }
                  }
               });
               startThread.start();
               return;
            }
            //ok, this live is no good, let's reset and try again
            //close this session factory, we're done with it
            clusterControl.close();
            backupQuorum.reset();
            if (replicationEndpoint.getChannel() != null)
            {
               replicationEndpoint.getChannel().close();
               replicationEndpoint.setChannel(null);
            }
         }
         while (signal == SharedNothingBackupQuorum.BACKUP_ACTIVATION.ALREADY_REPLICATING);

         hornetQServer.getClusterManager().getQuorumManager().unRegisterQuorum(backupQuorum);

         if (!isRemoteBackupUpToDate())
         {
            throw HornetQMessageBundle.BUNDLE.backupServerNotInSync();
         }

         replicaPolicy.getReplicatedPolicy().setReplicaPolicy(replicaPolicy);
         hornetQServer.setHAPolicy(replicaPolicy.getReplicatedPolicy());
         synchronized (hornetQServer)
         {
            if (!hornetQServer.isStarted())
               return;
            HornetQServerLogger.LOGGER.becomingLive(hornetQServer);
            hornetQServer.getNodeManager().stopBackup();
            hornetQServer.getStorageManager().start();
            hornetQServer.getBackupManager().activated();
            if (scalingDown)
            {
               hornetQServer.initialisePart2(true);
            }
            else
            {
               hornetQServer.setActivation(new SharedNothingLiveActivation(hornetQServer, replicaPolicy.getReplicatedPolicy()));
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
         }
      }
      catch (Exception e)
      {
         if ((e instanceof InterruptedException || e instanceof IllegalStateException) && !hornetQServer.isStarted())
            // do not log these errors if the server is being stopped.
            return;
         HornetQServerLogger.LOGGER.initializationError(e);
         e.printStackTrace();
      }
   }

   public void close(final boolean permanently, boolean restarting) throws Exception
   {
      synchronized (this)
      {
         if (backupQuorum != null)
            backupQuorum.causeExit(STOP);
         replicationEndpoint = null;
         closed = true;
      }
      //we have to check as the server policy may have changed
      if (hornetQServer.getHAPolicy().isBackup())
      {
         // To avoid a NPE cause by the stop
         NodeManager nodeManagerInUse = hornetQServer.getNodeManager();

         hornetQServer.interrupBackupThread(nodeManagerInUse);

         if (nodeManagerInUse != null)
         {
            nodeManagerInUse.stopBackup();
         }
      }
   }

   @Override
   public void preStorageClose() throws Exception
   {
      if (replicationEndpoint != null)
      {
         replicationEndpoint.stop();
      }
   }

   @Override
   public JournalLoader createJournalLoader(PostOffice postOffice, PagingManager pagingManager, StorageManager storageManager, QueueFactory queueFactory, NodeManager nodeManager, ManagementService managementService, GroupingHandler groupingHandler, Configuration configuration, HornetQServer parentServer) throws HornetQException
   {
      if (replicaPolicy.getScaleDownPolicy() != null)
      {
         return new BackupRecoveryJournalLoader(postOffice,
               pagingManager,
               storageManager,
               queueFactory,
               nodeManager,
               managementService,
               groupingHandler,
               configuration,
               parentServer,
               ScaleDownPolicy.getScaleDownConnector(replicaPolicy.getScaleDownPolicy(), hornetQServer),
               hornetQServer.getClusterManager().getClusterController());
      }
      else
      {
         return super.createJournalLoader(postOffice,
               pagingManager,
               storageManager,
               queueFactory,
               nodeManager,
               managementService,
               groupingHandler,
               configuration,
               parentServer);
      }
   }

   @Override
   public void haStarted()
   {
      hornetQServer.getClusterManager().getClusterController().setReplicatedClusterName(replicaPolicy.getClusterName());
   }


   /**
    * Wait for backup synchronization when using synchronization
    * @param timeout
    * @param unit
    * @see java.util.concurrent.CountDownLatch#await(long, TimeUnit)
    * @return {@code true} if the server was already initialized or if it was initialized within the
    *         timeout period, {@code false} otherwise.
    * @throws InterruptedException
    */
   public boolean waitForBackupSync(long timeout, TimeUnit unit) throws InterruptedException
   {
      return backupSyncLatch.await(timeout, unit);
   }

   /**
    * Live has notified this server that it is going to stop.
    */
   public void failOver(final ReplicationLiveIsStoppingMessage.LiveStopping finalMessage)
   {
      if (finalMessage == null)
      {
         backupQuorum.causeExit(FAILURE_REPLICATING);
      }
      else
      {
         backupQuorum.failOver(finalMessage);
      }
   }

   public ReplicationEndpoint getReplicationEndpoint()
   {
      return replicationEndpoint;
   }


   /**
    * Whether a remote backup server was in sync with its live server. If it was not in sync, it may
    * not take over the live's functions.
    * <p/>
    * A local backup server or a live server should always return {@code true}
    *
    * @return whether the backup is up-to-date, if the server is not a backup it always returns
    * {@code true}.
    */
   public boolean isRemoteBackupUpToDate()
   {
      return backupUpToDate;
   }

   public void setRemoteBackupUpToDate()
   {
      hornetQServer.getBackupManager().announceBackup();
      backupUpToDate = true;
      backupSyncLatch.countDown();
   }

   /**
    * @throws org.apache.activemq6.api.core.HornetQException
    */
   public void remoteFailOver(ReplicationLiveIsStoppingMessage.LiveStopping finalMessage) throws HornetQException
   {
      HornetQServerLogger.LOGGER.trace("Remote fail-over, got message=" + finalMessage + ", backupUpToDate=" +
            backupUpToDate);
      if (!hornetQServer.getHAPolicy().isBackup() || hornetQServer.getHAPolicy().isSharedStore())
      {
         throw new HornetQInternalErrorException();
      }

      if (!backupUpToDate)
      {
         failOver(null);
      }
      else
      {
         failOver(finalMessage);
      }
   }



   private class EndpointConnector implements Runnable
   {
      @Override
      public void run()
      {
         try
         {
            //we should only try once, if its not there we should move on.
            clusterControl.getSessionFactory().setReconnectAttempts(1);
            backupQuorum.setSessionFactory(clusterControl.getSessionFactory());
            //get the connection and request replication to live
            clusterControl.authorize();
            connectToReplicationEndpoint(clusterControl);
            replicationEndpoint.start();
            clusterControl.announceReplicatingBackupToLive(attemptFailBack, replicaPolicy.getClusterName());
         }
         catch (Exception e)
         {
            //we shouldn't stop the server just mark the connector as tried and unavailable
            HornetQServerLogger.LOGGER.replicationStartProblem(e);
            backupQuorum.causeExit(FAILURE_REPLICATING);
         }
      }

      private synchronized ReplicationEndpoint connectToReplicationEndpoint(final ClusterControl control) throws Exception
      {
         if (!hornetQServer.isStarted())
            return null;
         if (!hornetQServer.getHAPolicy().isBackup())
         {
            throw HornetQMessageBundle.BUNDLE.serverNotBackupServer();
         }

         Channel replicationChannel = control.createReplicationChannel();

         replicationChannel.setHandler(replicationEndpoint);

         if (replicationEndpoint.getChannel() != null)
         {
            throw HornetQMessageBundle.BUNDLE.alreadyHaveReplicationServer();
         }

         replicationEndpoint.setChannel(replicationChannel);

         return replicationEndpoint;
      }
   }
}

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
package org.hornetq.core.server.cluster.ha;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMemberImpl;
import org.hornetq.core.config.BackupStrategy;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.ActivationParams;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.cluster.ClusterControl;
import org.hornetq.core.server.cluster.ClusterController;
import org.hornetq.core.server.cluster.qourum.QuorumVote;
import org.hornetq.core.server.cluster.qourum.QuorumVoteHandler;
import org.hornetq.core.server.cluster.qourum.Vote;
import org.hornetq.spi.core.security.HornetQSecurityManager;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/*
* An HAManager takes care of any colocated backups in a VM. These are either pre configured backups or backups requested
* by other lives. It also takes care of the quorum voting to request backups.
* */
public class HAManager implements HornetQComponent
{
   private static final SimpleString REQUEST_BACKUP_QUORUM_VOTE = new SimpleString("RequestBackupQuorumVote");

   private final HAPolicy haPolicy;

   private final HornetQSecurityManager securityManager;

   private final  HornetQServer server;

   private Set<Configuration> backupServerConfigurations;

   private Map<String, HornetQServer> backupServers = new HashMap<>();

   private boolean started;

   public HAManager(HAPolicy haPolicy, HornetQSecurityManager securityManager, HornetQServer hornetQServer, Set<Configuration> backupServerConfigurations)
   {
      this.haPolicy = haPolicy;
      this.securityManager = securityManager;
      server = hornetQServer;
      this.backupServerConfigurations = backupServerConfigurations;
   }

   /**
    * starts the HA manager, any pre configured backups are started and if a backup is needed a quorum vote in initiated
    */
   public void start()
   {
      if (started)
         return;
      server.getClusterManager().getQuorumManager().registerQuorumHandler(new RequestBackupQuorumVoteHandler());
      if (backupServerConfigurations != null)
      {
         for (Configuration configuration : backupServerConfigurations)
         {
            HornetQServer backup = server.createBackupServer(configuration);
            backupServers.put(configuration.getName(), backup);
         }
      }
      //start the backups
      for (HornetQServer hornetQServer : backupServers.values())
      {
         try
         {
            hornetQServer.start();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }

      //vote for a backup if required
      if (haPolicy.isRequestBackup())
      {
         server.getClusterManager().getQuorumManager().vote(new RequestBackupQuorumVote());
      }
      started = true;
   }

   /**
    * stop any backups
    */
   public void stop()
   {
      for (HornetQServer hornetQServer : backupServers.values())
      {
         try
         {
            hornetQServer.stop();
         }
         catch (Exception e)
         {
            e.printStackTrace();
            //todo
         }
      }
      backupServers.clear();
      started = false;
   }

   @Override
   public boolean isStarted()
   {
      return started;
   }

   public synchronized boolean activateSharedStoreBackup(int backupSize, String journalDirectory, String bindingsDirectory, String largeMessagesDirectory, String pagingDirectory) throws Exception
   {
      if (backupServers.size() >= haPolicy.getMaxBackups() || backupSize != backupServers.size())
      {
         return false;
      }
      Configuration configuration = server.getConfiguration().copy();
      HornetQServer backup = server.createBackupServer(configuration);
      try
      {
         int portOffset = haPolicy.getBackupPortOffset() * (backupServers.size() + 1);
         String name = "colocated_backup_" + backupServers.size() + 1;
         updateSharedStoreConfiguration(configuration, haPolicy.getBackupStrategy(), name, portOffset, haPolicy.getRemoteConnectors(), journalDirectory, bindingsDirectory, largeMessagesDirectory, pagingDirectory);
         //make sure we don't restart as we are colocated
         configuration.getHAPolicy().setRestartBackup(false);
         backupServers.put(configuration.getName(), backup);
         backup.start();
      }
      catch (Exception e)
      {
         backup.stop();
         //todo log a warning
         return false;
      }
      return true;
   }

   /**
    * activate a backup server replicating from a specified node.
    *
    * @param backupSize the number of backups the requesting server thinks there are. if this is changed then we should
    * decline and the requesting server can cast a re vote
    * @param nodeID the id of the node to replicate from
    * @return true if the server was created and started
    * @throws Exception
    */
   public synchronized boolean activateReplicatedBackup(int backupSize, SimpleString nodeID) throws Exception
   {
      if (backupServers.size() >= haPolicy.getMaxBackups() || backupSize != backupServers.size())
      {
         return false;
      }
      Configuration configuration = server.getConfiguration().copy();
      HornetQServer backup = server.createBackupServer(configuration);
      try
      {
         TopologyMember member = server.getClusterManager().getDefaultConnection(null).getTopology().getMember(nodeID.toString());
         int portOffset = haPolicy.getBackupPortOffset() * (backupServers.size() + 1);
         String name = "colocated_backup_" + backupServers.size() + 1;
         updateReplicatedConfiguration(configuration, haPolicy.getBackupStrategy(), name, portOffset, haPolicy.getRemoteConnectors());
         backup.addActivationParam(ActivationParams.REPLICATION_ENDPOINT, member);
         backupServers.put(configuration.getName(), backup);
         backup.start();
      }
      catch (Exception e)
      {
         backup.stop();
         HornetQServerLogger.LOGGER.activateReplicatedBackupFailed(e);
         return false;
      }
      return true;
   }

   /**
    * return the current backup servers
    *
    * @return the backups
    */
   public Map<String, HornetQServer> getBackupServers()
   {
      return backupServers;
   }

   /**
    * send a request to a live server to start a backup for us
    *
    * @param connectorPair the connector for the node to request a backup from
    * @param backupSize the current size of the requested nodes backups
    * @return true if the request wa successful.
    * @throws Exception
    */
   private boolean requestBackup(Pair<TransportConfiguration, TransportConfiguration> connectorPair, int backupSize) throws Exception
   {
      ClusterController clusterController = server.getClusterManager().getClusterController();
      try
      (
            ClusterControl clusterControl = clusterController.connectToNode(connectorPair.getA());
      )
      {
         clusterControl.authorize();
         if (haPolicy.getPolicyType() == HAPolicy.POLICY_TYPE.COLOCATED_SHARED_STORE)
         {

            return clusterControl.requestSharedStoreBackup(backupSize,
                                                    server.getConfiguration().getJournalDirectory(),
                                                    server.getConfiguration().getBindingsDirectory(),
                                                    server.getConfiguration().getLargeMessagesDirectory(),
                                                    server.getConfiguration().getPagingDirectory());
         }
         else
         {
            return clusterControl.requestReplicatedBackup(backupSize, server.getNodeID());

         }
      }
   }

   /**
    * update the backups configuration
    *  @param backupConfiguration the configuration to update
    * @param backupStrategy the strategy for the backup
    * @param name the new name of the backup
    * @param portOffset the offset for the acceptors and any connectors that need changing
    * @param remoteConnectors the connectors that don't need off setting, typically remote
    * @param journalDirectory
    * @param bindingsDirectory
    * @param largeMessagesDirectory
    * @param pagingDirectory
    */
   private static void updateSharedStoreConfiguration(Configuration backupConfiguration,
                                                      BackupStrategy backupStrategy,
                                                      String name,
                                                      int portOffset,
                                                      List<String> remoteConnectors,
                                                      String journalDirectory,
                                                      String bindingsDirectory,
                                                      String largeMessagesDirectory,
                                                      String pagingDirectory)
   {
      backupConfiguration.getHAPolicy().setBackupStrategy(backupStrategy);
      backupConfiguration.setName(name);
      backupConfiguration.setJournalDirectory(journalDirectory);
      backupConfiguration.setBindingsDirectory(bindingsDirectory);
      backupConfiguration.setLargeMessagesDirectory(largeMessagesDirectory);
      backupConfiguration.setPagingDirectory(pagingDirectory);
      backupConfiguration.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_SHARED_STORE);
      updateAcceptorsAndConnectors(backupConfiguration, portOffset, remoteConnectors);
   }

   /**
    * update the backups configuration
    *
    * @param backupConfiguration the configuration to update
    * @param backupStrategy the strategy for the backup
    * @param name the new name of the backup
    * @param portOffset the offset for the acceptors and any connectors that need changing
    * @param remoteConnectors the connectors that don't need off setting, typically remote
    */
   private static void updateReplicatedConfiguration(Configuration backupConfiguration,
                                                     BackupStrategy backupStrategy,
                                                     String name,
                                                     int portOffset,
                                                     List<String> remoteConnectors)
   {
      backupConfiguration.getHAPolicy().setBackupStrategy(backupStrategy);
      backupConfiguration.setName(name);
      backupConfiguration.setJournalDirectory(backupConfiguration.getJournalDirectory() + name);
      backupConfiguration.setPagingDirectory(backupConfiguration.getPagingDirectory() + name);
      backupConfiguration.setLargeMessagesDirectory(backupConfiguration.getLargeMessagesDirectory() + name);
      backupConfiguration.setBindingsDirectory(backupConfiguration.getBindingsDirectory() + name);
      backupConfiguration.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_REPLICATED);
      updateAcceptorsAndConnectors(backupConfiguration, portOffset, remoteConnectors);
   }

   private static void updateAcceptorsAndConnectors(Configuration backupConfiguration, int portOffset, List<String> remoteConnectors)
   {
      //we only do this if we are a full server, if scale down then our acceptors wont be needed and our connectors will
      // be the same as the parent server
      if (backupConfiguration.getHAPolicy().getBackupStrategy() == BackupStrategy.FULL)
      {
         Set<TransportConfiguration> acceptors = backupConfiguration.getAcceptorConfigurations();
         for (TransportConfiguration acceptor : acceptors)
         {
            updatebackupParams(backupConfiguration.getName(), portOffset, acceptor.getParams());
         }
         Map<String, TransportConfiguration> connectorConfigurations = backupConfiguration.getConnectorConfigurations();
         for (Map.Entry<String, TransportConfiguration> entry : connectorConfigurations.entrySet())
         {
            //check to make sure we aren't a remote connector as this shouldn't be changed
            if (!remoteConnectors.contains(entry.getValue().getName()))
            {
               updatebackupParams(backupConfiguration.getName(), portOffset, entry.getValue().getParams());
            }
         }
      }
      else if (backupConfiguration.getHAPolicy().getBackupStrategy() == BackupStrategy.SCALE_DOWN)
      {
         //if we are scaling down then we wont need any acceptors but clear anyway for belts and braces
         backupConfiguration.getAcceptorConfigurations().clear();
      }
   }

   private static void updatebackupParams(String name, int portOffset, Map<String, Object> params)
   {
      if (params != null)
      {
         Object port = params.get("port");
         if (port != null)
         {
            Integer integer = Integer.valueOf(port.toString());
            integer += portOffset;
            params.put("port", integer.toString());
         }
         Object serverId = params.get("server-id");
         if (serverId != null)
         {
            params.put("server-id", serverId.toString() + "(" + name + ")");
         }
      }
   }

   public HAPolicy getHAPolicy()
   {
      return haPolicy;
   }

   public ServerLocatorInternal getScaleDownConnector() throws HornetQException
   {
      if (!haPolicy.getScaleDownConnectors().isEmpty())
      {
         return (ServerLocatorInternal) HornetQClient.createServerLocatorWithHA(connectorNameListToArray(haPolicy.getScaleDownConnectors()));
      }
      else if (haPolicy.getScaleDownDiscoveryGroup() != null)
      {
         DiscoveryGroupConfiguration dg = server.getConfiguration().getDiscoveryGroupConfigurations().get(haPolicy.getScaleDownDiscoveryGroup());

         if (dg == null)
         {
            throw HornetQMessageBundle.BUNDLE.noDiscoveryGroupFound(dg);
         }
         return  (ServerLocatorInternal) HornetQClient.createServerLocatorWithHA(dg);
      }
      else
      {
         Map<String, TransportConfiguration> connectorConfigurations = server.getConfiguration().getConnectorConfigurations();
         for (TransportConfiguration transportConfiguration : connectorConfigurations.values())
         {
            if (transportConfiguration.getFactoryClassName().equals(InVMConnectorFactory.class.getName()))
            {
               return (ServerLocatorInternal) HornetQClient.createServerLocatorWithHA(transportConfiguration);
            }
         }
      }
      throw HornetQMessageBundle.BUNDLE.noConfigurationFoundForScaleDown();
   }



   private TransportConfiguration[] connectorNameListToArray(final List<String> connectorNames)
   {
      TransportConfiguration[] tcConfigs = (TransportConfiguration[]) Array.newInstance(TransportConfiguration.class,
            connectorNames.size());
      int count = 0;
      for (String connectorName : connectorNames)
      {
         TransportConfiguration connector = server.getConfiguration().getConnectorConfigurations().get(connectorName);

         if (connector == null)
         {
            HornetQServerLogger.LOGGER.bridgeNoConnector(connectorName);

            return null;
         }

         tcConfigs[count++] = connector;
      }

      return tcConfigs;
   }
   /**
    * A vote handler for incoming backup request votes
    */
   private final class RequestBackupQuorumVoteHandler implements QuorumVoteHandler
   {
      @Override
      public Vote vote(Vote vote)
      {
         return new RequestBackupVote(backupServers.size(), server.getNodeID().toString(), backupServers.size() < haPolicy.getMaxBackups());
      }

      @Override
      public SimpleString getQuorumName()
      {
         return REQUEST_BACKUP_QUORUM_VOTE;
      }

      @Override
      public Vote decode(HornetQBuffer voteBuffer)
      {
         RequestBackupVote requestBackupVote = new RequestBackupVote();
         requestBackupVote.decode(voteBuffer);
         return requestBackupVote;
      }
   }

   /**
    * a quorum vote for backup requests
    */
   private final class RequestBackupQuorumVote extends QuorumVote<RequestBackupVote, Pair<String, Integer>>
   {
      //the available nodes that we can request
      private final List<Pair<String, Integer>> nodes = new ArrayList<>();

      public RequestBackupQuorumVote()
      {
         super(REQUEST_BACKUP_QUORUM_VOTE);
      }

      @Override
      public Vote connected()
      {
         return new RequestBackupVote();
      }

      @Override
      public Vote notConnected()
      {
         return new RequestBackupVote();
      }

      @Override
      public void vote(RequestBackupVote vote)
      {
         //if the returned vote is available add it to the nodes we can request
         if (vote.backupAvailable)
         {
            nodes.add(vote.getVote());
         }
      }

      @Override
      public Pair<String, Integer> getDecision()
      {
         //sort the nodes by how many backups they have and choose the first
         Collections.sort(nodes, new Comparator<Pair<String, Integer>>()
         {
            @Override
            public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2)
            {
               return o1.getB().compareTo(o2.getB());
            }
         });
         return nodes.get(0);
      }

      @Override
      public void allVotesCast(Topology voteTopology)
      {
         //if we have any nodes that we can request then send a request
         if (nodes.size() > 0)
         {
            Pair<String, Integer> decision = getDecision();
            TopologyMemberImpl member = voteTopology.getMember(decision.getA());
            try
            {
               boolean backupStarted = requestBackup(member.getConnector(), decision.getB().intValue());
               if (!backupStarted)
               {
                  nodes.clear();
                  server.getScheduledPool().schedule(new Runnable()
                  {
                     @Override
                     public void run()
                     {
                        server.getClusterManager().getQuorumManager().vote(new RequestBackupQuorumVote());
                     }
                  }, haPolicy.getBackupRequestRetryInterval(), TimeUnit.MILLISECONDS);
               }
            }
            catch (Exception e)
            {
               e.printStackTrace();
               //todo
            }
         }
         else
         {
            nodes.clear();
            server.getScheduledPool().schedule(new Runnable()
            {
               @Override
               public void run()
               {
                  server.getClusterManager().getQuorumManager().vote(RequestBackupQuorumVote.this);
               }
            }, haPolicy.getBackupRequestRetryInterval(), TimeUnit.MILLISECONDS);
         }
      }

      @Override
      public SimpleString getName()
      {
         return REQUEST_BACKUP_QUORUM_VOTE;
      }
   }

   class RequestBackupVote extends Vote<Pair<String, Integer>>
   {
      private int backupsSize;
      private String nodeID;
      private boolean backupAvailable;

      public RequestBackupVote()
      {
         backupsSize = -1;
      }

      public RequestBackupVote(int backupsSize, String nodeID, boolean backupAvailable)
      {
         this.backupsSize = backupsSize;
         this.nodeID = nodeID;
         this.backupAvailable = backupAvailable;
      }

      @Override
      public void encode(HornetQBuffer buff)
      {
         buff.writeInt(backupsSize);
         buff.writeNullableString(nodeID);
         buff.writeBoolean(backupAvailable);
      }

      @Override
      public void decode(HornetQBuffer buff)
      {
         backupsSize = buff.readInt();
         nodeID = buff.readNullableString();
         backupAvailable = buff.readBoolean();
      }

      @Override
      public boolean isRequestServerVote()
      {
         return true;
      }

      @Override
      public Pair<String, Integer> getVote()
      {
         return new Pair<>(nodeID, backupsSize);
      }
   }
}

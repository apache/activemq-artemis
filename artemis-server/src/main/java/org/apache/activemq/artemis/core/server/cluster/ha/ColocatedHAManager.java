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
package org.apache.activemq.artemis.core.server.cluster.ha;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActivationParams;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.cluster.ClusterControl;
import org.apache.activemq.artemis.core.server.cluster.ClusterController;

public class ColocatedHAManager implements HAManager {

   private final ColocatedPolicy haPolicy;

   private final ActiveMQServer server;

   private final Map<String, ActiveMQServer> backupServers = new HashMap<>();

   private boolean started;

   public ColocatedHAManager(ColocatedPolicy haPolicy, ActiveMQServer activeMQServer) {
      this.haPolicy = haPolicy;
      server = activeMQServer;
   }

   /**
    * starts the HA manager.
    */
   @Override
   public void start() {
      if (started)
         return;

      server.getActivation().haStarted();

      started = true;
   }

   /**
    * stop any backups
    */
   @Override
   public void stop() {
      for (ActiveMQServer activeMQServer : backupServers.values()) {
         try {
            activeMQServer.stop();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
         }
      }
      backupServers.clear();
      started = false;
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   public synchronized boolean activateBackup(int backupSize,
                                              String journalDirectory,
                                              String bindingsDirectory,
                                              String largeMessagesDirectory,
                                              String pagingDirectory,
                                              SimpleString nodeID) throws Exception {
      if (backupServers.size() >= haPolicy.getMaxBackups() || backupSize != backupServers.size()) {
         return false;
      }
      if (haPolicy.getBackupPolicy().isSharedStore()) {
         return activateSharedStoreBackup(journalDirectory, bindingsDirectory, largeMessagesDirectory, pagingDirectory);
      } else {
         return activateReplicatedBackup(nodeID);
      }
   }

   /**
    * return the current backup servers
    *
    * @return the backups
    */
   @Override
   public Map<String, ActiveMQServer> getBackupServers() {
      return backupServers;
   }

   /**
    * send a request to a live server to start a backup for us
    *
    * @param connectorPair the connector for the node to request a backup from
    * @param backupSize    the current size of the requested nodes backups
    * @param replicated
    * @return true if the request wa successful.
    * @throws Exception
    */
   public boolean requestBackup(Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                                int backupSize,
                                boolean replicated) throws Exception {
      ClusterController clusterController = server.getClusterManager().getClusterController();
      try
         (
            ClusterControl clusterControl = clusterController.connectToNode(connectorPair.getA());
         ) {
         clusterControl.authorize();
         if (replicated) {
            return clusterControl.requestReplicatedBackup(backupSize, server.getNodeID());
         } else {
            return clusterControl.requestSharedStoreBackup(backupSize, server.getConfiguration().getJournalLocation().getAbsolutePath(), server.getConfiguration().getBindingsLocation().getAbsolutePath(), server.getConfiguration().getLargeMessagesLocation().getAbsolutePath(), server.getConfiguration().getPagingLocation().getAbsolutePath());

         }
      }
   }

   private synchronized boolean activateSharedStoreBackup(String journalDirectory,
                                                          String bindingsDirectory,
                                                          String largeMessagesDirectory,
                                                          String pagingDirectory) throws Exception {
      Configuration configuration = server.getConfiguration().copy();
      ActiveMQServer backup = server.createBackupServer(configuration);
      try {
         int portOffset = haPolicy.getBackupPortOffset() * (backupServers.size() + 1);
         String name = "colocated_backup_" + backupServers.size() + 1;
         //make sure we don't restart as we are colocated
         haPolicy.getBackupPolicy().setRestartBackup(false);
         //set the backup policy
         backup.setHAPolicy(haPolicy.getBackupPolicy());
         updateSharedStoreConfiguration(configuration, name, portOffset, haPolicy.getExcludedConnectors(), journalDirectory, bindingsDirectory, largeMessagesDirectory, pagingDirectory, haPolicy.getBackupPolicy().getScaleDownPolicy() == null);

         backupServers.put(configuration.getName(), backup);
         backup.start();
      } catch (Exception e) {
         backup.stop();
         ActiveMQServerLogger.LOGGER.activateSharedStoreSlaveFailed(e);
         return false;
      }
      ActiveMQServerLogger.LOGGER.activatingSharedStoreSlave();
      return true;
   }

   /**
    * activate a backup server replicating from a specified node.
    *
    * decline and the requesting server can cast a re vote
    *
    * @param nodeID the id of the node to replicate from
    * @return true if the server was created and started
    * @throws Exception
    */
   private synchronized boolean activateReplicatedBackup(SimpleString nodeID) throws Exception {
      Configuration configuration = server.getConfiguration().copy();
      ActiveMQServer backup = server.createBackupServer(configuration);
      try {
         TopologyMember member = server.getClusterManager().getDefaultConnection(null).getTopology().getMember(nodeID.toString());
         int portOffset = haPolicy.getBackupPortOffset() * (backupServers.size() + 1);
         String name = "colocated_backup_" + backupServers.size() + 1;
         //make sure we don't restart as we are colocated
         haPolicy.getBackupPolicy().setRestartBackup(false);
         //set the backup policy
         backup.setHAPolicy(haPolicy.getBackupPolicy());
         updateReplicatedConfiguration(configuration, name, portOffset, haPolicy.getExcludedConnectors(), haPolicy.getBackupPolicy().getScaleDownPolicy() == null);
         backup.addActivationParam(ActivationParams.REPLICATION_ENDPOINT, member);
         backupServers.put(configuration.getName(), backup);
         backup.start();
      } catch (Exception e) {
         backup.stop();
         ActiveMQServerLogger.LOGGER.activateReplicatedBackupFailed(e);
         return false;
      }
      ActiveMQServerLogger.LOGGER.activatingReplica(nodeID);
      return true;
   }

   /**
    * update the backups configuration
    *
    * @param backupConfiguration    the configuration to update
    * @param name                   the new name of the backup
    * @param portOffset             the offset for the acceptors and any connectors that need changing
    * @param remoteConnectors       the connectors that don't need off setting, typically remote
    * @param journalDirectory
    * @param bindingsDirectory
    * @param largeMessagesDirectory
    * @param pagingDirectory
    * @param fullServer
    */
   private static void updateSharedStoreConfiguration(Configuration backupConfiguration,
                                                      String name,
                                                      int portOffset,
                                                      List<String> remoteConnectors,
                                                      String journalDirectory,
                                                      String bindingsDirectory,
                                                      String largeMessagesDirectory,
                                                      String pagingDirectory,
                                                      boolean fullServer) {
      backupConfiguration.setName(name);
      backupConfiguration.setJournalDirectory(journalDirectory);
      backupConfiguration.setBindingsDirectory(bindingsDirectory);
      backupConfiguration.setLargeMessagesDirectory(largeMessagesDirectory);
      backupConfiguration.setPagingDirectory(pagingDirectory);
      updateAcceptorsAndConnectors(backupConfiguration, portOffset, remoteConnectors, fullServer);
   }

   /**
    * update the backups configuration
    *
    * @param backupConfiguration the configuration to update
    * @param name                the new name of the backup
    * @param portOffset          the offset for the acceptors and any connectors that need changing
    * @param remoteConnectors    the connectors that don't need off setting, typically remote
    */
   private static void updateReplicatedConfiguration(Configuration backupConfiguration,
                                                     String name,
                                                     int portOffset,
                                                     List<String> remoteConnectors,
                                                     boolean fullServer) {
      backupConfiguration.setName(name);
      backupConfiguration.setJournalDirectory(backupConfiguration.getJournalDirectory() + name);
      backupConfiguration.setPagingDirectory(backupConfiguration.getPagingDirectory() + name);
      backupConfiguration.setLargeMessagesDirectory(backupConfiguration.getLargeMessagesDirectory() + name);
      backupConfiguration.setBindingsDirectory(backupConfiguration.getBindingsDirectory() + name);
      updateAcceptorsAndConnectors(backupConfiguration, portOffset, remoteConnectors, fullServer);
   }

   private static void updateAcceptorsAndConnectors(Configuration backupConfiguration,
                                                    int portOffset,
                                                    List<String> remoteConnectors,
                                                    boolean fullServer) {
      //we only do this if we are a full server, if scale down then our acceptors wont be needed and our connectors will
      // be the same as the parent server
      if (fullServer) {
         Set<TransportConfiguration> acceptors = backupConfiguration.getAcceptorConfigurations();
         for (TransportConfiguration acceptor : acceptors) {
            updatebackupParams(backupConfiguration.getName(), portOffset, acceptor.getParams());
         }
         Map<String, TransportConfiguration> connectorConfigurations = backupConfiguration.getConnectorConfigurations();
         for (Map.Entry<String, TransportConfiguration> entry : connectorConfigurations.entrySet()) {
            //check to make sure we aren't a remote connector as this shouldn't be changed
            if (!remoteConnectors.contains(entry.getValue().getName())) {
               updatebackupParams(backupConfiguration.getName(), portOffset, entry.getValue().getParams());
            }
         }
      } else {
         //if we are scaling down then we wont need any acceptors but clear anyway for belts and braces
         backupConfiguration.getAcceptorConfigurations().clear();
      }
   }

   private static void updatebackupParams(String name, int portOffset, Map<String, Object> params) {
      if (params != null) {
         Object port = params.get("port");
         if (port != null) {
            Integer integer = Integer.valueOf(port.toString());
            integer += portOffset;
            params.put("port", integer.toString());
         }
         Object serverId = params.get("serverId");
         if (serverId != null) {
            Integer newid = Integer.parseInt(serverId.toString()) + portOffset;
            params.put("serverId", newid.toString());
         }
      }
   }
}

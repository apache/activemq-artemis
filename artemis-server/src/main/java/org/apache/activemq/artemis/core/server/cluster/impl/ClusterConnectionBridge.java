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
package org.apache.activemq.artemis.core.server.cluster.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.message.impl.MessageImpl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.cluster.ActiveMQServerSideProtocolManagerFactory;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.core.server.cluster.MessageFlowRecord;
import org.apache.activemq.artemis.core.server.cluster.Transformer;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jboss.logging.Logger;

/**
 * A bridge with extra functionality only available when the server is clustered.
 * <p>
 * Such as such adding extra properties and setting up notifications between the nodes.
 */
public class ClusterConnectionBridge extends BridgeImpl {

   private static final Logger logger = Logger.getLogger(ClusterConnectionBridge.class);

   private final ClusterConnection clusterConnection;

   private final ClusterManager clusterManager;

   private final MessageFlowRecord flowRecord;

   private final SimpleString managementAddress;

   private final SimpleString managementNotificationAddress;

   private ClientConsumer notifConsumer;

   private final SimpleString idsHeaderName;

   private final long targetNodeEventUID;

   private final ServerLocatorInternal discoveryLocator;

   private final String storeAndForwardPrefix;

   public ClusterConnectionBridge(final ClusterConnection clusterConnection,
                                  final ClusterManager clusterManager,
                                  final ServerLocatorInternal targetLocator,
                                  final ServerLocatorInternal discoveryLocator,
                                  final int initialConnectAttempts,
                                  final int reconnectAttempts,
                                  final long retryInterval,
                                  final double retryMultiplier,
                                  final long maxRetryInterval,
                                  final UUID nodeUUID,
                                  final long targetNodeEventUID,
                                  final String targetNodeID,
                                  final SimpleString name,
                                  final Queue queue,
                                  final Executor executor,
                                  final Filter filterString,
                                  final SimpleString forwardingAddress,
                                  final ScheduledExecutorService scheduledExecutor,
                                  final Transformer transformer,
                                  final boolean useDuplicateDetection,
                                  final String user,
                                  final String password,
                                  final StorageManager storageManager,
                                  final SimpleString managementAddress,
                                  final SimpleString managementNotificationAddress,
                                  final MessageFlowRecord flowRecord,
                                  final TransportConfiguration connector,
                                  final String storeAndForwardPrefix) {
      super(targetLocator, initialConnectAttempts, reconnectAttempts, 0, // reconnectAttemptsOnSameNode means nothing on the clustering bridge since we always try the same
            retryInterval, retryMultiplier, maxRetryInterval, nodeUUID, name, queue, executor, filterString, forwardingAddress, scheduledExecutor, transformer, useDuplicateDetection, user, password, storageManager);

      this.discoveryLocator = discoveryLocator;

      idsHeaderName = MessageImpl.HDR_ROUTE_TO_IDS.concat(name);

      this.clusterConnection = clusterConnection;

      this.clusterManager = clusterManager;

      this.targetNodeEventUID = targetNodeEventUID;
      this.targetNodeID = targetNodeID;
      this.managementAddress = managementAddress;
      this.managementNotificationAddress = managementNotificationAddress;
      this.flowRecord = flowRecord;

      // we need to disable DLQ check on the clustered bridges
      queue.setInternalQueue(true);

      if (logger.isTraceEnabled()) {
         logger.trace("Setting up bridge between " + clusterConnection.getConnector() + " and " + targetLocator, new Exception("trace"));
      }

      this.storeAndForwardPrefix = storeAndForwardPrefix;
   }

   @Override
   protected ClientSessionFactoryInternal createSessionFactory() throws Exception {
      serverLocator.setProtocolManagerFactory(ActiveMQServerSideProtocolManagerFactory.getInstance(serverLocator));
      ClientSessionFactoryInternal factory = (ClientSessionFactoryInternal) serverLocator.createSessionFactory(targetNodeID);
      setSessionFactory(factory);

      if (factory == null) {
         return null;
      }
      factory.setReconnectAttempts(0);
      factory.getConnection().addFailureListener(this);
      return factory;
   }

   @Override
   protected ServerMessage beforeForward(final ServerMessage message) {
      // We make a copy of the message, then we strip out the unwanted routing id headers and leave
      // only
      // the one pertinent for the address node - this is important since different queues on different
      // nodes could have same queue ids
      // Note we must copy since same message may get routed to other nodes which require different headers
      ServerMessage messageCopy = message.copy();

      if (logger.isTraceEnabled()) {
         logger.trace("Clustered bridge  copied message " + message + " as " + messageCopy + " before delivery");
      }

      // TODO - we can optimise this

      Set<SimpleString> propNames = new HashSet<>(messageCopy.getPropertyNames());

      byte[] queueIds = message.getBytesProperty(idsHeaderName);

      if (queueIds == null) {
         // Sanity check only
         ActiveMQServerLogger.LOGGER.noQueueIdDefined(message, messageCopy, idsHeaderName);
         throw new IllegalStateException("no queueIDs defined");
      }

      for (SimpleString propName : propNames) {
         if (propName.startsWith(MessageImpl.HDR_ROUTE_TO_IDS)) {
            messageCopy.removeProperty(propName);
         }
      }

      messageCopy.putBytesProperty(MessageImpl.HDR_ROUTE_TO_IDS, queueIds);

      messageCopy = super.beforeForward(messageCopy);

      return messageCopy;
   }

   private void setupNotificationConsumer() throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug("Setting up notificationConsumer between " + this.clusterConnection.getConnector() +
                         " and " +
                         flowRecord.getBridge().getForwardingConnection() +
                         " clusterConnection = " +
                         this.clusterConnection.getName() +
                         " on server " +
                         clusterConnection.getServer());
      }
      if (flowRecord != null) {
         flowRecord.reset();

         if (notifConsumer != null) {
            try {
               logger.debug("Closing notification Consumer for reopening " + notifConsumer +
                               " on bridge " +
                               this.getName());
               notifConsumer.close();

               notifConsumer = null;
            } catch (ActiveMQException e) {
               ActiveMQServerLogger.LOGGER.errorClosingConsumer(e);
            }
         }

         // Get the queue data

         String qName = "notif." + UUIDGenerator.getInstance().generateStringUUID() +
            "." +
            clusterConnection.getServer();

         SimpleString notifQueueName = new SimpleString(qName);

         String filterString = flowRecord.getAddress();

         SimpleString filter = new SimpleString(ManagementHelper.HDR_BINDING_TYPE + "<>" +
                                                   BindingType.DIVERT.toInt() +
                                                   " AND " +
                                                   ManagementHelper.HDR_NOTIFICATION_TYPE +
                                                   " IN ('" +
                                                   CoreNotificationType.BINDING_ADDED +
                                                   "','" +
                                                   CoreNotificationType.BINDING_REMOVED +
                                                   "','" +
                                                   CoreNotificationType.CONSUMER_CREATED +
                                                   "','" +
                                                   CoreNotificationType.CONSUMER_CLOSED +
                                                   "','" +
                                                   CoreNotificationType.PROPOSAL +
                                                   "','" +
                                                   CoreNotificationType.PROPOSAL_RESPONSE +
                                                   "','" +
                                                   CoreNotificationType.UNPROPOSAL +
                                                   "') AND " +
                                                   ManagementHelper.HDR_DISTANCE +
                                                   "<" +
                                                   flowRecord.getMaxHops() +
                                                   " AND (" +
                                                   createSelectorFromAddress(appendIgnoresToFilter(flowRecord.getAddress())) +
                                                   ")");

         sessionConsumer.createTemporaryQueue(managementNotificationAddress, notifQueueName, filter);

         notifConsumer = sessionConsumer.createConsumer(notifQueueName);

         notifConsumer.setMessageHandler(flowRecord);

         sessionConsumer.start();

         ClientMessage message = sessionConsumer.createMessage(false);
         if (logger.isTraceEnabled()) {
            logger.trace("Requesting sendQueueInfoToQueue through " + this, new Exception("trace"));
         }
         ManagementHelper.putOperationInvocation(message, ResourceNames.CORE_SERVER, "sendQueueInfoToQueue", notifQueueName.toString(), flowRecord.getAddress());

         try (ClientProducer prod = sessionConsumer.createProducer(managementAddress)) {
            if (logger.isDebugEnabled()) {
               logger.debug("Cluster connection bridge on " + clusterConnection + " requesting information on queues");
            }

            prod.send(message);
         }
      }
   }


   /**
    * Takes in a string of an address filter or comma separated list and generates an appropriate JMS selector for
    * filtering queues.
    *
    * @param address
    */
   public static String createSelectorFromAddress(String address) {
      StringBuilder stringBuilder = new StringBuilder();

      // Support standard address (not a list) case.
      if (!address.contains(",")) {
         if (address.startsWith("!")) {
            stringBuilder.append(ManagementHelper.HDR_ADDRESS + " NOT LIKE '" + address.substring(1, address.length()) + "%'");
         } else {
            stringBuilder.append(ManagementHelper.HDR_ADDRESS + " LIKE '" + address + "%'");
         }
         return stringBuilder.toString();
      }

      // For comma separated lists build a JMS selector statement based on the list items
      return buildSelectorFromArray(address.split(","));
   }

   public static String buildSelectorFromArray(String[] list) {
      List<String> includes = new ArrayList<>();
      List<String> excludes = new ArrayList<>();

      // Split the list into addresses to match and addresses to exclude.
      for (String s : list) {
         if (s.startsWith("!")) {
            excludes.add(s.substring(1, s.length()));
         } else {
            includes.add(s);
         }
      }

      // Build the address matching part of the selector
      StringBuilder builder = new StringBuilder("(");
      if (includes.size() > 0) {
         if (excludes.size() > 0)
            builder.append("(");
         for (int i = 0; i < includes.size(); i++) {
            builder.append("(" + ManagementHelper.HDR_ADDRESS + " LIKE '" + includes.get(i) + "%')");
            if (i < includes.size() - 1)
               builder.append(" OR ");
         }
         if (excludes.size() > 0)
            builder.append(")");
      }

      // Build the address exclusion part of the selector
      if (excludes.size() > 0) {
         if (includes.size() > 0)
            builder.append(" AND (");
         for (int i = 0; i < excludes.size(); i++) {
            builder.append("(" + ManagementHelper.HDR_ADDRESS + " NOT LIKE '" + excludes.get(i) + "%')");
            if (i < excludes.size() - 1)
               builder.append(" AND ");
         }
         if (includes.size() > 0)
            builder.append(")");
      }
      builder.append(")");
      return builder.toString();
   }

   private String appendIgnoresToFilter(String filterString) {
      if (filterString != null && !filterString.isEmpty()) {
         filterString += ",";
      }
      filterString += "!" + storeAndForwardPrefix;
      filterString += ",!" + managementAddress;
      filterString += ",!" + managementNotificationAddress;
      return filterString;
   }

   @Override
   protected void afterConnect() throws Exception {
      super.afterConnect();
      setupNotificationConsumer();
   }

   @Override
   protected void tryScheduleRetryReconnect(final ActiveMQExceptionType type) {
      if (type != ActiveMQExceptionType.DISCONNECTED) {
         scheduleRetryConnect();
      }
   }

   @Override
   protected void fail(final boolean permanently) {
      logger.debug("Cluster Bridge " + this.getName() + " failed, permanently=" + permanently);
      super.fail(permanently);

      if (permanently) {
         logger.debug("cluster node for bridge " + this.getName() + " is permanently down");
         clusterConnection.removeRecord(targetNodeID);
      } else {
         clusterConnection.disconnectRecord(targetNodeID);
      }
   }

   @Override
   protected boolean isPlainCoreBridge() {
      return false;
   }
}

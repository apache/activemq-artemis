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
package org.apache.activemq.artemis.jms.management.impl;

import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.management.Parameter;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.api.jms.management.ConnectionFactoryControl;
import org.apache.activemq.artemis.api.jms.management.DestinationControl;
import org.apache.activemq.artemis.api.jms.management.JMSQueueControl;
import org.apache.activemq.artemis.api.jms.management.JMSServerControl;
import org.apache.activemq.artemis.api.jms.management.TopicControl;
import org.apache.activemq.artemis.core.client.impl.Topology;
import org.apache.activemq.artemis.core.client.impl.TopologyMemberImpl;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.management.impl.AbstractControl;
import org.apache.activemq.artemis.core.management.impl.MBeanInfoHelper;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.jms.server.ActiveMQJMSServerLogger;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.artemis.jms.server.management.JMSNotificationType;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.JsonLoader;
import org.apache.activemq.artemis.utils.TypedProperties;

public class JMSServerControlImpl extends AbstractControl implements JMSServerControl, NotificationEmitter, org.apache.activemq.artemis.core.server.management.NotificationListener {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JMSServerManager server;

   private final NotificationBroadcasterSupport broadcaster;

   private final AtomicLong notifSeq = new AtomicLong(0);

   // Static --------------------------------------------------------

   private static String[] convert(final Object[] bindings) {
      String[] theBindings = new String[bindings.length];
      for (int i = 0, bindingsLength = bindings.length; i < bindingsLength; i++) {
         theBindings[i] = bindings[i].toString().trim();
      }
      return theBindings;
   }

   private static String[] toArray(final String commaSeparatedString) {
      if (commaSeparatedString == null || commaSeparatedString.trim().length() == 0) {
         return new String[0];
      }
      String[] values = commaSeparatedString.split(",");
      String[] trimmed = new String[values.length];
      for (int i = 0; i < values.length; i++) {
         trimmed[i] = values[i].trim();
         trimmed[i] = trimmed[i].replace("&comma;", ",");
      }
      return trimmed;
   }

   public static MBeanNotificationInfo[] getNotificationInfos() {
      JMSNotificationType[] values = JMSNotificationType.values();
      String[] names = new String[values.length];
      for (int i = 0; i < values.length; i++) {
         names[i] = values[i].toString();
      }
      return new MBeanNotificationInfo[]{new MBeanNotificationInfo(names, JMSServerControl.class.getName(), "Notifications emitted by a JMS Server")};
   }

   // Constructors --------------------------------------------------

   public JMSServerControlImpl(final JMSServerManager server) throws Exception {
      super(JMSServerControl.class, server.getActiveMQServer().getStorageManager());
      this.server = server;
      broadcaster = new NotificationBroadcasterSupport();
      server.getActiveMQServer().getManagementService().addNotificationListener(this);
   }

   // Public --------------------------------------------------------

   // JMSServerControlMBean implementation --------------------------

   /**
    * See the interface definition for the javadoc.
    */
   @Override
   public void createConnectionFactory(String name,
                                       boolean ha,
                                       boolean useDiscovery,
                                       int cfType,
                                       String[] connectorNames,
                                       Object[] bindings) throws Exception {
      checkStarted();

      clearIO();

      try {
         if (useDiscovery) {
            if (connectorNames == null || connectorNames.length == 0) {
               throw new IllegalArgumentException("no discovery group name supplied");
            }
            server.createConnectionFactory(name, ha, JMSFactoryType.valueOf(cfType), connectorNames[0], JMSServerControlImpl.convert(bindings));
         } else {
            List<String> connectorList = new ArrayList<>(connectorNames.length);

            for (String str : connectorNames) {
               connectorList.add(str);
            }

            server.createConnectionFactory(name, ha, JMSFactoryType.valueOf(cfType), connectorList, JMSServerControlImpl.convert(bindings));
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void createConnectionFactory(String name,
                                       boolean ha,
                                       boolean useDiscovery,
                                       int cfType,
                                       String connectors,
                                       String bindings,
                                       String clientID,
                                       long clientFailureCheckPeriod,
                                       long connectionTTL,
                                       long callTimeout,
                                       long callFailoverTimeout,
                                       int minLargeMessageSize,
                                       boolean compressLargeMessages,
                                       int consumerWindowSize,
                                       int consumerMaxRate,
                                       int confirmationWindowSize,
                                       int producerWindowSize,
                                       int producerMaxRate,
                                       boolean blockOnAcknowledge,
                                       boolean blockOnDurableSend,
                                       boolean blockOnNonDurableSend,
                                       boolean autoGroup,
                                       boolean preAcknowledge,
                                       String loadBalancingPolicyClassName,
                                       int transactionBatchSize,
                                       int dupsOKBatchSize,
                                       boolean useGlobalPools,
                                       int scheduledThreadPoolMaxSize,
                                       int threadPoolMaxSize,
                                       long retryInterval,
                                       double retryIntervalMultiplier,
                                       long maxRetryInterval,
                                       int reconnectAttempts,
                                       boolean failoverOnInitialConnection,
                                       String groupId) throws Exception {
      createConnectionFactory(name, ha, useDiscovery, cfType, toArray(connectors), toArray(bindings), clientID, clientFailureCheckPeriod, connectionTTL, callTimeout, callFailoverTimeout, minLargeMessageSize, compressLargeMessages, consumerWindowSize, consumerMaxRate, confirmationWindowSize, producerWindowSize, producerMaxRate, blockOnAcknowledge, blockOnDurableSend, blockOnNonDurableSend, autoGroup, preAcknowledge, loadBalancingPolicyClassName, transactionBatchSize, dupsOKBatchSize, useGlobalPools, scheduledThreadPoolMaxSize, threadPoolMaxSize, retryInterval, retryIntervalMultiplier, maxRetryInterval, reconnectAttempts, failoverOnInitialConnection, groupId);
   }

   @Override
   public void createConnectionFactory(String name,
                                       boolean ha,
                                       boolean useDiscovery,
                                       int cfType,
                                       String[] connectorNames,
                                       String[] bindings,
                                       String clientID,
                                       long clientFailureCheckPeriod,
                                       long connectionTTL,
                                       long callTimeout,
                                       long callFailoverTimeout,
                                       int minLargeMessageSize,
                                       boolean compressLargeMessages,
                                       int consumerWindowSize,
                                       int consumerMaxRate,
                                       int confirmationWindowSize,
                                       int producerWindowSize,
                                       int producerMaxRate,
                                       boolean blockOnAcknowledge,
                                       boolean blockOnDurableSend,
                                       boolean blockOnNonDurableSend,
                                       boolean autoGroup,
                                       boolean preAcknowledge,
                                       String loadBalancingPolicyClassName,
                                       int transactionBatchSize,
                                       int dupsOKBatchSize,
                                       boolean useGlobalPools,
                                       int scheduledThreadPoolMaxSize,
                                       int threadPoolMaxSize,
                                       long retryInterval,
                                       double retryIntervalMultiplier,
                                       long maxRetryInterval,
                                       int reconnectAttempts,
                                       boolean failoverOnInitialConnection,
                                       String groupId) throws Exception {
      checkStarted();

      clearIO();

      try {
         ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl().setName(name).setHA(ha).setBindings(bindings).setFactoryType(JMSFactoryType.valueOf(cfType)).setClientID(clientID).setClientFailureCheckPeriod(clientFailureCheckPeriod).setConnectionTTL(connectionTTL).setCallTimeout(callTimeout).setCallFailoverTimeout(callFailoverTimeout).setMinLargeMessageSize(minLargeMessageSize).setCompressLargeMessages(compressLargeMessages).setConsumerWindowSize(consumerWindowSize).setConsumerMaxRate(consumerMaxRate).setConfirmationWindowSize(confirmationWindowSize).setProducerWindowSize(producerWindowSize).setProducerMaxRate(producerMaxRate).setBlockOnAcknowledge(blockOnAcknowledge).setBlockOnDurableSend(blockOnDurableSend).setBlockOnNonDurableSend(blockOnNonDurableSend).setAutoGroup(autoGroup).setPreAcknowledge(preAcknowledge).setTransactionBatchSize(transactionBatchSize).setDupsOKBatchSize(dupsOKBatchSize).setUseGlobalPools(useGlobalPools).setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize).setThreadPoolMaxSize(threadPoolMaxSize).setRetryInterval(retryInterval).setRetryIntervalMultiplier(retryIntervalMultiplier).setMaxRetryInterval(maxRetryInterval).setReconnectAttempts(reconnectAttempts).setFailoverOnInitialConnection(failoverOnInitialConnection).setGroupID(groupId);

         if (useDiscovery) {
            configuration.setDiscoveryGroupName(connectorNames[0]);
         } else {
            ArrayList<String> connectorNamesList = new ArrayList<>();
            for (String nameC : connectorNames) {
               connectorNamesList.add(nameC);
            }
            configuration.setConnectorNames(connectorNamesList);
         }

         if (loadBalancingPolicyClassName != null && !loadBalancingPolicyClassName.trim().equals("")) {
            configuration.setLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
         }

         server.createConnectionFactory(true, configuration, bindings);
      } finally {
         blockOnIO();
      }
   }

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a single live-backup pair of servers.
    * <br>
    * The ConnectionFactory is bound to the Registry for all the specified bindings Strings.
    */
   @Override
   public void createConnectionFactory(String name,
                                       boolean ha,
                                       boolean useDiscovery,
                                       int cfType,
                                       String connectors,
                                       String bindings) throws Exception {
      createConnectionFactory(name, ha, useDiscovery, cfType, toArray(connectors), toArray(bindings));
   }

   @Override
   public boolean createQueue(final String name) throws Exception {
      return createQueue(name, null, null, true);
   }

   @Override
   public boolean createQueue(final String name, final String bindings) throws Exception {
      return createQueue(name, bindings, null, true);
   }

   @Override
   public boolean createQueue(String name, String bindings, String selector) throws Exception {
      return createQueue(name, bindings, selector, true);
   }

   @Override
   public boolean createQueue(@Parameter(name = "name", desc = "Name of the queue to create") String name,
                              @Parameter(name = "bindings", desc = "comma-separated list of Registry bindings (use '&comma;' if u need to use commas in your bindings name)") String bindings,
                              @Parameter(name = "selector", desc = "the jms selector") String selector,
                              @Parameter(name = "durable", desc = "is the queue persistent and resilient to restart") boolean durable) throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.createQueue(true, name, selector, durable, JMSServerControlImpl.toArray(bindings));
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean destroyQueue(final String name) throws Exception {
      return destroyQueue(name, false);
   }

   @Override
   public boolean destroyQueue(final String name, final boolean removeConsumers) throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.destroyQueue(name, removeConsumers);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean createTopic(String name) throws Exception {
      return createTopic(name, null);
   }

   @Override
   public boolean createTopic(final String topicName, final String bindings) throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.createTopic(true, topicName, JMSServerControlImpl.toArray(bindings));
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean destroyTopic(final String name) throws Exception {
      return destroyTopic(name, true);
   }

   @Override
   public boolean destroyTopic(final String name, final boolean removeConsumers) throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.destroyTopic(name, removeConsumers);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void destroyConnectionFactory(final String name) throws Exception {
      checkStarted();

      clearIO();

      try {
         server.destroyConnectionFactory(name);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isStarted() {
      return server.isStarted();
   }

   @Override
   public String getVersion() {
      checkStarted();

      return server.getVersion();
   }

   @Override
   public String[] getQueueNames() {
      checkStarted();

      clearIO();

      try {
         Object[] queueControls = server.getActiveMQServer().getManagementService().getResources(JMSQueueControl.class);
         String[] names = new String[queueControls.length];
         for (int i = 0; i < queueControls.length; i++) {
            JMSQueueControl queueControl = (JMSQueueControl) queueControls[i];
            names[i] = queueControl.getName();
         }
         return names;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getTopicNames() {
      checkStarted();

      clearIO();

      try {
         Object[] topicControls = server.getActiveMQServer().getManagementService().getResources(TopicControl.class);
         String[] names = new String[topicControls.length];
         for (int i = 0; i < topicControls.length; i++) {
            TopicControl topicControl = (TopicControl) topicControls[i];
            names[i] = topicControl.getName();
         }
         return names;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getConnectionFactoryNames() {
      checkStarted();

      clearIO();

      try {
         Object[] cfControls = server.getActiveMQServer().getManagementService().getResources(ConnectionFactoryControl.class);
         String[] names = new String[cfControls.length];
         for (int i = 0; i < cfControls.length; i++) {
            ConnectionFactoryControl cfControl = (ConnectionFactoryControl) cfControls[i];
            names[i] = cfControl.getName();
         }
         return names;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getNodeID() {
      return server.getActiveMQServer().getNodeID().toString();
   }

   // NotificationEmitter implementation ----------------------------

   @Override
   public void removeNotificationListener(final NotificationListener listener,
                                          final NotificationFilter filter,
                                          final Object handback) throws ListenerNotFoundException {
      broadcaster.removeNotificationListener(listener, filter, handback);
   }

   @Override
   public void removeNotificationListener(final NotificationListener listener) throws ListenerNotFoundException {
      broadcaster.removeNotificationListener(listener);
   }

   @Override
   public void addNotificationListener(final NotificationListener listener,
                                       final NotificationFilter filter,
                                       final Object handback) throws IllegalArgumentException {
      broadcaster.addNotificationListener(listener, filter, handback);
   }

   @Override
   public MBeanNotificationInfo[] getNotificationInfo() {
      return JMSServerControlImpl.getNotificationInfos();
   }

   @Override
   public String[] listRemoteAddresses() throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.listRemoteAddresses();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] listRemoteAddresses(final String ipAddress) throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.listRemoteAddresses(ipAddress);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean closeConnectionsForAddress(final String ipAddress) throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.closeConnectionsForAddress(ipAddress);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean closeConsumerConnectionsForAddress(final String address) throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.closeConsumerConnectionsForAddress(address);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean closeConnectionsForUser(final String userName) throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.closeConnectionsForUser(userName);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] listConnectionIDs() throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.listConnectionIDs();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listConnectionsAsJSON() throws Exception {
      checkStarted();

      clearIO();

      try {
         JsonArrayBuilder array = JsonLoader.createArrayBuilder();

         Set<RemotingConnection> connections = server.getActiveMQServer().getRemotingService().getConnections();

         Set<ServerSession> sessions = server.getActiveMQServer().getSessions();

         Map<Object, ServerSession> jmsSessions = new HashMap<>();

         // First separate the real jms sessions, after all we are only interested in those here on the *jms* server controller
         for (ServerSession session : sessions) {
            if (session.getMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY) != null) {
               jmsSessions.put(session.getConnectionID(), session);
            }
         }

         for (RemotingConnection connection : connections) {
            ServerSession session = jmsSessions.get(connection.getID());
            if (session != null) {
               JsonObjectBuilder objectBuilder = JsonLoader.createObjectBuilder().add("connectionID", connection.getID().toString()).add("clientAddress", connection.getRemoteAddress()).add("creationTime", connection.getCreationTime());

               if (session.getMetaData(ClientSession.JMS_SESSION_CLIENT_ID_PROPERTY) != null) {
                  objectBuilder.add("clientID", session.getMetaData(ClientSession.JMS_SESSION_CLIENT_ID_PROPERTY));
               }

               if (session.getUsername() != null) {
                  objectBuilder.add("principal", session.getUsername());
               }

               array.add(objectBuilder.build());
            }
         }
         return array.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listConsumersAsJSON(String connectionID) throws Exception {
      checkStarted();

      clearIO();

      try {
         JsonArrayBuilder array = JsonLoader.createArrayBuilder();

         Set<RemotingConnection> connections = server.getActiveMQServer().getRemotingService().getConnections();
         for (RemotingConnection connection : connections) {
            if (connectionID.equals(connection.getID().toString())) {
               List<ServerSession> sessions = server.getActiveMQServer().getSessions(connectionID);
               for (ServerSession session : sessions) {
                  Set<ServerConsumer> consumers = session.getServerConsumers();
                  for (ServerConsumer consumer : consumers) {
                     JsonObject obj = toJSONObject(consumer);
                     if (obj != null) {
                        array.add(obj);
                     }
                  }
               }
            }
         }
         return array.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listAllConsumersAsJSON() throws Exception {
      checkStarted();

      clearIO();

      try {
         JsonArray jsonArray = toJsonArray(server.getActiveMQServer().getSessions());
         return jsonArray.toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] listSessions(final String connectionID) throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.listSessions(connectionID);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listPreparedTransactionDetailsAsJSON() throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.listPreparedTransactionDetailsAsJSON();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listPreparedTransactionDetailsAsHTML() throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.listPreparedTransactionDetailsAsHTML();
      } finally {
         blockOnIO();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   /* (non-Javadoc)
    * @see org.apache.activemq.artemis.core.management.impl.AbstractControl#fillMBeanOperationInfo()
    */
   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(JMSServerControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(JMSServerControl.class);
   }

   // Private -------------------------------------------------------

   private void checkStarted() {
      if (!server.isStarted()) {
         throw new IllegalStateException("ActiveMQ Artemis JMS Server is not started. It can not be managed yet");
      }
   }

   // Inner classes -------------------------------------------------

   @Override
   public String[] listTargetDestinations(String sessionID) throws Exception {
      String[] addresses = server.getActiveMQServer().getActiveMQServerControl().listTargetAddresses(sessionID);
      Map<String, DestinationControl> allDests = new HashMap<>();

      Object[] queueControls = server.getActiveMQServer().getManagementService().getResources(JMSQueueControl.class);
      for (Object queueControl2 : queueControls) {
         JMSQueueControl queueControl = (JMSQueueControl) queueControl2;
         allDests.put(queueControl.getAddress(), queueControl);
      }

      Object[] topicControls = server.getActiveMQServer().getManagementService().getResources(TopicControl.class);
      for (Object topicControl2 : topicControls) {
         TopicControl topicControl = (TopicControl) topicControl2;
         allDests.put(topicControl.getAddress(), topicControl);
      }

      List<String> destinations = new ArrayList<>();
      for (String addresse : addresses) {
         DestinationControl control = allDests.get(addresse);
         if (control != null) {
            destinations.add(control.getAddress());
         }
      }
      return destinations.toArray(new String[destinations.size()]);
   }

   @Override
   public String getLastSentMessageID(String sessionID, String address) throws Exception {
      ServerSession session = server.getActiveMQServer().getSessionByID(sessionID);
      if (session != null) {
         return session.getLastSentMessageID(address);
      }
      return null;
   }

   @Override
   public String getSessionCreationTime(String sessionID) throws Exception {
      ServerSession session = server.getActiveMQServer().getSessionByID(sessionID);
      if (session != null) {
         return String.valueOf(session.getCreationTime());
      }
      return null;
   }

   @Override
   public String listSessionsAsJSON(final String connectionID) throws Exception {
      checkStarted();

      clearIO();

      try {
         return server.listSessionsAsJSON(connectionID);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listNetworkTopology() throws Exception {
      checkStarted();

      clearIO();
      try {
         JsonArrayBuilder brokers = JsonLoader.createArrayBuilder();
         ClusterManager clusterManager = server.getActiveMQServer().getClusterManager();
         if (clusterManager != null) {
            Set<ClusterConnection> clusterConnections = clusterManager.getClusterConnections();
            for (ClusterConnection clusterConnection : clusterConnections) {
               Topology topology = clusterConnection.getTopology();
               Collection<TopologyMemberImpl> members = topology.getMembers();
               for (TopologyMemberImpl member : members) {

                  JsonObjectBuilder obj = JsonLoader.createObjectBuilder();
                  TransportConfiguration live = member.getLive();
                  if (live != null) {
                     obj.add("nodeID", member.getNodeId()).add("live", live.getParams().get("host") + ":" + live.getParams().get("port"));
                     TransportConfiguration backup = member.getBackup();
                     if (backup != null) {
                        obj.add("backup", backup.getParams().get("host") + ":" + backup.getParams().get("port"));
                     }
                  }
                  brokers.add(obj);
               }
            }
         }
         return brokers.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String closeConnectionWithClientID(final String clientID) throws Exception {
      return server.getActiveMQServer().destroyConnectionWithSessionMetadata(ClientSession.JMS_SESSION_CLIENT_ID_PROPERTY, clientID);
   }

   private String determineJMSDestinationType(Queue queue) {
      String result;
      if (server.getActiveMQServer().getAddressInfo(SimpleString.toSimpleString(queue.getAddress().toString())).getRoutingType() == AddressInfo.RoutingType.ANYCAST) {
         if (queue.isTemporary()) {
            result = "tempqueue";
         } else {
            result = "queue";
         }
      } else if (server.getActiveMQServer().getAddressInfo(SimpleString.toSimpleString(queue.getAddress().toString())).getRoutingType() == AddressInfo.RoutingType.MULTICAST) {
         if (queue.isTemporary()) {
            result = "temptopic";
         } else {
            result = "topic";
         }
      } else {
         ActiveMQJMSServerLogger.LOGGER.debug("JMSServerControlImpl.determineJMSDestinationType() " + queue);
         // not related to JMS
         return null;
      }
      return result;
   }

   private JsonObject toJSONObject(ServerConsumer consumer) {
      AddressInfo addressInfo = server.getActiveMQServer().getAddressInfo(SimpleString.toSimpleString(consumer.getQueue().getAddress().toString()));
      if (addressInfo == null) {
         return null;
      }
      JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("consumerID", consumer.getID()).add("connectionID", consumer.getConnectionID().toString()).add("sessionID", consumer.getSessionID()).add("queueName", consumer.getQueue().getName().toString()).add("browseOnly", consumer.isBrowseOnly()).add("creationTime", consumer.getCreationTime()).add("destinationName", consumer.getQueue().getAddress().toString()).add("destinationType", determineJMSDestinationType(consumer.getQueue()));
      // JMS consumer with message filter use the queue's filter
      Filter queueFilter = consumer.getQueue().getFilter();
      if (queueFilter != null) {
         obj.add("filter", queueFilter.getFilterString().toString());
      }

      if (addressInfo.getRoutingType().equals(AddressInfo.RoutingType.MULTICAST)) {
         if (consumer.getQueue().isTemporary()) {
            obj.add("durable", false);
         } else {
            obj.add("durable", true);
         }
      } else {
         obj.add("durable", false);
      }

      return obj.build();
   }

   @Override
   public void onNotification(org.apache.activemq.artemis.core.server.management.Notification notification) {
      if (!(notification.getType() instanceof JMSNotificationType))
         return;
      JMSNotificationType type = (JMSNotificationType) notification.getType();
      TypedProperties prop = notification.getProperties();

      this.broadcaster.sendNotification(new Notification(type.toString(), this, notifSeq.incrementAndGet(), prop.getSimpleStringProperty(JMSNotificationType.MESSAGE).toString()));
   }

   private JsonArray toJsonArray(Collection<ServerSession> sessions) {
      JsonArrayBuilder array = JsonLoader.createArrayBuilder();

      for (ServerSession session : sessions) {
         Set<ServerConsumer> consumers = session.getServerConsumers();
         for (ServerConsumer consumer : consumers) {
            JsonObject obj = toJSONObject(consumer);
            if (obj != null) {
               array.add(obj);
            }
         }
      }
      return array.build();
   }

}

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

import javax.jms.JMSRuntimeException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.management.Parameter;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.api.jms.management.ConnectionFactoryControl;
import org.apache.activemq.artemis.api.jms.management.DestinationControl;
import org.apache.activemq.artemis.api.jms.management.JMSQueueControl;
import org.apache.activemq.artemis.api.jms.management.JMSServerControl;
import org.apache.activemq.artemis.api.jms.management.TopicControl;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.management.impl.AbstractControl;
import org.apache.activemq.artemis.core.management.impl.MBeanInfoHelper;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.server.ActiveMQJMSServerLogger;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.artemis.jms.server.management.JMSNotificationType;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.TypedProperties;
import org.apache.activemq.artemis.utils.json.JSONArray;
import org.apache.activemq.artemis.utils.json.JSONObject;

public class JMSServerControlImpl extends AbstractControl implements JMSServerControl, NotificationEmitter,
                                                                     org.apache.activemq.artemis.core.server.management.NotificationListener
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JMSServerManager server;

   private final NotificationBroadcasterSupport broadcaster;

   private final AtomicLong notifSeq = new AtomicLong(0);

   // Static --------------------------------------------------------

   private static String[] convert(final Object[] bindings)
   {
      String[] theBindings = new String[bindings.length];
      for (int i = 0, bindingsLength = bindings.length; i < bindingsLength; i++)
      {
         theBindings[i] = bindings[i].toString().trim();
      }
      return theBindings;
   }

   private static String[] toArray(final String commaSeparatedString)
   {
      if (commaSeparatedString == null || commaSeparatedString.trim().length() == 0)
      {
         return new String[0];
      }
      String[] values = commaSeparatedString.split(",");
      String[] trimmed = new String[values.length];
      for (int i = 0; i < values.length; i++)
      {
         trimmed[i] = values[i].trim();
         trimmed[i] = trimmed[i].replace("&comma;", ",");
      }
      return trimmed;
   }

   private static String[] determineJMSDestination(String coreAddress)
   {
      String[] result = new String[2]; // destination name & type
      if (coreAddress.startsWith(ActiveMQDestination.JMS_QUEUE_ADDRESS_PREFIX))
      {
         result[0] = coreAddress.substring(ActiveMQDestination.JMS_QUEUE_ADDRESS_PREFIX.length());
         result[1] = "queue";
      }
      else if (coreAddress.startsWith(ActiveMQDestination.JMS_TEMP_QUEUE_ADDRESS_PREFIX))
      {
         result[0] = coreAddress.substring(ActiveMQDestination.JMS_TEMP_QUEUE_ADDRESS_PREFIX.length());
         result[1] = "tempqueue";
      }
      else if (coreAddress.startsWith(ActiveMQDestination.JMS_TOPIC_ADDRESS_PREFIX))
      {
         result[0] = coreAddress.substring(ActiveMQDestination.JMS_TOPIC_ADDRESS_PREFIX.length());
         result[1] = "topic";
      }
      else if (coreAddress.startsWith(ActiveMQDestination.JMS_TEMP_TOPIC_ADDRESS_PREFIX))
      {
         result[0] = coreAddress.substring(ActiveMQDestination.JMS_TEMP_TOPIC_ADDRESS_PREFIX.length());
         result[1] = "temptopic";
      }
      else
      {
         ActiveMQJMSServerLogger.LOGGER.debug("JMSServerControlImpl.determineJMSDestination()" + coreAddress);
         // not related to JMS
         return null;
      }
      return result;
   }

   public static MBeanNotificationInfo[] getNotificationInfos()
   {
      JMSNotificationType[] values = JMSNotificationType.values();
      String[] names = new String[values.length];
      for (int i = 0; i < values.length; i++)
      {
         names[i] = values[i].toString();
      }
      return new MBeanNotificationInfo[]{new MBeanNotificationInfo(names,
                                                                   JMSServerControl.class.getName(),
                                                                   "Notifications emitted by a JMS Server")};
   }

   // Constructors --------------------------------------------------

   public JMSServerControlImpl(final JMSServerManager server) throws Exception
   {
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
   public void createConnectionFactory(String name,
                                       boolean ha,
                                       boolean useDiscovery,
                                       int cfType,
                                       String[] connectorNames,
                                       Object[] bindings) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         if (useDiscovery)
         {
            if (connectorNames == null || connectorNames.length == 0)
            {
               throw new IllegalArgumentException("no discovery group name supplied");
            }
            server.createConnectionFactory(name,
                                           ha,
                                           JMSFactoryType.valueOf(cfType),
                                           connectorNames[0],
                                           JMSServerControlImpl.convert(bindings));
         }
         else
         {
            List<String> connectorList = new ArrayList<String>(connectorNames.length);

            for (String str : connectorNames)
            {
               connectorList.add(str);
            }

            server.createConnectionFactory(name,
                                           ha,
                                           JMSFactoryType.valueOf(cfType),
                                           connectorList,
                                           JMSServerControlImpl.convert(bindings));
         }
      }
      finally
      {
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
                                       String groupId) throws Exception
   {
      createConnectionFactory(name,
                              ha,
                              useDiscovery,
                              cfType,
                              toArray(connectors),
                              toArray(bindings),
                              clientID,
                              clientFailureCheckPeriod,
                              connectionTTL,
                              callTimeout,
                              callFailoverTimeout,
                              minLargeMessageSize,
                              compressLargeMessages,
                              consumerWindowSize,
                              consumerMaxRate,
                              confirmationWindowSize,
                              producerWindowSize,
                              producerMaxRate,
                              blockOnAcknowledge,
                              blockOnDurableSend,
                              blockOnNonDurableSend,
                              autoGroup,
                              preAcknowledge,
                              loadBalancingPolicyClassName,
                              transactionBatchSize,
                              dupsOKBatchSize,
                              useGlobalPools,
                              scheduledThreadPoolMaxSize,
                              threadPoolMaxSize,
                              retryInterval,
                              retryIntervalMultiplier,
                              maxRetryInterval,
                              reconnectAttempts,
                              failoverOnInitialConnection,
                              groupId);
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
                                       String groupId) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl()
            .setName(name)
            .setHA(ha)
            .setBindings(bindings)
            .setFactoryType(JMSFactoryType.valueOf(cfType))
            .setClientID(clientID)
            .setClientFailureCheckPeriod(clientFailureCheckPeriod)
            .setConnectionTTL(connectionTTL)
            .setCallTimeout(callTimeout)
            .setCallFailoverTimeout(callFailoverTimeout)
            .setMinLargeMessageSize(minLargeMessageSize)
            .setCompressLargeMessages(compressLargeMessages)
            .setConsumerWindowSize(consumerWindowSize)
            .setConsumerMaxRate(consumerMaxRate)
            .setConfirmationWindowSize(confirmationWindowSize)
            .setProducerWindowSize(producerWindowSize)
            .setProducerMaxRate(producerMaxRate)
            .setBlockOnAcknowledge(blockOnAcknowledge)
            .setBlockOnDurableSend(blockOnDurableSend)
            .setBlockOnNonDurableSend(blockOnNonDurableSend)
            .setAutoGroup(autoGroup)
            .setPreAcknowledge(preAcknowledge)
            .setTransactionBatchSize(transactionBatchSize)
            .setDupsOKBatchSize(dupsOKBatchSize)
            .setUseGlobalPools(useGlobalPools)
            .setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize)
            .setThreadPoolMaxSize(threadPoolMaxSize)
            .setRetryInterval(retryInterval)
            .setRetryIntervalMultiplier(retryIntervalMultiplier)
            .setMaxRetryInterval(maxRetryInterval)
            .setReconnectAttempts(reconnectAttempts)
            .setFailoverOnInitialConnection(failoverOnInitialConnection)
            .setGroupID(groupId);

         if (useDiscovery)
         {
            configuration.setDiscoveryGroupName(connectorNames[0]);
         }
         else
         {
            ArrayList<String> connectorNamesList = new ArrayList<String>();
            for (String nameC : connectorNames)
            {
               connectorNamesList.add(nameC);
            }
            configuration.setConnectorNames(connectorNamesList);
         }

         if (loadBalancingPolicyClassName != null && !loadBalancingPolicyClassName.trim().equals(""))
         {
            configuration.setLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
         }

         server.createConnectionFactory(true, configuration, bindings);
      }
      finally
      {
         blockOnIO();
      }
   }

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a single live-backup pair of servers.
    * <br>
    * The ConnectionFactory is bound to the Registry for all the specified bindings Strings.
    */
   public void createConnectionFactory(String name,
                                       boolean ha,
                                       boolean useDiscovery,
                                       int cfType,
                                       String connectors,
                                       String bindings) throws Exception
   {
      createConnectionFactory(name, ha, useDiscovery, cfType, toArray(connectors), toArray(bindings));
   }

   public boolean createQueue(final String name) throws Exception
   {
      return createQueue(name, null, null, true);
   }

   public boolean createQueue(final String name, final String bindings) throws Exception
   {
      return createQueue(name, bindings, null, true);
   }

   @Override
   public boolean createQueue(String name, String bindings, String selector) throws Exception
   {
      return createQueue(name, bindings, selector, true);
   }

   public boolean createQueue(@Parameter(name = "name", desc = "Name of the queue to create") String name,
                              @Parameter(name = "bindings", desc = "comma-separated list of Registry bindings (use '&comma;' if u need to use commas in your bindings name)") String bindings,
                              @Parameter(name = "selector", desc = "the jms selector") String selector,
                              @Parameter(name = "durable", desc = "is the queue persistent and resilient to restart") boolean durable) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.createQueue(true, name, selector, durable,
               JMSServerControlImpl.toArray(bindings));
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean destroyQueue(final String name) throws Exception
   {
      return destroyQueue(name, false);
   }

   public boolean destroyQueue(final String name, final boolean removeConsumers) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.destroyQueue(name, removeConsumers);
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean createTopic(String name) throws Exception
   {
      return createTopic(name, null);
   }

   public boolean createTopic(final String topicName, final String bindings) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.createTopic(true, topicName, JMSServerControlImpl.toArray(bindings));
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean destroyTopic(final String name) throws Exception
   {
      return destroyTopic(name, true);
   }


   public boolean destroyTopic(final String name, final boolean removeConsumers) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.destroyTopic(name, removeConsumers);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void destroyConnectionFactory(final String name) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         server.destroyConnectionFactory(name);
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isStarted()
   {
      return server.isStarted();
   }

   public String getVersion()
   {
      checkStarted();

      return server.getVersion();
   }

   public String[] getQueueNames()
   {
      checkStarted();

      clearIO();

      try
      {
         Object[] queueControls = server.getActiveMQServer().getManagementService().getResources(JMSQueueControl.class);
         String[] names = new String[queueControls.length];
         for (int i = 0; i < queueControls.length; i++)
         {
            JMSQueueControl queueControl = (JMSQueueControl) queueControls[i];
            names[i] = queueControl.getName();
         }
         return names;
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] getTopicNames()
   {
      checkStarted();

      clearIO();

      try
      {
         Object[] topicControls = server.getActiveMQServer().getManagementService().getResources(TopicControl.class);
         String[] names = new String[topicControls.length];
         for (int i = 0; i < topicControls.length; i++)
         {
            TopicControl topicControl = (TopicControl) topicControls[i];
            names[i] = topicControl.getName();
         }
         return names;
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] getConnectionFactoryNames()
   {
      checkStarted();

      clearIO();

      try
      {
         Object[] cfControls = server.getActiveMQServer()
            .getManagementService()
            .getResources(ConnectionFactoryControl.class);
         String[] names = new String[cfControls.length];
         for (int i = 0; i < cfControls.length; i++)
         {
            ConnectionFactoryControl cfControl = (ConnectionFactoryControl) cfControls[i];
            names[i] = cfControl.getName();
         }
         return names;
      }
      finally
      {
         blockOnIO();
      }
   }

   // NotificationEmitter implementation ----------------------------

   public void removeNotificationListener(final NotificationListener listener,
                                          final NotificationFilter filter,
                                          final Object handback) throws ListenerNotFoundException
   {
      broadcaster.removeNotificationListener(listener, filter, handback);
   }

   public void removeNotificationListener(final NotificationListener listener) throws ListenerNotFoundException
   {
      broadcaster.removeNotificationListener(listener);
   }

   public void addNotificationListener(final NotificationListener listener,
                                       final NotificationFilter filter,
                                       final Object handback) throws IllegalArgumentException
   {
      broadcaster.addNotificationListener(listener, filter, handback);
   }

   public MBeanNotificationInfo[] getNotificationInfo()
   {
      return JMSServerControlImpl.getNotificationInfos();
   }

   public String[] listRemoteAddresses() throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.listRemoteAddresses();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] listRemoteAddresses(final String ipAddress) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.listRemoteAddresses(ipAddress);
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.closeConnectionsForAddress(ipAddress);
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean closeConsumerConnectionsForAddress(final String address) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.closeConsumerConnectionsForAddress(address);
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean closeConnectionsForUser(final String userName) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.closeConnectionsForUser(userName);
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] listConnectionIDs() throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.listConnectionIDs();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String listConnectionsAsJSON() throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         JSONArray array = new JSONArray();

         Set<RemotingConnection> connections = server.getActiveMQServer().getRemotingService().getConnections();

         Set<ServerSession> sessions = server.getActiveMQServer().getSessions();

         Map<Object, ServerSession> jmsSessions = new HashMap<Object, ServerSession>();

         // First separate the real jms sessions, after all we are only interested in those here on the *jms* server controller
         for (ServerSession session : sessions)
         {
            if (session.getMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY) != null)
            {
               jmsSessions.put(session.getConnectionID(), session);
            }
         }

         for (RemotingConnection connection : connections)
         {
            ServerSession session = jmsSessions.get(connection.getID());
            if (session != null)
            {
               JSONObject obj = new JSONObject();
               obj.put("connectionID", connection.getID().toString());
               obj.put("clientAddress", connection.getRemoteAddress());
               obj.put("creationTime", connection.getCreationTime());
               // Notice: this will be null when the user haven't set the client-id
               obj.put("clientID", session.getMetaData(ClientSession.JMS_SESSION_CLIENT_ID_PROPERTY));
               obj.put("principal", session.getUsername());
               array.put(obj);
            }
         }
         return array.toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String listConsumersAsJSON(String connectionID) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         JSONArray array = new JSONArray();

         Set<RemotingConnection> connections = server.getActiveMQServer().getRemotingService().getConnections();
         for (RemotingConnection connection : connections)
         {
            if (connectionID.equals(connection.getID().toString()))
            {
               List<ServerSession> sessions = server.getActiveMQServer().getSessions(connectionID);
               for (ServerSession session : sessions)
               {
                  Set<ServerConsumer> consumers = session.getServerConsumers();
                  for (ServerConsumer consumer : consumers)
                  {
                     JSONObject obj = toJSONObject(consumer);
                     if (obj != null)
                     {
                        array.put(obj);
                     }
                  }
               }
            }
         }
         return array.toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String listAllConsumersAsJSON() throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         JSONArray array = new JSONArray();

         Set<ServerSession> sessions = server.getActiveMQServer().getSessions();
         for (ServerSession session : sessions)
         {
            Set<ServerConsumer> consumers = session.getServerConsumers();
            for (ServerConsumer consumer : consumers)
            {
               JSONObject obj = toJSONObject(consumer);
               if (obj != null)
               {
                  array.put(obj);
               }
            }
         }
         return array.toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] listSessions(final String connectionID) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.listSessions(connectionID);
      }
      finally
      {
         blockOnIO();
      }
   }

   public String listPreparedTransactionDetailsAsJSON() throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.listPreparedTransactionDetailsAsJSON();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String listPreparedTransactionDetailsAsHTML() throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.listPreparedTransactionDetailsAsHTML();
      }
      finally
      {
         blockOnIO();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   /* (non-Javadoc)
    * @see org.apache.activemq.artemis.core.management.impl.AbstractControl#fillMBeanOperationInfo()
    */
   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo()
   {
      return MBeanInfoHelper.getMBeanOperationsInfo(JMSServerControl.class);
   }

   // Private -------------------------------------------------------

   private void checkStarted()
   {
      if (!server.isStarted())
      {
         throw new IllegalStateException("ActiveMQ Artemis JMS Server is not started. It can not be managed yet");
      }
   }

   // Inner classes -------------------------------------------------

   public String[] listTargetDestinations(String sessionID) throws Exception
   {
      String[] addresses = server.getActiveMQServer().getActiveMQServerControl().listTargetAddresses(sessionID);
      Map<String, DestinationControl> allDests = new HashMap<String, DestinationControl>();

      Object[] queueControls = server.getActiveMQServer().getManagementService().getResources(JMSQueueControl.class);
      for (Object queueControl2 : queueControls)
      {
         JMSQueueControl queueControl = (JMSQueueControl) queueControl2;
         allDests.put(queueControl.getAddress(), queueControl);
      }

      Object[] topicControls = server.getActiveMQServer().getManagementService().getResources(TopicControl.class);
      for (Object topicControl2 : topicControls)
      {
         TopicControl topicControl = (TopicControl) topicControl2;
         allDests.put(topicControl.getAddress(), topicControl);
      }

      List<String> destinations = new ArrayList<String>();
      for (String addresse : addresses)
      {
         DestinationControl control = allDests.get(addresse);
         if (control != null)
         {
            destinations.add(control.getAddress());
         }
      }
      return destinations.toArray(new String[0]);
   }

   public String getLastSentMessageID(String sessionID, String address) throws Exception
   {
      ServerSession session = server.getActiveMQServer().getSessionByID(sessionID);
      if (session != null)
      {
         return session.getLastSentMessageID(address);
      }
      return null;
   }

   public String getSessionCreationTime(String sessionID) throws Exception
   {
      ServerSession session = server.getActiveMQServer().getSessionByID(sessionID);
      if (session != null)
      {
         return String.valueOf(session.getCreationTime());
      }
      return null;
   }

   public String listSessionsAsJSON(final String connectionID) throws Exception
   {
      checkStarted();

      clearIO();

      JSONArray array = new JSONArray();
      try
      {
         List<ServerSession> sessions = server.getActiveMQServer().getSessions(connectionID);
         for (ServerSession sess : sessions)
         {
            JSONObject obj = new JSONObject();
            obj.put("sessionID", sess.getName());
            obj.put("creationTime", sess.getCreationTime());
            array.put(obj);
         }
      }
      finally
      {
         blockOnIO();
      }
      return array.toString();
   }

   public String closeConnectionWithClientID(final String clientID) throws Exception
   {
      return server.getActiveMQServer().destroyConnectionWithSessionMetadata(ClientSession.JMS_SESSION_CLIENT_ID_PROPERTY, clientID);
   }

   private JSONObject toJSONObject(ServerConsumer consumer) throws Exception
   {
      JSONObject obj = new JSONObject();
      obj.put("consumerID", consumer.getID());
      obj.put("connectionID", consumer.getConnectionID());
      obj.put("sessionID", consumer.getSessionID());
      obj.put("queueName", consumer.getQueue().getName().toString());
      obj.put("browseOnly", consumer.isBrowseOnly());
      obj.put("creationTime", consumer.getCreationTime());
      // JMS consumer with message filter use the queue's filter
      Filter queueFilter = consumer.getQueue().getFilter();
      if (queueFilter != null)
      {
         obj.put("filter", queueFilter.getFilterString().toString());
      }
      String[] destinationInfo = determineJMSDestination(consumer.getQueue().getAddress().toString());
      if (destinationInfo == null)
      {
         return null;
      }
      obj.put("destinationName", destinationInfo[0]);
      obj.put("destinationType", destinationInfo[1]);
      if (destinationInfo[1].equals("topic"))
      {
         try
         {
            ActiveMQDestination.decomposeQueueNameForDurableSubscription(consumer.getQueue()
                                                                            .getName()
                                                                            .toString());
            obj.put("durable", true);
         }
         catch (IllegalArgumentException e)
         {
            obj.put("durable", false);
         }
         catch (JMSRuntimeException e)
         {
            obj.put("durable", false);
         }
      }
      else
      {
         obj.put("durable", false);
      }

      return obj;
   }

   @Override
   public void onNotification(org.apache.activemq.artemis.core.server.management.Notification notification)
   {
      if (!(notification.getType() instanceof JMSNotificationType)) return;
      JMSNotificationType type = (JMSNotificationType) notification.getType();
      TypedProperties prop = notification.getProperties();

      this.broadcaster.sendNotification(new Notification(type.toString(), this,
            notifSeq.incrementAndGet(), prop.getSimpleStringProperty(JMSNotificationType.MESSAGE).toString()));
   }

}

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
package org.apache.activemq.artemis.tests.integration.management;

import org.apache.activemq.artemis.api.core.ActiveMQAddressDoesNotExistException;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.Parameter;
import org.apache.activemq.artemis.api.core.management.ResourceNames;

import java.util.Map;

public class ActiveMQServerControlUsingCoreTest extends ActiveMQServerControlTest {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static String[] toStringArray(final Object[] res) {
      String[] names = new String[res.length];
      for (int i = 0; i < res.length; i++) {
         names[i] = res[i].toString();
      }
      return names;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // ActiveMQServerControlTest overrides --------------------------

   // the core messaging proxy doesn't work when the server is stopped so we cant run these 2 tests
   @Override
   public void testScaleDownWithOutConnector() throws Exception {
   }

   @Override
   public void testScaleDownWithConnector() throws Exception {
   }

   @Override
   protected ActiveMQServerControl createManagementControl() throws Exception {
      return new ActiveMQServerControl() {
         @Override
         public void updateDuplicateIdCache(String address, Object[] ids) {

         }

         @Override
         public void scaleDown(String connector) throws Exception {
            throw new UnsupportedOperationException();
         }

         private final CoreMessagingProxy proxy = new CoreMessagingProxy(addServerLocator(createInVMNonHALocator()), ResourceNames.BROKER);

         @Override
         public boolean isSharedStore() {
            return (Boolean) proxy.retrieveAttributeValue("sharedStore");
         }

         @Override
         public boolean closeConnectionsForAddress(final String ipAddress) throws Exception {
            return (Boolean) proxy.invokeOperation("closeConnectionsForAddress", ipAddress);
         }

         @Override
         public boolean closeConsumerConnectionsForAddress(final String address) throws Exception {
            return (Boolean) proxy.invokeOperation("closeConsumerConnectionsForAddress", address);
         }

         @Override
         public boolean closeConnectionsForUser(final String userName) throws Exception {
            return (Boolean) proxy.invokeOperation("closeConnectionsForUser", userName);
         }

         @Override
         public boolean commitPreparedTransaction(final String transactionAsBase64) throws Exception {
            return (Boolean) proxy.invokeOperation("commitPreparedTransaction", transactionAsBase64);
         }

         @Override
         public void createQueue(final String address, final String name) throws Exception {
            proxy.invokeOperation("createQueue", address, name);
         }

         @Override
         public void createAddress(@Parameter(name = "name", desc = "The name of the address") String name, @Parameter(name = "routingType", desc = "the routing type of the address either 0 for multicast or 1 for anycast") int routingType, @Parameter(name = "defaultDeleteOnNoConsumers", desc = "Whether or not a queue with this address is deleted when it has no consumers") boolean defaultDeleteOnNoConsumers, @Parameter(name = "defaultMaxConsumers", desc = "The maximim number of consumer a queue with this address can have") int defaultMaxConsumers) throws Exception {
            proxy.invokeOperation("createAddress", name, routingType, defaultDeleteOnNoConsumers, defaultMaxConsumers);
         }

         @Override
         public void createAddress(@Parameter(name = "name", desc = "The name of the address") String name,
                                   @Parameter(name = "routingType", desc = "The routing type for the address either 'MULTICAST' or 'ANYCAST'") String routingType,
                                   @Parameter(name = "defaultDeleteOnNoConsumers", desc = "Whether or not a queue with this address is deleted when it has no consumers") boolean defaultDeleteOnNoConsumers,
                                   @Parameter(name = "defaultMaxConsumers", desc = "The maximim number of consumer a queue with this address can have") int defaultMaxConsumers) throws Exception {
            proxy.invokeOperation("createAddress", name, routingType, defaultDeleteOnNoConsumers, defaultMaxConsumers);
         }

         @Override
         public void deleteAddress(@Parameter(name = "name", desc = "The name of the address") String name) throws Exception {
            proxy.invokeOperation("deleteAddress", name);
         }

         @Override
         public void createQueue(final String address,
                                 final String name,
                                 final String filter,
                                 final boolean durable) throws Exception {
            proxy.invokeOperation("createQueue", address, name, filter, durable);
         }

         @Override
         public void createQueue(final String address, final String name, final boolean durable) throws Exception {
            proxy.invokeOperation("createQueue", address, name, durable);
         }

         @Override
         public void createQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                                 @Parameter(name = "name", desc = "Name of the queue") String name,
                                 @Parameter(name = "filter", desc = "Filter of the queue") String filter,
                                 @Parameter(name = "durable", desc = "Is the queue durable?") boolean durable,
                                 @Parameter(name = "maxConsumers", desc = "The maximum number of consumers allowed on this queue at any one time") int maxConsumers,
                                 @Parameter(name = "deleteOnNoConsumers", desc = "Delete this queue when the last consumer disconnects") boolean deleteOnNoConsumers,
                                 @Parameter(name = "autoCreateAddress", desc = "Create an address with default values should a matching address not be found") boolean autoCreateAddress) throws Exception {

         }

         @Override
         public void deployQueue(final String address,
                                 final String name,
                                 final String filter,
                                 final boolean durable) throws Exception {
            proxy.invokeOperation("deployQueue", address, name, filter, durable);
         }

         @Override
         public void deployQueue(final String address, final String name, final String filterString) throws Exception {
            proxy.invokeOperation("deployQueue", address, name);
         }

         @Override
         public void destroyQueue(final String name) throws Exception {
            proxy.invokeOperation("destroyQueue", name);
         }

         @Override
         public void destroyQueue(final String name, final boolean removeConsumers) throws Exception {
            proxy.invokeOperation("destroyQueue", name, removeConsumers);
         }

         @Override
         public void destroyQueue(@Parameter(name = "name", desc = "Name of the queue to destroy") String name,
                                  @Parameter(name = "removeConsumers", desc = "Remove consumers of this queue") boolean removeConsumers,
                                  boolean autoDeleteAddress) throws Exception {
         }

         @Override
         public void disableMessageCounters() throws Exception {
            proxy.invokeOperation("disableMessageCounters");
         }

         @Override
         public void enableMessageCounters() throws Exception {
            proxy.invokeOperation("enableMessageCounters");
         }

         @Override
         public String getBindingsDirectory() {
            return (String) proxy.retrieveAttributeValue("bindingsDirectory");
         }

         @Override
         public int getConnectionCount() {
            return (Integer) proxy.retrieveAttributeValue("connectionCount", Integer.class);
         }

         @Override
         public long getTotalConnectionCount() {
            return (Long) proxy.retrieveAttributeValue("totalConnectionCount", Long.class);
         }

         @Override
         public long getTotalMessageCount() {
            return (Long) proxy.retrieveAttributeValue("totalMessageCount", Long.class);
         }

         @Override
         public long getTotalMessagesAdded() {
            return (Long) proxy.retrieveAttributeValue("totalMessagesAdded", Long.class);
         }

         @Override
         public long getTotalMessagesAcknowledged() {
            return (Long) proxy.retrieveAttributeValue("totalMessagesAcknowledged", Long.class);
         }

         @Override
         public long getTotalConsumerCount() {
            return (Long) proxy.retrieveAttributeValue("totalConsumerCount", Long.class);
         }

         @Override
         public long getConnectionTTLOverride() {
            return (Long) proxy.retrieveAttributeValue("connectionTTLOverride", Long.class);
         }

         @Override
         public Object[] getConnectors() throws Exception {
            return (Object[]) proxy.retrieveAttributeValue("connectors");
         }

         @Override
         public String getConnectorsAsJSON() throws Exception {
            return (String) proxy.retrieveAttributeValue("connectorsAsJSON");
         }

         @Override
         public String[] getAddressNames() {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("addressNames"));
         }

         @Override
         public String[] getQueueNames() {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("queueNames", String.class));
         }

         @Override
         public String getUptime() {
            return null;
         }

         @Override
         public long getUptimeMillis() {
            return 0;
         }

         @Override
         public boolean isReplicaSync() {
            return false;
         }

         @Override
         public int getIDCacheSize() {
            return (Integer) proxy.retrieveAttributeValue("IDCacheSize", Integer.class);
         }

         public String[] getInterceptorClassNames() {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("incomingInterceptorClassNames"));
         }

         @Override
         public String[] getIncomingInterceptorClassNames() {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("incomingInterceptorClassNames"));
         }

         @Override
         public String[] getOutgoingInterceptorClassNames() {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("outgoingInterceptorClassNames"));
         }

         @Override
         public String getJournalDirectory() {
            return (String) proxy.retrieveAttributeValue("journalDirectory");
         }

         @Override
         public int getJournalFileSize() {
            return (Integer) proxy.retrieveAttributeValue("journalFileSize", Integer.class);
         }

         @Override
         public int getJournalMaxIO() {
            return (Integer) proxy.retrieveAttributeValue("journalMaxIO", Integer.class);
         }

         @Override
         public int getJournalMinFiles() {
            return (Integer) proxy.retrieveAttributeValue("journalMinFiles", Integer.class);
         }

         @Override
         public String getJournalType() {
            return (String) proxy.retrieveAttributeValue("journalType");
         }

         @Override
         public String getLargeMessagesDirectory() {
            return (String) proxy.retrieveAttributeValue("largeMessagesDirectory");
         }

         @Override
         public String getNodeID() {
            return (String) proxy.retrieveAttributeValue("nodeID");
         }

         @Override
         public String getManagementAddress() {
            return (String) proxy.retrieveAttributeValue("managementAddress");
         }

         @Override
         public String getManagementNotificationAddress() {
            return (String) proxy.retrieveAttributeValue("managementNotificationAddress");
         }

         @Override
         public int getMessageCounterMaxDayCount() {
            return (Integer) proxy.retrieveAttributeValue("messageCounterMaxDayCount", Integer.class);
         }

         @Override
         public long getMessageCounterSamplePeriod() {
            return (Long) proxy.retrieveAttributeValue("messageCounterSamplePeriod", Long.class);
         }

         @Override
         public long getMessageExpiryScanPeriod() {
            return (Long) proxy.retrieveAttributeValue("messageExpiryScanPeriod", Long.class);
         }

         @Override
         public long getMessageExpiryThreadPriority() {
            return (Long) proxy.retrieveAttributeValue("messageExpiryThreadPriority", Long.class);
         }

         @Override
         public String getPagingDirectory() {
            return (String) proxy.retrieveAttributeValue("pagingDirectory");
         }

         @Override
         public int getScheduledThreadPoolMaxSize() {
            return (Integer) proxy.retrieveAttributeValue("scheduledThreadPoolMaxSize", Integer.class);
         }

         @Override
         public int getThreadPoolMaxSize() {
            return (Integer) proxy.retrieveAttributeValue("threadPoolMaxSize", Integer.class);
         }

         @Override
         public long getSecurityInvalidationInterval() {
            return (Long) proxy.retrieveAttributeValue("securityInvalidationInterval", Long.class);
         }

         @Override
         public long getTransactionTimeout() {
            return (Long) proxy.retrieveAttributeValue("transactionTimeout", Long.class);
         }

         @Override
         public long getTransactionTimeoutScanPeriod() {
            return (Long) proxy.retrieveAttributeValue("transactionTimeoutScanPeriod", Long.class);
         }

         @Override
         public String getVersion() {
            return proxy.retrieveAttributeValue("version").toString();
         }

         @Override
         public boolean isBackup() {
            return (Boolean) proxy.retrieveAttributeValue("backup");
         }

         @Override
         public boolean isClustered() {
            return (Boolean) proxy.retrieveAttributeValue("clustered");
         }

         @Override
         public boolean isCreateBindingsDir() {
            return (Boolean) proxy.retrieveAttributeValue("createBindingsDir");
         }

         @Override
         public boolean isCreateJournalDir() {
            return (Boolean) proxy.retrieveAttributeValue("createJournalDir");
         }

         @Override
         public boolean isJournalSyncNonTransactional() {
            return (Boolean) proxy.retrieveAttributeValue("journalSyncNonTransactional");
         }

         @Override
         public boolean isJournalSyncTransactional() {
            return (Boolean) proxy.retrieveAttributeValue("journalSyncTransactional");
         }

         @Override
         public void setFailoverOnServerShutdown(boolean failoverOnServerShutdown) throws Exception {
            proxy.invokeOperation("setFailoverOnServerShutdown", failoverOnServerShutdown);
         }

         @Override
         public boolean isFailoverOnServerShutdown() {
            return (Boolean) proxy.retrieveAttributeValue("failoverOnServerShutdown");
         }

         public void setScaleDown(boolean scaleDown) throws Exception {
            proxy.invokeOperation("setEnabled", scaleDown);
         }

         public boolean isScaleDown() {
            return (Boolean) proxy.retrieveAttributeValue("scaleDown");
         }

         @Override
         public boolean isMessageCounterEnabled() {
            return (Boolean) proxy.retrieveAttributeValue("messageCounterEnabled");
         }

         @Override
         public boolean isPersistDeliveryCountBeforeDelivery() {
            return (Boolean) proxy.retrieveAttributeValue("persistDeliveryCountBeforeDelivery");
         }

         @Override
         public boolean isAsyncConnectionExecutionEnabled() {
            return (Boolean) proxy.retrieveAttributeValue("asyncConnectionExecutionEnabled");
         }

         @Override
         public boolean isPersistIDCache() {
            return (Boolean) proxy.retrieveAttributeValue("persistIDCache");
         }

         @Override
         public boolean isSecurityEnabled() {
            return (Boolean) proxy.retrieveAttributeValue("securityEnabled");
         }

         @Override
         public boolean isStarted() {
            return (Boolean) proxy.retrieveAttributeValue("started");
         }

         @Override
         public boolean isWildcardRoutingEnabled() {
            return (Boolean) proxy.retrieveAttributeValue("wildcardRoutingEnabled");
         }

         @Override
         public String[] listConnectionIDs() throws Exception {
            return (String[]) proxy.invokeOperation("listConnectionIDs");
         }

         @Override
         public String[] listPreparedTransactions() throws Exception {
            return (String[]) proxy.invokeOperation("listPreparedTransactions");
         }

         @Override
         public String listPreparedTransactionDetailsAsJSON() throws Exception {
            return (String) proxy.invokeOperation("listPreparedTransactionDetailsAsJSON");
         }

         @Override
         public String listPreparedTransactionDetailsAsHTML() throws Exception {
            return (String) proxy.invokeOperation("listPreparedTransactionDetailsAsHTML");
         }

         @Override
         public String[] listHeuristicCommittedTransactions() throws Exception {
            return (String[]) proxy.invokeOperation("listHeuristicCommittedTransactions");
         }

         @Override
         public String[] listHeuristicRolledBackTransactions() throws Exception {
            return (String[]) proxy.invokeOperation("listHeuristicRolledBackTransactions");
         }

         @Override
         public String[] listRemoteAddresses() throws Exception {
            return (String[]) proxy.invokeOperation("listRemoteAddresses");
         }

         @Override
         public String[] listRemoteAddresses(final String ipAddress) throws Exception {
            return (String[]) proxy.invokeOperation("listRemoteAddresses", ipAddress);
         }

         @Override
         public String[] listSessions(final String connectionID) throws Exception {
            return (String[]) proxy.invokeOperation("listSessions", connectionID);
         }

         @Override
         public void resetAllMessageCounterHistories() throws Exception {
            proxy.invokeOperation("resetAllMessageCounterHistories");
         }

         @Override
         public void resetAllMessageCounters() throws Exception {
            proxy.invokeOperation("resetAllMessageCounters");
         }

         @Override
         public boolean rollbackPreparedTransaction(final String transactionAsBase64) throws Exception {
            return (Boolean) proxy.invokeOperation("rollbackPreparedTransaction", transactionAsBase64);
         }

         @Override
         public void sendQueueInfoToQueue(final String queueName, final String address) throws Exception {
            proxy.invokeOperation("sendQueueInfoToQueue", queueName, address);
         }

         @Override
         public void setMessageCounterMaxDayCount(final int count) throws Exception {
            proxy.invokeOperation("setMessageCounterMaxDayCount", count);
         }

         @Override
         public void setMessageCounterSamplePeriod(final long newPeriod) throws Exception {
            proxy.invokeOperation("setMessageCounterSamplePeriod", newPeriod);
         }

         @Override
         public int getJournalBufferSize() {
            return (Integer) proxy.retrieveAttributeValue("JournalBufferSize", Integer.class);
         }

         @Override
         public int getJournalBufferTimeout() {
            return (Integer) proxy.retrieveAttributeValue("JournalBufferTimeout", Integer.class);
         }

         @Override
         public int getJournalCompactMinFiles() {
            return (Integer) proxy.retrieveAttributeValue("JournalCompactMinFiles", Integer.class);
         }

         @Override
         public int getJournalCompactPercentage() {
            return (Integer) proxy.retrieveAttributeValue("JournalCompactPercentage", Integer.class);
         }

         @Override
         public boolean isPersistenceEnabled() {
            return (Boolean) proxy.retrieveAttributeValue("PersistenceEnabled");
         }

         @Override
         public int getDiskScanPeriod() {
            return (Integer) proxy.retrieveAttributeValue("DiskScanPeriod", Integer.class);
         }

         @Override
         public int getMaxDiskUsage() {
            return (Integer) proxy.retrieveAttributeValue("MaxDiskUsage", Integer.class);
         }

         @Override
         public long getGlobalMaxSize() {
            return (Long) proxy.retrieveAttributeValue("GlobalMaxSize", Long.class);
         }

         @Override
         public void addSecuritySettings(String addressMatch,
                                         String sendRoles,
                                         String consumeRoles,
                                         String createDurableQueueRoles,
                                         String deleteDurableQueueRoles,
                                         String createNonDurableQueueRoles,
                                         String deleteNonDurableQueueRoles,
                                         String manageRoles) throws Exception {
            proxy.invokeOperation("addSecuritySettings", addressMatch, sendRoles, consumeRoles, createDurableQueueRoles, deleteDurableQueueRoles, createNonDurableQueueRoles, deleteNonDurableQueueRoles, manageRoles);
         }

         @Override
         public void addSecuritySettings(String addressMatch,
                                         String sendRoles,
                                         String consumeRoles,
                                         String createDurableQueueRoles,
                                         String deleteDurableQueueRoles,
                                         String createNonDurableQueueRoles,
                                         String deleteNonDurableQueueRoles,
                                         String manageRoles,
                                         String browseRoles) throws Exception {
            proxy.invokeOperation("addSecuritySettings", addressMatch, sendRoles, consumeRoles, createDurableQueueRoles, deleteDurableQueueRoles, createNonDurableQueueRoles, deleteNonDurableQueueRoles, manageRoles, browseRoles);
         }

         @Override
         public void addSecuritySettings(String addressMatch,
                                         String sendRoles,
                                         String consumeRoles,
                                         String createDurableQueueRoles,
                                         String deleteDurableQueueRoles,
                                         String createNonDurableQueueRoles,
                                         String deleteNonDurableQueueRoles,
                                         String manageRoles,
                                         String browseRoles,
                                         String createAddress) throws Exception {
            proxy.invokeOperation("addSecuritySettings", addressMatch, sendRoles, consumeRoles, createDurableQueueRoles, deleteDurableQueueRoles, createNonDurableQueueRoles, deleteNonDurableQueueRoles, manageRoles, browseRoles, createAddress);
         }

         @Override
         public void removeSecuritySettings(String addressMatch) throws Exception {
            proxy.invokeOperation("removeSecuritySettings", addressMatch);
         }

         @Override
         public Object[] getRoles(String addressMatch) throws Exception {
            return (Object[]) proxy.invokeOperation("getRoles", addressMatch);
         }

         @Override
         public String getRolesAsJSON(String addressMatch) throws Exception {
            return (String) proxy.invokeOperation("getRolesAsJSON", addressMatch);
         }

         @Override
         public void addAddressSettings(@Parameter(desc = "an address match", name = "addressMatch") String addressMatch,
                                        @Parameter(desc = "the dead letter address setting", name = "DLA") String DLA,
                                        @Parameter(desc = "the expiry address setting", name = "expiryAddress") String expiryAddress,
                                        @Parameter(desc = "the expiry delay setting", name = "expiryDelay") long expiryDelay,
                                        @Parameter(desc = "are any queues created for this address a last value queue", name = "lastValueQueue") boolean lastValueQueue,
                                        @Parameter(desc = "the delivery attempts", name = "deliveryAttempts") int deliveryAttempts,
                                        @Parameter(desc = "the max size in bytes", name = "maxSizeBytes") long maxSizeBytes,
                                        @Parameter(desc = "the page size in bytes", name = "pageSizeBytes") int pageSizeBytes,
                                        @Parameter(desc = "the max number of pages in the soft memory cache", name = "pageMaxCacheSize") int pageMaxCacheSize,
                                        @Parameter(desc = "the redelivery delay", name = "redeliveryDelay") long redeliveryDelay,
                                        @Parameter(desc = "the redelivery delay multiplier", name = "redeliveryMultiplier") double redeliveryMultiplier,
                                        @Parameter(desc = "the maximum redelivery delay", name = "maxRedeliveryDelay") long maxRedeliveryDelay,
                                        @Parameter(desc = "the redistribution delay", name = "redistributionDelay") long redistributionDelay,
                                        @Parameter(desc = "do we send to the DLA when there is no where to route the message", name = "sendToDLAOnNoRoute") boolean sendToDLAOnNoRoute,
                                        @Parameter(desc = "the policy to use when the address is full", name = "addressFullMessagePolicy") String addressFullMessagePolicy,
                                        @Parameter(desc = "when a consumer falls below this threshold in terms of messages consumed per second it will be considered 'slow'", name = "slowConsumerThreshold") long slowConsumerThreshold,
                                        @Parameter(desc = "how often (in seconds) to check for slow consumers", name = "slowConsumerCheckPeriod") long slowConsumerCheckPeriod,
                                        @Parameter(desc = "the policy to use when a slow consumer is detected", name = "slowConsumerPolicy") String slowConsumerPolicy,
                                        @Parameter(desc = "allow queues to be created automatically", name = "autoCreateJmsQueues") boolean autoCreateJmsQueues,
                                        @Parameter(desc = "allow auto-created queues to be deleted automatically", name = "autoDeleteJmsQueues") boolean autoDeleteJmsQueues,
                                        @Parameter(desc = "allow topics to be created automatically", name = "autoCreateJmsTopics") boolean autoCreateJmsTopics,
                                        @Parameter(desc = "allow auto-created topics to be deleted automatically", name = "autoDeleteJmsTopics") boolean autoDeleteJmsTopics) throws Exception {
            proxy.invokeOperation("addAddressSettings", addressMatch, DLA, expiryAddress, expiryDelay, lastValueQueue, deliveryAttempts, maxSizeBytes, pageSizeBytes, pageMaxCacheSize, redeliveryDelay, redeliveryMultiplier, maxRedeliveryDelay, redistributionDelay, sendToDLAOnNoRoute, addressFullMessagePolicy, slowConsumerThreshold, slowConsumerCheckPeriod, slowConsumerPolicy, autoCreateJmsQueues, autoDeleteJmsQueues, autoCreateJmsTopics, autoDeleteJmsTopics);
         }

         @Override
         public String listNetworkTopology() throws Exception {
            return (String) proxy.invokeOperation("listNetworkTopology");
         }

         @Override
         public String getAddressInfo(String address) throws ActiveMQAddressDoesNotExistException {
            return null;
         }

         @Override
         public String[] listBindingsForAddress(String address) throws Exception {
            return new String[0];
         }

         @Override
         public void removeAddressSettings(String addressMatch) throws Exception {
            proxy.invokeOperation("removeAddressSettings", addressMatch);
         }

         @Override
         public void createDivert(String name,
                                  String routingName,
                                  String address,
                                  String forwardingAddress,
                                  boolean exclusive,
                                  String filterString,
                                  String transformerClassName) throws Exception {
            proxy.invokeOperation("createDivert", name, routingName, address, forwardingAddress, exclusive, filterString, transformerClassName);
         }

         @Override
         public void destroyDivert(String name) throws Exception {
            proxy.invokeOperation("destroyDivert", name);
         }

         @Override
         public String[] getBridgeNames() {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("bridgeNames"));
         }

         @Override
         public void destroyBridge(String name) throws Exception {
            proxy.invokeOperation("destroyBridge", name);

         }

         @Override
         public void createConnectorService(String name, String factoryClass, Map<String, Object> parameters) throws Exception {
            proxy.invokeOperation("createConnectorService", name, factoryClass, parameters);
         }

         @Override
         public void destroyConnectorService(String name) throws Exception {
            proxy.invokeOperation("destroyConnectorService", name);
         }

         @Override
         public String[] getConnectorServices() {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("connectorServices"));
         }

         @Override
         public void forceFailover() throws Exception {
            proxy.invokeOperation("forceFailover");
         }

         public String getLiveConnectorName() throws Exception {
            return (String) proxy.retrieveAttributeValue("liveConnectorName");
         }

         @Override
         public String getAddressSettingsAsJSON(String addressMatch) throws Exception {
            return (String) proxy.invokeOperation("getAddressSettingsAsJSON", addressMatch);
         }

         @Override
         public String[] getDivertNames() {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("divertNames"));
         }

         @Override
         public void createBridge(String name,
                                  String queueName,
                                  String forwardingAddress,
                                  String filterString,
                                  String transformerClassName,
                                  long retryInterval,
                                  double retryIntervalMultiplier,
                                  int initialConnectAttempts,
                                  int reconnectAttempts,
                                  boolean useDuplicateDetection,
                                  int confirmationWindowSize,
                                  int producerWindowSize,
                                  long clientFailureCheckPeriod,
                                  String connectorNames,
                                  boolean useDiscovery,
                                  boolean ha,
                                  String user,
                                  String password) throws Exception {
            proxy.invokeOperation("createBridge", name, queueName, forwardingAddress, filterString, transformerClassName, retryInterval, retryIntervalMultiplier, initialConnectAttempts, reconnectAttempts, useDuplicateDetection, confirmationWindowSize, producerWindowSize, clientFailureCheckPeriod, connectorNames, useDiscovery, ha, user, password);
         }

         @Override
         public void createBridge(String name,
                                  String queueName,
                                  String forwardingAddress,
                                  String filterString,
                                  String transformerClassName,
                                  long retryInterval,
                                  double retryIntervalMultiplier,
                                  int initialConnectAttempts,
                                  int reconnectAttempts,
                                  boolean useDuplicateDetection,
                                  int confirmationWindowSize,
                                  long clientFailureCheckPeriod,
                                  String connectorNames,
                                  boolean useDiscovery,
                                  boolean ha,
                                  String user,
                                  String password) throws Exception {
            proxy.invokeOperation("createBridge", name, queueName, forwardingAddress, filterString, transformerClassName, retryInterval, retryIntervalMultiplier, initialConnectAttempts, reconnectAttempts, useDuplicateDetection, confirmationWindowSize, clientFailureCheckPeriod, connectorNames, useDiscovery, ha, user, password);
         }

         @Override
         public String listProducersInfoAsJSON() throws Exception {
            return (String) proxy.invokeOperation("listProducersInfoAsJSON");
         }

         @Override
         public String listConsumersAsJSON(String connectionID) throws Exception {
            return (String) proxy.invokeOperation("listConsumersAsJSON", connectionID);
         }

         @Override
         public String listAllConsumersAsJSON() throws Exception {
            return (String) proxy.invokeOperation("listAllConsumersAsJSON");
         }

         @Override
         public String listConnectionsAsJSON() throws Exception {
            return (String) proxy.invokeOperation("listConnectionsAsJSON");
         }

         @Override
         public String listSessionsAsJSON(@Parameter(desc = "a connection ID", name = "connectionID") String connectionID) throws Exception {
            return (String) proxy.invokeOperation("listSessionsAsJSON", connectionID);
         }
      };
   }

   @Override
   public boolean usingCore() {
      return true;
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

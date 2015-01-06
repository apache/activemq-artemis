/**
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
package org.apache.activemq.tests.integration.management;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.api.core.management.ActiveMQServerControl;
import org.apache.activemq.api.core.management.Parameter;
import org.apache.activemq.api.core.management.ResourceNames;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Before;

/**
 * A ActiveMQServerControlUsingCoreTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ActiveMQServerControlUsingCoreTest extends ActiveMQServerControlTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static String[] toStringArray(final Object[] res)
   {
      String[] names = new String[res.length];
      for (int i = 0; i < res.length; i++)
      {
         names[i] = res[i].toString();
      }
      return names;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // ActiveMQServerControlTest overrides --------------------------

   private ClientSession session;
   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true);
      session.start();

   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      session.close();

      session = null;

      locator.close();

      super.tearDown();
   }

   protected void restartServer() throws Exception
   {
      session.close();

      super.restartServer();

      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true);
      session.start();

   }

   // the core messaging proxy doesn't work when the server is stopped so we cant run these 2 tests
   @Override
   public void testScaleDownWithOutConnector() throws Exception
   {
   }

   @Override
   public void testScaleDownWithConnector() throws Exception
   {
   }

   @Override
   protected ActiveMQServerControl createManagementControl() throws Exception
   {
      return new ActiveMQServerControl()
      {
         @Override
         public void updateDuplicateIdCache(String address, Object[] ids)
         {

         }

         @Override
         public void scaleDown(String connector) throws Exception
         {
            throw new UnsupportedOperationException();
         }

         private final CoreMessagingProxy proxy = new CoreMessagingProxy(session, ResourceNames.CORE_SERVER);

         public boolean isSharedStore()
         {
            return (Boolean) proxy.retrieveAttributeValue("sharedStore");
         }

         public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
         {
            return (Boolean) proxy.invokeOperation("closeConnectionsForAddress", ipAddress);
         }

         public boolean closeConsumerConnectionsForAddress(final String address) throws Exception
         {
            return (Boolean) proxy.invokeOperation("closeConsumerConnectionsForAddress", address);
         }

         public boolean closeConnectionsForUser(final String userName) throws Exception
         {
            return (Boolean) proxy.invokeOperation("closeConnectionsForUser", userName);
         }

         public boolean commitPreparedTransaction(final String transactionAsBase64) throws Exception
         {
            return (Boolean) proxy.invokeOperation("commitPreparedTransaction", transactionAsBase64);
         }

         public void createQueue(final String address, final String name) throws Exception
         {
            proxy.invokeOperation("createQueue", address, name);
         }

         public void createQueue(final String address, final String name, final String filter, final boolean durable) throws Exception
         {
            proxy.invokeOperation("createQueue", address, name, filter, durable);
         }

         public void createQueue(final String address, final String name, final boolean durable) throws Exception
         {
            proxy.invokeOperation("createQueue", address, name, durable);
         }

         public void deployQueue(final String address, final String name, final String filter, final boolean durable) throws Exception
         {
            proxy.invokeOperation("deployQueue", address, name, filter, durable);
         }

         public void deployQueue(final String address, final String name, final String filterString) throws Exception
         {
            proxy.invokeOperation("deployQueue", address, name);
         }

         public void destroyQueue(final String name) throws Exception
         {
            proxy.invokeOperation("destroyQueue", name);
         }

         public void disableMessageCounters() throws Exception
         {
            proxy.invokeOperation("disableMessageCounters");
         }

         public void enableMessageCounters() throws Exception
         {
            proxy.invokeOperation("enableMessageCounters");
         }

         public String getBindingsDirectory()
         {
            return (String) proxy.retrieveAttributeValue("bindingsDirectory");
         }

         public int getConnectionCount()
         {
            return (Integer) proxy.retrieveAttributeValue("connectionCount");
         }

         public long getConnectionTTLOverride()
         {
            return (Long) proxy.retrieveAttributeValue("connectionTTLOverride", Long.class);
         }

         public Object[] getConnectors() throws Exception
         {
            return (Object[]) proxy.retrieveAttributeValue("connectors");
         }

         public String getConnectorsAsJSON() throws Exception
         {
            return (String) proxy.retrieveAttributeValue("connectorsAsJSON");
         }

         public String[] getAddressNames()
         {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("addressNames"));
         }

         public String[] getQueueNames()
         {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("queueNames"));
         }

         public int getIDCacheSize()
         {
            return (Integer) proxy.retrieveAttributeValue("IDCacheSize");
         }

         public String[] getInterceptorClassNames()
         {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("incomingInterceptorClassNames"));
         }

         public String[] getIncomingInterceptorClassNames()
         {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("incomingInterceptorClassNames"));
         }

         public String[] getOutgoingInterceptorClassNames()
         {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("outgoingInterceptorClassNames"));
         }

         public String getJournalDirectory()
         {
            return (String) proxy.retrieveAttributeValue("journalDirectory");
         }

         public int getJournalFileSize()
         {
            return (Integer) proxy.retrieveAttributeValue("journalFileSize");
         }

         public int getJournalMaxIO()
         {
            return (Integer) proxy.retrieveAttributeValue("journalMaxIO");
         }

         public int getJournalMinFiles()
         {
            return (Integer) proxy.retrieveAttributeValue("journalMinFiles");
         }

         public String getJournalType()
         {
            return (String) proxy.retrieveAttributeValue("journalType");
         }

         public String getLargeMessagesDirectory()
         {
            return (String) proxy.retrieveAttributeValue("largeMessagesDirectory");
         }

         public String getManagementAddress()
         {
            return (String) proxy.retrieveAttributeValue("managementAddress");
         }

         public String getManagementNotificationAddress()
         {
            return (String) proxy.retrieveAttributeValue("managementNotificationAddress");
         }

         public int getMessageCounterMaxDayCount()
         {
            return (Integer) proxy.retrieveAttributeValue("messageCounterMaxDayCount");
         }

         public long getMessageCounterSamplePeriod()
         {
            return (Long) proxy.retrieveAttributeValue("messageCounterSamplePeriod", Long.class);
         }

         public long getMessageExpiryScanPeriod()
         {
            return (Long) proxy.retrieveAttributeValue("messageExpiryScanPeriod", Long.class);
         }

         public long getMessageExpiryThreadPriority()
         {
            return (Long) proxy.retrieveAttributeValue("messageExpiryThreadPriority", Long.class);
         }

         public String getPagingDirectory()
         {
            return (String) proxy.retrieveAttributeValue("pagingDirectory");
         }

         public int getScheduledThreadPoolMaxSize()
         {
            return (Integer) proxy.retrieveAttributeValue("scheduledThreadPoolMaxSize");
         }

         public int getThreadPoolMaxSize()
         {
            return (Integer) proxy.retrieveAttributeValue("threadPoolMaxSize");
         }

         public long getSecurityInvalidationInterval()
         {
            return (Long) proxy.retrieveAttributeValue("securityInvalidationInterval", Long.class);
         }

         public long getTransactionTimeout()
         {
            return (Long) proxy.retrieveAttributeValue("transactionTimeout", Long.class);
         }

         public long getTransactionTimeoutScanPeriod()
         {
            return (Long) proxy.retrieveAttributeValue("transactionTimeoutScanPeriod", Long.class);
         }

         public String getVersion()
         {
            return (String) proxy.retrieveAttributeValue("version");
         }

         public boolean isBackup()
         {
            return (Boolean) proxy.retrieveAttributeValue("backup");
         }

         public boolean isClustered()
         {
            return (Boolean) proxy.retrieveAttributeValue("clustered");
         }

         public boolean isCreateBindingsDir()
         {
            return (Boolean) proxy.retrieveAttributeValue("createBindingsDir");
         }

         public boolean isCreateJournalDir()
         {
            return (Boolean) proxy.retrieveAttributeValue("createJournalDir");
         }

         public boolean isJournalSyncNonTransactional()
         {
            return (Boolean) proxy.retrieveAttributeValue("journalSyncNonTransactional");
         }

         public boolean isJournalSyncTransactional()
         {
            return (Boolean) proxy.retrieveAttributeValue("journalSyncTransactional");
         }

         public void setFailoverOnServerShutdown(boolean failoverOnServerShutdown) throws Exception
         {
            proxy.invokeOperation("setFailoverOnServerShutdown", failoverOnServerShutdown);
         }

         public boolean isFailoverOnServerShutdown()
         {
            return (Boolean) proxy.retrieveAttributeValue("failoverOnServerShutdown");
         }

         public void setScaleDown(boolean scaleDown) throws Exception
         {
            proxy.invokeOperation("setEnabled", scaleDown);
         }

         public boolean isScaleDown()
         {
            return (Boolean) proxy.retrieveAttributeValue("scaleDown");
         }

         public boolean isMessageCounterEnabled()
         {
            return (Boolean) proxy.retrieveAttributeValue("messageCounterEnabled");
         }

         public boolean isPersistDeliveryCountBeforeDelivery()
         {
            return (Boolean) proxy.retrieveAttributeValue("persistDeliveryCountBeforeDelivery");
         }

         public boolean isAsyncConnectionExecutionEnabled()
         {
            return (Boolean) proxy.retrieveAttributeValue("asyncConnectionExecutionEnabled");
         }

         public boolean isPersistIDCache()
         {
            return (Boolean) proxy.retrieveAttributeValue("persistIDCache");
         }

         public boolean isSecurityEnabled()
         {
            return (Boolean) proxy.retrieveAttributeValue("securityEnabled");
         }

         public boolean isStarted()
         {
            return (Boolean) proxy.retrieveAttributeValue("started");
         }

         public boolean isWildcardRoutingEnabled()
         {
            return (Boolean) proxy.retrieveAttributeValue("wildcardRoutingEnabled");
         }

         public String[] listConnectionIDs() throws Exception
         {
            return (String[]) proxy.invokeOperation("listConnectionIDs");
         }

         public String[] listPreparedTransactions() throws Exception
         {
            return (String[]) proxy.invokeOperation("listPreparedTransactions");
         }

         public String listPreparedTransactionDetailsAsJSON() throws Exception
         {
            return (String) proxy.invokeOperation("listPreparedTransactionDetailsAsJSON");
         }

         public String listPreparedTransactionDetailsAsHTML() throws Exception
         {
            return (String) proxy.invokeOperation("listPreparedTransactionDetailsAsHTML");
         }

         public String[] listHeuristicCommittedTransactions() throws Exception
         {
            return (String[]) proxy.invokeOperation("listHeuristicCommittedTransactions");
         }

         public String[] listHeuristicRolledBackTransactions() throws Exception
         {
            return (String[]) proxy.invokeOperation("listHeuristicRolledBackTransactions");
         }

         public String[] listRemoteAddresses() throws Exception
         {
            return (String[]) proxy.invokeOperation("listRemoteAddresses");
         }

         public String[] listRemoteAddresses(final String ipAddress) throws Exception
         {
            return (String[]) proxy.invokeOperation("listRemoteAddresses", ipAddress);
         }

         public String[] listSessions(final String connectionID) throws Exception
         {
            return (String[]) proxy.invokeOperation("listSessions", connectionID);
         }

         public void resetAllMessageCounterHistories() throws Exception
         {
            proxy.invokeOperation("resetAllMessageCounterHistories");
         }

         public void resetAllMessageCounters() throws Exception
         {
            proxy.invokeOperation("resetAllMessageCounters");
         }

         public boolean rollbackPreparedTransaction(final String transactionAsBase64) throws Exception
         {
            return (Boolean) proxy.invokeOperation("rollbackPreparedTransaction", transactionAsBase64);
         }

         public void sendQueueInfoToQueue(final String queueName, final String address) throws Exception
         {
            proxy.invokeOperation("sendQueueInfoToQueue", queueName, address);
         }

         public void setMessageCounterMaxDayCount(final int count) throws Exception
         {
            proxy.invokeOperation("setMessageCounterMaxDayCount", count);
         }

         public void setMessageCounterSamplePeriod(final long newPeriod) throws Exception
         {
            proxy.invokeOperation("setMessageCounterSamplePeriod", newPeriod);
         }

         public int getJournalBufferSize()
         {
            return (Integer) proxy.retrieveAttributeValue("JournalBufferSize");
         }

         public int getJournalBufferTimeout()
         {
            return (Integer) proxy.retrieveAttributeValue("JournalBufferTimeout");
         }

         public int getJournalCompactMinFiles()
         {
            return (Integer) proxy.retrieveAttributeValue("JournalCompactMinFiles");
         }

         public int getJournalCompactPercentage()
         {
            return (Integer) proxy.retrieveAttributeValue("JournalCompactPercentage");
         }

         public boolean isPersistenceEnabled()
         {
            return (Boolean) proxy.retrieveAttributeValue("PersistenceEnabled");
         }

         public void addSecuritySettings(String addressMatch,
                                         String sendRoles,
                                         String consumeRoles,
                                         String createDurableQueueRoles,
                                         String deleteDurableQueueRoles,
                                         String createNonDurableQueueRoles,
                                         String deleteNonDurableQueueRoles,
                                         String manageRoles) throws Exception
         {
            proxy.invokeOperation("addSecuritySettings",
                                  addressMatch,
                                  sendRoles,
                                  consumeRoles,
                                  createDurableQueueRoles,
                                  deleteDurableQueueRoles,
                                  createNonDurableQueueRoles,
                                  deleteNonDurableQueueRoles,
                                  manageRoles);
         }

         public void removeSecuritySettings(String addressMatch) throws Exception
         {
            proxy.invokeOperation("removeSecuritySettings", addressMatch);
         }

         public Object[] getRoles(String addressMatch) throws Exception
         {
            return (Object[]) proxy.invokeOperation("getRoles", addressMatch);
         }

         public String getRolesAsJSON(String addressMatch) throws Exception
         {
            return (String) proxy.invokeOperation("getRolesAsJSON", addressMatch);
         }

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
                                        @Parameter(desc = "allow auto-created queues to be deleted automatically", name = "autoDeleteJmsQueues") boolean autoDeleteJmsQueues) throws Exception
         {
            proxy.invokeOperation("addAddressSettings",
                                  addressMatch,
                                  DLA,
                                  expiryAddress,
                                  expiryDelay,
                                  lastValueQueue,
                                  deliveryAttempts,
                                  maxSizeBytes,
                                  pageSizeBytes,
                                  pageMaxCacheSize,
                                  redeliveryDelay,
                                  redeliveryMultiplier,
                                  maxRedeliveryDelay,
                                  redistributionDelay,
                                  sendToDLAOnNoRoute,
                                  addressFullMessagePolicy,
                                  slowConsumerThreshold,
                                  slowConsumerCheckPeriod,
                                  slowConsumerPolicy,
                                  autoCreateJmsQueues,
                                  autoDeleteJmsQueues);
         }

         public void removeAddressSettings(String addressMatch) throws Exception
         {
            proxy.invokeOperation("removeAddressSettings", addressMatch);
         }

         public void createDivert(String name,
                                  String routingName,
                                  String address,
                                  String forwardingAddress,
                                  boolean exclusive,
                                  String filterString,
                                  String transformerClassName) throws Exception
         {
            proxy.invokeOperation("createDivert",
                                  name,
                                  routingName,
                                  address,
                                  forwardingAddress,
                                  exclusive,
                                  filterString,
                                  transformerClassName);
         }

         public void destroyDivert(String name) throws Exception
         {
            proxy.invokeOperation("destroyDivert", name);
         }

         public String[] getBridgeNames()
         {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("bridgeNames"));
         }

         public void destroyBridge(String name) throws Exception
         {
            proxy.invokeOperation("destroyBridge", name);

         }

         public void forceFailover() throws Exception
         {
            proxy.invokeOperation("forceFailover");
         }

         public String getLiveConnectorName() throws Exception
         {
            return (String) proxy.retrieveAttributeValue("liveConnectorName");
         }

         public String getAddressSettingsAsJSON(String addressMatch) throws Exception
         {
            return (String) proxy.invokeOperation("getAddressSettingsAsJSON", addressMatch);
         }

         public String[] getDivertNames()
         {
            return ActiveMQServerControlUsingCoreTest.toStringArray((Object[]) proxy.retrieveAttributeValue("divertNames"));
         }

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
                                  String password) throws Exception
         {
            proxy.invokeOperation("createBridge",
                                  name,
                                  queueName,
                                  forwardingAddress,
                                  filterString,
                                  transformerClassName,
                                  retryInterval,
                                  retryIntervalMultiplier,
                                  initialConnectAttempts,
                                  reconnectAttempts,
                                  useDuplicateDetection,
                                  confirmationWindowSize,
                                  clientFailureCheckPeriod,
                                  connectorNames,
                                  useDiscovery,
                                  ha,
                                  user,
                                  password);
         }


         public String listProducersInfoAsJSON() throws Exception
         {
            return (String) proxy.invokeOperation("listProducersInfoAsJSON");
         }
      };
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

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
package org.apache.activemq.api.core.management;

import javax.management.MBeanOperationInfo;

/**
 * A HornetQServerControl is used to manage HornetQ servers.
 */
public interface HornetQServerControl
{
   /**
    * Returns this server's version.
    */
   String getVersion();

   /**
    * Returns the number of connections connected to this server.
    */
   int getConnectionCount();

   /**
    * Return whether this server is started.
    */
   boolean isStarted();

   /**
    * Returns the list of interceptors used by this server. Invoking this method is the same as invoking
    * <code>getIncomingInterceptorClassNames().</code>
    *
    * @see org.apache.activemq.api.core.Interceptor
    * @deprecated As of HornetQ 2.3.0.Final, replaced by
    * {@link #getIncomingInterceptorClassNames()} and
    * {@link #getOutgoingInterceptorClassNames()}
    */
   @Deprecated
   String[] getInterceptorClassNames();

   /**
    * Returns the list of interceptors used by this server for incoming messages.
    *
    * @see org.apache.activemq.api.core.Interceptor
    */
   String[] getIncomingInterceptorClassNames();

   /**
    * Returns the list of interceptors used by this server for outgoing messages.
    *
    * @see org.apache.activemq.api.core.Interceptor
    */
   String[] getOutgoingInterceptorClassNames();

   /**
    * Returns whether this server is clustered.
    */
   boolean isClustered();

   /**
    * Returns the maximum number of threads in the <em>scheduled</em> thread pool.
    */
   int getScheduledThreadPoolMaxSize();

   /**
    * Returns the maximum number of threads in the thread pool.
    */
   int getThreadPoolMaxSize();

   /**
    * Returns the interval time (in milliseconds) to invalidate security credentials.
    */
   long getSecurityInvalidationInterval();

   /**
    * Returns whether security is enabled for this server.
    */
   boolean isSecurityEnabled();

   /**
    * Returns the file system directory used to store bindings.
    */
   String getBindingsDirectory();

   /**
    * Returns the file system directory used to store journal log.
    */
   String getJournalDirectory();

   /**
    * Returns the type of journal used by this server (either {@code NIO} or {@code ASYNCIO}).
    */
   String getJournalType();

   /**
    * Returns whether the journal is synchronized when receiving transactional data.
    */
   boolean isJournalSyncTransactional();

   /**
    * Returns whether the journal is synchronized when receiving non-transactional data.
    */
   boolean isJournalSyncNonTransactional();

   /**
    * Returns the size (in bytes) of each journal files.
    */
   int getJournalFileSize();

   /**
    * Returns the number of journal files to pre-create.
    */
   int getJournalMinFiles();

   /**
    * Returns the maximum number of write requests that can be in the AIO queue at any given time.
    */
   int getJournalMaxIO();

   /**
    * Returns the size of the internal buffer on the journal.
    */
   int getJournalBufferSize();

   /**
    * Returns the timeout (in nanoseconds) used to flush internal buffers on the journal.
    */
   int getJournalBufferTimeout();

   /**
    * do any clients failover on a server shutdown
    */
   void setFailoverOnServerShutdown(boolean failoverOnServerShutdown) throws Exception;


   /**
    * returns if clients failover on a server shutdown
    */
   boolean isFailoverOnServerShutdown();

   /**
    * Returns the minimal number of journal files before compacting.
    */
   int getJournalCompactMinFiles();

   /**
    * Return the percentage of live data before compacting the journal.
    */
   int getJournalCompactPercentage();

   /**
    * Returns whether this server is using persistence and store data.
    */
   boolean isPersistenceEnabled();

   /**
    * Returns whether the bindings directory is created on this server startup.
    */
   boolean isCreateBindingsDir();

   /**
    * Returns whether the journal directory is created on this server startup.
    */
   boolean isCreateJournalDir();

   /**
    * Returns whether message counter is enabled for this server.
    */
   boolean isMessageCounterEnabled();

   /**
    * Returns the maximum number of days kept in memory for message counter.
    */
   int getMessageCounterMaxDayCount();

   /**
    * Sets the maximum number of days kept in memory for message counter.
    *
    * @param count value must be greater than 0
    */
   void setMessageCounterMaxDayCount(int count) throws Exception;

   /**
    * Returns the sample period (in milliseconds) to take message counter snapshot.
    */
   long getMessageCounterSamplePeriod();

   /**
    * Sets the sample period to take message counter snapshot.
    *
    * @param newPeriod value must be greater than 1000ms
    */
   void setMessageCounterSamplePeriod(long newPeriod) throws Exception;

   /**
    * Returns {@code true} if this server is a backup, {@code false} if it is a live server.
    * <br>
    * If a backup server has been activated, returns {@code false}.
    */
   boolean isBackup();

   /**
    * Returns whether this server shares its data store with a corresponding live or backup server.
    */
   boolean isSharedStore();

   /**
    * Returns the file system directory used to store paging files.
    */
   String getPagingDirectory();

   /**
    * Returns whether delivery count is persisted before messages are delivered to the consumers.
    */
   boolean isPersistDeliveryCountBeforeDelivery();

   /**
    * Returns the connection time to live.
    * <br>
    * This value overrides the connection time to live <em>sent by the client</em>.
    */
   long getConnectionTTLOverride();

   /**
    * Returns the management address of this server.
    * <br>
    * Clients can send management messages to this address to manage this server.
    */
   String getManagementAddress();

   /**
    * Returns the management notification address of this server.
    * <br>
    * Clients can bind queues to this address to receive management notifications emitted by this server.
    */
   String getManagementNotificationAddress();

   /**
    * Returns the size of the cache for pre-creating message IDs.
    */
   int getIDCacheSize();

   /**
    * Returns whether message ID cache is persisted.
    */
   boolean isPersistIDCache();

   /**
    * Returns the file system directory used to store large messages.
    */
   String getLargeMessagesDirectory();

   /**
    * Returns whether wildcard routing is supported by this server.
    */
   boolean isWildcardRoutingEnabled();

   /**
    * Returns the timeout (in milliseconds) after which transactions is removed
    * from the resource manager after it was created.
    */
   long getTransactionTimeout();

   /**
    * Returns the frequency (in milliseconds)  to scan transactions to detect which transactions
    * have timed out.
    */
   long getTransactionTimeoutScanPeriod();

   /**
    * Returns the frequency (in milliseconds)  to scan messages to detect which messages
    * have expired.
    */
   long getMessageExpiryScanPeriod();

   /**
    * Returns the priority of the thread used to scan message expiration.
    */
   long getMessageExpiryThreadPriority();

   /**
    * Returns whether code coming from connection is executed asynchronously or not.
    */
   boolean isAsyncConnectionExecutionEnabled();

   /**
    * Returns the connectors configured for this server.
    */
   Object[] getConnectors() throws Exception;

   /**
    * Returns the connectors configured for this server using JSON serialization.
    */
   String getConnectorsAsJSON() throws Exception;

   /**
    * Returns the addresses created on this server.
    */
   String[] getAddressNames();

   /**
    * Returns the names of the queues created on this server.
    */
   String[] getQueueNames();

   // Operations ----------------------------------------------------

   /**
    * Create a durable queue.
    * <br>
    * This method throws a {@link org.apache.activemq.api.core.HornetQQueueExistsException}) exception if the queue already exits.
    *
    * @param address address to bind the queue to
    * @param name    name of the queue
    */
   @Operation(desc = "Create a queue with the specified address", impact = MBeanOperationInfo.ACTION)
   void createQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name) throws Exception;

   /**
    * Create a queue.
    * <br>
    * This method throws a {@link org.apache.activemq.api.core.HornetQQueueExistsException}) exception if the queue already exits.
    *
    * @param address address to bind the queue to
    * @param name    name of the queue
    * @param filter  of the queue
    * @param durable whether the queue is durable
    */
   @Operation(desc = "Create a queue", impact = MBeanOperationInfo.ACTION)
   void createQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name,
                    @Parameter(name = "filter", desc = "Filter of the queue") String filter,
                    @Parameter(name = "durable", desc = "Is the queue durable?") boolean durable) throws Exception;

   /**
    * Create a queue.
    * <br>
    * This method throws a {@link org.apache.activemq.api.core.HornetQQueueExistsException}) exception if the queue already exits.
    *
    * @param address address to bind the queue to
    * @param name    name of the queue
    * @param durable whether the queue is durable
    */
   @Operation(desc = "Create a queue with the specified address, name and durability", impact = MBeanOperationInfo.ACTION)
   void createQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name,
                    @Parameter(name = "durable", desc = "Is the queue durable?") boolean durable) throws Exception;

   /**
    * Deploy a durable queue.
    * <br>
    * This method will do nothing if the queue with the given name already exists on the server.
    *
    * @param address address to bind the queue to
    * @param name    name of the queue
    * @param filter  of the queue
    */
   @Operation(desc = "Deploy a queue", impact = MBeanOperationInfo.ACTION)
   void deployQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name,
                    @Parameter(name = "filter", desc = "Filter of the queue") String filter) throws Exception;

   /**
    * Deploy a queue.
    * <br>
    * This method will do nothing if the queue with the given name already exists on the server.
    *
    * @param address address to bind the queue to
    * @param name    name of the queue
    * @param filter  of the queue
    * @param durable whether the queue is durable
    */
   @Operation(desc = "Deploy a queue", impact = MBeanOperationInfo.ACTION)
   void deployQueue(@Parameter(name = "address", desc = "Address of the queue") String address,
                    @Parameter(name = "name", desc = "Name of the queue") String name,
                    @Parameter(name = "filter", desc = "Filter of the queue") String filter,
                    @Parameter(name = "durable", desc = "Is the queue durable?") boolean durable) throws Exception;

   /**
    * Destroys the queue corresponding to the specified name.
    */
   @Operation(desc = "Destroy a queue", impact = MBeanOperationInfo.ACTION)
   void destroyQueue(@Parameter(name = "name", desc = "Name of the queue to destroy") String name) throws Exception;

   /**
    * Enables message counters for this server.
    */
   @Operation(desc = "Enable message counters", impact = MBeanOperationInfo.ACTION)
   void enableMessageCounters() throws Exception;

   /**
    * Disables message counters for this server.
    */
   @Operation(desc = "Disable message counters", impact = MBeanOperationInfo.ACTION)
   void disableMessageCounters() throws Exception;

   /**
    * Reset all message counters.
    */
   @Operation(desc = "Reset all message counters", impact = MBeanOperationInfo.ACTION)
   void resetAllMessageCounters() throws Exception;

   /**
    * Reset histories for all message counters.
    */
   @Operation(desc = "Reset all message counters history", impact = MBeanOperationInfo.ACTION)
   void resetAllMessageCounterHistories() throws Exception;

   /**
    * List all the prepared transaction, sorted by date, oldest first.
    * <br>
    * The Strings are Base-64 representation of the transaction XID and can be
    * used to heuristically commit or rollback the transactions.
    *
    * @see #commitPreparedTransaction(String)
    * @see #rollbackPreparedTransaction(String)
    */
   @Operation(desc = "List all the prepared transaction, sorted by date, oldest first")
   String[] listPreparedTransactions() throws Exception;

   /**
    * List all the prepared transaction, sorted by date,
    * oldest first, with details, in text format.
    */
   @Operation(desc = "List all the prepared transaction, sorted by date, oldest first, with details, in JSON format")
   String listPreparedTransactionDetailsAsJSON() throws Exception;

   /**
    * List all the prepared transaction, sorted by date,
    * oldest first, with details, in HTML format
    */
   @Operation(desc = "List all the prepared transaction, sorted by date, oldest first, with details, in HTML format")
   String listPreparedTransactionDetailsAsHTML() throws Exception;

   /**
    * List transactions which have been heuristically committed.
    */
   String[] listHeuristicCommittedTransactions() throws Exception;

   /**
    * List transactions which have been heuristically rolled back.
    */
   String[] listHeuristicRolledBackTransactions() throws Exception;

   /**
    * Heuristically commits a prepared transaction.
    *
    * @param transactionAsBase64 base 64 representation of a prepare transaction
    * @return {@code true} if the transaction was successfully committed, {@code false} else
    * @see #listPreparedTransactions()
    */
   @Operation(desc = "Commit a prepared transaction")
   boolean commitPreparedTransaction(@Parameter(desc = "the Base64 representation of a transaction", name = "transactionAsBase64") String transactionAsBase64) throws Exception;

   /**
    * Heuristically rolls back a prepared transaction.
    *
    * @param transactionAsBase64 base 64 representation of a prepare transaction
    * @return {@code true} if the transaction was successfully rolled back, {@code false} else
    * @see #listPreparedTransactions()
    */
   @Operation(desc = "Rollback a prepared transaction")
   boolean rollbackPreparedTransaction(@Parameter(desc = "the Base64 representation of a transaction", name = "transactionAsBase64") String transactionAsBase64) throws Exception;

   /**
    * Lists the addresses of all the clients connected to this address.
    */
   @Operation(desc = "List the client addresses", impact = MBeanOperationInfo.INFO)
   String[] listRemoteAddresses() throws Exception;

   /**
    * Lists the addresses of the clients connected to this address which matches the specified IP address.
    */
   @Operation(desc = "List the client addresses which match the given IP Address", impact = MBeanOperationInfo.INFO)
   String[] listRemoteAddresses(@Parameter(desc = "an IP address", name = "ipAddress") String ipAddress) throws Exception;

   /**
    * Closes all the connections of clients connected to this server which matches the specified IP address.
    */
   @Operation(desc = "Closes all the connections for the given IP Address", impact = MBeanOperationInfo.INFO)
   boolean closeConnectionsForAddress(@Parameter(desc = "an IP address", name = "ipAddress") String ipAddress) throws Exception;

   /**
    * Closes all the connections of clients connected to this server which matches the specified IP address.
    */
   @Operation(desc = "Closes all the consumer connections for the given HornetQ address", impact = MBeanOperationInfo.INFO)
   boolean closeConsumerConnectionsForAddress(@Parameter(desc = "a HornetQ address", name = "address") String address) throws Exception;

   /**
    * Closes all the connections of sessions with a matching user name.
    */
   @Operation(desc = "Closes all the connections for sessions with the given user name", impact = MBeanOperationInfo.INFO)
   boolean closeConnectionsForUser(@Parameter(desc = "a user name", name = "userName") String address) throws Exception;

   /**
    * Lists all the IDs of the connections connected to this server.
    */
   @Operation(desc = "List all the connection IDs", impact = MBeanOperationInfo.INFO)
   String[] listConnectionIDs() throws Exception;

   String listProducersInfoAsJSON() throws Exception;

   /**
    * Lists all the sessions IDs for the specified connection ID.
    */
   @Operation(desc = "List the sessions for the given connectionID", impact = MBeanOperationInfo.INFO)
   String[] listSessions(@Parameter(desc = "a connection ID", name = "connectionID") String connectionID) throws Exception;

   /**
    * This method is used by HornetQ clustering and must not be called by HornetQ clients.
    */
   void sendQueueInfoToQueue(String queueName, String address) throws Exception;

   @Operation(desc = "Add security settings for addresses matching the addressMatch", impact = MBeanOperationInfo.ACTION)
   void addSecuritySettings(
      @Parameter(desc = "an address match", name = "addressMatch") String addressMatch,
      @Parameter(desc = "a comma-separated list of roles allowed to send messages", name = "send") String sendRoles,
      @Parameter(desc = "a comma-separated list of roles allowed to consume messages", name = "consume") String consumeRoles,
      @Parameter(desc = "a comma-separated list of roles allowed to create durable queues", name = "createDurableQueueRoles") String createDurableQueueRoles,
      @Parameter(desc = "a comma-separated list of roles allowed to delete durable queues", name = "deleteDurableQueueRoles") String deleteDurableQueueRoles,
      @Parameter(desc = "a comma-separated list of roles allowed to create non durable queues", name = "createNonDurableQueueRoles") String createNonDurableQueueRoles,
      @Parameter(desc = "a comma-separated list of roles allowed to delete non durable queues", name = "deleteNonDurableQueueRoles") String deleteNonDurableQueueRoles,
      @Parameter(desc = "a comma-separated list of roles allowed to send management messages messages", name = "manage") String manageRoles) throws Exception;

   @Operation(desc = "Remove security settings for an address", impact = MBeanOperationInfo.ACTION)
   void removeSecuritySettings(@Parameter(desc = "an address match", name = "addressMatch") String addressMatch) throws Exception;

   @Operation(desc = "get roles for a specific address match", impact = MBeanOperationInfo.INFO)
   Object[] getRoles(@Parameter(desc = "an address match", name = "addressMatch") String addressMatch) throws Exception;

   @Operation(desc = "get roles (as a JSON string) for a specific address match", impact = MBeanOperationInfo.INFO)
   String getRolesAsJSON(@Parameter(desc = "an address match", name = "addressMatch") String addressMatch) throws Exception;

   /**
    * adds a new address setting for a specific address
    */
   @Operation(desc = "Add address settings for addresses matching the addressMatch", impact = MBeanOperationInfo.ACTION)
   void addAddressSettings(@Parameter(desc = "an address match", name = "addressMatch") String addressMatch,
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
                           @Parameter(desc = "the policy to use when a slow consumer is detected", name = "slowConsumerPolicy") String slowConsumerPolicy) throws Exception;

   void removeAddressSettings(String addressMatch) throws Exception;

   /**
    * returns the address settings as a JSON string
    */
   @Operation(desc = "returns the address settings as a JSON string for an address match", impact = MBeanOperationInfo.INFO)
   String getAddressSettingsAsJSON(@Parameter(desc = "an address match", name = "addressMatch") String addressMatch) throws Exception;

   String[] getDivertNames();

   @Operation(desc = "Create a Divert", impact = MBeanOperationInfo.ACTION)
   void createDivert(@Parameter(name = "name", desc = "Name of the divert") String name,
                     @Parameter(name = "routingName", desc = "Routing name of the divert") String routingName,
                     @Parameter(name = "address", desc = "Address to divert from") String address,
                     @Parameter(name = "forwardingAddress", desc = "Address to divert to") String forwardingAddress,
                     @Parameter(name = "exclusive", desc = "Is the divert exclusive?") boolean exclusive,
                     @Parameter(name = "filterString", desc = "Filter of the divert") String filterString,
                     @Parameter(name = "transformerClassName", desc = "Class name of the divert's transformer") String transformerClassName) throws Exception;

   @Operation(desc = "Destroy a Divert", impact = MBeanOperationInfo.ACTION)
   void destroyDivert(@Parameter(name = "name", desc = "Name of the divert") String name) throws Exception;

   String[] getBridgeNames();

   @Operation(desc = "Create a Bridge", impact = MBeanOperationInfo.ACTION)
   void createBridge(@Parameter(name = "name", desc = "Name of the bridge") String name,
                     @Parameter(name = "queueName", desc = "Name of the source queue") String queueName,
                     @Parameter(name = "forwardingAddress", desc = "Forwarding address") String forwardingAddress,
                     @Parameter(name = "filterString", desc = "Filter of the brdige") String filterString,
                     @Parameter(name = "transformerClassName", desc = "Class name of the bridge transformer") String transformerClassName,
                     @Parameter(name = "retryInterval", desc = "Connection retry interval") long retryInterval,
                     @Parameter(name = "retryIntervalMultiplier", desc = "Connection retry interval multiplier") double retryIntervalMultiplier,
                     @Parameter(name = "initialConnectAttempts", desc = "Number of initial connection attempts") int initialConnectAttempts,
                     @Parameter(name = "reconnectAttempts", desc = "Number of reconnection attempts") int reconnectAttempts,
                     @Parameter(name = "useDuplicateDetection", desc = "Use duplicate detection") boolean useDuplicateDetection,
                     @Parameter(name = "confirmationWindowSize", desc = "Confirmation window size") int confirmationWindowSize,
                     @Parameter(name = "clientFailureCheckPeriod", desc = "Period to check client failure") long clientFailureCheckPeriod,
                     @Parameter(name = "staticConnectorNames", desc = "comma separated list of connector names or name of discovery group if 'useDiscoveryGroup' is set to true") String connectorNames,
                     @Parameter(name = "useDiscoveryGroup", desc = "use discovery  group") boolean useDiscoveryGroup,
                     @Parameter(name = "ha", desc = "Is it using HA") boolean ha,
                     @Parameter(name = "user", desc = "User name") String user,
                     @Parameter(name = "password", desc = "User password") String password) throws Exception;


   @Operation(desc = "Destroy a bridge", impact = MBeanOperationInfo.ACTION)
   void destroyBridge(@Parameter(name = "name", desc = "Name of the bridge") String name) throws Exception;

   @Operation(desc = "force the server to stop and notify clients to failover", impact = MBeanOperationInfo.UNKNOWN)
   void forceFailover() throws Exception;

   void updateDuplicateIdCache(String address, Object[] ids) throws Exception;

   @Operation(desc = "force the server to stop and to scale down to another server", impact = MBeanOperationInfo.UNKNOWN)
   void scaleDown(@Parameter(name = "name", desc = "The connector to use to scale down, if not provided the first appropriate connector will be used")String connector) throws Exception;
}


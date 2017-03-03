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
package org.apache.activemq.artemis.api.config;

import org.apache.activemq.artemis.ArtemisConstants;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.DivertConfigurationRoutingType;
import org.apache.activemq.artemis.api.core.RoutingType;

/**
 * Default values of ActiveMQ Artemis configuration parameters.
 */
public final class ActiveMQDefaultConfiguration {
      /*
    * <p> In order to avoid compile time in-lining of constants, all access is done through methods
    * and all fields are PRIVATE STATIC but not FINAL. This is done following the recommendation at
    * <a href="http://docs.oracle.com/javase/specs/jls/se7/html/jls-13.html#jls-13.4.9">13.4.9.
    * final Fields and Constants</a>
    * @see http://docs.oracle.com/javase/specs/jls/se7/html/jls-13.html#jls-13.4.9
    */

   private ActiveMQDefaultConfiguration() {
      // Utility class
   }

   public static long getDefaultClientFailureCheckPeriod() {
      return DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;
   }

   public static long getDefaultFileDeployerScanPeriod() {
      return DEFAULT_FILE_DEPLOYER_SCAN_PERIOD;
   }

   public static int getDefaultJournalMaxIoAio() {
      return DEFAULT_JOURNAL_MAX_IO_AIO;
   }

   public static int getDefaultJournalBufferTimeoutAio() {
      return DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO;
   }

   public static int getDefaultJournalBufferSizeAio() {
      return DEFAULT_JOURNAL_BUFFER_SIZE_AIO;
   }

   public static int getDefaultJournalMaxIoNio() {
      return DEFAULT_JOURNAL_MAX_IO_NIO;
   }

   public static int getDefaultJournalBufferTimeoutNio() {
      return DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO;
   }

   public static int getDefaultJournalBufferSizeNio() {
      return DEFAULT_JOURNAL_BUFFER_SIZE_NIO;
   }

   public static String getPropMaskPassword() {
      return PROP_MASK_PASSWORD;
   }

   public static String getPropPasswordCodec() {
      return PROP_PASSWORD_CODEC;
   }

   /**
    * what kind of HA Policy should we use
    */
   public static String getDefaultHapolicyType() {
      return DEFAULT_HAPOLICY_TYPE;
   }

   /**
    * The backup strategy to use if we are a backup or for any colocated backups.
    */
   public static String getDefaultHapolicyBackupStrategy() {
      return DEFAULT_HAPOLICY_BACKUP_STRATEGY;
   }

   //shared by client and core/server
   // XXX not on schema?
   private static long DEFAULT_CLIENT_FAILURE_CHECK_PERIOD = 30000;

   // XXX not on schema?
   private static long DEFAULT_FILE_DEPLOYER_SCAN_PERIOD = 5000;

   // These defaults are applied depending on whether the journal type
   // is NIO or AIO.
   private static int DEFAULT_JOURNAL_MAX_IO_AIO = 500;
   private static int DEFAULT_JOURNAL_POOL_FILES = -1;
   private static int DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO = ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO;
   private static int DEFAULT_JOURNAL_BUFFER_SIZE_AIO = ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO;
   private static int DEFAULT_JOURNAL_MAX_IO_NIO = 1;
   private static int DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO = ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO;
   private static int DEFAULT_JOURNAL_BUFFER_SIZE_NIO = ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO;

   // XXX not on schema.
   //properties passed to acceptor/connectors.
   private static String PROP_MASK_PASSWORD = "activemq.usemaskedpassword";
   private static String PROP_PASSWORD_CODEC = "activemq.passwordcodec";

   // what kind of HA Policy should we use
   private static String DEFAULT_HAPOLICY_TYPE = "NONE";

   // The backup strategy to use if we are a backup or for any colocated backups.
   private static String DEFAULT_HAPOLICY_BACKUP_STRATEGY = "FULL";

   // -------------------------------------------------------------------
   // Following fields are generated from the activemq-schema.xsd annotations
   // -------------------------------------------------------------------

   // If true then the ActiveMQ Artemis Server will make use of any Protocol Managers that are in available on the classpath. If false then only the core protocol will be available, unless in Embedded mode where users can inject their own Protocol Managers.
   private static boolean DEFAULT_RESOLVE_PROTOCOLS = true;

   // true means that the server will load configuration from the configuration files
   private static boolean DEFAULT_FILE_DEPLOYMENT_ENABLED = false;

   // true means that the server will use the file based journal for persistence.
   private static boolean DEFAULT_PERSISTENCE_ENABLED = true;

   // true means that the server will sync data files
   private static boolean DEFAULT_JOURNAL_DATASYNC = true;

   // Maximum number of threads to use for the scheduled thread pool
   private static int DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE = 5;

   // Maximum number of threads to use for the thread pool. -1 means 'no limits'.
   private static int DEFAULT_THREAD_POOL_MAX_SIZE = 30;

   // true means that security is enabled
   private static boolean DEFAULT_SECURITY_ENABLED = true;

   // true means that graceful shutdown is enabled
   private static boolean DEFAULT_GRACEFUL_SHUTDOWN_ENABLED = false;

   // how long (in ms) to wait before forcing the server to stop even if clients are still connected (i.e circumventing graceful shutdown)
   private static long DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT = -1;

   // how long (in ms) to wait before invalidating the security cache
   private static long DEFAULT_SECURITY_INVALIDATION_INTERVAL = 10000;

   // how long (in ms) to wait to acquire a file lock on the journal
   private static long DEFAULT_JOURNAL_LOCK_ACQUISITION_TIMEOUT = -1;

   // true means that the server supports wild card routing
   private static boolean DEFAULT_WILDCARD_ROUTING_ENABLED = true;

   private static String DEFAULT_ADDRESS_PATH_SEPARATOR = ".";

   private static SimpleString DEFAULT_MANAGEMENT_ADDRESS = new SimpleString("activemq.management");

   // the name of the address that consumers bind to receive management notifications
   private static SimpleString DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS = new SimpleString("activemq.notifications");

   // The default address used for clustering, empty string means all addresses
   private static String DEFAULT_CLUSTER_ADDRESS = "";

   // Cluster username. It applies to all cluster configurations.
   private static String DEFAULT_CLUSTER_USER = "ACTIVEMQ.CLUSTER.ADMIN.USER";

   // Cluster password. It applies to all cluster configurations.
   private static String DEFAULT_CLUSTER_PASSWORD = "CHANGE ME!!";

   // This option controls whether passwords in server configuration need be masked. If set to "true" the passwords are masked.
   private static boolean DEFAULT_MASK_PASSWORD = false;

   // true means that the management API is available via JMX
   private static boolean DEFAULT_JMX_MANAGEMENT_ENABLED = true;

   // the JMX domain used to registered ActiveMQ Artemis MBeans in the MBeanServer
   private static String DEFAULT_JMX_DOMAIN = "org.apache.activemq.artemis";

   // the JMX domain used to registered ActiveMQ Artemis MBeans in the MBeanServer
   private static boolean DEFAULT_JMX_IS_USE_BROKER_NAME = true;

   // true means that message counters are enabled
   private static boolean DEFAULT_MESSAGE_COUNTER_ENABLED = false;

   // the sample period (in ms) to use for message counters
   private static long DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD = 10000;

   // how many days to keep message counter history
   private static int DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY = 10;

   // if set, this will override how long (in ms) to keep a connection alive without receiving a ping. -1 disables this setting.
   private static long DEFAULT_CONNECTION_TTL_OVERRIDE = -1;

   // should certain incoming packets on the server be handed off to a thread from the thread pool for processing or should they be handled on the remoting thread?
   private static boolean DEFAULT_ASYNC_CONNECTION_EXECUTION_ENABLED = true;

   // how long (in ms) before a transaction can be removed from the resource manager after create time
   private static long DEFAULT_TRANSACTION_TIMEOUT = 300000;

   // how often (in ms) to scan for timeout transactions
   private static long DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD = 1000;

   // how often (in ms) to scan for expired messages
   private static long DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD = 30000;

   // the priority of the thread expiring messages
   private static int DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY = 3;

   // the size of the cache for pre-creating message ID's
   private static int DEFAULT_ID_CACHE_SIZE = 20000;

   // true means that ID's are persisted to the journal
   private static boolean DEFAULT_PERSIST_ID_CACHE = true;

   // True means that the delivery count is persisted before delivery. False means that this only happens after a message has been cancelled.
   private static boolean DEFAULT_PERSIST_DELIVERY_COUNT_BEFORE_DELIVERY = false;

   // the directory to store paged messages in
   private static String DEFAULT_PAGING_DIR = "data/paging";

   // the directory to store the persisted bindings to
   private static String DEFAULT_BINDINGS_DIRECTORY = "data/bindings";

   // true means that the server will create the bindings directory on start up
   private static boolean DEFAULT_CREATE_BINDINGS_DIR = true;

   // The max number of concurrent reads allowed on paging
   private static int DEFAULT_MAX_CONCURRENT_PAGE_IO = 5;

   // the directory to store the journal files in
   private static String DEFAULT_JOURNAL_DIR = "data/journal";

   // true means that the journal directory will be created
   private static boolean DEFAULT_CREATE_JOURNAL_DIR = true;

   // if true wait for transaction data to be synchronized to the journal before returning response to client
   private static boolean DEFAULT_JOURNAL_SYNC_TRANSACTIONAL = true;

   // if true wait for non transaction data to be synced to the journal before returning response to client.
   private static boolean DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL = true;

   // Whether to log messages about the journal write rate
   private static boolean DEFAULT_JOURNAL_LOG_WRITE_RATE = false;

   // the size (in bytes) of each journal file
   private static int DEFAULT_JOURNAL_FILE_SIZE = 10485760;

   // how many journal files to pre-create
   private static int DEFAULT_JOURNAL_MIN_FILES = 2;

   // The percentage of live data on which we consider compacting the journal
   private static int DEFAULT_JOURNAL_COMPACT_PERCENTAGE = 30;

   // The minimal number of data files before we can start compacting
   private static int DEFAULT_JOURNAL_COMPACT_MIN_FILES = 10;

   // Interval to log server specific information (e.g. memory usage etc)
   private static long DEFAULT_SERVER_DUMP_INTERVAL = -1;

   // Percentage of available memory which will trigger a warning log
   private static int DEFAULT_MEMORY_WARNING_THRESHOLD = 25;

   // frequency to sample JVM memory in ms (or -1 to disable memory sampling)
   private static long DEFAULT_MEMORY_MEASURE_INTERVAL = -1;

   // the directory to store large messages
   private static String DEFAULT_LARGE_MESSAGES_DIR = "data/largemessages";

   // period in milliseconds between consecutive broadcasts
   private static long DEFAULT_BROADCAST_PERIOD = 2000;

   // Period the discovery group waits after receiving the last broadcast from a particular server before removing that servers connector pair entry from its list.
   private static int DEFAULT_BROADCAST_REFRESH_TIMEOUT = 10000;

   // how long to keep a connection alive in the absence of any data arriving from the client. This should be greater than the ping period.
   private static long DEFAULT_CONNECTION_TTL = 60000;

   // multiplier to apply to successive retry intervals
   private static double DEFAULT_RETRY_INTERVAL_MULTIPLIER = 1;

   // Limit to the retry-interval growth (due to retry-interval-multiplier)
   private static long DEFAULT_MAX_RETRY_INTERVAL = 2000;

   // maximum number of initial connection attempts, -1 means 'no limits'
   private static int DEFAULT_BRIDGE_INITIAL_CONNECT_ATTEMPTS = -1;

   // maximum number of retry attempts, -1 means 'no limits'
   private static int DEFAULT_BRIDGE_RECONNECT_ATTEMPTS = -1;

   // should duplicate detection headers be inserted in forwarded messages?
   private static boolean DEFAULT_BRIDGE_DUPLICATE_DETECTION = true;

   // Once the bridge has received this many bytes, it sends a confirmation
   private static int DEFAULT_BRIDGE_CONFIRMATION_WINDOW_SIZE = 1048576;

   // Producer flow control
   private static int DEFAULT_BRIDGE_PRODUCER_WINDOW_SIZE = -1;

   // Upon reconnection this configures the number of time the same node on the topology will be retried before resetting the server locator and using the initial connectors
   private static int DEFAULT_BRIDGE_CONNECT_SAME_NODE = 10;

   // The period (in milliseconds) used to check if the cluster connection has failed to receive pings from another server
   private static long DEFAULT_CLUSTER_FAILURE_CHECK_PERIOD = 30000;

   // how long to keep a connection alive in the absence of any data arriving from the client
   private static long DEFAULT_CLUSTER_CONNECTION_TTL = 60000;

   // How long to wait for a reply
   private static long DEFAULT_CLUSTER_CALL_TIMEOUT = 30000;

   // period (in ms) between successive retries
   private static long DEFAULT_CLUSTER_RETRY_INTERVAL = 500;

   // multiplier to apply to the retry-interval
   private static double DEFAULT_CLUSTER_RETRY_INTERVAL_MULTIPLIER = 1;

   // Maximum value for retry-interval
   private static long DEFAULT_CLUSTER_MAX_RETRY_INTERVAL = 2000;

   // How many attempts should be made to connect initially
   private static int DEFAULT_CLUSTER_INITIAL_CONNECT_ATTEMPTS = -1;

   // How many attempts should be made to reconnect after failure
   private static int DEFAULT_CLUSTER_RECONNECT_ATTEMPTS = -1;

   // should duplicate detection headers be inserted in forwarded messages?
   private static boolean DEFAULT_CLUSTER_DUPLICATE_DETECTION = true;

   private static boolean DEFAULT_CLUSTER_FORWARD_WHEN_NO_CONSUMERS = false;

   // how should messages be load balanced?
   private static String DEFAULT_CLUSTER_MESSAGE_LOAD_BALANCING_TYPE = "ON_DEMAND";

   // maximum number of hops cluster topology is propagated
   private static int DEFAULT_CLUSTER_MAX_HOPS = 1;

   // The size (in bytes) of the window used for confirming data from the server connected to.
   private static int DEFAULT_CLUSTER_CONFIRMATION_WINDOW_SIZE = 1048576;

   // How long to wait for a reply if in the middle of a fail-over. -1 means wait forever.
   private static long DEFAULT_CLUSTER_CALL_FAILOVER_TIMEOUT = -1;

   // how often the cluster connection will notify the cluster of its existence right after joining the cluster
   private static long DEFAULT_CLUSTER_NOTIFICATION_INTERVAL = 1000;

   // how many times this cluster connection will notify the cluster of its existence right after joining the cluster
   private static int DEFAULT_CLUSTER_NOTIFICATION_ATTEMPTS = 2;

   // whether this is an exclusive divert
   private static boolean DEFAULT_DIVERT_EXCLUSIVE = false;

   // how the divert should handle the message's routing type
   private static String DEFAULT_DIVERT_ROUTING_TYPE = DivertConfigurationRoutingType.STRIP.toString();

   // If true then the server will request a backup on another node
   private static boolean DEFAULT_HAPOLICY_REQUEST_BACKUP = false;

   // How many times the live server will try to request a backup, -1 means for ever.
   private static int DEFAULT_HAPOLICY_BACKUP_REQUEST_RETRIES = -1;

   // How long to wait for retries between attempts to request a backup server.
   private static long DEFAULT_HAPOLICY_BACKUP_REQUEST_RETRY_INTERVAL = 5000;

   // Whether or not this live server will accept backup requests from other live servers.
   private static int DEFAULT_HAPOLICY_MAX_BACKUPS = 1;

   // The offset to use for the Connectors and Acceptors when creating a new backup server.
   private static int DEFAULT_HAPOLICY_BACKUP_PORT_OFFSET = 100;

   // Whether to check the cluster for a (live) server using our own server ID when starting up. This option is only necessary for performing 'fail-back' on replicating servers. Strictly speaking this setting only applies to live servers and not to backups.
   private static boolean DEFAULT_CHECK_FOR_LIVE_SERVER = false;

   // This specifies how many times a replicated backup server can restart after moving its files on start. Once there are this number of backup journal files the server will stop permanently after if fails back.
   private static int DEFAULT_MAX_SAVED_REPLICATED_JOURNALS_SIZE = 2;

   // Will this server, if a backup, restart once it has been stopped because of failback or scaling down.
   private static boolean DEFAULT_RESTART_BACKUP = true;

   // Whether a server will automatically stop when another places a request to take over its place. The use case is when a regular server stops and its backup takes over its duties, later the main server restarts and requests the server (the former backup) to stop operating.
   private static boolean DEFAULT_ALLOW_AUTO_FAILBACK = true;

   // When a replica comes online this is how long the replicating server will wait for a confirmation from the replica that the replication synchronization process is complete
   private static long DEFAULT_INITIAL_REPLICATION_SYNC_TIMEOUT = 30000;

   // Will this backup server come live on a normal server shutdown
   private static boolean DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN = false;

   // Will the broker populate the message with the name of the validated user
   private static boolean DEFAULT_POPULATE_VALIDATED_USER = false;

   // its possible that you only want a server to partake in scale down as a receiver, via a group. In this case set scale-down to false
   private static boolean DEFAULT_SCALE_DOWN_ENABLED = true;

   // How long to wait for a decision
   private static int DEFAULT_GROUPING_HANDLER_TIMEOUT = 5000;

   // How long a group binding will be used, -1 means for ever. Bindings are removed after this wait elapses. On the remote node this is used to determine how often you should re-query the main coordinator in order to update the last time used accordingly.
   private static int DEFAULT_GROUPING_HANDLER_GROUP_TIMEOUT = -1;

   // How often the reaper will be run to check for timed out group bindings. Only valid for LOCAL handlers
   private static long DEFAULT_GROUPING_HANDLER_REAPER_PERIOD = 30000;

   // Which store type to use, options are FILE or DATABASE, FILE is default.
   private static String DEFAULT_STORE_TYPE = "FILE";

   // Default database url.  Derby database is used by default.
   private static String DEFAULT_DATABASE_URL = null;

   // Default JDBC Driver class name
   private static String DEFAULT_JDBC_DRIVER_CLASS_NAME = null;

   // Default message table name, used with Database storage type
   private static String DEFAULT_MESSAGE_TABLE_NAME = "MESSAGES";

   // Default bindings table name, used with Database storage type
   private static String DEFAULT_BINDINGS_TABLE_NAME = "BINDINGS";

   // Default large messages table name, used with Database storage type
   private static final String DEFAULT_LARGE_MESSAGES_TABLE_NAME = "LARGE_MESSAGES";

   // Default large messages table name, used with Database storage type
   private static final String DEFAULT_PAGE_STORE_TABLE_NAME = "PAGE_STORE";

   // Default period to wait between connection TTL checks
   public static final long DEFAULT_CONNECTION_TTL_CHECK_INTERVAL = 2000;

   // Default period to wait between configuration file checks
   public static final long DEFAULT_CONFIGURATION_FILE_REFRESH_PERIOD = 5000;

   public static final long DEFAULT_GLOBAL_MAX_SIZE = -1;

   public static final int DEFAULT_MAX_DISK_USAGE = 100;

   public static final int DEFAULT_DISK_SCAN = 5000;

   public static final int DEFAULT_MAX_QUEUE_CONSUMERS = -1;

   public static final boolean DEFAULT_PURGE_ON_NO_CONSUMERS = false;

   public static final RoutingType DEFAULT_ROUTING_TYPE = RoutingType.MULTICAST;

   public static final String DEFAULT_SYSTEM_PROPERTY_PREFIX = "brokerconfig.";

   public static String DEFAULT_NETWORK_CHECK_LIST = null;

   public static String DEFAULT_NETWORK_CHECK_URL_LIST = null;

   public static long DEFAULT_NETWORK_CHECK_PERIOD = 5000;

   public static int DEFAULT_NETWORK_CHECK_TIMEOUT = 1000;

   public static String DEFAULT_NETWORK_CHECK_NIC = null;

   public static final String DEFAULT_INTERNAL_NAMING_PREFIX = "$.artemis.internal.";

   public static boolean DEFAULT_VOTE_ON_REPLICATION_FAILURE = false;

   public static int DEFAULT_QUORUM_SIZE = -1;

   /**
    * If true then the ActiveMQ Artemis Server will make use of any Protocol Managers that are in available on the classpath. If false then only the core protocol will be available, unless in Embedded mode where users can inject their own Protocol Managers.
    */
   public static boolean isDefaultResolveProtocols() {
      return DEFAULT_RESOLVE_PROTOCOLS;
   }

   /**
    * true means that the server will load configuration from the configuration files
    */
   public static boolean isDefaultFileDeploymentEnabled() {
      return DEFAULT_FILE_DEPLOYMENT_ENABLED;
   }

   /**
    * true means that the server will use the file based journal for persistence.
    */
   public static boolean isDefaultPersistenceEnabled() {
      return DEFAULT_PERSISTENCE_ENABLED;
   }

   public static boolean isDefaultJournalDatasync() {
      return DEFAULT_JOURNAL_DATASYNC;
   }

   /**
    * Maximum number of threads to use for the scheduled thread pool
    */
   public static int getDefaultScheduledThreadPoolMaxSize() {
      return DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;
   }

   /**
    * Maximum number of threads to use for the thread pool. -1 means 'no limits'.
    */
   public static int getDefaultThreadPoolMaxSize() {
      return DEFAULT_THREAD_POOL_MAX_SIZE;
   }

   /**
    * true means that security is enabled
    */
   public static boolean isDefaultSecurityEnabled() {
      return DEFAULT_SECURITY_ENABLED;
   }

   /**
    * true means that graceful shutdown is enabled
    */
   public static boolean isDefaultGracefulShutdownEnabled() {
      return DEFAULT_GRACEFUL_SHUTDOWN_ENABLED;
   }

   /**
    * true means that graceful shutdown is enabled
    */
   public static long getDefaultGracefulShutdownTimeout() {
      return DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT;
   }

   /**
    * how long (in ms) to wait before invalidating the security cache
    */
   public static long getDefaultSecurityInvalidationInterval() {
      return DEFAULT_SECURITY_INVALIDATION_INTERVAL;
   }

   /**
    * how long (in ms) to wait to acquire a file lock on the journal
    */
   public static long getDefaultJournalLockAcquisitionTimeout() {
      return DEFAULT_JOURNAL_LOCK_ACQUISITION_TIMEOUT;
   }

   /**
    * true means that the server supports wild card routing
    */
   public static boolean isDefaultWildcardRoutingEnabled() {
      return DEFAULT_WILDCARD_ROUTING_ENABLED;
   }

   public static String getDefaultAddressPathSeparator() {
      return DEFAULT_ADDRESS_PATH_SEPARATOR;
   }

   /**
    * the name of the management address to send management messages to. It is prefixed with "jms.queue" so that JMS clients can send messages to it.
    */
   public static SimpleString getDefaultManagementAddress() {
      return DEFAULT_MANAGEMENT_ADDRESS;
   }

   /**
    * the name of the address that consumers bind to receive management notifications
    */
   public static SimpleString getDefaultManagementNotificationAddress() {
      return DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS;
   }

   /**
    * The default Cluster address for the Cluster connection
    */
   public static String getDefaultClusterAddress() {
      return DEFAULT_CLUSTER_ADDRESS;
   }

   /**
    * Cluster username. It applies to all cluster configurations.
    */
   public static String getDefaultClusterUser() {
      return DEFAULT_CLUSTER_USER;
   }

   /**
    * Cluster password. It applies to all cluster configurations.
    */
   public static String getDefaultClusterPassword() {
      return DEFAULT_CLUSTER_PASSWORD;
   }

   /**
    * This option controls whether passwords in server configuration need be masked. If set to "true" the passwords are masked.
    */
   public static boolean isDefaultMaskPassword() {
      return DEFAULT_MASK_PASSWORD;
   }

   /**
    * true means that the management API is available via JMX
    */
   public static boolean isDefaultJmxManagementEnabled() {
      return DEFAULT_JMX_MANAGEMENT_ENABLED;
   }

   /**
    * the JMX domain used to registered ActiveMQ Artemis MBeans in the MBeanServer
    */
   public static String getDefaultJmxDomain() {
      return DEFAULT_JMX_DOMAIN;
   }

   public static boolean isDefaultJMXUseBrokerName() {
      return DEFAULT_JMX_IS_USE_BROKER_NAME;
   }

   /**
    * true means that message counters are enabled
    */
   public static boolean isDefaultMessageCounterEnabled() {
      return DEFAULT_MESSAGE_COUNTER_ENABLED;
   }

   /**
    * the sample period (in ms) to use for message counters
    */
   public static long getDefaultMessageCounterSamplePeriod() {
      return DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD;
   }

   /**
    * how many days to keep message counter history
    */
   public static int getDefaultMessageCounterMaxDayHistory() {
      return DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY;
   }

   /**
    * if set, this will override how long (in ms) to keep a connection alive without receiving a ping. -1 disables this setting.
    */
   public static long getDefaultConnectionTtlOverride() {
      return DEFAULT_CONNECTION_TTL_OVERRIDE;
   }

   /**
    * should certain incoming packets on the server be handed off to a thread from the thread pool for processing or should they be handled on the remoting thread?
    */
   public static boolean isDefaultAsyncConnectionExecutionEnabled() {
      return DEFAULT_ASYNC_CONNECTION_EXECUTION_ENABLED;
   }

   /**
    * how long (in ms) before a transaction can be removed from the resource manager after create time
    */
   public static long getDefaultTransactionTimeout() {
      return DEFAULT_TRANSACTION_TIMEOUT;
   }

   /**
    * how often (in ms) to scan for timeout transactions
    */
   public static long getDefaultTransactionTimeoutScanPeriod() {
      return DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD;
   }

   /**
    * how often (in ms) to scan for expired messages
    */
   public static long getDefaultMessageExpiryScanPeriod() {
      return DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD;
   }

   /**
    * the priority of the thread expiring messages
    */
   public static int getDefaultMessageExpiryThreadPriority() {
      return DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY;
   }

   /**
    * the size of the cache for pre-creating message ID's
    */
   public static int getDefaultIdCacheSize() {
      return DEFAULT_ID_CACHE_SIZE;
   }

   /**
    * true means that ID's are persisted to the journal
    */
   public static boolean isDefaultPersistIdCache() {
      return DEFAULT_PERSIST_ID_CACHE;
   }

   /**
    * True means that the delivery count is persisted before delivery. False means that this only happens after a message has been cancelled.
    */
   public static boolean isDefaultPersistDeliveryCountBeforeDelivery() {
      return DEFAULT_PERSIST_DELIVERY_COUNT_BEFORE_DELIVERY;
   }

   /**
    * the directory to store paged messages in
    */
   public static String getDefaultPagingDir() {
      return DEFAULT_PAGING_DIR;
   }

   /**
    * the directory to store the persisted bindings to
    */
   public static String getDefaultBindingsDirectory() {
      return DEFAULT_BINDINGS_DIRECTORY;
   }

   /**
    * true means that the server will create the bindings directory on start up
    */
   public static boolean isDefaultCreateBindingsDir() {
      return DEFAULT_CREATE_BINDINGS_DIR;
   }

   /**
    * The max number of concurrent reads allowed on paging
    */
   public static int getDefaultMaxConcurrentPageIo() {
      return DEFAULT_MAX_CONCURRENT_PAGE_IO;
   }

   /**
    * the directory to store the journal files in
    */
   public static String getDefaultJournalDir() {
      return DEFAULT_JOURNAL_DIR;
   }

   /**
    * true means that the journal directory will be created
    */
   public static boolean isDefaultCreateJournalDir() {
      return DEFAULT_CREATE_JOURNAL_DIR;
   }

   /**
    * if true wait for transaction data to be synchronized to the journal before returning response to client
    */
   public static boolean isDefaultJournalSyncTransactional() {
      return DEFAULT_JOURNAL_SYNC_TRANSACTIONAL;
   }

   /**
    * if true wait for non transaction data to be synced to the journal before returning response to client.
    */
   public static boolean isDefaultJournalSyncNonTransactional() {
      return DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL;
   }

   /**
    * Whether to log messages about the journal write rate
    */
   public static boolean isDefaultJournalLogWriteRate() {
      return DEFAULT_JOURNAL_LOG_WRITE_RATE;
   }

   /**
    * the size (in bytes) of each journal file
    */
   public static int getDefaultJournalFileSize() {
      return DEFAULT_JOURNAL_FILE_SIZE;
   }

   /**
    * how many journal files to pre-create
    */
   public static int getDefaultJournalMinFiles() {
      return DEFAULT_JOURNAL_MIN_FILES;
   }

   /**
    * How many journal files can be resued
    *
    * @return
    */
   public static int getDefaultJournalPoolFiles() {
      return DEFAULT_JOURNAL_POOL_FILES;
   }

   /**
    * The percentage of live data on which we consider compacting the journal
    */
   public static int getDefaultJournalCompactPercentage() {
      return DEFAULT_JOURNAL_COMPACT_PERCENTAGE;
   }

   /**
    * The minimal number of data files before we can start compacting
    */
   public static int getDefaultJournalCompactMinFiles() {
      return DEFAULT_JOURNAL_COMPACT_MIN_FILES;
   }

   /**
    * Interval to log server specific information (e.g. memory usage etc)
    */
   public static long getDefaultServerDumpInterval() {
      return DEFAULT_SERVER_DUMP_INTERVAL;
   }

   /**
    * Percentage of available memory which will trigger a warning log
    */
   public static int getDefaultMemoryWarningThreshold() {
      return DEFAULT_MEMORY_WARNING_THRESHOLD;
   }

   /**
    * frequency to sample JVM memory in ms (or -1 to disable memory sampling)
    */
   public static long getDefaultMemoryMeasureInterval() {
      return DEFAULT_MEMORY_MEASURE_INTERVAL;
   }

   /**
    * the directory to store large messages
    */
   public static String getDefaultLargeMessagesDir() {
      return DEFAULT_LARGE_MESSAGES_DIR;
   }

   /**
    * period in milliseconds between consecutive broadcasts
    */
   public static long getDefaultBroadcastPeriod() {
      return DEFAULT_BROADCAST_PERIOD;
   }

   /**
    * Period the discovery group waits after receiving the last broadcast from a particular server before removing that servers connector pair entry from its list.
    */
   public static int getDefaultBroadcastRefreshTimeout() {
      return DEFAULT_BROADCAST_REFRESH_TIMEOUT;
   }

   /**
    * how long to keep a connection alive in the absence of any data arriving from the client. This should be greater than the ping period.
    */
   public static long getDefaultConnectionTtl() {
      return DEFAULT_CONNECTION_TTL;
   }

   /**
    * multiplier to apply to successive retry intervals
    */
   public static double getDefaultRetryIntervalMultiplier() {
      return DEFAULT_RETRY_INTERVAL_MULTIPLIER;
   }

   /**
    * Limit to the retry-interval growth (due to retry-interval-multiplier)
    */
   public static long getDefaultMaxRetryInterval() {
      return DEFAULT_MAX_RETRY_INTERVAL;
   }

   /**
    * maximum number of initial connection attempts, -1 means 'no limits'
    */
   public static int getDefaultBridgeInitialConnectAttempts() {
      return DEFAULT_BRIDGE_INITIAL_CONNECT_ATTEMPTS;
   }

   /**
    * maximum number of retry attempts, -1 means 'no limits'
    */
   public static int getDefaultBridgeReconnectAttempts() {
      return DEFAULT_BRIDGE_RECONNECT_ATTEMPTS;
   }

   /**
    * should duplicate detection headers be inserted in forwarded messages?
    */
   public static boolean isDefaultBridgeDuplicateDetection() {
      return DEFAULT_BRIDGE_DUPLICATE_DETECTION;
   }

   /**
    * Once the bridge has received this many bytes, it sends a confirmation
    */
   public static int getDefaultBridgeConfirmationWindowSize() {
      return DEFAULT_BRIDGE_CONFIRMATION_WINDOW_SIZE;
   }

   /**
    * Producer flow control
    */
   public static int getDefaultBridgeProducerWindowSize() {
      return DEFAULT_BRIDGE_PRODUCER_WINDOW_SIZE;
   }

   /**
    * Upon reconnection this configures the number of time the same node on the topology will be retried before reseting the server locator and using the initial connectors
    */
   public static int getDefaultBridgeConnectSameNode() {
      return DEFAULT_BRIDGE_CONNECT_SAME_NODE;
   }

   /**
    * The period (in milliseconds) used to check if the cluster connection has failed to receive pings from another server
    */
   public static long getDefaultClusterFailureCheckPeriod() {
      return DEFAULT_CLUSTER_FAILURE_CHECK_PERIOD;
   }

   /**
    * how long to keep a connection alive in the absence of any data arriving from the client
    */
   public static long getDefaultClusterConnectionTtl() {
      return DEFAULT_CLUSTER_CONNECTION_TTL;
   }

   /**
    * How long to wait for a reply
    */
   public static long getDefaultClusterCallTimeout() {
      return DEFAULT_CLUSTER_CALL_TIMEOUT;
   }

   /**
    * period (in ms) between successive retries
    */
   public static long getDefaultClusterRetryInterval() {
      return DEFAULT_CLUSTER_RETRY_INTERVAL;
   }

   /**
    * multiplier to apply to the retry-interval
    */
   public static double getDefaultClusterRetryIntervalMultiplier() {
      return DEFAULT_CLUSTER_RETRY_INTERVAL_MULTIPLIER;
   }

   /**
    * Maximum value for retry-interval
    */
   public static long getDefaultClusterMaxRetryInterval() {
      return DEFAULT_CLUSTER_MAX_RETRY_INTERVAL;
   }

   /**
    * How many attempts should be made to connect initially
    */
   public static int getDefaultClusterInitialConnectAttempts() {
      return DEFAULT_CLUSTER_INITIAL_CONNECT_ATTEMPTS;
   }

   /**
    * How many attempts should be made to reconnect after failure
    */
   public static int getDefaultClusterReconnectAttempts() {
      return DEFAULT_CLUSTER_RECONNECT_ATTEMPTS;
   }

   /**
    * should duplicate detection headers be inserted in forwarded messages?
    */
   public static boolean isDefaultClusterDuplicateDetection() {
      return DEFAULT_CLUSTER_DUPLICATE_DETECTION;
   }

   public static boolean isDefaultClusterForwardWhenNoConsumers() {
      return DEFAULT_CLUSTER_FORWARD_WHEN_NO_CONSUMERS;
   }

   /**
    * should messages be load balanced if there are no matching consumers on target?
    */
   public static String getDefaultClusterMessageLoadBalancingType() {
      return DEFAULT_CLUSTER_MESSAGE_LOAD_BALANCING_TYPE;
   }

   /**
    * maximum number of hops cluster topology is propagated
    */
   public static int getDefaultClusterMaxHops() {
      return DEFAULT_CLUSTER_MAX_HOPS;
   }

   /**
    * The size (in bytes) of the window used for confirming data from the server connected to.
    */
   public static int getDefaultClusterConfirmationWindowSize() {
      return DEFAULT_CLUSTER_CONFIRMATION_WINDOW_SIZE;
   }

   /**
    * How long to wait for a reply if in the middle of a fail-over. -1 means wait forever.
    */
   public static long getDefaultClusterCallFailoverTimeout() {
      return DEFAULT_CLUSTER_CALL_FAILOVER_TIMEOUT;
   }

   /**
    * how often the cluster connection will notify the cluster of its existence right after joining the cluster
    */
   public static long getDefaultClusterNotificationInterval() {
      return DEFAULT_CLUSTER_NOTIFICATION_INTERVAL;
   }

   /**
    * how many times this cluster connection will notify the cluster of its existence right after joining the cluster
    */
   public static int getDefaultClusterNotificationAttempts() {
      return DEFAULT_CLUSTER_NOTIFICATION_ATTEMPTS;
   }

   /**
    * whether this is an exclusive divert
    */
   public static boolean isDefaultDivertExclusive() {
      return DEFAULT_DIVERT_EXCLUSIVE;
   }

   /**
    * how the divert should handle the message's routing type
    */
   public static String getDefaultDivertRoutingType() {
      return DEFAULT_DIVERT_ROUTING_TYPE;
   }

   /**
    * If true then the server will request a backup on another node
    */
   public static boolean isDefaultHapolicyRequestBackup() {
      return DEFAULT_HAPOLICY_REQUEST_BACKUP;
   }

   /**
    * How many times the live server will try to request a backup, -1 means for ever.
    */
   public static int getDefaultHapolicyBackupRequestRetries() {
      return DEFAULT_HAPOLICY_BACKUP_REQUEST_RETRIES;
   }

   /**
    * How long to wait for retries between attempts to request a backup server.
    */
   public static long getDefaultHapolicyBackupRequestRetryInterval() {
      return DEFAULT_HAPOLICY_BACKUP_REQUEST_RETRY_INTERVAL;
   }

   /**
    * Whether or not this live server will accept backup requests from other live servers.
    */
   public static int getDefaultHapolicyMaxBackups() {
      return DEFAULT_HAPOLICY_MAX_BACKUPS;
   }

   /**
    * The offset to use for the Connectors and Acceptors when creating a new backup server.
    */
   public static int getDefaultHapolicyBackupPortOffset() {
      return DEFAULT_HAPOLICY_BACKUP_PORT_OFFSET;
   }

   /**
    * Whether to check the cluster for a (live) server using our own server ID when starting up. This option is only necessary for performing 'fail-back' on replicating servers. Strictly speaking this setting only applies to live servers and not to backups.
    */
   public static boolean isDefaultCheckForLiveServer() {
      return DEFAULT_CHECK_FOR_LIVE_SERVER;
   }

   /**
    * This specifies how many times a replicated backup server can restart after moving its files on start. Once there are this number of backup journal files the server will stop permanently after if fails back.
    */
   public static int getDefaultMaxSavedReplicatedJournalsSize() {
      return DEFAULT_MAX_SAVED_REPLICATED_JOURNALS_SIZE;
   }

   /**
    * Will this server, if a backup, restart once it has been stopped because of failback or scaling down.
    */
   public static boolean isDefaultRestartBackup() {
      return DEFAULT_RESTART_BACKUP;
   }

   /**
    * Whether a server will automatically stop when another places a request to take over its place. The use case is when a regular server stops and its backup takes over its duties, later the main server restarts and requests the server (the former backup) to stop operating.
    */
   public static boolean isDefaultAllowAutoFailback() {
      return DEFAULT_ALLOW_AUTO_FAILBACK;
   }

   /**
    * if we have to start as a replicated server this is the delay to wait before fail-back occurs
    */
   public static long getDefaultInitialReplicationSyncTimeout() {
      return DEFAULT_INITIAL_REPLICATION_SYNC_TIMEOUT;
   }

   /**
    * if we have to start as a replicated server this is the delay to wait before fail-back occurs
    *
    * @deprecated use getDefaultInitialReplicationSyncTimeout()
    */
   @Deprecated
   public static long getDefaultFailbackDelay() {
      return 5000;
   }

   /**
    * Will this backup server come live on a normal server shutdown
    */
   public static boolean isDefaultFailoverOnServerShutdown() {
      return DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;
   }

   /**
    * Will the broker populate the message with the name of the validated user
    */
   public static boolean isDefaultPopulateValidatedUser() {
      return DEFAULT_POPULATE_VALIDATED_USER;
   }

   /**
    * its possible that you only want a server to partake in scale down as a receiver, via a group. In this case set scale-down to false
    */
   public static boolean isDefaultScaleDownEnabled() {
      return DEFAULT_SCALE_DOWN_ENABLED;
   }

   /**
    * How long to wait for a decision
    */
   public static int getDefaultGroupingHandlerTimeout() {
      return DEFAULT_GROUPING_HANDLER_TIMEOUT;
   }

   /**
    * How long a group binding will be used, -1 means for ever. Bindings are removed after this wait elapses. On the remote node this is used to determine how often you should re-query the main coordinator in order to update the last time used accordingly.
    */
   public static int getDefaultGroupingHandlerGroupTimeout() {
      return DEFAULT_GROUPING_HANDLER_GROUP_TIMEOUT;
   }

   /**
    * How often the reaper will be run to check for timed out group bindings. Only valid for LOCAL handlers
    */
   public static long getDefaultGroupingHandlerReaperPeriod() {
      return DEFAULT_GROUPING_HANDLER_REAPER_PERIOD;
   }

   /**
    * The default storage type.  Options are FILE and DATABASE.
    */
   public static String getDefaultStoreType() {
      return DEFAULT_STORE_TYPE;
   }

   /**
    * The default database URL, used with DATABASE store type.
    */
   public static String getDefaultDatabaseUrl() {
      return DEFAULT_DATABASE_URL;
   }

   /**
    * The default Message Journal table name, used with DATABASE store.
    */
   public static String getDefaultMessageTableName() {
      return DEFAULT_MESSAGE_TABLE_NAME;
   }

   public static String getDefaultBindingsTableName() {
      return DEFAULT_BINDINGS_TABLE_NAME;
   }

   public static String getDefaultDriverClassName() {
      return DEFAULT_JDBC_DRIVER_CLASS_NAME;
   }

   public static String getDefaultLargeMessagesTableName() {
      return DEFAULT_LARGE_MESSAGES_TABLE_NAME;
   }

   public static String getDefaultPageStoreTableName() {
      return DEFAULT_PAGE_STORE_TABLE_NAME;
   }

   public static long getDefaultConnectionTtlCheckInterval() {
      return DEFAULT_CONNECTION_TTL_CHECK_INTERVAL;
   }

   public static long getDefaultConfigurationFileRefreshPeriod() {
      return DEFAULT_CONFIGURATION_FILE_REFRESH_PERIOD;
   }

   /**
    * The default global max size. -1 = no global max size.
    */
   public static long getDefaultMaxGlobalSize() {
      return DEFAULT_GLOBAL_MAX_SIZE;
   }

   public static int getDefaultMaxDiskUsage() {
      return DEFAULT_MAX_DISK_USAGE;
   }

   public static int getDefaultDiskScanPeriod() {
      return DEFAULT_DISK_SCAN;
   }

   public static int getDefaultMaxQueueConsumers() {
      return DEFAULT_MAX_QUEUE_CONSUMERS;
   }

   public static boolean getDefaultPurgeOnNoConsumers() {
      return DEFAULT_PURGE_ON_NO_CONSUMERS;
   }

   public static String getInternalNamingPrefix() {
      return DEFAULT_INTERNAL_NAMING_PREFIX;
   }

   public static RoutingType getDefaultRoutingType() {
      return DEFAULT_ROUTING_TYPE;
   }

   public static String getDefaultSystemPropertyPrefix() {
      return DEFAULT_SYSTEM_PROPERTY_PREFIX;
   }

   public static String getDefaultNetworkCheckList() {
      return DEFAULT_NETWORK_CHECK_LIST;
   }

   public static String getDefaultNetworkCheckURLList() {
      return DEFAULT_NETWORK_CHECK_URL_LIST;
   }

   public static long getDefaultNetworkCheckPeriod() {
      return DEFAULT_NETWORK_CHECK_PERIOD;
   }

   public static int getDefaultNetworkCheckTimeout() {
      return DEFAULT_NETWORK_CHECK_TIMEOUT;
   }

   public static String getDefaultNetworkCheckNic() {
      return DEFAULT_NETWORK_CHECK_NIC;
   }

   public static boolean getDefaultVoteOnReplicationFailure() {
      return DEFAULT_VOTE_ON_REPLICATION_FAILURE;
   }

   public static int getDefaultQuorumSize() {
      return DEFAULT_QUORUM_SIZE;
   }
}

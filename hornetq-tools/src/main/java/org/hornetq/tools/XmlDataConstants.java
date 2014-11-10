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

package org.hornetq.tools;

/**
 * The constants shared by <code>org.hornetq.tools.XmlDataImporter</code> and
 * <code>org.hornetq.tools.XmlDataExporter</code>.
 *
 * @author Justin Bertram
 */
public final class XmlDataConstants
{
   private XmlDataConstants()
   {
      // Utility
   }
   static final String XML_VERSION = "1.0";
   static final String DOCUMENT_PARENT = "hornetq-journal";
   static final String BINDINGS_PARENT = "bindings";
   static final String BINDINGS_CHILD = "binding";
   static final String BINDING_ADDRESS = "address";
   static final String BINDING_FILTER_STRING = "filter-string";
   static final String BINDING_QUEUE_NAME = "queue-name";
   static final String BINDING_ID = "id";
   static final String JMS_CONNECTION_FACTORY = "jms-connection-factory";
   static final String JMS_CONNECTION_FACTORIES = "jms-connection-factories";
   static final String MESSAGES_PARENT = "messages";
   static final String MESSAGES_CHILD = "message";
   static final String MESSAGE_ID = "id";
   static final String MESSAGE_PRIORITY = "priority";
   static final String MESSAGE_EXPIRATION = "expiration";
   static final String MESSAGE_TIMESTAMP = "timestamp";
   static final String DEFAULT_TYPE_PRETTY = "default";
   static final String BYTES_TYPE_PRETTY = "bytes";
   static final String MAP_TYPE_PRETTY = "map";
   static final String OBJECT_TYPE_PRETTY = "object";
   static final String STREAM_TYPE_PRETTY = "stream";
   static final String TEXT_TYPE_PRETTY = "text";
   static final String MESSAGE_TYPE = "type";
   static final String MESSAGE_IS_LARGE = "isLarge";
   static final String MESSAGE_USER_ID = "user-id";
   static final String MESSAGE_BODY = "body";
   static final String PROPERTIES_PARENT = "properties";
   static final String PROPERTIES_CHILD = "property";
   static final String PROPERTY_NAME = "name";
   static final String PROPERTY_VALUE = "value";
   static final String PROPERTY_TYPE = "type";
   static final String QUEUES_PARENT = "queues";
   static final String QUEUES_CHILD = "queue";
   static final String QUEUE_NAME = "name";
   static final String PROPERTY_TYPE_BOOLEAN = "boolean";
   static final String PROPERTY_TYPE_BYTE = "byte";
   static final String PROPERTY_TYPE_BYTES = "bytes";
   static final String PROPERTY_TYPE_SHORT = "short";
   static final String PROPERTY_TYPE_INTEGER = "integer";
   static final String PROPERTY_TYPE_LONG = "long";
   static final String PROPERTY_TYPE_FLOAT = "float";
   static final String PROPERTY_TYPE_DOUBLE = "double";
   static final String PROPERTY_TYPE_STRING = "string";
   static final String PROPERTY_TYPE_SIMPLE_STRING = "simple-string";

   static final String JMS_CONNECTION_FACTORY_NAME = "name";
   static final String JMS_CONNECTION_FACTORY_CLIENT_ID = "client-id";
   static final String JMS_CONNECTION_FACTORY_CALL_FAILOVER_TIMEOUT = "call-failover-timeout";
   static final String JMS_CONNECTION_FACTORY_CALL_TIMEOUT = "call-timeout";
   static final String JMS_CONNECTION_FACTORY_CLIENT_FAILURE_CHECK_PERIOD = "client-failure-check-period";
   static final String JMS_CONNECTION_FACTORY_CONFIRMATION_WINDOW_SIZE = "confirmation-window-size";
   static final String JMS_CONNECTION_FACTORY_CONNECTION_TTL = "connection-ttl";
   static final String JMS_CONNECTION_FACTORY_CONSUMER_MAX_RATE = "consumer-max-rate";
   static final String JMS_CONNECTION_FACTORY_CONSUMER_WINDOW_SIZE = "consumer-window-size";
   static final String JMS_CONNECTION_FACTORY_DISCOVERY_GROUP_NAME = "discovery-group-name";
   static final String JMS_CONNECTION_FACTORY_DUPS_OK_BATCH_SIZE = "dups-ok-batch-size";
   static final String JMS_CONNECTION_FACTORY_TYPE = "type";
   static final String JMS_CONNECTION_FACTORY_GROUP_ID = "group-id";
   static final String JMS_CONNECTION_FACTORY_LOAD_BALANCING_POLICY_CLASS_NAME = "load-balancing-policy-class-name";
   static final String JMS_CONNECTION_FACTORY_MAX_RETRY_INTERVAL = "max-retry-interval";
   static final String JMS_CONNECTION_FACTORY_MIN_LARGE_MESSAGE_SIZE = "min-large-message-size";
   static final String JMS_CONNECTION_FACTORY_PRODUCER_MAX_RATE = "producer-max-rate";
   static final String JMS_CONNECTION_FACTORY_PRODUCER_WINDOW_SIZE = "producer-window-size";
   static final String JMS_CONNECTION_FACTORY_RECONNECT_ATTEMPTS = "reconnect-attempts";
   static final String JMS_CONNECTION_FACTORY_RETRY_INTERVAL = "retry-interval";
   static final String JMS_CONNECTION_FACTORY_RETRY_INTERVAL_MULTIPLIER = "retry-interval-multiplier";
   static final String JMS_CONNECTION_FACTORY_SCHEDULED_THREAD_POOL_MAX_SIZE = "scheduled-thread-pool-max-size";
   static final String JMS_CONNECTION_FACTORY_THREAD_POOL_MAX_SIZE = "thread-pool-max-size";
   static final String JMS_CONNECTION_FACTORY_TRANSACTION_BATCH_SIZE = "transaction-batch-size";
   static final String JMS_CONNECTION_FACTORY_CONNECTORS = "connectors";
   static final String JMS_CONNECTION_FACTORY_CONNECTOR = "connector";
   static final String JMS_CONNECTION_FACTORY_AUTO_GROUP = "auto-group";
   static final String JMS_CONNECTION_FACTORY_BLOCK_ON_ACKNOWLEDGE = "block-on-acknowledge";
   static final String JMS_CONNECTION_FACTORY_BLOCK_ON_DURABLE_SEND = "block-on-durable-send";
   static final String JMS_CONNECTION_FACTORY_BLOCK_ON_NON_DURABLE_SEND = "block-on-non-durable-send";
   static final String JMS_CONNECTION_FACTORY_CACHE_LARGE_MESSAGES_CLIENT = "cache-large-messages-client";
   static final String JMS_CONNECTION_FACTORY_COMPRESS_LARGE_MESSAGES = "compress-large-messages";
   static final String JMS_CONNECTION_FACTORY_FAILOVER_ON_INITIAL_CONNECTION = "failover-on-initial-connection";
   static final String JMS_CONNECTION_FACTORY_HA = "ha";
   static final String JMS_CONNECTION_FACTORY_PREACKNOWLEDGE = "preacknowledge";
   static final String JMS_CONNECTION_FACTORY_USE_GLOBAL_POOLS = "use-global-pools";

   static final String JMS_DESTINATIONS = "jms-destinations";
   static final String JMS_DESTINATION = "jms-destination";
   static final String JMS_DESTINATION_NAME = "name";
   static final String JMS_DESTINATION_SELECTOR = "selector";
   static final String JMS_DESTINATION_TYPE = "type";

   static final String JMS_JNDI_ENTRIES = "entries";
   static final String JMS_JNDI_ENTRY = "entry";

   public static final String JNDI_COMPATIBILITY_PREFIX = "java:jboss/exported/";

   static final String NULL = "_HQ_NULL";
}
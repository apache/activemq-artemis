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
package org.apache.activemq.artemis.core.deployers.impl;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.ArtemisConstants;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.BroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.JGroupsFileBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ConfigurationUtils;
import org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.config.ScaleDownConfiguration;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.config.ha.ColocatedPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.LiveOnlyPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.config.impl.Validators;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.config.storage.FileStorageConfiguration;
import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.DivertConfigurationRoutingType;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.SecuritySettingPlugin;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.group.impl.GroupingHandlerConfiguration;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.DeletionPolicy;
import org.apache.activemq.artemis.core.settings.impl.ResourceLimitSettings;
import org.apache.activemq.artemis.core.settings.impl.SlowConsumerPolicy;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.ClassloadingUtil;
import org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec;
import org.apache.activemq.artemis.utils.PasswordMaskingUtil;
import org.apache.activemq.artemis.utils.XMLConfigurationUtil;
import org.apache.activemq.artemis.utils.XMLUtil;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzerPolicy;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.transform.dom.DOMSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

/**
 * Parses an XML document according to the {@literal artemis-configuration.xsd} schema.
 */
public final class FileConfigurationParser extends XMLConfigurationUtil {

   // Security Parsing
   public static final String SECURITY_ELEMENT_NAME = "security-setting";

   public static final String SECURITY_PLUGIN_ELEMENT_NAME = "security-setting-plugin";

   public static final String SECURITY_ROLE_MAPPING_NAME = "role-mapping";

   public static final String BROKER_PLUGINS_ELEMENT_NAME = "broker-plugins";

   public static final String BROKER_PLUGIN_ELEMENT_NAME = "broker-plugin";

   private static final String PERMISSION_ELEMENT_NAME = "permission";

   private static final String SETTING_ELEMENT_NAME = "setting";

   private static final String TYPE_ATTR_NAME = "type";

   private static final String ROLES_ATTR_NAME = "roles";

   private static final String NAME_ATTR_NAME = "name";

   private static final String VALUE_ATTR_NAME = "value";

   private static final String ROLE_FROM_ATTR_NAME = "from";

   private static final String ROLE_TO_ATTR_NAME = "to";

   static final String CREATEDURABLEQUEUE_NAME = "createDurableQueue";

   private static final String DELETEDURABLEQUEUE_NAME = "deleteDurableQueue";

   private static final String CREATE_NON_DURABLE_QUEUE_NAME = "createNonDurableQueue";

   private static final String DELETE_NON_DURABLE_QUEUE_NAME = "deleteNonDurableQueue";

   // HORNETQ-309 we keep supporting these attribute names for compatibility
   private static final String CREATETEMPQUEUE_NAME = "createTempQueue";

   private static final String DELETETEMPQUEUE_NAME = "deleteTempQueue";

   private static final String SEND_NAME = "send";

   private static final String CONSUME_NAME = "consume";

   private static final String MANAGE_NAME = "manage";

   private static final String BROWSE_NAME = "browse";

   private static final String CREATEADDRESS_NAME = "createAddress";

   private static final String DELETEADDRESS_NAME = "deleteAddress";

   // Address parsing

   private static final String DEAD_LETTER_ADDRESS_NODE_NAME = "dead-letter-address";

   private static final String EXPIRY_ADDRESS_NODE_NAME = "expiry-address";

   private static final String EXPIRY_DELAY_NODE_NAME = "expiry-delay";

   private static final String REDELIVERY_DELAY_NODE_NAME = "redelivery-delay";

   private static final String REDELIVERY_DELAY_MULTIPLIER_NODE_NAME = "redelivery-delay-multiplier";

   private static final String MAX_REDELIVERY_DELAY_NODE_NAME = "max-redelivery-delay";

   private static final String MAX_DELIVERY_ATTEMPTS = "max-delivery-attempts";

   private static final String MAX_SIZE_BYTES_NODE_NAME = "max-size-bytes";

   private static final String ADDRESS_FULL_MESSAGE_POLICY_NODE_NAME = "address-full-policy";

   private static final String PAGE_SIZE_BYTES_NODE_NAME = "page-size-bytes";

   private static final String PAGE_MAX_CACHE_SIZE_NODE_NAME = "page-max-cache-size";

   private static final String MESSAGE_COUNTER_HISTORY_DAY_LIMIT_NODE_NAME = "message-counter-history-day-limit";

   private static final String LVQ_NODE_NAME = "last-value-queue";

   private static final String DEFAULT_LVQ_NODE_NAME = "default-last-value-queue";

   private static final String DEFAULT_EXCLUSIVE_NODE_NAME = "default-exclusive-queue";

   private static final String REDISTRIBUTION_DELAY_NODE_NAME = "redistribution-delay";

   private static final String SEND_TO_DLA_ON_NO_ROUTE = "send-to-dla-on-no-route";

   private static final String SLOW_CONSUMER_THRESHOLD_NODE_NAME = "slow-consumer-threshold";

   private static final String SLOW_CONSUMER_CHECK_PERIOD_NODE_NAME = "slow-consumer-check-period";

   private static final String SLOW_CONSUMER_POLICY_NODE_NAME = "slow-consumer-policy";

   private static final String AUTO_CREATE_JMS_QUEUES = "auto-create-jms-queues";

   private static final String AUTO_DELETE_JMS_QUEUES = "auto-delete-jms-queues";

   private static final String AUTO_CREATE_JMS_TOPICS = "auto-create-jms-topics";

   private static final String AUTO_DELETE_JMS_TOPICS = "auto-delete-jms-topics";

   private static final String AUTO_CREATE_QUEUES = "auto-create-queues";

   private static final String AUTO_DELETE_QUEUES = "auto-delete-queues";

   private static final String CONFIG_DELETE_QUEUES = "config-delete-queues";

   private static final String AUTO_CREATE_ADDRESSES = "auto-create-addresses";

   private static final String AUTO_DELETE_ADDRESSES = "auto-delete-addresses";

   private static final String CONFIG_DELETE_ADDRESSES = "config-delete-addresses";

   private static final String DEFAULT_PURGE_ON_NO_CONSUMERS = "default-purge-on-no-consumers";

   private static final String DEFAULT_MAX_CONSUMERS = "default-max-consumers";

   private static final String DEFAULT_QUEUE_ROUTING_TYPE = "default-queue-routing-type";

   private static final String DEFAULT_ADDRESS_ROUTING_TYPE = "default-address-routing-type";

   private static final String MANAGEMENT_BROWSE_PAGE_SIZE = "management-browse-page-size";

   private static final String MAX_CONNECTIONS_NODE_NAME = "max-connections";

   private static final String MAX_QUEUES_NODE_NAME = "max-queues";

   private static final String GLOBAL_MAX_SIZE = "global-max-size";

   private static final String MAX_DISK_USAGE = "max-disk-usage";

   private static final String DISK_SCAN_PERIOD = "disk-scan-period";

   private static final String INTERNAL_NAMING_PREFIX = "internal-naming-prefix";

   private static final String AMQP_USE_CORE_SUBSCRIPTION_NAMING = "amqp-use-core-subscription-naming";


   // Attributes ----------------------------------------------------

   private boolean validateAIO = false;

   /**
    * @return the validateAIO
    */
   public boolean isValidateAIO() {
      return validateAIO;
   }

   /**
    * @param validateAIO the validateAIO to set
    */
   public void setValidateAIO(final boolean validateAIO) {
      this.validateAIO = validateAIO;
   }

   public Configuration parseMainConfig(final InputStream input) throws Exception {
      Reader reader = new InputStreamReader(input);
      String xml = XMLUtil.readerToString(reader);
      xml = XMLUtil.replaceSystemProps(xml);
      Element e = XMLUtil.stringToElement(xml);
      SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      Schema schema = schemaFactory.newSchema(XMLUtil.findResource("schema/artemis-server.xsd"));
      Validator validator = schema.newValidator();
      try {
         validator.validate(new DOMSource(e));
      } catch (Exception ex) {
         ActiveMQServerLogger.LOGGER.error(ex.getMessage());
      }
      Configuration config = new ConfigurationImpl();
      parseMainConfig(e, config);

      return config;
   }

   public void parseMainConfig(final Element e, final Configuration config) throws Exception {

      config.setName(getString(e, "name", config.getName(), Validators.NO_CHECK));

      config.setSystemPropertyPrefix(getString(e, "system-property-prefix", config.getSystemPropertyPrefix(), Validators.NOT_NULL_OR_EMPTY));

      NodeList haPolicyNodes = e.getElementsByTagName("ha-policy");

      if (haPolicyNodes.getLength() > 0) {
         parseHAPolicyConfiguration((Element) haPolicyNodes.item(0), config);
      }

      //if we aren already set then set to default
      if (config.getHAPolicyConfiguration() == null) {
         config.setHAPolicyConfiguration(new LiveOnlyPolicyConfiguration());
      }

      NodeList storeTypeNodes = e.getElementsByTagName("store");

      if (storeTypeNodes.getLength() > 0) {
         parseStoreConfiguration((Element) storeTypeNodes.item(0), config);
      }

      config.setResolveProtocols(getBoolean(e, "resolve-protocols", config.isResolveProtocols()));

      config.setPersistenceEnabled(getBoolean(e, "persistence-enabled", config.isPersistenceEnabled()));

      config.setPersistDeliveryCountBeforeDelivery(getBoolean(e, "persist-delivery-count-before-delivery", config.isPersistDeliveryCountBeforeDelivery()));

      config.setScheduledThreadPoolMaxSize(getInteger(e, "scheduled-thread-pool-max-size", config.getScheduledThreadPoolMaxSize(), Validators.GT_ZERO));

      config.setThreadPoolMaxSize(getInteger(e, "thread-pool-max-size", config.getThreadPoolMaxSize(), Validators.MINUS_ONE_OR_GT_ZERO));

      config.setSecurityEnabled(getBoolean(e, "security-enabled", config.isSecurityEnabled()));

      config.setGracefulShutdownEnabled(getBoolean(e, "graceful-shutdown-enabled", config.isGracefulShutdownEnabled()));

      config.setGracefulShutdownTimeout(getLong(e, "graceful-shutdown-timeout", config.getGracefulShutdownTimeout(), Validators.MINUS_ONE_OR_GE_ZERO));

      config.setJMXManagementEnabled(getBoolean(e, "jmx-management-enabled", config.isJMXManagementEnabled()));

      config.setJMXDomain(getString(e, "jmx-domain", config.getJMXDomain(), Validators.NOT_NULL_OR_EMPTY));

      config.setJMXUseBrokerName(getBoolean(e, "jmx-use-broker-name", config.isJMXUseBrokerName()));

      config.setSecurityInvalidationInterval(getLong(e, "security-invalidation-interval", config.getSecurityInvalidationInterval(), Validators.GT_ZERO));

      config.setConnectionTTLOverride(getLong(e, "connection-ttl-override", config.getConnectionTTLOverride(), Validators.MINUS_ONE_OR_GT_ZERO));

      config.setEnabledAsyncConnectionExecution(getBoolean(e, "async-connection-execution-enabled", config.isAsyncConnectionExecutionEnabled()));

      config.setTransactionTimeout(getLong(e, "transaction-timeout", config.getTransactionTimeout(), Validators.GT_ZERO));

      config.setTransactionTimeoutScanPeriod(getLong(e, "transaction-timeout-scan-period", config.getTransactionTimeoutScanPeriod(), Validators.GT_ZERO));

      config.setMessageExpiryScanPeriod(getLong(e, "message-expiry-scan-period", config.getMessageExpiryScanPeriod(), Validators.MINUS_ONE_OR_GT_ZERO));

      config.setMessageExpiryThreadPriority(getInteger(e, "message-expiry-thread-priority", config.getMessageExpiryThreadPriority(), Validators.THREAD_PRIORITY_RANGE));

      config.setIDCacheSize(getInteger(e, "id-cache-size", config.getIDCacheSize(), Validators.GT_ZERO));

      config.setPersistIDCache(getBoolean(e, "persist-id-cache", config.isPersistIDCache()));

      config.setManagementAddress(new SimpleString(getString(e, "management-address", config.getManagementAddress().toString(), Validators.NOT_NULL_OR_EMPTY)));

      config.setManagementNotificationAddress(new SimpleString(getString(e, "management-notification-address", config.getManagementNotificationAddress().toString(), Validators.NOT_NULL_OR_EMPTY)));

      config.setMaskPassword(getBoolean(e, "mask-password", null));

      config.setPasswordCodec(getString(e, "password-codec", DefaultSensitiveStringCodec.class.getName(), Validators.NOT_NULL_OR_EMPTY));

      config.setPopulateValidatedUser(getBoolean(e, "populate-validated-user", config.isPopulateValidatedUser()));

      config.setConnectionTtlCheckInterval(getLong(e, "connection-ttl-check-interval", config.getConnectionTtlCheckInterval(), Validators.GT_ZERO));

      config.setConfigurationFileRefreshPeriod(getLong(e, "configuration-file-refresh-period", config.getConfigurationFileRefreshPeriod(), Validators.GT_ZERO));

      long globalMaxSize = getTextBytesAsLongBytes(e, GLOBAL_MAX_SIZE, -1, Validators.MINUS_ONE_OR_GT_ZERO);

      if (globalMaxSize > 0) {
         // We only set it if it's not set on the XML, otherwise getGlobalMaxSize will calculate it.
         // We do it this way because it will be valid also on the case of embedded
         config.setGlobalMaxSize(globalMaxSize);
      }

      config.setMaxDiskUsage(getInteger(e, MAX_DISK_USAGE, config.getMaxDiskUsage(), Validators.PERCENTAGE_OR_MINUS_ONE));

      config.setDiskScanPeriod(getInteger(e, DISK_SCAN_PERIOD, config.getDiskScanPeriod(), Validators.MINUS_ONE_OR_GT_ZERO));

      config.setInternalNamingPrefix(getString(e, INTERNAL_NAMING_PREFIX, config.getInternalNamingPrefix(), Validators.NO_CHECK));

      config.setAmqpUseCoreSubscriptionNaming(getBoolean(e, AMQP_USE_CORE_SUBSCRIPTION_NAMING, config.isAmqpUseCoreSubscriptionNaming()));


      // parsing cluster password
      String passwordText = getString(e, "cluster-password", null, Validators.NO_CHECK);

      final Boolean maskText = config.isMaskPassword();

      if (passwordText != null) {
         String resolvedPassword = PasswordMaskingUtil.resolveMask(maskText, passwordText, config.getPasswordCodec());
         config.setClusterPassword(resolvedPassword);
      }

      config.setClusterUser(getString(e, "cluster-user", config.getClusterUser(), Validators.NO_CHECK));

      NodeList interceptorNodes = e.getElementsByTagName("remoting-interceptors");

      ArrayList<String> incomingInterceptorList = new ArrayList<>();

      if (interceptorNodes.getLength() > 0) {
         NodeList interceptors = interceptorNodes.item(0).getChildNodes();

         for (int i = 0; i < interceptors.getLength(); i++) {
            if ("class-name".equalsIgnoreCase(interceptors.item(i).getNodeName())) {
               String clazz = getTrimmedTextContent(interceptors.item(i));

               incomingInterceptorList.add(clazz);
            }
         }
      }

      NodeList incomingInterceptorNodes = e.getElementsByTagName("remoting-incoming-interceptors");

      if (incomingInterceptorNodes.getLength() > 0) {
         NodeList interceptors = incomingInterceptorNodes.item(0).getChildNodes();

         for (int i = 0; i < interceptors.getLength(); i++) {
            if ("class-name".equalsIgnoreCase(interceptors.item(i).getNodeName())) {
               String clazz = getTrimmedTextContent(interceptors.item(i));

               incomingInterceptorList.add(clazz);
            }
         }
      }

      config.setIncomingInterceptorClassNames(incomingInterceptorList);

      NodeList outgoingInterceptorNodes = e.getElementsByTagName("remoting-outgoing-interceptors");

      ArrayList<String> outgoingInterceptorList = new ArrayList<>();

      if (outgoingInterceptorNodes.getLength() > 0) {
         NodeList interceptors = outgoingInterceptorNodes.item(0).getChildNodes();

         for (int i = 0; i < interceptors.getLength(); i++) {
            if ("class-name".equalsIgnoreCase(interceptors.item(i).getNodeName())) {
               String clazz = interceptors.item(i).getTextContent();

               outgoingInterceptorList.add(clazz);
            }
         }
      }

      config.setOutgoingInterceptorClassNames(outgoingInterceptorList);

      NodeList connectorNodes = e.getElementsByTagName("connector");

      for (int i = 0; i < connectorNodes.getLength(); i++) {
         Element connectorNode = (Element) connectorNodes.item(i);

         TransportConfiguration connectorConfig = parseConnectorTransportConfiguration(connectorNode, config);

         if (connectorConfig.getName() == null) {
            ActiveMQServerLogger.LOGGER.connectorWithNoName();

            continue;
         }

         if (config.getConnectorConfigurations().containsKey(connectorConfig.getName())) {
            ActiveMQServerLogger.LOGGER.connectorAlreadyDeployed(connectorConfig.getName());

            continue;
         }

         config.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);
      }

      NodeList acceptorNodes = e.getElementsByTagName("acceptor");

      for (int i = 0; i < acceptorNodes.getLength(); i++) {
         Element acceptorNode = (Element) acceptorNodes.item(i);

         TransportConfiguration acceptorConfig = parseAcceptorTransportConfiguration(acceptorNode, config);

         config.getAcceptorConfigurations().add(acceptorConfig);
      }

      NodeList bgNodes = e.getElementsByTagName("broadcast-group");

      for (int i = 0; i < bgNodes.getLength(); i++) {
         Element bgNode = (Element) bgNodes.item(i);

         parseBroadcastGroupConfiguration(bgNode, config);
      }

      NodeList dgNodes = e.getElementsByTagName("discovery-group");

      for (int i = 0; i < dgNodes.getLength(); i++) {
         Element dgNode = (Element) dgNodes.item(i);

         parseDiscoveryGroupConfiguration(dgNode, config);
      }

      NodeList brNodes = e.getElementsByTagName("bridge");

      for (int i = 0; i < brNodes.getLength(); i++) {
         Element mfNode = (Element) brNodes.item(i);

         parseBridgeConfiguration(mfNode, config);
      }

      NodeList gaNodes = e.getElementsByTagName("grouping-handler");

      for (int i = 0; i < gaNodes.getLength(); i++) {
         Element gaNode = (Element) gaNodes.item(i);

         parseGroupingHandlerConfiguration(gaNode, config);
      }

      NodeList ccNodes = e.getElementsByTagName("cluster-connection");

      for (int i = 0; i < ccNodes.getLength(); i++) {
         Element ccNode = (Element) ccNodes.item(i);

         parseClusterConnectionConfiguration(ccNode, config);
      }

      NodeList ccNodesURI = e.getElementsByTagName("cluster-connection-uri");

      for (int i = 0; i < ccNodesURI.getLength(); i++) {
         Element ccNode = (Element) ccNodesURI.item(i);

         parseClusterConnectionConfigurationURI(ccNode, config);
      }

      NodeList dvNodes = e.getElementsByTagName("divert");

      for (int i = 0; i < dvNodes.getLength(); i++) {
         Element dvNode = (Element) dvNodes.item(i);

         parseDivertConfiguration(dvNode, config);
      }

      // Persistence config

      config.setLargeMessagesDirectory(getString(e, "large-messages-directory", config.getLargeMessagesDirectory(), Validators.NOT_NULL_OR_EMPTY));

      config.setBindingsDirectory(getString(e, "bindings-directory", config.getBindingsDirectory(), Validators.NOT_NULL_OR_EMPTY));

      config.setCreateBindingsDir(getBoolean(e, "create-bindings-dir", config.isCreateBindingsDir()));

      config.setJournalDirectory(getString(e, "journal-directory", config.getJournalDirectory(), Validators.NOT_NULL_OR_EMPTY));

      config.setPageMaxConcurrentIO(getInteger(e, "page-max-concurrent-io", config.getPageMaxConcurrentIO(), Validators.MINUS_ONE_OR_GT_ZERO));

      config.setPagingDirectory(getString(e, "paging-directory", config.getPagingDirectory(), Validators.NOT_NULL_OR_EMPTY));

      config.setCreateJournalDir(getBoolean(e, "create-journal-dir", config.isCreateJournalDir()));

      String s = getString(e, "journal-type", config.getJournalType().toString(), Validators.JOURNAL_TYPE);

      config.setJournalType(JournalType.getType(s));

      if (config.getJournalType() == JournalType.ASYNCIO) {
         // https://jira.jboss.org/jira/browse/HORNETQ-295
         // We do the check here to see if AIO is supported so we can use the correct defaults and/or use
         // correct settings in xml
         // If we fall back later on these settings can be ignored
         boolean supportsAIO = AIOSequentialFileFactory.isSupported();

         if (!supportsAIO) {
            if (validateAIO) {
               ActiveMQServerLogger.LOGGER.AIONotFound();
            }
            config.setJournalType(JournalType.NIO);
         }
      }

      config.setJournalDatasync(getBoolean(e, "journal-datasync", config.isJournalDatasync()));

      config.setJournalSyncTransactional(getBoolean(e, "journal-sync-transactional", config.isJournalSyncTransactional()));

      config.setJournalSyncNonTransactional(getBoolean(e, "journal-sync-non-transactional", config.isJournalSyncNonTransactional()));

      config.setJournalFileSize(getTextBytesAsIntBytes(e, "journal-file-size", config.getJournalFileSize(), Validators.GT_ZERO));

      int journalBufferTimeout = getInteger(e, "journal-buffer-timeout", config.getJournalType() == JournalType.ASYNCIO ? ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO : ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO, Validators.GE_ZERO);

      int journalBufferSize = getTextBytesAsIntBytes(e, "journal-buffer-size", config.getJournalType() == JournalType.ASYNCIO ? ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO : ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO, Validators.GT_ZERO);

      int journalMaxIO = getInteger(e, "journal-max-io", config.getJournalType() == JournalType.ASYNCIO ? ActiveMQDefaultConfiguration.getDefaultJournalMaxIoAio() : ActiveMQDefaultConfiguration.getDefaultJournalMaxIoNio(), Validators.GT_ZERO);

      if (config.getJournalType() == JournalType.ASYNCIO) {
         config.setJournalBufferTimeout_AIO(journalBufferTimeout);
         config.setJournalBufferSize_AIO(journalBufferSize);
         config.setJournalMaxIO_AIO(journalMaxIO);
      } else {
         config.setJournalBufferTimeout_NIO(journalBufferTimeout);
         config.setJournalBufferSize_NIO(journalBufferSize);
         config.setJournalMaxIO_NIO(journalMaxIO);
      }

      config.setJournalFileOpenTimeout(getInteger(e, "journal-file-open-timeout", ActiveMQDefaultConfiguration.getDefaultJournalFileOpenTimeout(), Validators.GT_ZERO));

      config.setJournalMinFiles(getInteger(e, "journal-min-files", config.getJournalMinFiles(), Validators.GT_ZERO));

      config.setJournalPoolFiles(getInteger(e, "journal-pool-files", config.getJournalPoolFiles(), Validators.MINUS_ONE_OR_GT_ZERO));

      config.setJournalCompactMinFiles(getInteger(e, "journal-compact-min-files", config.getJournalCompactMinFiles(), Validators.GE_ZERO));

      config.setJournalCompactPercentage(getInteger(e, "journal-compact-percentage", config.getJournalCompactPercentage(), Validators.PERCENTAGE));

      config.setLogJournalWriteRate(getBoolean(e, "log-journal-write-rate", ActiveMQDefaultConfiguration.isDefaultJournalLogWriteRate()));

      if (e.hasAttribute("wild-card-routing-enabled")) {
         config.setWildcardRoutingEnabled(getBoolean(e, "wild-card-routing-enabled", config.isWildcardRoutingEnabled()));
      }

      config.setMessageCounterEnabled(getBoolean(e, "message-counter-enabled", config.isMessageCounterEnabled()));

      config.setMessageCounterSamplePeriod(getLong(e, "message-counter-sample-period", config.getMessageCounterSamplePeriod(), Validators.GT_ZERO));

      config.setMessageCounterMaxDayHistory(getInteger(e, "message-counter-max-day-history", config.getMessageCounterMaxDayHistory(), Validators.GT_ZERO));

      config.setServerDumpInterval(getLong(e, "server-dump-interval", config.getServerDumpInterval(), Validators.MINUS_ONE_OR_GT_ZERO)); // in milliseconds

      config.setMemoryWarningThreshold(getInteger(e, "memory-warning-threshold", config.getMemoryWarningThreshold(), Validators.PERCENTAGE));

      config.setMemoryMeasureInterval(getLong(e, "memory-measure-interval", config.getMemoryMeasureInterval(), Validators.MINUS_ONE_OR_GT_ZERO));

      config.setNetworkCheckList(getString(e, "network-check-list", config.getNetworkCheckList(), Validators.NO_CHECK));

      config.setNetworkCheckURLList(getString(e, "network-check-URL-list", config.getNetworkCheckURLList(), Validators.NO_CHECK));

      config.setNetworkCheckPeriod(getLong(e, "network-check-period", config.getNetworkCheckPeriod(), Validators.GT_ZERO));

      config.setNetworkCheckTimeout(getInteger(e, "network-check-timeout", config.getNetworkCheckTimeout(), Validators.GT_ZERO));

      config.setNetworCheckNIC(getString(e, "network-check-NIC", config.getNetworkCheckNIC(), Validators.NO_CHECK));

      config.setNetworkCheckPing6Command(getString(e, "network-check-ping6-command", config.getNetworkCheckPing6Command(), Validators.NO_CHECK));

      config.setNetworkCheckPingCommand(getString(e, "network-check-ping-command", config.getNetworkCheckPingCommand(), Validators.NO_CHECK));

      config.setCriticalAnalyzer(getBoolean(e, "critical-analyzer", config.isCriticalAnalyzer()));

      config.setCriticalAnalyzerTimeout(getLong(e, "critical-analyzer-timeout", config.getCriticalAnalyzerTimeout(), Validators.GE_ZERO));

      config.setCriticalAnalyzerCheckPeriod(getLong(e, "critical-analyzer-check-period", config.getCriticalAnalyzerCheckPeriod(), Validators.GE_ZERO));

      config.setCriticalAnalyzerPolicy(CriticalAnalyzerPolicy.valueOf(getString(e, "critical-analyzer-policy", config.getCriticalAnalyzerPolicy().name(), Validators.NOT_NULL_OR_EMPTY)));

      parseAddressSettings(e, config);

      parseResourceLimits(e, config);

      parseQueues(e, config);

      parseAddresses(e, config);

      parseSecurity(e, config);

      parseBrokerPlugins(e, config);

      NodeList connectorServiceConfigs = e.getElementsByTagName("connector-service");

      ArrayList<ConnectorServiceConfiguration> configs = new ArrayList<>();

      for (int i = 0; i < connectorServiceConfigs.getLength(); i++) {
         Element node = (Element) connectorServiceConfigs.item(i);

         configs.add((parseConnectorService(node)));
      }

      config.setConnectorServiceConfigurations(configs);

      NodeList wildCardConfiguration = e.getElementsByTagName("wildcard-addresses");

      if (wildCardConfiguration.getLength() > 0) {
         parseWildcardConfiguration((Element) wildCardConfiguration.item(0), config);
      }
   }

   /**
    * @param e
    * @param config
    */
   private void parseSecurity(final Element e, final Configuration config) {
      NodeList elements = e.getElementsByTagName("security-settings");
      if (elements.getLength() != 0) {
         Element node = (Element) elements.item(0);
         NodeList list = node.getElementsByTagName(SECURITY_ROLE_MAPPING_NAME);
         for (int i = 0; i < list.getLength(); i++) {
            Map<String, Set<String>> roleMappings = parseSecurityRoleMapping(list.item(i));
            for (Map.Entry<String, Set<String>> roleMapping : roleMappings.entrySet()) {
               config.addSecurityRoleNameMapping(roleMapping.getKey(), roleMapping.getValue());
            }
         }
         list = node.getElementsByTagName(SECURITY_ELEMENT_NAME);
         for (int i = 0; i < list.getLength(); i++) {
            Pair<String, Set<Role>> securityItem = parseSecurityRoles(list.item(i), config.getSecurityRoleNameMappings());
            config.putSecurityRoles(securityItem.getA(), securityItem.getB());
         }
         list = node.getElementsByTagName(SECURITY_PLUGIN_ELEMENT_NAME);
         for (int i = 0; i < list.getLength(); i++) {
            Pair<SecuritySettingPlugin, Map<String, String>> securityItem = parseSecuritySettingPlugins(list.item(i));
            config.addSecuritySettingPlugin(securityItem.getA().init(securityItem.getB()));
         }
      }
   }

   private void parseBrokerPlugins(final Element e, final Configuration config) {
      NodeList brokerPlugins = e.getElementsByTagName(BROKER_PLUGINS_ELEMENT_NAME);
      if (brokerPlugins.getLength() != 0) {
         Element node = (Element) brokerPlugins.item(0);
         NodeList list = node.getElementsByTagName(BROKER_PLUGIN_ELEMENT_NAME);
         for (int i = 0; i < list.getLength(); i++) {
            ActiveMQServerPlugin plugin = parseActiveMQServerPlugin(list.item(i));
            config.registerBrokerPlugin(plugin);
         }
      }
   }

   private ActiveMQServerPlugin parseActiveMQServerPlugin(Node item) {
      final String clazz = item.getAttributes().getNamedItem("class-name").getNodeValue();

      Map<String, String> properties = getMapOfChildPropertyElements(item);

      ActiveMQServerPlugin serverPlugin = AccessController.doPrivileged(new PrivilegedAction<ActiveMQServerPlugin>() {
         @Override
         public ActiveMQServerPlugin run() {
            return (ActiveMQServerPlugin) ClassloadingUtil.newInstanceFromClassLoader(clazz);
         }
      });

      serverPlugin.init(properties);

      return serverPlugin;
   }

   private Map<String, String> getMapOfChildPropertyElements(Node item) {
      Map<String, String> properties = new HashMap<>();
      NodeList children = item.getChildNodes();
      for (int i = 0; i < children.getLength(); i++) {
         Node child = children.item(i);
         if (child.getNodeName().equals("property")) {
            String key = getAttributeValue(child, "key");
            String value = getAttributeValue(child, "value");
            properties.put(key, value);
         }
      }
      return properties;
   }

   /**
    * @param e
    * @param config
    */
   private void parseQueues(final Element e, final Configuration config) {
      NodeList elements = e.getElementsByTagName("queues");
      if (elements.getLength() != 0) {
         Element node = (Element) elements.item(0);
         config.setQueueConfigurations(parseQueueConfigurations(node, ActiveMQDefaultConfiguration.DEFAULT_ROUTING_TYPE));
      }
   }

   private List<CoreQueueConfiguration> parseQueueConfigurations(final Element node, RoutingType routingType) {
      List<CoreQueueConfiguration> queueConfigurations = new ArrayList<>();
      NodeList list = node.getElementsByTagName("queue");
      for (int i = 0; i < list.getLength(); i++) {
         CoreQueueConfiguration queueConfig = parseQueueConfiguration(list.item(i));
         queueConfig.setRoutingType(routingType);
         queueConfigurations.add(queueConfig);
      }
      return queueConfigurations;
   }

   /**
    * @param e
    * @param config
    */
   private void parseAddresses(final Element e, final Configuration config) {
      NodeList elements = e.getElementsByTagName("addresses");

      if (elements.getLength() != 0) {
         Element node = (Element) elements.item(0);
         NodeList list = node.getElementsByTagName("address");
         for (int i = 0; i < list.getLength(); i++) {
            CoreAddressConfiguration addrConfig = parseAddressConfiguration(list.item(i));
            config.getAddressConfigurations().add(addrConfig);
         }
      }
   }

   /**
    * @param e
    * @param config
    */
   private void parseAddressSettings(final Element e, final Configuration config) {
      NodeList elements = e.getElementsByTagName("address-settings");

      if (elements.getLength() != 0) {
         Element node = (Element) elements.item(0);
         NodeList list = node.getElementsByTagName("address-setting");
         for (int i = 0; i < list.getLength(); i++) {
            Pair<String, AddressSettings> addressSettings = parseAddressSettings(list.item(i));
            config.getAddressesSettings().put(addressSettings.getA(), addressSettings.getB());
         }
      }
   }

   /**
    * @param e
    * @param config
    */
   private void parseResourceLimits(final Element e, final Configuration config) {
      NodeList elements = e.getElementsByTagName("resource-limit-settings");

      if (elements.getLength() != 0) {
         Element node = (Element) elements.item(0);
         NodeList list = node.getElementsByTagName("resource-limit-setting");
         for (int i = 0; i < list.getLength(); i++) {
            config.addResourceLimitSettings(parseResourceLimitSettings(list.item(i)));
         }
      }
   }

   /**
    * @param node
    * @return
    */
   protected Pair<String, Set<Role>> parseSecurityRoles(final Node node, final Map<String, Set<String>> roleMappings) {
      final String match = node.getAttributes().getNamedItem("match").getNodeValue();

      Set<Role> securityRoles = new HashSet<>();

      Pair<String, Set<Role>> securityMatch = new Pair<>(match, securityRoles);

      ArrayList<String> send = new ArrayList<>();
      ArrayList<String> consume = new ArrayList<>();
      ArrayList<String> createDurableQueue = new ArrayList<>();
      ArrayList<String> deleteDurableQueue = new ArrayList<>();
      ArrayList<String> createNonDurableQueue = new ArrayList<>();
      ArrayList<String> deleteNonDurableQueue = new ArrayList<>();
      ArrayList<String> manageRoles = new ArrayList<>();
      ArrayList<String> browseRoles = new ArrayList<>();
      ArrayList<String> createAddressRoles = new ArrayList<>();
      ArrayList<String> deleteAddressRoles = new ArrayList<>();
      ArrayList<String> allRoles = new ArrayList<>();
      NodeList children = node.getChildNodes();
      for (int i = 0; i < children.getLength(); i++) {
         Node child = children.item(i);
         final String name = child.getNodeName();
         if (PERMISSION_ELEMENT_NAME.equalsIgnoreCase(name)) {
            final String type = getAttributeValue(child, TYPE_ATTR_NAME);
            final String roleString = getAttributeValue(child, ROLES_ATTR_NAME);
            String[] roles = roleString.split(",");
            String[] mappedRoles = getMappedRoleNames(roles, roleMappings);

            for (String role : mappedRoles) {
               if (SEND_NAME.equals(type)) {
                  send.add(role.trim());
               } else if (CONSUME_NAME.equals(type)) {
                  consume.add(role.trim());
               } else if (CREATEDURABLEQUEUE_NAME.equals(type)) {
                  createDurableQueue.add(role.trim());
               } else if (DELETEDURABLEQUEUE_NAME.equals(type)) {
                  deleteDurableQueue.add(role.trim());
               } else if (CREATE_NON_DURABLE_QUEUE_NAME.equals(type)) {
                  createNonDurableQueue.add(role.trim());
               } else if (DELETE_NON_DURABLE_QUEUE_NAME.equals(type)) {
                  deleteNonDurableQueue.add(role.trim());
               } else if (CREATETEMPQUEUE_NAME.equals(type)) {
                  createNonDurableQueue.add(role.trim());
               } else if (DELETETEMPQUEUE_NAME.equals(type)) {
                  deleteNonDurableQueue.add(role.trim());
               } else if (MANAGE_NAME.equals(type)) {
                  manageRoles.add(role.trim());
               } else if (BROWSE_NAME.equals(type)) {
                  browseRoles.add(role.trim());
               } else if (CREATEADDRESS_NAME.equals(type)) {
                  createAddressRoles.add(role.trim());
               } else if (DELETEADDRESS_NAME.equals(type)) {
                  deleteAddressRoles.add(role.trim());
               } else {
                  ActiveMQServerLogger.LOGGER.rolePermissionConfigurationError(type);
               }
               if (!allRoles.contains(role.trim())) {
                  allRoles.add(role.trim());
               }
            }
         }
      }

      for (String role : allRoles) {
         securityRoles.add(new Role(role, send.contains(role), consume.contains(role), createDurableQueue.contains(role), deleteDurableQueue.contains(role), createNonDurableQueue.contains(role), deleteNonDurableQueue.contains(role), manageRoles.contains(role), browseRoles.contains(role)));
      }

      return securityMatch;
   }

   /**
    * Translate and expand a set of role names to a set of mapped role names, also includes the original role names
    * @param roles the original set of role names
    * @param roleMappings a one-to-many mapping of original role names to mapped role names
    * @return the final set of mapped role names
    */
   private String[] getMappedRoleNames(String[] roles, Map<String, Set<String>> roleMappings) {
      Set<String> mappedRoles = new HashSet<>();
      for (String role : roles) {
         if (roleMappings.containsKey(role)) {
            mappedRoles.addAll(roleMappings.get(role));
         }
         mappedRoles.add(role);
      }
      return mappedRoles.toArray(new String[mappedRoles.size()]);
   }

   private Pair<SecuritySettingPlugin, Map<String, String>> parseSecuritySettingPlugins(Node item) {
      final String clazz = item.getAttributes().getNamedItem("class-name").getNodeValue();
      final Map<String, String> settings = new HashMap<>();
      NodeList children = item.getChildNodes();
      for (int j = 0; j < children.getLength(); j++) {
         Node child = children.item(j);
         final String nodeName = child.getNodeName();
         if (SETTING_ELEMENT_NAME.equalsIgnoreCase(nodeName)) {
            final String settingName = getAttributeValue(child, NAME_ATTR_NAME);
            final String settingValue = getAttributeValue(child, VALUE_ATTR_NAME);
            settings.put(settingName, settingValue);
         }
      }

      SecuritySettingPlugin securitySettingPlugin = AccessController.doPrivileged(new PrivilegedAction<SecuritySettingPlugin>() {
         @Override
         public SecuritySettingPlugin run() {
            return (SecuritySettingPlugin) ClassloadingUtil.newInstanceFromClassLoader(clazz);
         }
      });

      return new Pair<>(securitySettingPlugin, settings);
   }

   /**
    * Computes the map of internal ActiveMQ role names to sets of external (e.g. LDAP) role names.  For example, given a role
    * "myrole" with a DN of "cn=myrole,dc=local,dc=com":
    *      from="cn=myrole,dc=local,dc=com", to="amq,admin,guest"
    *      from="cn=myOtherRole,dc=local,dc=com", to="amq"
    * The resulting map will consist of:
    *      amq => {"cn=myrole,dc=local,dc=com","cn=myOtherRole",dc=local,dc=com"}
    *      admin => {"cn=myrole,dc=local,dc=com"}
    *      guest => {"cn=myrole,dc=local,dc=com"}
    * @param item the role-mapping node
    * @return the map of local ActiveMQ role names to the set of mapped role names
    */
   private Map<String, Set<String>> parseSecurityRoleMapping(Node item) {
      Map<String, Set<String>> mappedRoleNames = new HashMap<>();
      String externalRoleName = getAttributeValue(item, ROLE_FROM_ATTR_NAME).trim();
      Set<String> internalRoleNames = new HashSet<>();
      Collections.addAll(internalRoleNames, getAttributeValue(item, ROLE_TO_ATTR_NAME).split(","));
      for (String internalRoleName : internalRoleNames) {
         internalRoleName = internalRoleName.trim();
         if (mappedRoleNames.containsKey(internalRoleName)) {
            mappedRoleNames.get(internalRoleName).add(externalRoleName);
         } else {
            Set<String> externalRoleNames = new HashSet<>();
            externalRoleNames.add(externalRoleName);
            if ((internalRoleName.length() > 0) && (externalRoleName.length() > 0)) {
               mappedRoleNames.put(internalRoleName, externalRoleNames);
            }
         }
      }
      return mappedRoleNames;
   }

   /**
    * @param node
    * @return
    */
   protected Pair<String, AddressSettings> parseAddressSettings(final Node node) {
      String match = getAttributeValue(node, "match");

      NodeList children = node.getChildNodes();

      AddressSettings addressSettings = new AddressSettings();

      Pair<String, AddressSettings> setting = new Pair<>(match, addressSettings);

      for (int i = 0; i < children.getLength(); i++) {
         final Node child = children.item(i);
         final String name = child.getNodeName();
         if (DEAD_LETTER_ADDRESS_NODE_NAME.equalsIgnoreCase(name)) {
            SimpleString queueName = new SimpleString(getTrimmedTextContent(child));
            addressSettings.setDeadLetterAddress(queueName);
         } else if (EXPIRY_ADDRESS_NODE_NAME.equalsIgnoreCase(name)) {
            SimpleString queueName = new SimpleString(getTrimmedTextContent(child));
            addressSettings.setExpiryAddress(queueName);
         } else if (EXPIRY_DELAY_NODE_NAME.equalsIgnoreCase(name)) {
            addressSettings.setExpiryDelay(XMLUtil.parseLong(child));
         } else if (REDELIVERY_DELAY_NODE_NAME.equalsIgnoreCase(name)) {
            addressSettings.setRedeliveryDelay(XMLUtil.parseLong(child));
         } else if (REDELIVERY_DELAY_MULTIPLIER_NODE_NAME.equalsIgnoreCase(name)) {
            addressSettings.setRedeliveryMultiplier(XMLUtil.parseDouble(child));
         } else if (MAX_REDELIVERY_DELAY_NODE_NAME.equalsIgnoreCase(name)) {
            addressSettings.setMaxRedeliveryDelay(XMLUtil.parseLong(child));
         } else if (MAX_SIZE_BYTES_NODE_NAME.equalsIgnoreCase(name)) {
            addressSettings.setMaxSizeBytes(ByteUtil.convertTextBytes(getTrimmedTextContent(child)));
         } else if (PAGE_SIZE_BYTES_NODE_NAME.equalsIgnoreCase(name)) {
            addressSettings.setPageSizeBytes(ByteUtil.convertTextBytes(getTrimmedTextContent(child)));
         } else if (PAGE_MAX_CACHE_SIZE_NODE_NAME.equalsIgnoreCase(name)) {
            addressSettings.setPageCacheMaxSize(XMLUtil.parseInt(child));
         } else if (MESSAGE_COUNTER_HISTORY_DAY_LIMIT_NODE_NAME.equalsIgnoreCase(name)) {
            addressSettings.setMessageCounterHistoryDayLimit(XMLUtil.parseInt(child));
         } else if (ADDRESS_FULL_MESSAGE_POLICY_NODE_NAME.equalsIgnoreCase(name)) {
            String value = getTrimmedTextContent(child);
            Validators.ADDRESS_FULL_MESSAGE_POLICY_TYPE.validate(ADDRESS_FULL_MESSAGE_POLICY_NODE_NAME, value);
            AddressFullMessagePolicy policy = Enum.valueOf(AddressFullMessagePolicy.class, value);
            addressSettings.setAddressFullMessagePolicy(policy);
         } else if (LVQ_NODE_NAME.equalsIgnoreCase(name) || DEFAULT_LVQ_NODE_NAME.equalsIgnoreCase(name)) {
            addressSettings.setDefaultLastValueQueue(XMLUtil.parseBoolean(child));
         } else if (DEFAULT_EXCLUSIVE_NODE_NAME.equalsIgnoreCase(name)) {
            addressSettings.setDefaultExclusiveQueue(XMLUtil.parseBoolean(child));
         } else if (MAX_DELIVERY_ATTEMPTS.equalsIgnoreCase(name)) {
            addressSettings.setMaxDeliveryAttempts(XMLUtil.parseInt(child));
         } else if (REDISTRIBUTION_DELAY_NODE_NAME.equalsIgnoreCase(name)) {
            addressSettings.setRedistributionDelay(XMLUtil.parseLong(child));
         } else if (SEND_TO_DLA_ON_NO_ROUTE.equalsIgnoreCase(name)) {
            addressSettings.setSendToDLAOnNoRoute(XMLUtil.parseBoolean(child));
         } else if (SLOW_CONSUMER_THRESHOLD_NODE_NAME.equalsIgnoreCase(name)) {
            long slowConsumerThreshold = XMLUtil.parseLong(child);
            Validators.MINUS_ONE_OR_GT_ZERO.validate(SLOW_CONSUMER_THRESHOLD_NODE_NAME, slowConsumerThreshold);

            addressSettings.setSlowConsumerThreshold(slowConsumerThreshold);
         } else if (SLOW_CONSUMER_CHECK_PERIOD_NODE_NAME.equalsIgnoreCase(name)) {
            long slowConsumerCheckPeriod = XMLUtil.parseLong(child);
            Validators.GT_ZERO.validate(SLOW_CONSUMER_CHECK_PERIOD_NODE_NAME, slowConsumerCheckPeriod);

            addressSettings.setSlowConsumerCheckPeriod(slowConsumerCheckPeriod);
         } else if (SLOW_CONSUMER_POLICY_NODE_NAME.equalsIgnoreCase(name)) {
            String value = getTrimmedTextContent(child);
            Validators.SLOW_CONSUMER_POLICY_TYPE.validate(SLOW_CONSUMER_POLICY_NODE_NAME, value);
            SlowConsumerPolicy policy = Enum.valueOf(SlowConsumerPolicy.class, value);
            addressSettings.setSlowConsumerPolicy(policy);
         } else if (AUTO_CREATE_JMS_QUEUES.equalsIgnoreCase(name)) {
            addressSettings.setAutoCreateJmsQueues(XMLUtil.parseBoolean(child));
         } else if (AUTO_DELETE_JMS_QUEUES.equalsIgnoreCase(name)) {
            addressSettings.setAutoDeleteJmsQueues(XMLUtil.parseBoolean(child));
         } else if (AUTO_CREATE_JMS_TOPICS.equalsIgnoreCase(name)) {
            addressSettings.setAutoCreateJmsTopics(XMLUtil.parseBoolean(child));
         } else if (AUTO_DELETE_JMS_TOPICS.equalsIgnoreCase(name)) {
            addressSettings.setAutoDeleteJmsTopics(XMLUtil.parseBoolean(child));
         } else if (AUTO_CREATE_QUEUES.equalsIgnoreCase(name)) {
            addressSettings.setAutoCreateQueues(XMLUtil.parseBoolean(child));
         } else if (AUTO_DELETE_QUEUES.equalsIgnoreCase(name)) {
            addressSettings.setAutoDeleteQueues(XMLUtil.parseBoolean(child));
         } else if (CONFIG_DELETE_QUEUES.equalsIgnoreCase(name)) {
            String value = getTrimmedTextContent(child);
            Validators.DELETION_POLICY_TYPE.validate(CONFIG_DELETE_QUEUES, value);
            DeletionPolicy policy = Enum.valueOf(DeletionPolicy.class, value);
            addressSettings.setConfigDeleteQueues(policy);
         } else if (AUTO_CREATE_ADDRESSES.equalsIgnoreCase(name)) {
            addressSettings.setAutoCreateAddresses(XMLUtil.parseBoolean(child));
         } else if (AUTO_DELETE_ADDRESSES.equalsIgnoreCase(name)) {
            addressSettings.setAutoDeleteAddresses(XMLUtil.parseBoolean(child));
         } else if (CONFIG_DELETE_ADDRESSES.equalsIgnoreCase(name)) {
            String value = getTrimmedTextContent(child);
            Validators.DELETION_POLICY_TYPE.validate(CONFIG_DELETE_ADDRESSES, value);
            DeletionPolicy policy = Enum.valueOf(DeletionPolicy.class, value);
            addressSettings.setConfigDeleteAddresses(policy);
         } else if (MANAGEMENT_BROWSE_PAGE_SIZE.equalsIgnoreCase(name)) {
            addressSettings.setManagementBrowsePageSize(XMLUtil.parseInt(child));
         } else if (DEFAULT_PURGE_ON_NO_CONSUMERS.equalsIgnoreCase(name)) {
            addressSettings.setDefaultPurgeOnNoConsumers(XMLUtil.parseBoolean(child));
         } else if (DEFAULT_MAX_CONSUMERS.equalsIgnoreCase(name)) {
            addressSettings.setDefaultMaxConsumers(XMLUtil.parseInt(child));
         } else if (DEFAULT_QUEUE_ROUTING_TYPE.equalsIgnoreCase(name)) {
            String value = getTrimmedTextContent(child);
            Validators.ROUTING_TYPE.validate(DEFAULT_QUEUE_ROUTING_TYPE, value);
            RoutingType routingType = RoutingType.valueOf(value);
            addressSettings.setDefaultQueueRoutingType(routingType);
         } else if (DEFAULT_ADDRESS_ROUTING_TYPE.equalsIgnoreCase(name)) {
            String value = getTrimmedTextContent(child);
            Validators.ROUTING_TYPE.validate(DEFAULT_ADDRESS_ROUTING_TYPE, value);
            RoutingType routingType = RoutingType.valueOf(value);
            addressSettings.setDefaultAddressRoutingType(routingType);
         }
      }
      return setting;
   }

   /**
    * @param node
    * @return
    */
   protected ResourceLimitSettings parseResourceLimitSettings(final Node node) {
      ResourceLimitSettings resourceLimitSettings = new ResourceLimitSettings();

      resourceLimitSettings.setMatch(SimpleString.toSimpleString(getAttributeValue(node, "match")));

      NodeList children = node.getChildNodes();

      for (int i = 0; i < children.getLength(); i++) {
         final Node child = children.item(i);
         final String name = child.getNodeName();
         if (MAX_CONNECTIONS_NODE_NAME.equalsIgnoreCase(name)) {
            resourceLimitSettings.setMaxConnections(XMLUtil.parseInt(child));
         } else if (MAX_QUEUES_NODE_NAME.equalsIgnoreCase(name)) {
            resourceLimitSettings.setMaxQueues(XMLUtil.parseInt(child));
         }
      }
      return resourceLimitSettings;
   }

   protected CoreQueueConfiguration parseQueueConfiguration(final Node node) {
      String name = getAttributeValue(node, "name");
      String address = null;
      String filterString = null;
      boolean durable = true;
      int maxConsumers = ActiveMQDefaultConfiguration.getDefaultMaxQueueConsumers();
      boolean purgeOnNoConsumers = ActiveMQDefaultConfiguration.getDefaultPurgeOnNoConsumers();
      String user = null;
      Boolean exclusive = null;
      Boolean lastValue = null;

      NamedNodeMap attributes = node.getAttributes();
      for (int i = 0; i < attributes.getLength(); i++) {
         Node item = attributes.item(i);
         if (item.getNodeName().equals("max-consumers")) {
            maxConsumers = Integer.parseInt(item.getNodeValue());
            Validators.MAX_QUEUE_CONSUMERS.validate(name, maxConsumers);
         } else if (item.getNodeName().equals("purge-on-no-consumers")) {
            purgeOnNoConsumers = Boolean.parseBoolean(item.getNodeValue());
         } else if (item.getNodeName().equals("exclusive")) {
            exclusive = Boolean.parseBoolean(item.getNodeValue());
         } else if (item.getNodeName().equals("last-value")) {
            lastValue = Boolean.parseBoolean(item.getNodeValue());
         }
      }

      NodeList children = node.getChildNodes();
      for (int j = 0; j < children.getLength(); j++) {
         Node child = children.item(j);

         if (child.getNodeName().equals("address")) {
            address = getTrimmedTextContent(child);
         } else if (child.getNodeName().equals("filter")) {
            filterString = getAttributeValue(child, "string");
         } else if (child.getNodeName().equals("durable")) {
            durable = XMLUtil.parseBoolean(child);
         } else if (child.getNodeName().equals("user")) {
            user = getTrimmedTextContent(child);
         }
      }

      return new CoreQueueConfiguration().setAddress(address).setName(name).setFilterString(filterString).setDurable(durable).setMaxConsumers(maxConsumers).setPurgeOnNoConsumers(purgeOnNoConsumers).setUser(user)
                                         .setExclusive(exclusive).setLastValue(lastValue);
   }

   protected CoreAddressConfiguration parseAddressConfiguration(final Node node) {
      CoreAddressConfiguration addressConfiguration = new CoreAddressConfiguration();

      String name = getAttributeValue(node, "name");
      addressConfiguration.setName(name);

      List<CoreQueueConfiguration> queueConfigurations = new ArrayList<>();
      NodeList children = node.getChildNodes();
      for (int j = 0; j < children.getLength(); j++) {
         Node child = children.item(j);
         if (child.getNodeName().equals("multicast")) {
            addressConfiguration.addRoutingType(RoutingType.MULTICAST);
            queueConfigurations.addAll(parseQueueConfigurations((Element) child, RoutingType.MULTICAST));
         } else if (child.getNodeName().equals("anycast")) {
            addressConfiguration.addRoutingType(RoutingType.ANYCAST);
            queueConfigurations.addAll(parseQueueConfigurations((Element) child, RoutingType.ANYCAST));
         }
      }

      for (CoreQueueConfiguration coreQueueConfiguration : queueConfigurations) {
         coreQueueConfiguration.setAddress(name);
      }

      addressConfiguration.setQueueConfigurations(queueConfigurations);
      return addressConfiguration;
   }

   private TransportConfiguration parseAcceptorTransportConfiguration(final Element e,
                                                                      final Configuration mainConfig) throws Exception {
      Node nameNode = e.getAttributes().getNamedItem("name");

      String name = nameNode != null ? nameNode.getNodeValue() : null;

      String uri = e.getChildNodes().item(0).getNodeValue();

      List<TransportConfiguration> configurations = ConfigurationUtils.parseAcceptorURI(name, uri);

      Map<String, Object> params = configurations.get(0).getParams();

      if (mainConfig.isMaskPassword() != null) {
         params.put(ActiveMQDefaultConfiguration.getPropMaskPassword(), mainConfig.isMaskPassword());
      }

      if (mainConfig.getPasswordCodec() != null) {
         params.put(ActiveMQDefaultConfiguration.getPropPasswordCodec(), mainConfig.getPasswordCodec());
      }

      return configurations.get(0);
   }

   private TransportConfiguration parseConnectorTransportConfiguration(final Element e,
                                                                       final Configuration mainConfig) throws Exception {
      Node nameNode = e.getAttributes().getNamedItem("name");

      String name = nameNode != null ? nameNode.getNodeValue() : null;

      String uri = e.getChildNodes().item(0).getNodeValue();

      List<TransportConfiguration> configurations = ConfigurationUtils.parseConnectorURI(name, uri);

      Map<String, Object> params = configurations.get(0).getParams();

      if (mainConfig.isMaskPassword() != null) {
         params.put(ActiveMQDefaultConfiguration.getPropMaskPassword(), mainConfig.isMaskPassword());
      }

      if (mainConfig.getPasswordCodec() != null) {
         params.put(ActiveMQDefaultConfiguration.getPropPasswordCodec(), mainConfig.getPasswordCodec());
      }

      return configurations.get(0);
   }

   private static final ArrayList<String> POLICY_LIST = new ArrayList<>();

   static {
      POLICY_LIST.add("colocated");
      POLICY_LIST.add("live-only");
      POLICY_LIST.add("replicated");
      POLICY_LIST.add("replica");
      POLICY_LIST.add("shared-store-master");
      POLICY_LIST.add("shared-store-slave");
   }

   private static final ArrayList<String> HA_LIST = new ArrayList<>();

   static {
      HA_LIST.add("live-only");
      HA_LIST.add("shared-store");
      HA_LIST.add("replication");
   }

   private static final ArrayList<String> STORE_TYPE_LIST = new ArrayList<>();

   static {
      STORE_TYPE_LIST.add("database-store");
      STORE_TYPE_LIST.add("file-store");
   }

   private void parseStoreConfiguration(final Element e, final Configuration mainConfig) {
      for (String storeType : STORE_TYPE_LIST) {
         NodeList storeNodeList = e.getElementsByTagName(storeType);
         if (storeNodeList.getLength() > 0) {
            Element storeNode = (Element) storeNodeList.item(0);
            if (storeNode.getTagName().equals("database-store")) {
               mainConfig.setStoreConfiguration(createDatabaseStoreConfig(storeNode));
            } else if (storeNode.getTagName().equals("file-store")) {
               mainConfig.setStoreConfiguration(createFileStoreConfig(storeNode));
            }
         }
      }
   }

   private void parseHAPolicyConfiguration(final Element e, final Configuration mainConfig) {
      for (String haType : HA_LIST) {
         NodeList haNodeList = e.getElementsByTagName(haType);
         if (haNodeList.getLength() > 0) {
            Element haNode = (Element) haNodeList.item(0);
            if (haNode.getTagName().equals("replication")) {
               NodeList masterNodeList = e.getElementsByTagName("master");
               if (masterNodeList.getLength() > 0) {
                  Element masterNode = (Element) masterNodeList.item(0);
                  mainConfig.setHAPolicyConfiguration(createReplicatedHaPolicy(masterNode));
               }
               NodeList slaveNodeList = e.getElementsByTagName("slave");
               if (slaveNodeList.getLength() > 0) {
                  Element slaveNode = (Element) slaveNodeList.item(0);
                  mainConfig.setHAPolicyConfiguration(createReplicaHaPolicy(slaveNode));
               }
               NodeList colocatedNodeList = e.getElementsByTagName("colocated");
               if (colocatedNodeList.getLength() > 0) {
                  Element colocatedNode = (Element) colocatedNodeList.item(0);
                  mainConfig.setHAPolicyConfiguration(createColocatedHaPolicy(colocatedNode, true));
               }
            } else if (haNode.getTagName().equals("shared-store")) {
               NodeList masterNodeList = e.getElementsByTagName("master");
               if (masterNodeList.getLength() > 0) {
                  Element masterNode = (Element) masterNodeList.item(0);
                  mainConfig.setHAPolicyConfiguration(createSharedStoreMasterHaPolicy(masterNode));
               }
               NodeList slaveNodeList = e.getElementsByTagName("slave");
               if (slaveNodeList.getLength() > 0) {
                  Element slaveNode = (Element) slaveNodeList.item(0);
                  mainConfig.setHAPolicyConfiguration(createSharedStoreSlaveHaPolicy(slaveNode));
               }
               NodeList colocatedNodeList = e.getElementsByTagName("colocated");
               if (colocatedNodeList.getLength() > 0) {
                  Element colocatedNode = (Element) colocatedNodeList.item(0);
                  mainConfig.setHAPolicyConfiguration(createColocatedHaPolicy(colocatedNode, false));
               }
            } else if (haNode.getTagName().equals("live-only")) {
               NodeList noneNodeList = e.getElementsByTagName("live-only");
               Element noneNode = (Element) noneNodeList.item(0);
               mainConfig.setHAPolicyConfiguration(createLiveOnlyHaPolicy(noneNode));
            }
         }
      }
   }

   private LiveOnlyPolicyConfiguration createLiveOnlyHaPolicy(Element policyNode) {
      LiveOnlyPolicyConfiguration configuration = new LiveOnlyPolicyConfiguration();

      configuration.setScaleDownConfiguration(parseScaleDownConfig(policyNode));

      return configuration;
   }

   private ReplicatedPolicyConfiguration createReplicatedHaPolicy(Element policyNode) {
      ReplicatedPolicyConfiguration configuration = new ReplicatedPolicyConfiguration();

      configuration.setCheckForLiveServer(getBoolean(policyNode, "check-for-live-server", configuration.isCheckForLiveServer()));

      configuration.setGroupName(getString(policyNode, "group-name", configuration.getGroupName(), Validators.NO_CHECK));

      configuration.setClusterName(getString(policyNode, "cluster-name", configuration.getClusterName(), Validators.NO_CHECK));

      configuration.setInitialReplicationSyncTimeout(getLong(policyNode, "initial-replication-sync-timeout", configuration.getInitialReplicationSyncTimeout(), Validators.GT_ZERO));

      configuration.setVoteOnReplicationFailure(getBoolean(policyNode, "vote-on-replication-failure", configuration.getVoteOnReplicationFailure()));

      configuration.setVoteRetries(getInteger(policyNode, "vote-retries", configuration.getVoteRetries(), Validators.MINUS_ONE_OR_GE_ZERO));

      configuration.setVoteRetryWait(getLong(policyNode, "vote-retry-wait", configuration.getVoteRetryWait(), Validators.GT_ZERO));

      configuration.setQuorumSize(getInteger(policyNode, "quorum-size", configuration.getQuorumSize(), Validators.MINUS_ONE_OR_GT_ZERO));

      return configuration;
   }

   private ReplicaPolicyConfiguration createReplicaHaPolicy(Element policyNode) {
      ReplicaPolicyConfiguration configuration = new ReplicaPolicyConfiguration();

      configuration.setRestartBackup(getBoolean(policyNode, "restart-backup", configuration.isRestartBackup()));

      configuration.setGroupName(getString(policyNode, "group-name", configuration.getGroupName(), Validators.NO_CHECK));

      configuration.setAllowFailBack(getBoolean(policyNode, "allow-failback", configuration.isAllowFailBack()));

      configuration.setInitialReplicationSyncTimeout(getLong(policyNode, "initial-replication-sync-timeout", configuration.getInitialReplicationSyncTimeout(), Validators.GT_ZERO));

      configuration.setClusterName(getString(policyNode, "cluster-name", configuration.getClusterName(), Validators.NO_CHECK));

      configuration.setMaxSavedReplicatedJournalsSize(getInteger(policyNode, "max-saved-replicated-journals-size", configuration.getMaxSavedReplicatedJournalsSize(), Validators.MINUS_ONE_OR_GE_ZERO));

      configuration.setScaleDownConfiguration(parseScaleDownConfig(policyNode));

      configuration.setVoteOnReplicationFailure(getBoolean(policyNode, "vote-on-replication-failure", configuration.getVoteOnReplicationFailure()));

      configuration.setVoteRetries(getInteger(policyNode, "vote-retries", configuration.getVoteRetries(), Validators.MINUS_ONE_OR_GE_ZERO));

      configuration.setVoteRetryWait(getLong(policyNode, "vote-retry-wait", configuration.getVoteRetryWait(), Validators.GT_ZERO));

      configuration.setQuorumSize(getInteger(policyNode, "quorum-size", configuration.getQuorumSize(), Validators.MINUS_ONE_OR_GT_ZERO));

      return configuration;
   }

   private SharedStoreMasterPolicyConfiguration createSharedStoreMasterHaPolicy(Element policyNode) {
      SharedStoreMasterPolicyConfiguration configuration = new SharedStoreMasterPolicyConfiguration();

      configuration.setFailoverOnServerShutdown(getBoolean(policyNode, "failover-on-shutdown", configuration.isFailoverOnServerShutdown()));
      configuration.setWaitForActivation(getBoolean(policyNode, "wait-for-activation", configuration.isWaitForActivation()));

      return configuration;
   }

   private SharedStoreSlavePolicyConfiguration createSharedStoreSlaveHaPolicy(Element policyNode) {
      SharedStoreSlavePolicyConfiguration configuration = new SharedStoreSlavePolicyConfiguration();

      configuration.setAllowFailBack(getBoolean(policyNode, "allow-failback", configuration.isAllowFailBack()));

      configuration.setFailoverOnServerShutdown(getBoolean(policyNode, "failover-on-shutdown", configuration.isFailoverOnServerShutdown()));

      configuration.setRestartBackup(getBoolean(policyNode, "restart-backup", configuration.isRestartBackup()));

      configuration.setScaleDownConfiguration(parseScaleDownConfig(policyNode));

      return configuration;
   }

   private ColocatedPolicyConfiguration createColocatedHaPolicy(Element policyNode, boolean replicated) {
      ColocatedPolicyConfiguration configuration = new ColocatedPolicyConfiguration();

      boolean requestBackup = getBoolean(policyNode, "request-backup", configuration.isRequestBackup());

      configuration.setRequestBackup(requestBackup);

      int backupRequestRetries = getInteger(policyNode, "backup-request-retries", configuration.getBackupRequestRetries(), Validators.MINUS_ONE_OR_GE_ZERO);

      configuration.setBackupRequestRetries(backupRequestRetries);

      long backupRequestRetryInterval = getLong(policyNode, "backup-request-retry-interval", configuration.getBackupRequestRetryInterval(), Validators.GT_ZERO);

      configuration.setBackupRequestRetryInterval(backupRequestRetryInterval);

      int maxBackups = getInteger(policyNode, "max-backups", configuration.getMaxBackups(), Validators.GE_ZERO);

      configuration.setMaxBackups(maxBackups);

      int backupPortOffset = getInteger(policyNode, "backup-port-offset", configuration.getBackupPortOffset(), Validators.GT_ZERO);

      configuration.setBackupPortOffset(backupPortOffset);

      NodeList remoteConnectorNode = policyNode.getElementsByTagName("excludes");

      if (remoteConnectorNode != null && remoteConnectorNode.getLength() > 0) {
         NodeList remoteConnectors = remoteConnectorNode.item(0).getChildNodes();
         for (int i = 0; i < remoteConnectors.getLength(); i++) {
            Node child = remoteConnectors.item(i);
            if (child.getNodeName().equals("connector-ref")) {
               String connectorName = getTrimmedTextContent(child);
               configuration.getExcludedConnectors().add(connectorName);
            }
         }
      }

      NodeList masterNodeList = policyNode.getElementsByTagName("master");
      if (masterNodeList.getLength() > 0) {
         Element masterNode = (Element) masterNodeList.item(0);
         configuration.setLiveConfig(replicated ? createReplicatedHaPolicy(masterNode) : createSharedStoreMasterHaPolicy(masterNode));
      }
      NodeList slaveNodeList = policyNode.getElementsByTagName("slave");
      if (slaveNodeList.getLength() > 0) {
         Element slaveNode = (Element) slaveNodeList.item(0);
         configuration.setBackupConfig(replicated ? createReplicaHaPolicy(slaveNode) : createSharedStoreSlaveHaPolicy(slaveNode));
      }

      return configuration;
   }

   private ScaleDownConfiguration parseScaleDownConfig(Element policyNode) {
      NodeList scaleDownNode = policyNode.getElementsByTagName("scale-down");

      if (scaleDownNode.getLength() > 0) {
         ScaleDownConfiguration scaleDownConfiguration = new ScaleDownConfiguration();

         Element scaleDownElement = (Element) scaleDownNode.item(0);

         scaleDownConfiguration.setEnabled(getBoolean(scaleDownElement, "enabled", scaleDownConfiguration.isEnabled()));

         NodeList discoveryGroupRef = scaleDownElement.getElementsByTagName("discovery-group-ref");

         if (discoveryGroupRef.item(0) != null) {
            scaleDownConfiguration.setDiscoveryGroup(discoveryGroupRef.item(0).getAttributes().getNamedItem("discovery-group-name").getNodeValue());
         }

         String scaleDownGroupName = getString(scaleDownElement, "group-name", scaleDownConfiguration.getGroupName(), Validators.NO_CHECK);

         scaleDownConfiguration.setGroupName(scaleDownGroupName);

         NodeList scaleDownConnectorNode = scaleDownElement.getElementsByTagName("connectors");

         if (scaleDownConnectorNode != null && scaleDownConnectorNode.getLength() > 0) {
            NodeList scaleDownConnectors = scaleDownConnectorNode.item(0).getChildNodes();
            for (int i = 0; i < scaleDownConnectors.getLength(); i++) {
               Node child = scaleDownConnectors.item(i);
               if (child.getNodeName().equals("connector-ref")) {
                  String connectorName = getTrimmedTextContent(child);

                  scaleDownConfiguration.getConnectors().add(connectorName);
               }
            }
         }
         return scaleDownConfiguration;
      }
      return null;
   }

   private DatabaseStorageConfiguration createDatabaseStoreConfig(Element storeNode) {
      DatabaseStorageConfiguration conf = new DatabaseStorageConfiguration();
      conf.setBindingsTableName(getString(storeNode, "bindings-table-name", conf.getBindingsTableName(), Validators.NO_CHECK));
      conf.setMessageTableName(getString(storeNode, "message-table-name", conf.getMessageTableName(), Validators.NO_CHECK));
      conf.setLargeMessageTableName(getString(storeNode, "large-message-table-name", conf.getJdbcConnectionUrl(), Validators.NO_CHECK));
      conf.setPageStoreTableName(getString(storeNode, "page-store-table-name", conf.getPageStoreTableName(), Validators.NO_CHECK));
      conf.setJdbcConnectionUrl(getString(storeNode, "jdbc-connection-url", conf.getJdbcConnectionUrl(), Validators.NO_CHECK));
      conf.setJdbcDriverClassName(getString(storeNode, "jdbc-driver-class-name", conf.getJdbcDriverClassName(), Validators.NO_CHECK));
      conf.setJdbcNetworkTimeout(getInteger(storeNode, "jdbc-network-timeout", conf.getJdbcNetworkTimeout(), Validators.NO_CHECK));
      conf.setJdbcLockAcquisitionTimeoutMillis(getLong(storeNode, "jdbc-lock-acquisition-timeout", conf.getJdbcLockAcquisitionTimeoutMillis(), Validators.NO_CHECK));
      conf.setJdbcLockRenewPeriodMillis(getLong(storeNode, "jdbc-lock-renew-period", conf.getJdbcLockRenewPeriodMillis(), Validators.NO_CHECK));
      conf.setJdbcLockExpirationMillis(getLong(storeNode, "jdbc-lock-expiration", conf.getJdbcLockExpirationMillis(), Validators.NO_CHECK));
      return conf;
   }

   private FileStorageConfiguration createFileStoreConfig(Element storeNode) {
      return new FileStorageConfiguration();
   }

   private void parseBroadcastGroupConfiguration(final Element e, final Configuration mainConfig) {
      String name = e.getAttribute("name");

      List<String> connectorNames = new ArrayList<>();

      NodeList children = e.getChildNodes();

      for (int j = 0; j < children.getLength(); j++) {
         Node child = children.item(j);

         if (child.getNodeName().equals("connector-ref")) {
            String connectorName = getString(e, "connector-ref", null, Validators.NOT_NULL_OR_EMPTY);

            connectorNames.add(connectorName);
         }
      }

      long broadcastPeriod = getLong(e, "broadcast-period", ActiveMQDefaultConfiguration.getDefaultBroadcastPeriod(), Validators.GT_ZERO);

      String localAddress = getString(e, "local-bind-address", null, Validators.NO_CHECK);

      int localBindPort = getInteger(e, "local-bind-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String groupAddress = getString(e, "group-address", null, Validators.NO_CHECK);

      int groupPort = getInteger(e, "group-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String jgroupsFile = getString(e, "jgroups-file", null, Validators.NO_CHECK);

      String jgroupsChannel = getString(e, "jgroups-channel", null, Validators.NO_CHECK);

      // TODO: validate if either jgroups or UDP is being filled

      BroadcastEndpointFactory endpointFactory;

      if (jgroupsFile != null) {
         endpointFactory = new JGroupsFileBroadcastEndpointFactory().setFile(jgroupsFile).setChannelName(jgroupsChannel);
      } else {
         endpointFactory = new UDPBroadcastEndpointFactory().setGroupAddress(groupAddress).setGroupPort(groupPort).setLocalBindAddress(localAddress).setLocalBindPort(localBindPort);
      }

      BroadcastGroupConfiguration config = new BroadcastGroupConfiguration().setName(name).setBroadcastPeriod(broadcastPeriod).setConnectorInfos(connectorNames).setEndpointFactory(endpointFactory);

      mainConfig.getBroadcastGroupConfigurations().add(config);
   }

   private void parseDiscoveryGroupConfiguration(final Element e, final Configuration mainConfig) {
      String name = e.getAttribute("name");

      long discoveryInitialWaitTimeout = getLong(e, "initial-wait-timeout", ActiveMQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT, Validators.GT_ZERO);

      long refreshTimeout = getLong(e, "refresh-timeout", ActiveMQDefaultConfiguration.getDefaultBroadcastRefreshTimeout(), Validators.GT_ZERO);

      String localBindAddress = getString(e, "local-bind-address", null, Validators.NO_CHECK);

      int localBindPort = getInteger(e, "local-bind-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String groupAddress = getString(e, "group-address", null, Validators.NO_CHECK);

      int groupPort = getInteger(e, "group-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String jgroupsFile = getString(e, "jgroups-file", null, Validators.NO_CHECK);

      String jgroupsChannel = getString(e, "jgroups-channel", null, Validators.NO_CHECK);

      // TODO: validate if either jgroups or UDP is being filled
      BroadcastEndpointFactory endpointFactory;
      if (jgroupsFile != null) {
         endpointFactory = new JGroupsFileBroadcastEndpointFactory().setFile(jgroupsFile).setChannelName(jgroupsChannel);
      } else {
         endpointFactory = new UDPBroadcastEndpointFactory().setGroupAddress(groupAddress).setGroupPort(groupPort).setLocalBindAddress(localBindAddress).setLocalBindPort(localBindPort);
      }

      DiscoveryGroupConfiguration config = new DiscoveryGroupConfiguration().setName(name).setRefreshTimeout(refreshTimeout).setDiscoveryInitialWaitTimeout(discoveryInitialWaitTimeout).setBroadcastEndpointFactory(endpointFactory);

      if (mainConfig.getDiscoveryGroupConfigurations().containsKey(name)) {
         ActiveMQServerLogger.LOGGER.discoveryGroupAlreadyDeployed(name);

         return;
      } else {
         mainConfig.getDiscoveryGroupConfigurations().put(name, config);
      }
   }

   private void parseClusterConnectionConfigurationURI(final Element e,
                                                       final Configuration mainConfig) throws Exception {
      String name = e.getAttribute("name");

      String uri = e.getAttribute("address");

      ClusterConnectionConfiguration config = mainConfig.addClusterConfiguration(name, uri);

      System.out.println("Adding cluster connection :: " + config);
   }

   private void parseClusterConnectionConfiguration(final Element e, final Configuration mainConfig) throws Exception {
      String name = e.getAttribute("name");

      String address = getString(e, "address", "", Validators.NO_CHECK);

      String connectorName = getString(e, "connector-ref", null, Validators.NOT_NULL_OR_EMPTY);

      if (!mainConfig.getConnectorConfigurations().containsKey(connectorName)) {
         ActiveMQServerLogger.LOGGER.connectorRefNotFound(connectorName, name);
         return;
      }

      boolean duplicateDetection = getBoolean(e, "use-duplicate-detection", ActiveMQDefaultConfiguration.isDefaultClusterDuplicateDetection());

      MessageLoadBalancingType messageLoadBalancingType;

      if (parameterExists(e, "forward-when-no-consumers")) {
         boolean forwardWhenNoConsumers = getBoolean(e, "forward-when-no-consumers", ActiveMQDefaultConfiguration.isDefaultClusterForwardWhenNoConsumers());
         if (forwardWhenNoConsumers) {
            messageLoadBalancingType = MessageLoadBalancingType.STRICT;
         } else {
            messageLoadBalancingType = MessageLoadBalancingType.ON_DEMAND;
         }
      } else {

         messageLoadBalancingType = Enum.valueOf(MessageLoadBalancingType.class, getString(e, "message-load-balancing", ActiveMQDefaultConfiguration.getDefaultClusterMessageLoadBalancingType(), Validators.MESSAGE_LOAD_BALANCING_TYPE));
      }

      int maxHops = getInteger(e, "max-hops", ActiveMQDefaultConfiguration.getDefaultClusterMaxHops(), Validators.GE_ZERO);

      long clientFailureCheckPeriod = getLong(e, "check-period", ActiveMQDefaultConfiguration.getDefaultClusterFailureCheckPeriod(), Validators.GT_ZERO);

      long connectionTTL = getLong(e, "connection-ttl", ActiveMQDefaultConfiguration.getDefaultClusterConnectionTtl(), Validators.GT_ZERO);

      long retryInterval = getLong(e, "retry-interval", ActiveMQDefaultConfiguration.getDefaultClusterRetryInterval(), Validators.GT_ZERO);

      long callTimeout = getLong(e, "call-timeout", ActiveMQClient.DEFAULT_CALL_TIMEOUT, Validators.GT_ZERO);

      long callFailoverTimeout = getLong(e, "call-failover-timeout", ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT, Validators.MINUS_ONE_OR_GT_ZERO);

      double retryIntervalMultiplier = getDouble(e, "retry-interval-multiplier", ActiveMQDefaultConfiguration.getDefaultClusterRetryIntervalMultiplier(), Validators.GT_ZERO);

      int minLargeMessageSize = getTextBytesAsIntBytes(e, "min-large-message-size", ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, Validators.GT_ZERO);

      long maxRetryInterval = getLong(e, "max-retry-interval", ActiveMQDefaultConfiguration.getDefaultClusterMaxRetryInterval(), Validators.GT_ZERO);

      int initialConnectAttempts = getInteger(e, "initial-connect-attempts", ActiveMQDefaultConfiguration.getDefaultClusterInitialConnectAttempts(), Validators.MINUS_ONE_OR_GE_ZERO);

      int reconnectAttempts = getInteger(e, "reconnect-attempts", ActiveMQDefaultConfiguration.getDefaultClusterReconnectAttempts(), Validators.MINUS_ONE_OR_GE_ZERO);

      int confirmationWindowSize = getTextBytesAsIntBytes(e, "confirmation-window-size", ActiveMQDefaultConfiguration.getDefaultClusterConfirmationWindowSize(), Validators.GT_ZERO);

      int producerWindowSize = getTextBytesAsIntBytes(e, "producer-window-size", ActiveMQDefaultConfiguration.getDefaultBridgeProducerWindowSize(), Validators.MINUS_ONE_OR_GT_ZERO);

      long clusterNotificationInterval = getLong(e, "notification-interval", ActiveMQDefaultConfiguration.getDefaultClusterNotificationInterval(), Validators.GT_ZERO);

      int clusterNotificationAttempts = getInteger(e, "notification-attempts", ActiveMQDefaultConfiguration.getDefaultClusterNotificationAttempts(), Validators.GT_ZERO);

      String scaleDownConnector = e.getAttribute("scale-down-connector");

      String discoveryGroupName = null;

      List<String> staticConnectorNames = new ArrayList<>();

      boolean allowDirectConnectionsOnly = false;

      NodeList children = e.getChildNodes();

      for (int j = 0; j < children.getLength(); j++) {
         Node child = children.item(j);

         if (child.getNodeName().equals("discovery-group-ref")) {
            discoveryGroupName = child.getAttributes().getNamedItem("discovery-group-name").getNodeValue();
         } else if (child.getNodeName().equals("static-connectors")) {
            Node attr = child.getAttributes().getNamedItem("allow-direct-connections-only");
            if (attr != null) {
               allowDirectConnectionsOnly = "true".equalsIgnoreCase(attr.getNodeValue()) || allowDirectConnectionsOnly;
            }
            getStaticConnectors(staticConnectorNames, child);
         }
      }

      ClusterConnectionConfiguration config = new ClusterConnectionConfiguration().setName(name).setAddress(address).setConnectorName(connectorName).setMinLargeMessageSize(minLargeMessageSize).setClientFailureCheckPeriod(clientFailureCheckPeriod).setConnectionTTL(connectionTTL).setRetryInterval(retryInterval).setRetryIntervalMultiplier(retryIntervalMultiplier).setMaxRetryInterval(maxRetryInterval).setInitialConnectAttempts(initialConnectAttempts).setReconnectAttempts(reconnectAttempts).setCallTimeout(callTimeout).setCallFailoverTimeout(callFailoverTimeout).setDuplicateDetection(duplicateDetection).setMessageLoadBalancingType(messageLoadBalancingType).setMaxHops(maxHops).setConfirmationWindowSize(confirmationWindowSize).setProducerWindowSize(producerWindowSize).setAllowDirectConnectionsOnly(allowDirectConnectionsOnly).setClusterNotificationInterval(clusterNotificationInterval).setClusterNotificationAttempts(clusterNotificationAttempts);

      if (discoveryGroupName == null) {
         config.setStaticConnectors(staticConnectorNames);
      } else {
         config.setDiscoveryGroupName(discoveryGroupName);
      }

      mainConfig.getClusterConfigurations().add(config);
   }

   private void parseGroupingHandlerConfiguration(final Element node, final Configuration mainConfiguration) {
      String name = node.getAttribute("name");
      String type = getString(node, "type", null, Validators.NOT_NULL_OR_EMPTY);
      String address = getString(node, "address", "", Validators.NO_CHECK);
      Integer timeout = getInteger(node, "timeout", ActiveMQDefaultConfiguration.getDefaultGroupingHandlerTimeout(), Validators.GT_ZERO);
      Long groupTimeout = getLong(node, "group-timeout", ActiveMQDefaultConfiguration.getDefaultGroupingHandlerGroupTimeout(), Validators.MINUS_ONE_OR_GT_ZERO);
      Long reaperPeriod = getLong(node, "reaper-period", ActiveMQDefaultConfiguration.getDefaultGroupingHandlerReaperPeriod(), Validators.GT_ZERO);
      mainConfiguration.setGroupingHandlerConfiguration(new GroupingHandlerConfiguration().setName(new SimpleString(name)).setType(type.equals(GroupingHandlerConfiguration.TYPE.LOCAL.getType()) ? GroupingHandlerConfiguration.TYPE.LOCAL : GroupingHandlerConfiguration.TYPE.REMOTE).setAddress(new SimpleString(address)).setTimeout(timeout).setGroupTimeout(groupTimeout).setReaperPeriod(reaperPeriod));
   }

   private TransformerConfiguration getTransformerConfiguration(final Node node) {
      Element element = (Element) node;
      String className = getString(element, "class-name", null, Validators.NO_CHECK);

      Map<String, String> properties = getMapOfChildPropertyElements(element);
      return new TransformerConfiguration(className).setProperties(properties);
   }

   private TransformerConfiguration getTransformerConfiguration(final String transformerClassName) {
      return new TransformerConfiguration(transformerClassName).setProperties(Collections.EMPTY_MAP);
   }

   private void parseBridgeConfiguration(final Element brNode, final Configuration mainConfig) throws Exception {
      String name = brNode.getAttribute("name");

      String queueName = getString(brNode, "queue-name", null, Validators.NOT_NULL_OR_EMPTY);

      String forwardingAddress = getString(brNode, "forwarding-address", null, Validators.NO_CHECK);

      String transformerClassName = getString(brNode, "transformer-class-name", null, Validators.NO_CHECK);

      // Default bridge conf
      int confirmationWindowSize = getTextBytesAsIntBytes(brNode, "confirmation-window-size", ActiveMQDefaultConfiguration.getDefaultBridgeConfirmationWindowSize(), Validators.GT_ZERO);

      int producerWindowSize = getTextBytesAsIntBytes(brNode, "producer-window-size", ActiveMQDefaultConfiguration.getDefaultBridgeConfirmationWindowSize(), Validators.GT_ZERO);

      long retryInterval = getLong(brNode, "retry-interval", ActiveMQClient.DEFAULT_RETRY_INTERVAL, Validators.GT_ZERO);

      long clientFailureCheckPeriod = getLong(brNode, "check-period", ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD, Validators.GT_ZERO);

      long connectionTTL = getLong(brNode, "connection-ttl", ActiveMQClient.DEFAULT_CONNECTION_TTL, Validators.GT_ZERO);

      int minLargeMessageSize = getTextBytesAsIntBytes(brNode, "min-large-message-size", ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, Validators.GT_ZERO);

      long maxRetryInterval = getLong(brNode, "max-retry-interval", ActiveMQClient.DEFAULT_MAX_RETRY_INTERVAL, Validators.GT_ZERO);

      double retryIntervalMultiplier = getDouble(brNode, "retry-interval-multiplier", ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER, Validators.GT_ZERO);

      int initialConnectAttempts = getInteger(brNode, "initial-connect-attempts", ActiveMQDefaultConfiguration.getDefaultBridgeInitialConnectAttempts(), Validators.MINUS_ONE_OR_GE_ZERO);

      int reconnectAttempts = getInteger(brNode, "reconnect-attempts", ActiveMQDefaultConfiguration.getDefaultBridgeReconnectAttempts(), Validators.MINUS_ONE_OR_GE_ZERO);

      int reconnectAttemptsSameNode = getInteger(brNode, "reconnect-attempts-same-node", ActiveMQDefaultConfiguration.getDefaultBridgeConnectSameNode(), Validators.MINUS_ONE_OR_GE_ZERO);

      boolean useDuplicateDetection = getBoolean(brNode, "use-duplicate-detection", ActiveMQDefaultConfiguration.isDefaultBridgeDuplicateDetection());

      String user = getString(brNode, "user", ActiveMQDefaultConfiguration.getDefaultClusterUser(), Validators.NO_CHECK);

      NodeList clusterPassNodes = brNode.getElementsByTagName("password");
      String password = null;

      if (clusterPassNodes.getLength() > 0) {
         Node passNode = clusterPassNodes.item(0);
         password = passNode.getTextContent();
      }

      if (password != null) {
         password = PasswordMaskingUtil.resolveMask(mainConfig.isMaskPassword(), password, mainConfig.getPasswordCodec());
      } else {
         password = ActiveMQDefaultConfiguration.getDefaultClusterPassword();
      }

      boolean ha = getBoolean(brNode, "ha", false);

      TransformerConfiguration transformerConfiguration = null;

      String filterString = null;

      List<String> staticConnectorNames = new ArrayList<>();

      String discoveryGroupName = null;

      NodeList children = brNode.getChildNodes();

      for (int j = 0; j < children.getLength(); j++) {
         Node child = children.item(j);

         if (child.getNodeName().equals("filter")) {
            filterString = child.getAttributes().getNamedItem("string").getNodeValue();
         } else if (child.getNodeName().equals("discovery-group-ref")) {
            discoveryGroupName = child.getAttributes().getNamedItem("discovery-group-name").getNodeValue();
         } else if (child.getNodeName().equals("static-connectors")) {
            getStaticConnectors(staticConnectorNames, child);
         } else if (child.getNodeName().equals("transformer")) {
            transformerConfiguration = getTransformerConfiguration(child);
         }
      }

      if (transformerConfiguration == null && transformerClassName != null) {
         transformerConfiguration = getTransformerConfiguration(transformerClassName);
      }

      BridgeConfiguration config = new BridgeConfiguration().setName(name).setQueueName(queueName).setForwardingAddress(forwardingAddress).setFilterString(filterString).setTransformerConfiguration(transformerConfiguration).setMinLargeMessageSize(minLargeMessageSize).setClientFailureCheckPeriod(clientFailureCheckPeriod).setConnectionTTL(connectionTTL).setRetryInterval(retryInterval).setMaxRetryInterval(maxRetryInterval).setRetryIntervalMultiplier(retryIntervalMultiplier).setInitialConnectAttempts(initialConnectAttempts).setReconnectAttempts(reconnectAttempts).setReconnectAttemptsOnSameNode(reconnectAttemptsSameNode).setUseDuplicateDetection(useDuplicateDetection).setConfirmationWindowSize(confirmationWindowSize).setProducerWindowSize(producerWindowSize).setHA(ha).setUser(user).setPassword(password);

      if (!staticConnectorNames.isEmpty()) {
         config.setStaticConnectors(staticConnectorNames);
      } else {
         config.setDiscoveryGroupName(discoveryGroupName);
      }

      mainConfig.getBridgeConfigurations().add(config);
   }

   private void getStaticConnectors(List<String> staticConnectorNames, Node child) {
      NodeList children2 = ((Element) child).getElementsByTagName("connector-ref");

      for (int k = 0; k < children2.getLength(); k++) {
         Element child2 = (Element) children2.item(k);

         String connectorName = child2.getChildNodes().item(0).getNodeValue();

         staticConnectorNames.add(connectorName);
      }
   }

   private void parseDivertConfiguration(final Element e, final Configuration mainConfig) {
      String name = e.getAttribute("name");

      String routingName = getString(e, "routing-name", null, Validators.NO_CHECK);

      String address = getString(e, "address", null, Validators.NOT_NULL_OR_EMPTY);

      String forwardingAddress = getString(e, "forwarding-address", null, Validators.NOT_NULL_OR_EMPTY);

      boolean exclusive = getBoolean(e, "exclusive", ActiveMQDefaultConfiguration.isDefaultDivertExclusive());

      String transformerClassName = getString(e, "transformer-class-name", null, Validators.NO_CHECK);

      DivertConfigurationRoutingType routingType = DivertConfigurationRoutingType.valueOf(getString(e, "routing-type", ActiveMQDefaultConfiguration.getDefaultDivertRoutingType(), Validators.DIVERT_ROUTING_TYPE));

      TransformerConfiguration transformerConfiguration = null;

      String filterString = null;

      NodeList children = e.getChildNodes();

      for (int j = 0; j < children.getLength(); j++) {
         Node child = children.item(j);

         if (child.getNodeName().equals("filter")) {
            filterString = getAttributeValue(child, "string");
         } else if (child.getNodeName().equals("transformer")) {
            transformerConfiguration = getTransformerConfiguration(child);
         }
      }

      if (transformerConfiguration == null && transformerClassName != null) {
         transformerConfiguration = getTransformerConfiguration(transformerClassName);
      }

      DivertConfiguration config = new DivertConfiguration().setName(name).setRoutingName(routingName).setAddress(address).setForwardingAddress(forwardingAddress).setExclusive(exclusive).setFilterString(filterString).setTransformerConfiguration(transformerConfiguration).setRoutingType(routingType);

      mainConfig.getDivertConfigurations().add(config);
   }

   /**
    * @param e
    * @return
    */
   protected void parseWildcardConfiguration(final Element e, final Configuration mainConfig) {
      WildcardConfiguration conf = mainConfig.getWildcardConfiguration();

      conf.setDelimiter(getString(e, "delimiter", Character.toString(conf.getDelimiter()), Validators.NO_CHECK).charAt(0));
      conf.setAnyWords(getString(e, "any-words", Character.toString(conf.getAnyWords()), Validators.NO_CHECK).charAt(0));
      conf.setSingleWord(getString(e, "single-word", Character.toString(conf.getSingleWord()), Validators.NO_CHECK).charAt(0));
      conf.setRoutingEnabled(getBoolean(e, "enabled", conf.isRoutingEnabled()));
      conf.setRoutingEnabled(getBoolean(e, "routing-enabled", conf.isRoutingEnabled()));
   }

   private ConnectorServiceConfiguration parseConnectorService(final Element e) {
      Node nameNode = e.getAttributes().getNamedItem("name");

      String name = nameNode != null ? nameNode.getNodeValue() : null;

      String clazz = getString(e, "factory-class", null, Validators.NOT_NULL_OR_EMPTY);

      Map<String, Object> params = new HashMap<>();

      NodeList paramsNodes = e.getElementsByTagName("param");

      for (int i = 0; i < paramsNodes.getLength(); i++) {
         Node paramNode = paramsNodes.item(i);

         NamedNodeMap attributes = paramNode.getAttributes();

         Node nkey = attributes.getNamedItem("key");

         String key = nkey.getTextContent();

         Node nValue = attributes.getNamedItem("value");

         params.put(key, nValue.getTextContent());
      }

      return new ConnectorServiceConfiguration().setFactoryClassName(clazz).setParams(params).setName(name);
   }
}

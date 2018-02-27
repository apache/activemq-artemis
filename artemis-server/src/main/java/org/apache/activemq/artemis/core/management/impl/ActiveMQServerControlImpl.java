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
package org.apache.activemq.artemis.core.management.impl;

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
import javax.transaction.xa.Xid;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQAddressDoesNotExistException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.BridgeControl;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.DivertControl;
import org.apache.activemq.artemis.api.core.management.Parameter;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.client.impl.Topology;
import org.apache.activemq.artemis.core.client.impl.TopologyMemberImpl;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.management.impl.view.AddressView;
import org.apache.activemq.artemis.core.management.impl.view.ConnectionView;
import org.apache.activemq.artemis.core.management.impl.view.ConsumerView;
import org.apache.activemq.artemis.core.management.impl.view.ProducerView;
import org.apache.activemq.artemis.core.management.impl.view.QueueView;
import org.apache.activemq.artemis.core.management.impl.view.SessionView;
import org.apache.activemq.artemis.core.messagecounter.MessageCounterManager;
import org.apache.activemq.artemis.core.messagecounter.impl.MessageCounterManagerImpl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.config.PersistedAddressSetting;
import org.apache.activemq.artemis.core.persistence.config.PersistedRoles;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.ConnectorServiceFactory;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.DivertConfigurationRoutingType;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerProducer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.core.server.cluster.ha.HAPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.LiveOnlyPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ScaleDownPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreSlavePolicy;
import org.apache.activemq.artemis.core.server.group.GroupingHandler;
import org.apache.activemq.artemis.core.server.impl.Activation;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.SharedNothingLiveActivation;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.SlowConsumerPolicy;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionDetail;
import org.apache.activemq.artemis.core.transaction.TransactionDetailFactory;
import org.apache.activemq.artemis.core.transaction.impl.CoreTransactionDetail;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.JsonLoader;
import org.apache.activemq.artemis.utils.ListUtil;
import org.apache.activemq.artemis.utils.SecurityFormatter;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.jboss.logging.Logger;

public class ActiveMQServerControlImpl extends AbstractControl implements ActiveMQServerControl, NotificationEmitter, org.apache.activemq.artemis.core.server.management.NotificationListener {
   // Constants -----------------------------------------------------
   private static final Logger logger = Logger.getLogger(ActiveMQServerControlImpl.class);

   // Attributes ----------------------------------------------------

   private final PostOffice postOffice;

   private final Configuration configuration;

   private final ResourceManager resourceManager;

   private final RemotingService remotingService;

   private final ActiveMQServer server;

   private final MessageCounterManager messageCounterManager;

   private final NotificationBroadcasterSupport broadcaster;

   private final AtomicLong notifSeq = new AtomicLong(0);
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ActiveMQServerControlImpl(final PostOffice postOffice,
                                    final Configuration configuration,
                                    final ResourceManager resourceManager,
                                    final RemotingService remotingService,
                                    final ActiveMQServer messagingServer,
                                    final MessageCounterManager messageCounterManager,
                                    final StorageManager storageManager,
                                    final NotificationBroadcasterSupport broadcaster) throws Exception {
      super(ActiveMQServerControl.class, storageManager);
      this.postOffice = postOffice;
      this.configuration = configuration;
      this.resourceManager = resourceManager;
      this.remotingService = remotingService;
      server = messagingServer;
      this.messageCounterManager = messageCounterManager;
      this.broadcaster = broadcaster;
      server.getManagementService().addNotificationListener(this);
   }

   // ActiveMQServerControlMBean implementation --------------------

   @Override
   public boolean isStarted() {
      clearIO();
      try {
         return server.isStarted();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getVersion() {
      checkStarted();

      clearIO();
      try {
         return server.getVersion().getFullVersion();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isBackup() {
      checkStarted();

      clearIO();
      try {
         return server.getHAPolicy().isBackup();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isSharedStore() {
      checkStarted();

      clearIO();
      try {
         return server.getHAPolicy().isSharedStore();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getBindingsDirectory() {
      checkStarted();

      clearIO();
      try {
         return configuration.getBindingsDirectory();
      } finally {
         blockOnIO();
      }
   }

   public String[] getInterceptorClassNames() {
      checkStarted();

      clearIO();
      try {
         return configuration.getIncomingInterceptorClassNames().toArray(new String[configuration.getIncomingInterceptorClassNames().size()]);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getIncomingInterceptorClassNames() {
      checkStarted();

      clearIO();
      try {
         return configuration.getIncomingInterceptorClassNames().toArray(new String[configuration.getIncomingInterceptorClassNames().size()]);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getOutgoingInterceptorClassNames() {
      checkStarted();

      clearIO();
      try {
         return configuration.getOutgoingInterceptorClassNames().toArray(new String[configuration.getOutgoingInterceptorClassNames().size()]);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getJournalBufferSize() {
      checkStarted();

      clearIO();
      try {
         return configuration.getJournalType() == JournalType.ASYNCIO ? configuration.getJournalBufferSize_AIO() : configuration.getJournalBufferSize_NIO();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getJournalBufferTimeout() {
      checkStarted();

      clearIO();
      try {
         return configuration.getJournalType() == JournalType.ASYNCIO ? configuration.getJournalBufferTimeout_AIO() : configuration.getJournalBufferTimeout_NIO();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void setFailoverOnServerShutdown(boolean failoverOnServerShutdown) {
      checkStarted();

      clearIO();
      try {
         HAPolicy haPolicy = server.getHAPolicy();
         if (haPolicy instanceof SharedStoreSlavePolicy) {
            ((SharedStoreSlavePolicy) haPolicy).setFailoverOnServerShutdown(failoverOnServerShutdown);
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isFailoverOnServerShutdown() {
      checkStarted();

      clearIO();
      try {
         HAPolicy haPolicy = server.getHAPolicy();
         if (haPolicy instanceof SharedStoreSlavePolicy) {
            return ((SharedStoreSlavePolicy) haPolicy).isFailoverOnServerShutdown();
         } else {
            return false;
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getJournalMaxIO() {
      checkStarted();

      clearIO();
      try {
         return configuration.getJournalType() == JournalType.ASYNCIO ? configuration.getJournalMaxIO_AIO() : configuration.getJournalMaxIO_NIO();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getJournalDirectory() {
      checkStarted();

      clearIO();
      try {
         return configuration.getJournalDirectory();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getJournalFileSize() {
      checkStarted();

      clearIO();
      try {
         return configuration.getJournalFileSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getJournalMinFiles() {
      checkStarted();

      clearIO();
      try {
         return configuration.getJournalMinFiles();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getJournalCompactMinFiles() {
      checkStarted();

      clearIO();
      try {
         return configuration.getJournalCompactMinFiles();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getJournalCompactPercentage() {
      checkStarted();

      clearIO();
      try {
         return configuration.getJournalCompactPercentage();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isPersistenceEnabled() {
      checkStarted();

      clearIO();
      try {
         return configuration.isPersistenceEnabled();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getJournalType() {
      checkStarted();

      clearIO();
      try {
         return configuration.getJournalType().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getPagingDirectory() {
      checkStarted();

      clearIO();
      try {
         return configuration.getPagingDirectory();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getScheduledThreadPoolMaxSize() {
      checkStarted();

      clearIO();
      try {
         return configuration.getScheduledThreadPoolMaxSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getThreadPoolMaxSize() {
      checkStarted();

      clearIO();
      try {
         return configuration.getThreadPoolMaxSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getSecurityInvalidationInterval() {
      checkStarted();

      clearIO();
      try {
         return configuration.getSecurityInvalidationInterval();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isClustered() {
      checkStarted();

      clearIO();
      try {
         return configuration.isClustered();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isCreateBindingsDir() {
      checkStarted();

      clearIO();
      try {
         return configuration.isCreateBindingsDir();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isCreateJournalDir() {
      checkStarted();

      clearIO();
      try {
         return configuration.isCreateJournalDir();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isJournalSyncNonTransactional() {
      checkStarted();

      clearIO();
      try {
         return configuration.isJournalSyncNonTransactional();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isJournalSyncTransactional() {
      checkStarted();

      clearIO();
      try {
         return configuration.isJournalSyncTransactional();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isSecurityEnabled() {
      checkStarted();

      clearIO();
      try {
         return configuration.isSecurityEnabled();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isAsyncConnectionExecutionEnabled() {
      checkStarted();

      clearIO();
      try {
         return configuration.isAsyncConnectionExecutionEnabled();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getDiskScanPeriod() {
      checkStarted();

      clearIO();
      try {
         return configuration.getDiskScanPeriod();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getMaxDiskUsage() {
      checkStarted();

      clearIO();
      try {
         return configuration.getMaxDiskUsage();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getGlobalMaxSize() {
      checkStarted();

      clearIO();
      try {
         return configuration.getGlobalMaxSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getAddressMemoryUsage() {
      checkStarted();
      clearIO();
      try {
         //this should not happen but if it does, return -1 to highlight it is not working
         if (server.getPagingManager() == null) {
            return -1L;
         }
         return server.getPagingManager().getGlobalSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getAddressMemoryUsagePercentage() {
      long globalMaxSize = getGlobalMaxSize();
      // no max size set implies 0% used
      if (globalMaxSize <= 0) {
         return 0;
      }

      long memoryUsed = getAddressMemoryUsage();
      if (memoryUsed <= 0) {
         return 0;
      }

      double result = (100D * memoryUsed) / globalMaxSize;
      return (int) result;
   }

   @Override
   public boolean freezeReplication() {
      Activation activation = server.getActivation();
      if (activation instanceof SharedNothingLiveActivation) {
         SharedNothingLiveActivation liveActivation = (SharedNothingLiveActivation) activation;
         liveActivation.freezeReplication();
         return true;
      }
      return false;
   }

   private enum AddressInfoTextFormatter {
      Long {
         @Override
         public StringBuilder format(AddressInfo addressInfo, StringBuilder output) {
            output.append("Address [name=").append(addressInfo.getName());
            output.append(", routingTypes={");
            final EnumSet<RoutingType> routingTypes = addressInfo.getRoutingTypes();
            if (!routingTypes.isEmpty()) {
               for (RoutingType routingType : routingTypes) {
                  output.append(routingType).append(',');
               }
               // delete hanging comma
               output.deleteCharAt(output.length() - 1);
            }
            output.append('}');
            output.append(", autoCreated=").append(addressInfo.isAutoCreated());
            output.append(']');
            return output;
         }
      };

      public abstract StringBuilder format(AddressInfo addressInfo, StringBuilder output);
   }

   public enum QueueTextFormatter {
      Long {
         @Override
         StringBuilder format(Queue queue, StringBuilder output) {
            output.append("Queue [name=").append(queue.getName());
            output.append(", address=").append(queue.getAddress());
            output.append(", routingType=").append(queue.getRoutingType());
            final Filter filter = queue.getFilter();
            if (filter != null) {
               output.append(", filter=").append(filter.getFilterString());
            }
            output.append(", durable=").append(queue.isDurable());
            final int maxConsumers = queue.getMaxConsumers();
            if (maxConsumers != Queue.MAX_CONSUMERS_UNLIMITED) {
               output.append(", maxConsumers=").append(queue.getMaxConsumers());
            }
            output.append(", purgeOnNoConsumers=").append(queue.isPurgeOnNoConsumers());
            output.append(", autoCreateAddress=").append(queue.isAutoCreated());
            output.append(']');
            return output;
         }
      };

      abstract StringBuilder format(Queue queue, StringBuilder output);
   }

   @Override
   public String createAddress(String name, String routingTypes) throws Exception {
      checkStarted();

      clearIO();
      try {
         EnumSet<RoutingType> set = EnumSet.noneOf(RoutingType.class);
         for (String routingType : ListUtil.toList(routingTypes)) {
            set.add(RoutingType.valueOf(routingType));
         }
         final AddressInfo addressInfo = new AddressInfo(new SimpleString(name), set);
         if (server.addAddressInfo(addressInfo)) {
            return AddressInfoTextFormatter.Long.format(addressInfo, new StringBuilder()).toString();
         } else {
            throw ActiveMQMessageBundle.BUNDLE.addressAlreadyExists(addressInfo.getName());
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String updateAddress(String name, String routingTypes) throws Exception {
      checkStarted();

      clearIO();
      try {
         final EnumSet<RoutingType> routingTypeSet;
         if (routingTypes == null) {
            routingTypeSet = null;
         } else {
            routingTypeSet = EnumSet.noneOf(RoutingType.class);
            final String[] routingTypeNames = routingTypes.split(",");
            for (String routingTypeName : routingTypeNames) {
               routingTypeSet.add(RoutingType.valueOf(routingTypeName));
            }
         }
         if (!server.updateAddressInfo(SimpleString.toSimpleString(name), routingTypeSet)) {
            throw ActiveMQMessageBundle.BUNDLE.addressDoesNotExist(SimpleString.toSimpleString(name));
         }
         return AddressInfoTextFormatter.Long.format(server.getAddressInfo(SimpleString.toSimpleString(name)), new StringBuilder()).toString();
      } finally {
         blockOnIO();
      }
   }


   @Override
   public void deleteAddress(String name) throws Exception {
      deleteAddress(name, false);
   }

   @Override
   public void deleteAddress(String name, boolean force) throws Exception {
      checkStarted();

      clearIO();
      try {
         server.removeAddressInfo(new SimpleString(name), null, force);
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Deprecated
   @Override
   public void deployQueue(final String address, final String name, final String filterString) throws Exception {
      deployQueue(address, name, filterString, true);
   }

   @Deprecated
   @Override
   public void deployQueue(final String address,
                           final String name,
                           final String filterStr,
                           final boolean durable) throws Exception {
      checkStarted();

      SimpleString filter = filterStr == null ? null : new SimpleString(filterStr);
      clearIO();
      try {
         server.createQueue(SimpleString.toSimpleString(address), server.getAddressSettingsRepository().getMatch(address).getDefaultQueueRoutingType(), new SimpleString(name), filter, durable, false);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void createQueue(final String address, final String name) throws Exception {
      createQueue(address, name, true);
   }

   @Override
   public void createQueue(final String address, final String name, final String routingType) throws Exception {
      createQueue(address, name, true, routingType);
   }

   @Override
   public void createQueue(final String address, final String name, final boolean durable) throws Exception {
      createQueue(address, name, null, durable);
   }

   @Override
   public void createQueue(final String address, final String name, final boolean durable, final String routingType) throws Exception {
      createQueue(address, name, null, durable, routingType);
   }

   @Override
   public void createQueue(final String address, final String name, final String filterStr, final boolean durable) throws Exception {
      createQueue(address, name, filterStr, durable, server.getAddressSettingsRepository().getMatch(address == null ? name : address).getDefaultQueueRoutingType().toString());
   }


   @Override
   public void createQueue(final String address, final String name, final String filterStr, final boolean durable, final String routingType) throws Exception {
      AddressSettings addressSettings = server.getAddressSettingsRepository().getMatch(address == null ? name : address);
      createQueue(address, routingType, name, filterStr, durable, addressSettings.getDefaultMaxConsumers(), addressSettings.isDefaultPurgeOnNoConsumers(), addressSettings.isAutoCreateAddresses());
   }

   @Override
   public String createQueue(String address,
                             String routingType,
                             String name,
                             String filterStr,
                             boolean durable,
                             int maxConsumers,
                             boolean purgeOnNoConsumers,
                             boolean autoCreateAddress) throws Exception {
      checkStarted();

      clearIO();

      SimpleString filter = filterStr == null ? null : new SimpleString(filterStr);
      try {
         if (filterStr != null && !filterStr.trim().equals("")) {
            filter = new SimpleString(filterStr);
         }

         final Queue queue = server.createQueue(SimpleString.toSimpleString(address), RoutingType.valueOf(routingType.toUpperCase()), new SimpleString(name), filter, durable, false, maxConsumers, purgeOnNoConsumers, autoCreateAddress);
         return QueueTextFormatter.Long.format(queue, new StringBuilder()).toString();
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String updateQueue(String name,
                             String routingType,
                             Integer maxConsumers,
                             Boolean purgeOnNoConsumers) throws Exception {
      return updateQueue(name, routingType, maxConsumers, purgeOnNoConsumers, null);
   }

   @Override
   public String updateQueue(String name,
                             String routingType,
                             Integer maxConsumers,
                             Boolean purgeOnNoConsumers,
                             Boolean exclusive) throws Exception {
      checkStarted();

      clearIO();

      try {
         final Queue queue = server.updateQueue(name, routingType != null ? RoutingType.valueOf(routingType) : null, maxConsumers, purgeOnNoConsumers, exclusive);
         if (queue == null) {
            throw ActiveMQMessageBundle.BUNDLE.noSuchQueue(new SimpleString(name));
         }
         return QueueTextFormatter.Long.format(queue, new StringBuilder()).toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getQueueNames() {
      return getQueueNames(null);
   }

   @Override
   public String[] getQueueNames(String routingType) {
      checkStarted();

      clearIO();
      try {
         Object[] queueControls = server.getManagementService().getResources(QueueControl.class);
         List<String> names = new ArrayList<>();
         for (int i = 0; i < queueControls.length; i++) {
            QueueControl queueControl = (QueueControl) queueControls[i];
            if (routingType != null && queueControl.getRoutingType().equals(routingType.toUpperCase())) {
               names.add(queueControl.getName());
            } else if (routingType == null) {
               names.add(queueControl.getName());
            }
         }

         String[] result = new String[names.size()];
         return names.toArray(result);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getUptime() {
      checkStarted();

      clearIO();
      try {
         return server.getUptime();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getUptimeMillis() {
      checkStarted();

      clearIO();
      try {
         return server.getUptimeMillis();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isReplicaSync() {
      checkStarted();

      clearIO();
      try {
         return server.isReplicaSync();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getAddressNames() {
      checkStarted();

      clearIO();
      try {
         Object[] addresses = server.getManagementService().getResources(AddressControl.class);
         String[] names = new String[addresses.length];
         for (int i = 0; i < addresses.length; i++) {
            AddressControl address = (AddressControl) addresses[i];
            names[i] = address.getAddress();
         }

         return names;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void destroyQueue(final String name, final boolean removeConsumers, final boolean autoDeleteAddress) throws Exception {
      checkStarted();

      clearIO();
      try {
         SimpleString queueName = new SimpleString(name);
         server.destroyQueue(queueName, null, !removeConsumers, removeConsumers, autoDeleteAddress);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void destroyQueue(final String name, final boolean removeConsumers) throws Exception {
      destroyQueue(name, removeConsumers, false);
   }

   @Override
   public void destroyQueue(final String name) throws Exception {
      destroyQueue(name, false);
   }

   @Override
   public String getAddressInfo(String address) throws ActiveMQAddressDoesNotExistException {
      checkStarted();

      clearIO();
      try {
         final AddressInfo addressInfo = server.getAddressInfo(SimpleString.toSimpleString(address));
         if (addressInfo == null) {
            throw ActiveMQMessageBundle.BUNDLE.addressDoesNotExist(SimpleString.toSimpleString(address));
         } else {
            return AddressInfoTextFormatter.Long.format(addressInfo, new StringBuilder()).toString();
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listBindingsForAddress(String address) throws Exception {
      checkStarted();

      clearIO();
      try {
         final Bindings bindings = server.getPostOffice().getBindingsForAddress(new SimpleString(address));
         return bindings.getBindings().stream().map(Binding::toManagementString).collect(Collectors.joining(","));
      } finally {
         blockOnIO();
      }
   }


   @Override
   public String listAddresses(String separator) throws Exception {
      checkStarted();

      clearIO();
      try {
         final Set<SimpleString> addresses = server.getPostOffice().getAddresses();
         TreeSet<SimpleString> sortAddress = new TreeSet<>(new Comparator<SimpleString>() {
            @Override
            public int compare(SimpleString o1, SimpleString o2) {
               return o1.toString().compareToIgnoreCase(o2.toString());
            }
         });

         sortAddress.addAll(addresses);

         StringBuilder result = new StringBuilder();
         for (SimpleString string : sortAddress) {
            if (result.length() > 0) {
               result.append(separator);
            }
            result.append(string);
         }

         return result.toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getConnectionCount() {
      checkStarted();

      clearIO();
      try {
         return server.getConnectionCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getTotalConnectionCount() {
      checkStarted();

      clearIO();
      try {
         return server.getTotalConnectionCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getTotalMessageCount() {
      checkStarted();

      clearIO();
      try {
         return server.getTotalMessageCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getTotalMessagesAdded() {
      checkStarted();

      clearIO();
      try {
         return server.getTotalMessagesAdded();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getTotalMessagesAcknowledged() {
      checkStarted();

      clearIO();
      try {
         return server.getTotalMessagesAcknowledged();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getTotalConsumerCount() {
      checkStarted();

      clearIO();
      try {
         return server.getTotalConsumerCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void enableMessageCounters() {
      checkStarted();

      clearIO();
      try {
         setMessageCounterEnabled(true);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void disableMessageCounters() {
      checkStarted();

      clearIO();
      try {
         setMessageCounterEnabled(false);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void resetAllMessageCounters() {
      checkStarted();

      clearIO();
      try {
         messageCounterManager.resetAllCounters();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void resetAllMessageCounterHistories() {
      checkStarted();

      clearIO();
      try {
         messageCounterManager.resetAllCounterHistories();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isMessageCounterEnabled() {
      checkStarted();

      clearIO();
      try {
         return configuration.isMessageCounterEnabled();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public synchronized long getMessageCounterSamplePeriod() {
      checkStarted();

      clearIO();
      try {
         return messageCounterManager.getSamplePeriod();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public synchronized void setMessageCounterSamplePeriod(final long newPeriod) {
      checkStarted();

      checkStarted();

      clearIO();
      try {
         if (newPeriod < MessageCounterManagerImpl.MIN_SAMPLE_PERIOD) {
            if (newPeriod <= 0) {
               throw ActiveMQMessageBundle.BUNDLE.periodMustGreaterThanZero(newPeriod);
            }
            ActiveMQServerLogger.LOGGER.invalidMessageCounterPeriod(newPeriod);
         }

         if (messageCounterManager != null && newPeriod != messageCounterManager.getSamplePeriod()) {
            messageCounterManager.reschedule(newPeriod);
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getMessageCounterMaxDayCount() {
      checkStarted();

      clearIO();
      try {
         return messageCounterManager.getMaxDayCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void setMessageCounterMaxDayCount(final int count) {
      checkStarted();

      clearIO();
      try {
         if (count <= 0) {
            throw ActiveMQMessageBundle.BUNDLE.greaterThanZero(count);
         }
         messageCounterManager.setMaxDayCount(count);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] listPreparedTransactions() {
      checkStarted();

      clearIO();
      try {
         DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);

         Map<Xid, Long> xids = resourceManager.getPreparedTransactionsWithCreationTime();
         ArrayList<Entry<Xid, Long>> xidsSortedByCreationTime = new ArrayList<>(xids.entrySet());
         Collections.sort(xidsSortedByCreationTime, new Comparator<Entry<Xid, Long>>() {
            @Override
            public int compare(final Entry<Xid, Long> entry1, final Entry<Xid, Long> entry2) {
               // sort by creation time, oldest first
               return (int) (entry1.getValue() - entry2.getValue());
            }
         });
         String[] s = new String[xidsSortedByCreationTime.size()];
         int i = 0;
         for (Map.Entry<Xid, Long> entry : xidsSortedByCreationTime) {
            Date creation = new Date(entry.getValue());
            Xid xid = entry.getKey();
            s[i++] = dateFormat.format(creation) + " base64: " + XidImpl.toBase64String(xid) + " " + xid.toString();
         }
         return s;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listPreparedTransactionDetailsAsJSON() throws Exception {
      return listPreparedTransactionDetailsAsJSON((xid, tx, creation) -> new CoreTransactionDetail(xid, tx, creation));
   }

   public String listPreparedTransactionDetailsAsJSON(TransactionDetailFactory factory) throws Exception {
      checkStarted();

      clearIO();
      try {
         Map<Xid, Long> xids = resourceManager.getPreparedTransactionsWithCreationTime();
         if (xids == null || xids.size() == 0) {
            return "";
         }

         ArrayList<Entry<Xid, Long>> xidsSortedByCreationTime = new ArrayList<>(xids.entrySet());
         Collections.sort(xidsSortedByCreationTime, new Comparator<Entry<Xid, Long>>() {
            @Override
            public int compare(final Entry<Xid, Long> entry1, final Entry<Xid, Long> entry2) {
               // sort by creation time, oldest first
               return (int) (entry1.getValue() - entry2.getValue());
            }
         });

         JsonArrayBuilder txDetailListJson = JsonLoader.createArrayBuilder();
         for (Map.Entry<Xid, Long> entry : xidsSortedByCreationTime) {
            Xid xid = entry.getKey();

            Transaction tx = resourceManager.getTransaction(xid);

            if (tx == null) {
               continue;
            }

            TransactionDetail detail = factory.createTransactionDetail(xid, tx, entry.getValue());

            txDetailListJson.add(detail.toJSON());
         }
         return txDetailListJson.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listPreparedTransactionDetailsAsHTML() throws Exception {
      return listPreparedTransactionDetailsAsHTML((xid, tx, creation) -> new CoreTransactionDetail(xid, tx, creation));
   }

   public String listPreparedTransactionDetailsAsHTML(TransactionDetailFactory factory) throws Exception {
      checkStarted();

      clearIO();
      try {
         Map<Xid, Long> xids = resourceManager.getPreparedTransactionsWithCreationTime();
         if (xids == null || xids.size() == 0) {
            return "<h3>*** Prepared Transaction Details ***</h3><p>No entry.</p>";
         }

         ArrayList<Entry<Xid, Long>> xidsSortedByCreationTime = new ArrayList<>(xids.entrySet());
         Collections.sort(xidsSortedByCreationTime, new Comparator<Entry<Xid, Long>>() {
            @Override
            public int compare(final Entry<Xid, Long> entry1, final Entry<Xid, Long> entry2) {
               // sort by creation time, oldest first
               return (int) (entry1.getValue() - entry2.getValue());
            }
         });

         StringBuilder html = new StringBuilder();
         html.append("<h3>*** Prepared Transaction Details ***</h3>");

         for (Map.Entry<Xid, Long> entry : xidsSortedByCreationTime) {
            Xid xid = entry.getKey();

            Transaction tx = resourceManager.getTransaction(xid);

            if (tx == null) {
               continue;
            }

            TransactionDetail detail = factory.createTransactionDetail(xid, tx, entry.getValue());

            JsonObject txJson = detail.toJSON();

            html.append("<table border=\"1\">");
            html.append("<tr><th>creation_time</th>");
            html.append("<td>" + txJson.get(TransactionDetail.KEY_CREATION_TIME) + "</td>");
            html.append("<th>xid_as_base_64</th>");
            html.append("<td colspan=\"3\">" + txJson.get(TransactionDetail.KEY_XID_AS_BASE64) + "</td></tr>");
            html.append("<tr><th>xid_format_id</th>");
            html.append("<td>" + txJson.get(TransactionDetail.KEY_XID_FORMAT_ID) + "</td>");
            html.append("<th>xid_global_txid</th>");
            html.append("<td>" + txJson.get(TransactionDetail.KEY_XID_GLOBAL_TXID) + "</td>");
            html.append("<th>xid_branch_qual</th>");
            html.append("<td>" + txJson.get(TransactionDetail.KEY_XID_BRANCH_QUAL) + "</td></tr>");

            html.append("<tr><th colspan=\"6\">Message List</th></tr>");
            html.append("<tr><td colspan=\"6\">");
            html.append("<table border=\"1\" cellspacing=\"0\" cellpadding=\"0\">");

            JsonArray msgs = txJson.getJsonArray(TransactionDetail.KEY_TX_RELATED_MESSAGES);
            for (int i = 0; i < msgs.size(); i++) {
               JsonObject msgJson = msgs.getJsonObject(i);
               JsonObject props = msgJson.getJsonObject(TransactionDetail.KEY_MSG_PROPERTIES);
               StringBuilder propstr = new StringBuilder();
               Set<String> keys = props.keySet();
               for (String key : keys) {
                  propstr.append(key);
                  propstr.append("=");
                  propstr.append(props.get(key));
                  propstr.append(", ");
               }

               html.append("<th>operation_type</th>");
               html.append("<td>" + msgJson.get(TransactionDetail.KEY_MSG_OP_TYPE) + "</th>");
               html.append("<th>message_type</th>");
               html.append("<td>" + msgJson.get(TransactionDetail.KEY_MSG_TYPE) + "</td></tr>");
               html.append("<tr><th>properties</th>");
               html.append("<td colspan=\"3\">" + propstr.toString() + "</td></tr>");
            }
            html.append("</table></td></tr>");
            html.append("</table><br>");
         }

         return html.toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] listHeuristicCommittedTransactions() {
      checkStarted();

      clearIO();
      try {
         List<Xid> xids = resourceManager.getHeuristicCommittedTransactions();
         String[] s = new String[xids.size()];
         int i = 0;
         for (Xid xid : xids) {
            s[i++] = XidImpl.toBase64String(xid);
         }
         return s;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] listHeuristicRolledBackTransactions() {
      checkStarted();

      clearIO();
      try {
         List<Xid> xids = resourceManager.getHeuristicRolledbackTransactions();
         String[] s = new String[xids.size()];
         int i = 0;
         for (Xid xid : xids) {
            s[i++] = XidImpl.toBase64String(xid);
         }
         return s;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public synchronized boolean commitPreparedTransaction(final String transactionAsBase64) throws Exception {
      checkStarted();

      clearIO();
      try {
         List<Xid> xids = resourceManager.getPreparedTransactions();

         for (Xid xid : xids) {
            if (XidImpl.toBase64String(xid).equals(transactionAsBase64)) {
               Transaction transaction = resourceManager.removeTransaction(xid);
               transaction.commit(false);
               long recordID = server.getStorageManager().storeHeuristicCompletion(xid, true);
               storageManager.waitOnOperations();
               resourceManager.putHeuristicCompletion(recordID, xid, true);
               return true;
            }
         }
         return false;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public synchronized boolean rollbackPreparedTransaction(final String transactionAsBase64) throws Exception {
      checkStarted();

      clearIO();
      try {

         List<Xid> xids = resourceManager.getPreparedTransactions();

         for (Xid xid : xids) {
            if (XidImpl.toBase64String(xid).equals(transactionAsBase64)) {
               Transaction transaction = resourceManager.removeTransaction(xid);
               transaction.rollback();
               long recordID = server.getStorageManager().storeHeuristicCompletion(xid, false);
               server.getStorageManager().waitOnOperations();
               resourceManager.putHeuristicCompletion(recordID, xid, false);
               return true;
            }
         }
         return false;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] listRemoteAddresses() {
      checkStarted();

      clearIO();
      try {
         Set<RemotingConnection> connections = remotingService.getConnections();

         String[] remoteAddresses = new String[connections.size()];
         int i = 0;
         for (RemotingConnection connection : connections) {
            remoteAddresses[i++] = connection.getRemoteAddress();
         }
         return remoteAddresses;
      } finally {
         blockOnIO();
      }

   }

   @Override
   public String[] listRemoteAddresses(final String ipAddress) {
      checkStarted();

      clearIO();
      try {
         Set<RemotingConnection> connections = remotingService.getConnections();
         List<String> remoteConnections = new ArrayList<>();
         for (RemotingConnection connection : connections) {
            String remoteAddress = connection.getRemoteAddress();
            if (remoteAddress.contains(ipAddress)) {
               remoteConnections.add(connection.getRemoteAddress());
            }
         }
         return remoteConnections.toArray(new String[remoteConnections.size()]);
      } finally {
         blockOnIO();
      }

   }

   @Override
   public boolean closeConnectionsForAddress(final String ipAddress) {
      checkStarted();

      clearIO();
      try {
         boolean closed = false;
         Set<RemotingConnection> connections = remotingService.getConnections();
         for (RemotingConnection connection : connections) {
            String remoteAddress = connection.getRemoteAddress();
            if (remoteAddress.contains(ipAddress)) {
               connection.fail(ActiveMQMessageBundle.BUNDLE.connectionsClosedByManagement(ipAddress));
               remotingService.removeConnection(connection.getID());
               closed = true;
            }
         }

         return closed;
      } finally {
         blockOnIO();
      }

   }

   @Override
   public boolean closeConsumerConnectionsForAddress(final String address) {
      boolean closed = false;
      checkStarted();

      clearIO();
      try {
         for (Binding binding : postOffice.getMatchingBindings(SimpleString.toSimpleString(address)).getBindings()) {
            if (binding instanceof LocalQueueBinding) {
               Queue queue = ((LocalQueueBinding) binding).getQueue();
               for (Consumer consumer : queue.getConsumers()) {
                  if (consumer instanceof ServerConsumer) {
                     ServerConsumer serverConsumer = (ServerConsumer) consumer;
                     RemotingConnection connection = null;

                     for (RemotingConnection potentialConnection : remotingService.getConnections()) {
                        if (potentialConnection.getID().toString().equals(serverConsumer.getConnectionID())) {
                           connection = potentialConnection;
                        }
                     }

                     if (connection != null) {
                        remotingService.removeConnection(connection.getID());
                        connection.fail(ActiveMQMessageBundle.BUNDLE.consumerConnectionsClosedByManagement(address));
                        closed = true;
                     }
                  }
               }
            }
         }
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.failedToCloseConsumerConnectionsForAddress(address, e);
      } finally {
         blockOnIO();
      }
      return closed;
   }

   @Override
   public boolean closeConnectionsForUser(final String userName) {
      boolean closed = false;
      checkStarted();

      clearIO();
      try {
         for (ServerSession serverSession : server.getSessions()) {
            if (serverSession.getUsername() != null && serverSession.getUsername().equals(userName)) {
               RemotingConnection connection = null;

               for (RemotingConnection potentialConnection : remotingService.getConnections()) {
                  if (potentialConnection.getID().toString().equals(serverSession.getConnectionID().toString())) {
                     connection = potentialConnection;
                  }
               }

               if (connection != null) {
                  remotingService.removeConnection(connection.getID());
                  connection.fail(ActiveMQMessageBundle.BUNDLE.connectionsForUserClosedByManagement(userName));
                  closed = true;
               }
            }
         }
      } finally {
         blockOnIO();
      }
      return closed;
   }

   @Override
   public boolean closeConnectionWithID(final String ID) {
      checkStarted();

      clearIO();
      try {
         for (RemotingConnection connection : remotingService.getConnections()) {
            if (connection.getID().toString().equals(ID)) {
               remotingService.removeConnection(connection.getID());
               connection.fail(ActiveMQMessageBundle.BUNDLE.connectionWithIDClosedByManagement(ID));
               return true;
            }
         }
      } finally {
         blockOnIO();
      }
      return false;
   }

   @Override
   public boolean closeSessionWithID(final String connectionID, final String ID) throws Exception {
      checkStarted();

      clearIO();
      try {
         List<ServerSession> sessions = server.getSessions(connectionID);
         for (ServerSession session : sessions) {
            if (session.getName().equals(ID.toString())) {
               session.close(true);
               return true;
            }
         }

      } finally {
         blockOnIO();
      }
      return false;
   }

   @Override
   public boolean closeConsumerWithID(final String sessionID, final String ID) throws Exception {
      checkStarted();

      clearIO();
      try {
         Set<ServerSession> sessions = server.getSessions();
         for (ServerSession session : sessions) {
            if (session.getName().equals(sessionID.toString())) {
               Set<ServerConsumer> serverConsumers = session.getServerConsumers();
               for (ServerConsumer serverConsumer : serverConsumers) {
                  if (serverConsumer.sequentialID() == Long.valueOf(ID)) {
                     serverConsumer.disconnect();
                     return true;
                  }
               }
            }
         }

      } finally {
         blockOnIO();
      }
      return false;
   }

   @Override
   public String[] listConnectionIDs() {
      checkStarted();

      clearIO();
      try {
         Set<RemotingConnection> connections = remotingService.getConnections();
         String[] connectionIDs = new String[connections.size()];
         int i = 0;
         for (RemotingConnection connection : connections) {
            connectionIDs[i++] = connection.getID().toString();
         }
         return connectionIDs;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] listSessions(final String connectionID) {
      checkStarted();

      clearIO();
      try {
         List<ServerSession> sessions = server.getSessions(connectionID);
         String[] sessionIDs = new String[sessions.size()];
         int i = 0;
         for (ServerSession serverSession : sessions) {
            sessionIDs[i++] = serverSession.getName();
         }
         return sessionIDs;
      } finally {
         blockOnIO();
      }
   }

   /* (non-Javadoc)
   * @see org.apache.activemq.artemis.api.core.management.ActiveMQServerControl#listProducersInfoAsJSON()
   */
   @Override
   public String listProducersInfoAsJSON() throws Exception {
      JsonArrayBuilder producers = JsonLoader.createArrayBuilder();

      for (ServerSession session : server.getSessions()) {
         session.describeProducersInfo(producers);
      }

      return producers.build().toString();
   }

   @Override
   public String listConnections(String options, int page, int pageSize) throws Exception {
      checkStarted();
      clearIO();
      try {
         server.getPostOffice().getAddresses();
         ConnectionView view = new ConnectionView(server);
         view.setCollection(server.getRemotingService().getConnections());
         view.setOptions(options);
         return view.getResultsAsJson(page, pageSize);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listSessions(String options, int page, int pageSize) throws Exception {
      checkStarted();
      clearIO();
      try {
         SessionView view = new SessionView();
         view.setCollection(server.getSessions());
         view.setOptions(options);
         return view.getResultsAsJson(page, pageSize);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listConsumers(String options, int page, int pageSize) throws Exception {
      checkStarted();
      clearIO();
      try {
         Set<ServerConsumer> consumers = new HashSet();
         for (ServerSession session : server.getSessions()) {
            consumers.addAll(session.getServerConsumers());
         }
         ConsumerView view = new ConsumerView(server);
         view.setCollection(consumers);
         view.setOptions(options);
         return view.getResultsAsJson(page, pageSize);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listAddresses(String options, int page, int pageSize) throws Exception {
      checkStarted();
      clearIO();
      try {
         final Set<SimpleString> addresses = server.getPostOffice().getAddresses();
         List<AddressInfo> addressInfo = new ArrayList<>();
         for (SimpleString address : addresses) {
            AddressInfo info = server.getPostOffice().getAddressInfo(address);
            //ignore if no longer available
            if (info != null) {
               addressInfo.add(info);
            }
         }
         AddressView view = new AddressView(server);
         view.setCollection(addressInfo);
         view.setOptions(options);
         return view.getResultsAsJson(page, pageSize);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listQueues(String options, int page, int pageSize) throws Exception {
      checkStarted();

      clearIO();
      try {
         List<QueueControl> queues = new ArrayList<>();
         Object[] qs = server.getManagementService().getResources(QueueControl.class);
         for (int i = 0; i < qs.length; i++) {
            queues.add((QueueControl) qs[i]);
         }
         QueueView view = new QueueView(server);
         view.setCollection(queues);
         view.setOptions(options);
         return view.getResultsAsJson(page, pageSize);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listProducers(@Parameter(name = "Options") String options,
                               @Parameter(name = "Page Number") int page,
                               @Parameter(name = "Page Size") int pageSize) throws Exception {
      checkStarted();
      clearIO();
      try {
         Set<ServerProducer> producers = new HashSet<>();
         for (ServerSession session : server.getSessions()) {
            producers.addAll(session.getServerProducers().values());
         }
         ProducerView view = new ProducerView(server);
         view.setCollection(producers);
         view.setOptions(options);
         return view.getResultsAsJson(page, pageSize);
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

         Set<RemotingConnection> connections = server.getRemotingService().getConnections();

         for (RemotingConnection connection : connections) {
            JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("connectionID", connection.getID().toString()).add("clientAddress", connection.getRemoteAddress()).add("creationTime", connection.getCreationTime()).add("implementation", connection.getClass().getSimpleName()).add("sessionCount", server.getSessions(connection.getID().toString()).size());
            array.add(obj);
         }
         return array.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listSessionsAsJSON(final String connectionID) throws Exception {
      checkStarted();

      clearIO();

      JsonArrayBuilder array = JsonLoader.createArrayBuilder();
      try {
         List<ServerSession> sessions = server.getSessions(connectionID);
         for (ServerSession sess : sessions) {
            JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("sessionID", sess.getName()).add("creationTime", sess.getCreationTime()).add("consumerCount", sess.getServerConsumers().size());

            if (sess.getValidatedUser() != null) {
               obj.add("principal", sess.getValidatedUser());
            }

            array.add(obj);
         }
      } finally {
         blockOnIO();
      }
      return array.build().toString();
   }

   @Override
   public String listConsumersAsJSON(String connectionID) throws Exception {
      checkStarted();

      clearIO();

      try {
         JsonArrayBuilder array = JsonLoader.createArrayBuilder();

         Set<RemotingConnection> connections = server.getRemotingService().getConnections();
         for (RemotingConnection connection : connections) {
            if (connectionID.equals(connection.getID().toString())) {
               List<ServerSession> sessions = server.getSessions(connectionID);
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
         JsonArrayBuilder array = JsonLoader.createArrayBuilder();

         Set<ServerSession> sessions = server.getSessions();
         for (ServerSession session : sessions) {
            Set<ServerConsumer> consumers = session.getServerConsumers();
            for (ServerConsumer consumer : consumers) {
               JsonObject obj = toJSONObject(consumer);
               if (obj != null) {
                  array.add(obj);
               }
            }
         }
         return array.build().toString();
      } finally {
         blockOnIO();
      }
   }

   private JsonObject toJSONObject(ServerConsumer consumer) throws Exception {
      JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("consumerID", consumer.getID()).add("connectionID", consumer.getConnectionID().toString()).add("sessionID", consumer.getSessionID()).add("queueName", consumer.getQueue().getName().toString()).add("browseOnly", consumer.isBrowseOnly()).add("creationTime", consumer.getCreationTime()).add("deliveringCount", consumer.getDeliveringMessages().size());
      if (consumer.getFilter() != null) {
         obj.add("filter", consumer.getFilter().getFilterString().toString());
      }

      return obj.build();
   }

   @Override
   public Object[] getConnectors() throws Exception {
      checkStarted();

      clearIO();
      try {
         Collection<TransportConfiguration> connectorConfigurations = configuration.getConnectorConfigurations().values();

         Object[] ret = new Object[connectorConfigurations.size()];

         int i = 0;
         for (TransportConfiguration config : connectorConfigurations) {
            Object[] tc = new Object[3];

            tc[0] = config.getName();
            tc[1] = config.getFactoryClassName();
            tc[2] = config.getParams();

            ret[i++] = tc;
         }

         return ret;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getConnectorsAsJSON() throws Exception {
      checkStarted();

      clearIO();
      try {
         JsonArrayBuilder array = JsonLoader.createArrayBuilder();

         for (TransportConfiguration config : configuration.getConnectorConfigurations().values()) {
            array.add(config.toJson());
         }

         return array.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void addSecuritySettings(final String addressMatch,
                                   final String sendRoles,
                                   final String consumeRoles,
                                   final String createDurableQueueRoles,
                                   final String deleteDurableQueueRoles,
                                   final String createNonDurableQueueRoles,
                                   final String deleteNonDurableQueueRoles,
                                   final String manageRoles) throws Exception {
      addSecuritySettings(addressMatch, sendRoles, consumeRoles, createDurableQueueRoles, deleteDurableQueueRoles, createNonDurableQueueRoles, deleteNonDurableQueueRoles, manageRoles, "");
   }

   @Override
   public void addSecuritySettings(final String addressMatch,
                                   final String sendRoles,
                                   final String consumeRoles,
                                   final String createDurableQueueRoles,
                                   final String deleteDurableQueueRoles,
                                   final String createNonDurableQueueRoles,
                                   final String deleteNonDurableQueueRoles,
                                   final String manageRoles,
                                   final String browseRoles) throws Exception {
      addSecuritySettings(addressMatch, sendRoles, consumeRoles, createDurableQueueRoles, deleteDurableQueueRoles, createNonDurableQueueRoles, deleteNonDurableQueueRoles, manageRoles, browseRoles, "", "");
   }

   @Override
   public void addSecuritySettings(final String addressMatch,
                                   final String sendRoles,
                                   final String consumeRoles,
                                   final String createDurableQueueRoles,
                                   final String deleteDurableQueueRoles,
                                   final String createNonDurableQueueRoles,
                                   final String deleteNonDurableQueueRoles,
                                   final String manageRoles,
                                   final String browseRoles,
                                   final String createAddressRoles,
                                   final String deleteAddressRoles) throws Exception {
      checkStarted();

      clearIO();
      try {
         Set<Role> roles = SecurityFormatter.createSecurity(sendRoles, consumeRoles, createDurableQueueRoles, deleteDurableQueueRoles, createNonDurableQueueRoles, deleteNonDurableQueueRoles, manageRoles, browseRoles, createAddressRoles, deleteAddressRoles);

         server.getSecurityRepository().addMatch(addressMatch, roles);

         PersistedRoles persistedRoles = new PersistedRoles(addressMatch, sendRoles, consumeRoles, createDurableQueueRoles, deleteDurableQueueRoles, createNonDurableQueueRoles, deleteNonDurableQueueRoles, manageRoles, browseRoles, createAddressRoles, deleteAddressRoles);

         storageManager.storeSecurityRoles(persistedRoles);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void removeSecuritySettings(final String addressMatch) throws Exception {
      checkStarted();

      clearIO();
      try {
         server.getSecurityRepository().removeMatch(addressMatch);
         storageManager.deleteSecurityRoles(new SimpleString(addressMatch));
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Object[] getRoles(final String addressMatch) throws Exception {
      checkStarted();

      checkStarted();

      clearIO();
      try {
         Set<Role> roles = server.getSecurityRepository().getMatch(addressMatch);

         Object[] objRoles = new Object[roles.size()];

         int i = 0;
         for (Role role : roles) {
            objRoles[i++] = new Object[]{role.getName(), CheckType.SEND.hasRole(role), CheckType.CONSUME.hasRole(role), CheckType.CREATE_DURABLE_QUEUE.hasRole(role), CheckType.DELETE_DURABLE_QUEUE.hasRole(role), CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role), CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role), CheckType.MANAGE.hasRole(role)};
         }
         return objRoles;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getRolesAsJSON(final String addressMatch) throws Exception {
      checkStarted();

      clearIO();
      try {
         JsonArrayBuilder json = JsonLoader.createArrayBuilder();
         Set<Role> roles = server.getSecurityRepository().getMatch(addressMatch);

         for (Role role : roles) {
            json.add(role.toJson());
         }
         return json.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getAddressSettingsAsJSON(final String address) throws Exception {
      checkStarted();

      AddressSettings addressSettings = server.getAddressSettingsRepository().getMatch(address);
      String policy = addressSettings.getAddressFullMessagePolicy() == AddressFullMessagePolicy.PAGE ? "PAGE" : addressSettings.getAddressFullMessagePolicy() == AddressFullMessagePolicy.BLOCK ? "BLOCK" : addressSettings.getAddressFullMessagePolicy() == AddressFullMessagePolicy.DROP ? "DROP" : "FAIL";
      String consumerPolicy = addressSettings.getSlowConsumerPolicy() == SlowConsumerPolicy.NOTIFY ? "NOTIFY" : "KILL";
      JsonObjectBuilder settings = JsonLoader.createObjectBuilder();
      if (addressSettings.getDeadLetterAddress() != null) {
         settings.add("DLA", addressSettings.getDeadLetterAddress().toString());
      }
      if (addressSettings.getExpiryAddress() != null) {
         settings.add("expiryAddress", addressSettings.getExpiryAddress().toString());
      }
      return settings.add("expiryDelay", addressSettings.getExpiryDelay())
            .add("maxDeliveryAttempts", addressSettings.getMaxDeliveryAttempts())
            .add("pageCacheMaxSize", addressSettings.getPageCacheMaxSize())
            .add("maxSizeBytes", addressSettings.getMaxSizeBytes())
            .add("pageSizeBytes", addressSettings.getPageSizeBytes())
            .add("redeliveryDelay", addressSettings.getRedeliveryDelay())
            .add("redeliveryMultiplier", addressSettings.getRedeliveryMultiplier())
            .add("maxRedeliveryDelay", addressSettings.getMaxRedeliveryDelay())
            .add("redistributionDelay", addressSettings.getRedistributionDelay())
            .add("lastValueQueue", addressSettings.isDefaultLastValueQueue())
            .add("sendToDLAOnNoRoute", addressSettings.isSendToDLAOnNoRoute())
            .add("addressFullMessagePolicy", policy)
            .add("slowConsumerThreshold", addressSettings.getSlowConsumerThreshold())
            .add("slowConsumerCheckPeriod", addressSettings.getSlowConsumerCheckPeriod())
            .add("slowConsumerPolicy", consumerPolicy)
            .add("autoCreateJmsQueues", addressSettings.isAutoCreateJmsQueues())
            .add("autoCreateJmsTopics", addressSettings.isAutoCreateJmsTopics())
            .add("autoDeleteJmsQueues", addressSettings.isAutoDeleteJmsQueues())
            .add("autoDeleteJmsTopics", addressSettings.isAutoDeleteJmsQueues())
            .add("autoCreateQueues", addressSettings.isAutoCreateQueues())
            .add("autoDeleteQueues", addressSettings.isAutoDeleteQueues())
            .add("autoCreateAddress", addressSettings.isAutoCreateAddresses())
            .add("autoDeleteAddress", addressSettings.isAutoDeleteAddresses())
            .build()
            .toString();
   }

   @Override
   public void addAddressSettings(final String address,
                                  final String DLA,
                                  final String expiryAddress,
                                  final long expiryDelay,
                                  final boolean lastValueQueue,
                                  final int deliveryAttempts,
                                  final long maxSizeBytes,
                                  final int pageSizeBytes,
                                  final int pageMaxCacheSize,
                                  final long redeliveryDelay,
                                  final double redeliveryMultiplier,
                                  final long maxRedeliveryDelay,
                                  final long redistributionDelay,
                                  final boolean sendToDLAOnNoRoute,
                                  final String addressFullMessagePolicy,
                                  final long slowConsumerThreshold,
                                  final long slowConsumerCheckPeriod,
                                  final String slowConsumerPolicy,
                                  final boolean autoCreateJmsQueues,
                                  final boolean autoDeleteJmsQueues,
                                  final boolean autoCreateJmsTopics,
                                  final boolean autoDeleteJmsTopics) throws Exception {
      addAddressSettings(address, DLA, expiryAddress, expiryDelay, lastValueQueue, deliveryAttempts, maxSizeBytes, pageSizeBytes, pageMaxCacheSize, redeliveryDelay, redeliveryMultiplier, maxRedeliveryDelay, redistributionDelay, sendToDLAOnNoRoute, addressFullMessagePolicy, slowConsumerThreshold, slowConsumerCheckPeriod, slowConsumerPolicy, autoCreateJmsQueues, autoDeleteJmsQueues, autoCreateJmsTopics, autoDeleteJmsTopics, AddressSettings.DEFAULT_AUTO_CREATE_QUEUES, AddressSettings.DEFAULT_AUTO_DELETE_QUEUES, AddressSettings.DEFAULT_AUTO_CREATE_ADDRESSES, AddressSettings.DEFAULT_AUTO_DELETE_ADDRESSES);
   }

   @Override
   public void addAddressSettings(final String address,
                                  final String DLA,
                                  final String expiryAddress,
                                  final long expiryDelay,
                                  final boolean lastValueQueue,
                                  final int deliveryAttempts,
                                  final long maxSizeBytes,
                                  final int pageSizeBytes,
                                  final int pageMaxCacheSize,
                                  final long redeliveryDelay,
                                  final double redeliveryMultiplier,
                                  final long maxRedeliveryDelay,
                                  final long redistributionDelay,
                                  final boolean sendToDLAOnNoRoute,
                                  final String addressFullMessagePolicy,
                                  final long slowConsumerThreshold,
                                  final long slowConsumerCheckPeriod,
                                  final String slowConsumerPolicy,
                                  final boolean autoCreateJmsQueues,
                                  final boolean autoDeleteJmsQueues,
                                  final boolean autoCreateJmsTopics,
                                  final boolean autoDeleteJmsTopics,
                                  final boolean autoCreateQueues,
                                  final boolean autoDeleteQueues,
                                  final boolean autoCreateAddresses,
                                  final boolean autoDeleteAddresses) throws Exception {
      checkStarted();

      // JBPAPP-6334 requested this to be pageSizeBytes > maxSizeBytes
      if (pageSizeBytes > maxSizeBytes && maxSizeBytes > 0) {
         throw new IllegalStateException("pageSize has to be lower than maxSizeBytes. Invalid argument (" + pageSizeBytes + " < " + maxSizeBytes + ")");
      }

      if (maxSizeBytes < -1) {
         throw new IllegalStateException("Invalid argument on maxSizeBytes");
      }

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setDeadLetterAddress(DLA == null ? null : new SimpleString(DLA));
      addressSettings.setExpiryAddress(expiryAddress == null ? null : new SimpleString(expiryAddress));
      addressSettings.setExpiryDelay(expiryDelay);
      addressSettings.setDefaultLastValueQueue(lastValueQueue);
      addressSettings.setMaxDeliveryAttempts(deliveryAttempts);
      addressSettings.setPageCacheMaxSize(pageMaxCacheSize);
      addressSettings.setMaxSizeBytes(maxSizeBytes);
      addressSettings.setPageSizeBytes(pageSizeBytes);
      addressSettings.setRedeliveryDelay(redeliveryDelay);
      addressSettings.setRedeliveryMultiplier(redeliveryMultiplier);
      addressSettings.setMaxRedeliveryDelay(maxRedeliveryDelay);
      addressSettings.setRedistributionDelay(redistributionDelay);
      addressSettings.setSendToDLAOnNoRoute(sendToDLAOnNoRoute);
      if (addressFullMessagePolicy == null) {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      } else if (addressFullMessagePolicy.equalsIgnoreCase("PAGE")) {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      } else if (addressFullMessagePolicy.equalsIgnoreCase("DROP")) {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);
      } else if (addressFullMessagePolicy.equalsIgnoreCase("BLOCK")) {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
      } else if (addressFullMessagePolicy.equalsIgnoreCase("FAIL")) {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);
      }
      addressSettings.setSlowConsumerThreshold(slowConsumerThreshold);
      addressSettings.setSlowConsumerCheckPeriod(slowConsumerCheckPeriod);
      if (slowConsumerPolicy == null) {
         addressSettings.setSlowConsumerPolicy(SlowConsumerPolicy.NOTIFY);
      } else if (slowConsumerPolicy.equalsIgnoreCase("NOTIFY")) {
         addressSettings.setSlowConsumerPolicy(SlowConsumerPolicy.NOTIFY);
      } else if (slowConsumerPolicy.equalsIgnoreCase("KILL")) {
         addressSettings.setSlowConsumerPolicy(SlowConsumerPolicy.KILL);
      }
      addressSettings.setAutoCreateJmsQueues(autoCreateJmsQueues);
      addressSettings.setAutoDeleteJmsQueues(autoDeleteJmsQueues);
      addressSettings.setAutoCreateJmsTopics(autoCreateJmsTopics);
      addressSettings.setAutoDeleteJmsTopics(autoDeleteJmsTopics);
      addressSettings.setAutoCreateQueues(autoCreateQueues);
      addressSettings.setAutoDeleteQueues(autoDeleteQueues);
      addressSettings.setAutoCreateAddresses(autoCreateAddresses);
      addressSettings.setAutoDeleteAddresses(autoDeleteAddresses);
      server.getAddressSettingsRepository().addMatch(address, addressSettings);

      storageManager.storeAddressSetting(new PersistedAddressSetting(new SimpleString(address), addressSettings));
   }

   @Override
   public void removeAddressSettings(final String addressMatch) throws Exception {
      checkStarted();

      server.getAddressSettingsRepository().removeMatch(addressMatch);
      storageManager.deleteAddressSetting(new SimpleString(addressMatch));
   }

   @Override
   public void sendQueueInfoToQueue(final String queueName, final String address) throws Exception {
      checkStarted();

      clearIO();
      try {
         postOffice.sendQueueInfoToQueue(new SimpleString(queueName), new SimpleString(address == null ? "" : address));

         GroupingHandler handler = server.getGroupingHandler();
         if (handler != null) {
            // the group handler would miss responses if the group was requested before the reset was done
            // on that case we ask the groupinghandler to replay its send in case it's waiting for the information
            handler.resendPending();
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getDivertNames() {
      checkStarted();

      clearIO();
      try {
         Object[] diverts = server.getManagementService().getResources(DivertControl.class);
         String[] names = new String[diverts.length];
         for (int i = 0; i < diverts.length; i++) {
            DivertControl divert = (DivertControl) diverts[i];
            names[i] = divert.getUniqueName();
         }

         return names;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void createDivert(final String name,
                            final String routingName,
                            final String address,
                            final String forwardingAddress,
                            final boolean exclusive,
                            final String filterString,
                            final String transformerClassName) throws Exception {
      createDivert(name, routingName, address, forwardingAddress, exclusive, filterString, transformerClassName, ActiveMQDefaultConfiguration.getDefaultDivertRoutingType());
   }

   @Override
   public void createDivert(final String name,
                            final String routingName,
                            final String address,
                            final String forwardingAddress,
                            final boolean exclusive,
                            final String filterString,
                            final String transformerClassName,
                            final String routingType) throws Exception {
      createDivert(name, routingName, address, forwardingAddress, exclusive, filterString, transformerClassName, (String) null, routingType);
   }

   @Override
   public void createDivert(final String name,
                            final String routingName,
                            final String address,
                            final String forwardingAddress,
                            final boolean exclusive,
                            final String filterString,
                            final String transformerClassName,
                            final String transformerPropertiesAsJSON,
                            final String routingType) throws Exception {
      createDivert(name, routingName, address, forwardingAddress, exclusive, filterString, transformerClassName, JsonUtil.readJsonProperties(transformerPropertiesAsJSON), routingType);
   }

   @Override
   public void createDivert(final String name,
                            final String routingName,
                            final String address,
                            final String forwardingAddress,
                            final boolean exclusive,
                            final String filterString,
                            final String transformerClassName,
                            final Map<String, String> transformerProperties,
                            final String routingType) throws Exception {
      checkStarted();

      clearIO();
      try {
         TransformerConfiguration transformerConfiguration = transformerClassName == null ? null : new TransformerConfiguration(transformerClassName).setProperties(transformerProperties);
         DivertConfiguration config = new DivertConfiguration().setName(name).setRoutingName(routingName).setAddress(address).setForwardingAddress(forwardingAddress).setExclusive(exclusive).setFilterString(filterString).setTransformerConfiguration(transformerConfiguration).setRoutingType(DivertConfigurationRoutingType.valueOf(routingType));
         server.deployDivert(config);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void destroyDivert(final String name) throws Exception {
      checkStarted();

      clearIO();
      try {
         server.destroyDivert(SimpleString.toSimpleString(name));
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getBridgeNames() {
      checkStarted();

      clearIO();
      try {
         Object[] bridges = server.getManagementService().getResources(BridgeControl.class);
         String[] names = new String[bridges.length];
         for (int i = 0; i < bridges.length; i++) {
            BridgeControl bridge = (BridgeControl) bridges[i];
            names[i] = bridge.getName();
         }

         return names;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void createBridge(final String name,
                            final String queueName,
                            final String forwardingAddress,
                            final String filterString,
                            final String transformerClassName,
                            final long retryInterval,
                            final double retryIntervalMultiplier,
                            final int initialConnectAttempts,
                            final int reconnectAttempts,
                            final boolean useDuplicateDetection,
                            final int confirmationWindowSize,
                            final int producerWindowSize,
                            final long clientFailureCheckPeriod,
                            final String staticConnectorsOrDiscoveryGroup,
                            boolean useDiscoveryGroup,
                            final boolean ha,
                            final String user,
                            final String password) throws Exception {
      createBridge(name,
                   queueName,
                   forwardingAddress,
                   filterString,
                   transformerClassName,
                   (String) null,
                   retryInterval,
                   retryIntervalMultiplier,
                   initialConnectAttempts,
                   reconnectAttempts,
                   useDuplicateDetection,
                   confirmationWindowSize,
                   producerWindowSize,
                   clientFailureCheckPeriod,
                   staticConnectorsOrDiscoveryGroup,
                   useDiscoveryGroup,
                   ha,
                   user,
                   password);
   }

   @Override
   public void createBridge(final String name,
                            final String queueName,
                            final String forwardingAddress,
                            final String filterString,
                            final String transformerClassName,
                            final String transformerPropertiesAsJSON,
                            final long retryInterval,
                            final double retryIntervalMultiplier,
                            final int initialConnectAttempts,
                            final int reconnectAttempts,
                            final boolean useDuplicateDetection,
                            final int confirmationWindowSize,
                            final int producerWindowSize,
                            final long clientFailureCheckPeriod,
                            final String staticConnectorsOrDiscoveryGroup,
                            boolean useDiscoveryGroup,
                            final boolean ha,
                            final String user,
                            final String password) throws Exception {
      createBridge(name,
                   queueName,
                   forwardingAddress,
                   filterString,
                   transformerClassName,
                   JsonUtil.readJsonProperties(transformerPropertiesAsJSON),
                   retryInterval,
                   retryIntervalMultiplier,
                   initialConnectAttempts,
                   reconnectAttempts,
                   useDuplicateDetection,
                   confirmationWindowSize,
                   producerWindowSize,
                   clientFailureCheckPeriod,
                   staticConnectorsOrDiscoveryGroup,
                   useDiscoveryGroup,
                   ha,
                   user,
                   password);
   }

   @Override
   public void createBridge(final String name,
                            final String queueName,
                            final String forwardingAddress,
                            final String filterString,
                            final String transformerClassName,
                            final Map<String, String> transformerProperties,
                            final long retryInterval,
                            final double retryIntervalMultiplier,
                            final int initialConnectAttempts,
                            final int reconnectAttempts,
                            final boolean useDuplicateDetection,
                            final int confirmationWindowSize,
                            final int producerWindowSize,
                            final long clientFailureCheckPeriod,
                            final String staticConnectorsOrDiscoveryGroup,
                            boolean useDiscoveryGroup,
                            final boolean ha,
                            final String user,
                            final String password) throws Exception {
      checkStarted();

      clearIO();

      try {
         TransformerConfiguration transformerConfiguration = transformerClassName == null ? null : new TransformerConfiguration(transformerClassName).setProperties(transformerProperties);
         BridgeConfiguration config = new BridgeConfiguration().setName(name).setQueueName(queueName).setForwardingAddress(forwardingAddress).setFilterString(filterString).setTransformerConfiguration(transformerConfiguration).setClientFailureCheckPeriod(clientFailureCheckPeriod).setRetryInterval(retryInterval).setRetryIntervalMultiplier(retryIntervalMultiplier).setInitialConnectAttempts(initialConnectAttempts).setReconnectAttempts(reconnectAttempts).setUseDuplicateDetection(useDuplicateDetection).setConfirmationWindowSize(confirmationWindowSize).setProducerWindowSize(producerWindowSize).setHA(ha).setUser(user).setPassword(password);

         if (useDiscoveryGroup) {
            config.setDiscoveryGroupName(staticConnectorsOrDiscoveryGroup);
         } else {
            config.setStaticConnectors(ListUtil.toList(staticConnectorsOrDiscoveryGroup));
         }

         server.deployBridge(config);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void createBridge(final String name,
                            final String queueName,
                            final String forwardingAddress,
                            final String filterString,
                            final String transformerClassName,
                            final long retryInterval,
                            final double retryIntervalMultiplier,
                            final int initialConnectAttempts,
                            final int reconnectAttempts,
                            final boolean useDuplicateDetection,
                            final int confirmationWindowSize,
                            final long clientFailureCheckPeriod,
                            final String staticConnectorsOrDiscoveryGroup,
                            boolean useDiscoveryGroup,
                            final boolean ha,
                            final String user,
                            final String password) throws Exception {
      checkStarted();

      clearIO();

      try {
         TransformerConfiguration transformerConfiguration = transformerClassName == null ? null : new TransformerConfiguration(transformerClassName);
         BridgeConfiguration config = new BridgeConfiguration().setName(name).setQueueName(queueName).setForwardingAddress(forwardingAddress).setFilterString(filterString).setTransformerConfiguration(transformerConfiguration).setClientFailureCheckPeriod(clientFailureCheckPeriod).setRetryInterval(retryInterval).setRetryIntervalMultiplier(retryIntervalMultiplier).setInitialConnectAttempts(initialConnectAttempts).setReconnectAttempts(reconnectAttempts).setUseDuplicateDetection(useDuplicateDetection).setConfirmationWindowSize(confirmationWindowSize).setHA(ha).setUser(user).setPassword(password);

         if (useDiscoveryGroup) {
            config.setDiscoveryGroupName(staticConnectorsOrDiscoveryGroup);
         } else {
            config.setStaticConnectors(ListUtil.toList(staticConnectorsOrDiscoveryGroup));
         }

         server.deployBridge(config);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void destroyBridge(final String name) throws Exception {
      checkStarted();

      clearIO();
      try {
         server.destroyBridge(name);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void createConnectorService(final String name, final String factoryClass, final Map<String, Object> parameters) throws Exception {
      checkStarted();

      clearIO();

      try {
         final ConnectorServiceConfiguration config = new ConnectorServiceConfiguration().setName(name).setFactoryClassName(factoryClass).setParams(parameters);
         ConnectorServiceFactory factory = server.getServiceRegistry().getConnectorService(config);
         server.getConnectorsService().createService(config, factory);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void destroyConnectorService(final String name) throws Exception {
      checkStarted();

      clearIO();

      try {
         server.getConnectorsService().destroyService(name);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getConnectorServices() {
      checkStarted();

      clearIO();

      try {
         return server.getConnectorsService().getConnectors().keySet().toArray(new String[0]);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void forceFailover() throws Exception {
      checkStarted();

      clearIO();

      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               server.fail(true);
            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
            }
         }
      };
      t.start();
   }

   @Override
   public void updateDuplicateIdCache(String address, Object[] ids) throws Exception {
      clearIO();
      try {
         DuplicateIDCache duplicateIDCache = server.getPostOffice().getDuplicateIDCache(new SimpleString(address));
         for (Object id : ids) {
            duplicateIDCache.addToCache(((String) id).getBytes(), null);
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void scaleDown(String connector) throws Exception {
      checkStarted();

      clearIO();
      HAPolicy haPolicy = server.getHAPolicy();
      if (haPolicy instanceof LiveOnlyPolicy) {
         LiveOnlyPolicy liveOnlyPolicy = (LiveOnlyPolicy) haPolicy;

         if (liveOnlyPolicy.getScaleDownPolicy() == null) {
            liveOnlyPolicy.setScaleDownPolicy(new ScaleDownPolicy());
         }

         liveOnlyPolicy.getScaleDownPolicy().setEnabled(true);

         if (connector != null) {
            liveOnlyPolicy.getScaleDownPolicy().getConnectors().add(0, connector);
         }

         server.fail(true);
      }

   }


   @Override
   public String listNetworkTopology() throws Exception {
      checkStarted();

      clearIO();
      try {
         JsonArrayBuilder brokers = JsonLoader.createArrayBuilder();
         ClusterManager clusterManager = server.getClusterManager();
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


   // NotificationEmitter implementation ----------------------------

   @Override
   public void removeNotificationListener(final NotificationListener listener,
                                          final NotificationFilter filter,
                                          final Object handback) throws ListenerNotFoundException {
      clearIO();
      try {
         broadcaster.removeNotificationListener(listener, filter, handback);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void removeNotificationListener(final NotificationListener listener) throws ListenerNotFoundException {
      clearIO();
      try {
         broadcaster.removeNotificationListener(listener);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void addNotificationListener(final NotificationListener listener,
                                       final NotificationFilter filter,
                                       final Object handback) throws IllegalArgumentException {
      clearIO();
      try {
         broadcaster.addNotificationListener(listener, filter, handback);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public MBeanNotificationInfo[] getNotificationInfo() {
      CoreNotificationType[] values = CoreNotificationType.values();
      String[] names = new String[values.length];
      for (int i = 0; i < values.length; i++) {
         names[i] = values[i].toString();
      }
      return new MBeanNotificationInfo[]{new MBeanNotificationInfo(names, this.getClass().getName(), "Notifications emitted by a Core Server")};
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private synchronized void setMessageCounterEnabled(final boolean enable) {
      if (isStarted()) {
         if (configuration.isMessageCounterEnabled() && !enable) {
            stopMessageCounters();
         } else if (!configuration.isMessageCounterEnabled() && enable) {
            startMessageCounters();
         }
      }
      configuration.setMessageCounterEnabled(enable);
   }

   private void startMessageCounters() {
      messageCounterManager.start();
   }

   private void stopMessageCounters() {
      messageCounterManager.stop();

      messageCounterManager.resetAllCounters();

      messageCounterManager.resetAllCounterHistories();
   }

   @Override
   public long getConnectionTTLOverride() {
      return configuration.getConnectionTTLOverride();
   }

   @Override
   public int getIDCacheSize() {
      return configuration.getIDCacheSize();
   }

   @Override
   public String getLargeMessagesDirectory() {
      return configuration.getLargeMessagesDirectory();
   }

   @Override
   public String getManagementAddress() {
      return configuration.getManagementAddress().toString();
   }

   @Override
   public String getNodeID() {
      return server.getNodeID().toString();
   }

   @Override
   public String getManagementNotificationAddress() {
      return configuration.getManagementNotificationAddress().toString();
   }

   @Override
   public long getMessageExpiryScanPeriod() {
      return configuration.getMessageExpiryScanPeriod();
   }

   @Override
   public long getMessageExpiryThreadPriority() {
      return configuration.getMessageExpiryThreadPriority();
   }

   @Override
   public long getTransactionTimeout() {
      return configuration.getTransactionTimeout();
   }

   @Override
   public long getTransactionTimeoutScanPeriod() {
      return configuration.getTransactionTimeoutScanPeriod();
   }

   @Override
   public boolean isPersistDeliveryCountBeforeDelivery() {
      return configuration.isPersistDeliveryCountBeforeDelivery();
   }

   @Override
   public boolean isPersistIDCache() {
      return configuration.isPersistIDCache();
   }

   @Override
   public boolean isWildcardRoutingEnabled() {
      return configuration.isWildcardRoutingEnabled();
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(ActiveMQServerControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(ActiveMQServerControl.class);
   }

   private void checkStarted() {
      if (!server.isStarted()) {
         throw new IllegalStateException("Broker is not started. It can not be managed yet");
      }
   }

   public String[] listTargetAddresses(final String sessionID) {
      ServerSession session = server.getSessionByID(sessionID);
      if (session != null) {
         return session.getTargetAddresses();
      }
      return new String[0];
   }

   @Override
   public void onNotification(org.apache.activemq.artemis.core.server.management.Notification notification) {
      if (!(notification.getType() instanceof CoreNotificationType))
         return;
      CoreNotificationType type = (CoreNotificationType) notification.getType();
      TypedProperties prop = notification.getProperties();

      this.broadcaster.sendNotification(new Notification(type.toString(), this, notifSeq.incrementAndGet(), notification.toString()));
   }

}


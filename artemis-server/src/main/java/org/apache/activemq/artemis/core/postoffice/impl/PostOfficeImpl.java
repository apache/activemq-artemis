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
package org.apache.activemq.artemis.core.postoffice.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.apache.activemq.artemis.api.core.ActiveMQDuplicateIdException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.core.management.NotificationType;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.AddressManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.BindingsFactory;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.postoffice.QueueInfo;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.group.GroupingHandler;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperation;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.jboss.logging.Logger;

/**
 * This is the class that will make the routing to Queues and decide which consumer will get the messages
 * It's the queue component on distributing the messages * *
 */
public class PostOfficeImpl implements PostOffice, NotificationListener, BindingsFactory {

   private static final Logger logger = Logger.getLogger(PostOfficeImpl.class);

   public static final SimpleString HDR_RESET_QUEUE_DATA = new SimpleString("_AMQ_RESET_QUEUE_DATA");

   public static final SimpleString HDR_RESET_QUEUE_DATA_COMPLETE = new SimpleString("_AMQ_RESET_QUEUE_DATA_COMPLETE");

   public static final SimpleString BRIDGE_CACHE_STR = new SimpleString("BRIDGE.");

   private final AddressManager addressManager;

   private final QueueFactory queueFactory;

   private final StorageManager storageManager;

   private final PagingManager pagingManager;

   private volatile boolean started;

   private final ManagementService managementService;

   private Reaper reaperRunnable;

   private final long reaperPeriod;

   private final int reaperPriority;

   private final ConcurrentMap<SimpleString, DuplicateIDCache> duplicateIDCaches = new ConcurrentHashMap<>();

   private final int idCacheSize;

   private final boolean persistIDCache;

   private final Map<SimpleString, QueueInfo> queueInfos = new HashMap<>();

   private final Object notificationLock = new Object();

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final ActiveMQServer server;

   private final Object addressLock = new Object();

   public PostOfficeImpl(final ActiveMQServer server,
                         final StorageManager storageManager,
                         final PagingManager pagingManager,
                         final QueueFactory bindableFactory,
                         final ManagementService managementService,
                         final long reaperPeriod,
                         final int reaperPriority,
                         final WildcardConfiguration wildcardConfiguration,
                         final int idCacheSize,
                         final boolean persistIDCache,
                         final HierarchicalRepository<AddressSettings> addressSettingsRepository) {
      this.storageManager = storageManager;

      queueFactory = bindableFactory;

      this.managementService = managementService;

      this.pagingManager = pagingManager;

      this.reaperPeriod = reaperPeriod;

      this.reaperPriority = reaperPriority;

      if (wildcardConfiguration.isRoutingEnabled()) {
         addressManager = new WildcardAddressManager(this, wildcardConfiguration, storageManager);
      } else {
         addressManager = new SimpleAddressManager(this, wildcardConfiguration, storageManager);
      }

      this.idCacheSize = idCacheSize;

      this.persistIDCache = persistIDCache;

      this.addressSettingsRepository = addressSettingsRepository;

      this.server = server;
   }

   // ActiveMQComponent implementation ---------------------------------------

   @Override
   public synchronized void start() throws Exception {
      if (started)
         return;

      managementService.addNotificationListener(this);

      // Injecting the postoffice (itself) on queueFactory for paging-control
      queueFactory.setPostOffice(this);

      // The flag started needs to be set before starting the Reaper Thread
      // This is to avoid thread leakages where the Reaper would run beyond the life cycle of the
      // PostOffice
      started = true;
   }

   @Override
   public synchronized void stop() throws Exception {
      started = false;

      managementService.removeNotificationListener(this);

      if (reaperRunnable != null)
         reaperRunnable.stop();

      addressManager.clear();

      queueInfos.clear();
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   // NotificationListener implementation -------------------------------------

   @Override
   public void onNotification(final Notification notification) {
      if (!(notification.getType() instanceof CoreNotificationType))
         return;

      if (logger.isTraceEnabled()) {
         logger.trace("Receiving notification : " + notification + " on server " + this.server);
      }
      synchronized (notificationLock) {
         CoreNotificationType type = (CoreNotificationType) notification.getType();

         switch (type) {
            case BINDING_ADDED: {
               TypedProperties props = notification.getProperties();

               if (!props.containsProperty(ManagementHelper.HDR_BINDING_TYPE)) {
                  throw ActiveMQMessageBundle.BUNDLE.bindingTypeNotSpecified();
               }

               Integer bindingType = props.getIntProperty(ManagementHelper.HDR_BINDING_TYPE);

               if (bindingType == BindingType.DIVERT_INDEX) {
                  // We don't propagate diverts
                  return;
               }

               SimpleString routingName = props.getSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME);

               SimpleString clusterName = props.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

               SimpleString address = props.getSimpleStringProperty(ManagementHelper.HDR_ADDRESS);

               if (!props.containsProperty(ManagementHelper.HDR_BINDING_ID)) {
                  throw ActiveMQMessageBundle.BUNDLE.bindingIdNotSpecified();
               }

               long id = props.getLongProperty(ManagementHelper.HDR_BINDING_ID);

               SimpleString filterString = props.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

               if (!props.containsProperty(ManagementHelper.HDR_DISTANCE)) {
                  logger.debug("PostOffice notification / BINDING_ADDED: HDR_DISANCE not specified, giving up propagation on notifications");
                  return;
               }

               int distance = props.getIntProperty(ManagementHelper.HDR_DISTANCE);

               QueueInfo info = new QueueInfo(routingName, clusterName, address, filterString, id, distance);

               queueInfos.put(clusterName, info);

               break;
            }
            case BINDING_REMOVED: {
               TypedProperties props = notification.getProperties();

               if (!props.containsProperty(ManagementHelper.HDR_CLUSTER_NAME)) {
                  logger.debug("PostOffice notification / BINDING_REMOVED: HDR_CLUSTER_NAME not specified, giving up propagation on notifications");
                  return;
               }

               SimpleString clusterName = props.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

               QueueInfo info = queueInfos.remove(clusterName);

               if (info == null) {
                  logger.debug("PostOffice notification / BINDING_REMOVED: Cannot find queue info for queue \" + clusterName");
                  return;
               }

               break;
            }
            case CONSUMER_CREATED: {
               TypedProperties props = notification.getProperties();

               if (!props.containsProperty(ManagementHelper.HDR_CLUSTER_NAME)) {
                  logger.debug("PostOffice notification / CONSUMER_CREATED: No clusterName defined");
                  return;
               }

               SimpleString clusterName = props.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

               SimpleString filterString = props.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

               QueueInfo info = queueInfos.get(clusterName);

               if (info == null) {
                  logger.debug("PostOffice notification / CONSUMER_CREATED: Could not find queue created on clusterName = " + clusterName);
                  return;
               }

               info.incrementConsumers();

               if (filterString != null) {
                  List<SimpleString> filterStrings = info.getFilterStrings();

                  if (filterStrings == null) {
                     filterStrings = new ArrayList<>();

                     info.setFilterStrings(filterStrings);
                  }

                  filterStrings.add(filterString);
               }

               if (!props.containsProperty(ManagementHelper.HDR_DISTANCE)) {
                  logger.debug("PostOffice notification / CONSUMER_CREATED: No distance specified");
                  return;
               }

               int distance = props.getIntProperty(ManagementHelper.HDR_DISTANCE);

               if (distance > 0) {
                  SimpleString queueName = props.getSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME);

                  if (queueName == null) {
                     logger.debug("PostOffice notification / CONSUMER_CREATED: No queue defined");
                     return;
                  }

                  Binding binding = getBinding(queueName);

                  if (binding != null) {
                     // We have a local queue
                     Queue queue = (Queue) binding.getBindable();

                     AddressSettings addressSettings = addressSettingsRepository.getMatch(binding.getAddress().toString());

                     long redistributionDelay = addressSettings.getRedistributionDelay();

                     if (redistributionDelay != -1) {
                        queue.addRedistributor(redistributionDelay);
                     }
                  }
               }

               break;
            }
            case CONSUMER_CLOSED: {
               TypedProperties props = notification.getProperties();

               SimpleString clusterName = props.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

               if (clusterName == null) {
                  logger.debug("PostOffice notification / CONSUMER_CLOSED: No cluster name");
                  return;
               }

               SimpleString filterString = props.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

               QueueInfo info = queueInfos.get(clusterName);

               if (info == null) {
                  return;
               }

               info.decrementConsumers();

               if (filterString != null) {
                  List<SimpleString> filterStrings = info.getFilterStrings();

                  filterStrings.remove(filterString);
               }

               // The consumer count should never be < 0 but we should catch here just in case.
               if (info.getNumberOfConsumers() <= 0) {
                  if (!props.containsProperty(ManagementHelper.HDR_DISTANCE)) {
                     logger.debug("PostOffice notification / CONSUMER_CLOSED: HDR_DISTANCE not defined");
                     return;
                  }

                  int distance = props.getIntProperty(ManagementHelper.HDR_DISTANCE);

                  if (distance == 0) {
                     SimpleString queueName = props.getSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME);

                     if (queueName == null) {
                        logger.debug("PostOffice notification / CONSUMER_CLOSED: No queue name");
                        return;
                     }

                     Binding binding = getBinding(queueName);

                     if (binding == null) {
                        logger.debug("PostOffice notification / CONSUMER_CLOSED: Could not find queue " + queueName);
                        return;
                     }

                     Queue queue = (Queue) binding.getBindable();

                     AddressSettings addressSettings = addressSettingsRepository.getMatch(binding.getAddress().toString());

                     long redistributionDelay = addressSettings.getRedistributionDelay();

                     if (redistributionDelay != -1) {
                        queue.addRedistributor(redistributionDelay);
                     }
                  }
               }

               break;
            }
            default: {
               break;
            }
         }
      }
   }

   // PostOffice implementation -----------------------------------------------

   @Override
   public void reloadAddressInfo(AddressInfo addressInfo) throws Exception {
      internalAddressInfo(addressInfo, true);
   }

   @Override
   public boolean addAddressInfo(AddressInfo addressInfo) throws Exception {
      return internalAddressInfo(addressInfo, false);
   }

   private boolean internalAddressInfo(AddressInfo addressInfo, boolean reload) throws Exception {
      synchronized (addressLock) {
         if (server.hasBrokerPlugins()) {
            server.callBrokerPlugins(plugin -> plugin.beforeAddAddress(addressInfo, reload));
         }

         boolean result;
         if (reload) {
            result = addressManager.reloadAddressInfo(addressInfo);
         } else {
            result = addressManager.addAddressInfo(addressInfo);
         }
         // only register address if it is new
         if (result) {
            try {
               if (!addressInfo.isInternal()) {
                  managementService.registerAddress(addressInfo);
               }
               if (server.hasBrokerPlugins()) {
                  server.callBrokerPlugins(plugin -> plugin.afterAddAddress(addressInfo, reload));
               }
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
         return result;
      }
   }

   @Override
   public QueueBinding updateQueue(SimpleString name,
                                   RoutingType routingType,
                                   Integer maxConsumers,
                                   Boolean purgeOnNoConsumers,
                                   Boolean exclusive) throws Exception {
      synchronized (addressLock) {
         final QueueBinding queueBinding = (QueueBinding) addressManager.getBinding(name);
         if (queueBinding == null) {
            return null;
         }

         final Queue queue = queueBinding.getQueue();

         boolean changed = false;

         //validate update
         if (maxConsumers != null && maxConsumers.intValue() != Queue.MAX_CONSUMERS_UNLIMITED) {
            final int consumerCount = queue.getConsumerCount();
            if (consumerCount > maxConsumers) {
               throw ActiveMQMessageBundle.BUNDLE.invalidMaxConsumersUpdate(name.toString(), maxConsumers, consumerCount);
            }
         }
         if (routingType != null) {
            final SimpleString address = queue.getAddress();
            final AddressInfo addressInfo = addressManager.getAddressInfo(address);
            final EnumSet<RoutingType> addressRoutingTypes = addressInfo.getRoutingTypes();
            if (!addressRoutingTypes.contains(routingType)) {
               throw ActiveMQMessageBundle.BUNDLE.invalidRoutingTypeUpdate(name.toString(), routingType, address.toString(), addressRoutingTypes);
            }
         }

         //atomic update
         if (maxConsumers != null && queue.getMaxConsumers() != maxConsumers.intValue()) {
            changed = true;
            queue.setMaxConsumer(maxConsumers);
         }
         if (routingType != null && queue.getRoutingType() != routingType) {
            changed = true;
            queue.setRoutingType(routingType);
         }
         if (purgeOnNoConsumers != null && queue.isPurgeOnNoConsumers() != purgeOnNoConsumers.booleanValue()) {
            changed = true;
            queue.setPurgeOnNoConsumers(purgeOnNoConsumers);
         }
         if (exclusive != null && queue.isExclusive() != exclusive.booleanValue()) {
            changed = true;
            queue.setExclusive(exclusive);
         }

         if (changed) {
            final long txID = storageManager.generateID();
            try {
               storageManager.updateQueueBinding(txID, queueBinding);
               storageManager.commitBindings(txID);
            } catch (Throwable throwable) {
               storageManager.rollback(txID);
               logger.warn(throwable.getMessage(), throwable);
               throw throwable;
            }
         }

         return queueBinding;
      }
   }

   @Override
   public AddressInfo updateAddressInfo(SimpleString addressName,
                                        EnumSet<RoutingType> routingTypes) throws Exception {
      synchronized (addressLock) {
         if (server.hasBrokerPlugins()) {
            server.callBrokerPlugins(plugin -> plugin.beforeUpdateAddress(addressName, routingTypes));
         }

         final AddressInfo address = addressManager.updateAddressInfo(addressName, routingTypes);
         if (server.hasBrokerPlugins()) {
            server.callBrokerPlugins(plugin -> plugin.afterUpdateAddress(address));
         }

         return address;
      }
   }


   @Override
   public AddressInfo removeAddressInfo(SimpleString address) throws Exception {
      return removeAddressInfo(address, false);
   }

   @Override
   public AddressInfo removeAddressInfo(SimpleString address, boolean force) throws Exception {
      synchronized (addressLock) {
         if (server.hasBrokerPlugins()) {
            server.callBrokerPlugins(plugin -> plugin.beforeRemoveAddress(address));
         }

         final Bindings bindingsForAddress = getDirectBindings(address);
         if (force) {
            for (Binding binding : bindingsForAddress.getBindings()) {
               if (binding instanceof QueueBinding) {
                  ((QueueBinding)binding).getQueue().deleteQueue(true);
               }
            }

         } else {
            if (bindingsForAddress.getBindings().size() > 0) {
               throw ActiveMQMessageBundle.BUNDLE.addressHasBindings(address);
            }
         }
         managementService.unregisterAddress(address);
         final AddressInfo addressInfo = addressManager.removeAddressInfo(address);
         if (server.hasBrokerPlugins()) {
            server.callBrokerPlugins(plugin -> plugin.afterRemoveAddress(address, addressInfo));
         }

         return addressInfo;
      }
   }

   @Override
   public AddressInfo getAddressInfo(SimpleString addressName) {
      synchronized (addressLock) {
         return addressManager.getAddressInfo(addressName);
      }
   }

   @Override
   public List<Queue> listQueuesForAddress(SimpleString address) throws Exception {
      Bindings bindingsForAddress = getBindingsForAddress(address);
      List<Queue> queues = new ArrayList<>();
      for (Binding b : bindingsForAddress.getBindings()) {
         if (b instanceof QueueBinding) {
            Queue q = ((QueueBinding) b).getQueue();
            queues.add(q);
         }
      }
      return queues;
   }

   // TODO - needs to be synchronized to prevent happening concurrently with activate()
   // (and possible removeBinding and other methods)
   // Otherwise can have situation where createQueue comes in before failover, then failover occurs
   // and post office is activated but queue remains unactivated after failover so delivery never occurs
   // even though failover is complete
   @Override
   public synchronized void addBinding(final Binding binding) throws Exception {
      if (server.hasBrokerPlugins()) {
         server.callBrokerPlugins(plugin -> plugin.beforeAddBinding(binding));
      }

      addressManager.addBinding(binding);

      TypedProperties props = new TypedProperties();

      props.putIntProperty(ManagementHelper.HDR_BINDING_TYPE, binding.getType().toInt());

      props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

      props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

      props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

      props.putLongProperty(ManagementHelper.HDR_BINDING_ID, binding.getID());

      props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

      Filter filter = binding.getFilter();

      if (filter != null) {
         props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, filter.getFilterString());
      }

      String uid = UUIDGenerator.getInstance().generateStringUUID();

      if (logger.isDebugEnabled()) {
         logger.debug("ClusterCommunication::Sending notification for addBinding " + binding + " from server " + server);
      }

      managementService.sendNotification(new Notification(uid, CoreNotificationType.BINDING_ADDED, props));

      if (server.hasBrokerPlugins()) {
         server.callBrokerPlugins(plugin -> plugin.afterAddBinding(binding));
      }

   }

   @Override
   public synchronized Binding removeBinding(final SimpleString uniqueName,
                                             Transaction tx,
                                             boolean deleteData) throws Exception {

      if (server.hasBrokerPlugins()) {
         server.callBrokerPlugins(plugin -> plugin.beforeRemoveBinding(uniqueName, tx, deleteData));
      }

      addressSettingsRepository.clearCache();

      Binding binding = addressManager.removeBinding(uniqueName, tx);

      if (binding == null) {
         throw new ActiveMQNonExistentQueueException();
      }

      if (deleteData && addressManager.getBindingsForRoutingAddress(binding.getAddress()) == null) {
         pagingManager.deletePageStore(binding.getAddress());

         deleteDuplicateCache(binding.getAddress());
      }

      if (binding.getType() == BindingType.LOCAL_QUEUE) {
         Queue queue = (Queue) binding.getBindable();
         managementService.unregisterQueue(uniqueName, binding.getAddress(), queue.getRoutingType());
      } else if (binding.getType() == BindingType.DIVERT) {
         managementService.unregisterDivert(uniqueName, binding.getAddress());
      }

      if (binding.getType() != BindingType.DIVERT) {
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

         props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

         props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

         props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

         props.putLongProperty(ManagementHelper.HDR_BINDING_ID, binding.getID());

         if (binding.getFilter() == null) {
            props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, null);
         } else {
            props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, binding.getFilter().getFilterString());
         }

         managementService.sendNotification(new Notification(null, CoreNotificationType.BINDING_REMOVED, props));
      }

      binding.close();

      if (server.hasBrokerPlugins()) {
         server.callBrokerPlugins(plugin -> plugin.afterRemoveBinding(binding, tx, deleteData) );
      }

      return binding;
   }

   private void deleteDuplicateCache(SimpleString address) throws Exception {
      DuplicateIDCache cache = duplicateIDCaches.remove(address);

      if (cache != null) {
         cache.clear();
      }

      cache = duplicateIDCaches.remove(BRIDGE_CACHE_STR.concat(address));

      if (cache != null) {
         cache.clear();
      }
   }

   @Override
   public boolean isAddressBound(final SimpleString address) throws Exception {
      Bindings bindings = getBindingsForAddress(address);
      return bindings != null && !bindings.getBindings().isEmpty();
   }

   @Override
   public Bindings getBindingsForAddress(final SimpleString address) throws Exception {
      Bindings bindings = addressManager.getBindingsForRoutingAddress(address);

      if (bindings == null) {
         bindings = createBindings(address);
      }

      return bindings;
   }

   @Override
   public Bindings lookupBindingsForAddress(final SimpleString address) throws Exception {
      return addressManager.getBindingsForRoutingAddress(address);
   }

   @Override
   public Binding getBinding(final SimpleString name) {
      return addressManager.getBinding(name);
   }

   @Override
   public Bindings getMatchingBindings(final SimpleString address) throws Exception {
      return addressManager.getMatchingBindings(address);
   }

   @Override
   public Bindings getDirectBindings(final SimpleString address) throws Exception {
      return addressManager.getDirectBindings(address);
   }

   @Override
   public Map<SimpleString, Binding> getAllBindings() {
      return addressManager.getBindings();
   }

   @Override
   public RoutingStatus route(final Message message, final boolean direct) throws Exception {
      return route(message, (Transaction) null, direct);
   }

   @Override
   public RoutingStatus route(final Message message, final Transaction tx, final boolean direct) throws Exception {
      return route(message, new RoutingContextImpl(tx), direct);
   }

   @Override
   public RoutingStatus route(Message message,
                              Transaction tx,
                              boolean direct,
                              boolean rejectDuplicates) throws Exception {
      return route(message, new RoutingContextImpl(tx), direct, rejectDuplicates, null);
   }

   @Override
   public RoutingStatus route(final Message message,
                              final Transaction tx,
                              final boolean direct,
                              final boolean rejectDuplicates,
                              final Binding binding) throws Exception {
      return route(message, new RoutingContextImpl(tx), direct, rejectDuplicates, binding);
   }

   @Override
   public RoutingStatus route(final Message message,
                              final RoutingContext context,
                              final boolean direct) throws Exception {
      return route(message, context, direct, true, null);
   }

   @Override
   public RoutingStatus route(final Message message,
                              final RoutingContext context,
                              final boolean direct,
                              boolean rejectDuplicates,
                              final Binding bindingMove) throws Exception {

      RoutingStatus result;
      // Sanity check
      if (message.getRefCount() > 0) {
         throw new IllegalStateException("Message cannot be routed more than once");
      }

      setPagingStore(context.getAddress(message), message);

      AtomicBoolean startedTX = new AtomicBoolean(false);

      final SimpleString address = context.getAddress(message);

      applyExpiryDelay(message, address);

      if (!checkDuplicateID(message, context, rejectDuplicates, startedTX)) {
         return RoutingStatus.DUPLICATED_ID;
      }

      message.cleanupInternalProperties();

      Bindings bindings = addressManager.getBindingsForRoutingAddress(context.getAddress(message));

      // TODO auto-create queues here?
      // first check for the auto-queue creation thing
      if (bindings == null) {
         // There is no queue with this address, we will check if it needs to be created
         //         if (queueCreator.create(address)) {
         // TODO: this is not working!!!!
         // reassign bindings if it was created
         //            bindings = addressManager.getBindingsForRoutingAddress(address);
         //         }
      }
      if (bindingMove != null) {
         bindingMove.route(message, context);
      } else if (bindings != null) {
         bindings.route(message, context);
      } else {
         // this is a debug and not warn because this could be a regular scenario on publish-subscribe queues (or topic subscriptions on JMS)
         if (logger.isDebugEnabled()) {
            logger.debug("Couldn't find any bindings for address=" + address + " on message=" + message);
         }
      }

      if (server.hasBrokerPlugins()) {
         server.callBrokerPlugins(plugin -> plugin.beforeMessageRoute(message, context, direct, rejectDuplicates));
      }

      if (logger.isTraceEnabled()) {
         logger.trace("Message after routed=" + message);
      }

      if (context.getQueueCount() == 0) {
         // Send to DLA if appropriate

         AddressSettings addressSettings = addressSettingsRepository.getMatch(address.toString());

         boolean sendToDLA = addressSettings.isSendToDLAOnNoRoute();

         if (sendToDLA) {
            // Send to the DLA for the address

            SimpleString dlaAddress = addressSettings.getDeadLetterAddress();

            if (logger.isDebugEnabled()) {
               logger.debug("sending message to dla address = " + dlaAddress + ", message=" + message);
            }

            if (dlaAddress == null) {
               result = RoutingStatus.NO_BINDINGS;
               ActiveMQServerLogger.LOGGER.noDLA(address);
            } else {
               message.referenceOriginalMessage(message, null);

               message.setAddress(dlaAddress);

               message.reencode();

               route(message, context.getTransaction(), false);
               result = RoutingStatus.NO_BINDINGS_DLA;
            }
         } else {
            result = RoutingStatus.NO_BINDINGS;

            if (logger.isDebugEnabled()) {
               logger.debug("Message " + message + " is not going anywhere as it didn't have a binding on address:" + address);
            }

            if (message.isLargeMessage()) {
               ((LargeServerMessage) message).deleteFile();
            }
         }
      } else {
         result = RoutingStatus.OK;
         try {
            processRoute(message, context, direct);
         } catch (ActiveMQAddressFullException e) {
            if (startedTX.get()) {
               context.getTransaction().rollback();
            } else if (context.getTransaction() != null) {
               context.getTransaction().markAsRollbackOnly(e);
            }
            throw e;
         }
      }

      if (startedTX.get()) {
         context.getTransaction().commit();
      }

      if (server.hasBrokerPlugins()) {
         server.callBrokerPlugins(plugin -> plugin.afterMessageRoute(message, context, direct, rejectDuplicates, result));
      }

      return result;
   }

   // HORNETQ-1029
   private void applyExpiryDelay(Message message, SimpleString address) {
      long expirationOverride = addressSettingsRepository.getMatch(address.toString()).getExpiryDelay();

      // A -1 <expiry-delay> means don't do anything
      if (expirationOverride >= 0) {
         // only override the exiration on messages where the expiration hasn't been set by the user
         if (message.getExpiration() == 0) {
            message.setExpiration(System.currentTimeMillis() + expirationOverride);
         }
      }
   }

   @Override
   public MessageReference reroute(final Message message, final Queue queue, final Transaction tx) throws Exception {

      setPagingStore(queue.getAddress(), message);

      MessageReference reference = MessageReference.Factory.createReference(message, queue);

      Long scheduledDeliveryTime = message.getScheduledDeliveryTime();
      if (scheduledDeliveryTime != null) {
         reference.setScheduledDeliveryTime(scheduledDeliveryTime);
      }

      message.incrementDurableRefCount();

      message.incrementRefCount();

      if (tx == null) {
         queue.reload(reference);
      } else {
         List<MessageReference> refs = new ArrayList<>(1);

         refs.add(reference);

         tx.addOperation(new AddOperation(refs));
      }

      return reference;
   }

   /**
    * The redistribution can't process the route right away as we may be dealing with a large message which will need to be processed on a different thread
    */
   @Override
   public Pair<RoutingContext, Message> redistribute(final Message message,
                                                     final Queue originatingQueue,
                                                     final Transaction tx) throws Exception {
      Bindings bindings = addressManager.getBindingsForRoutingAddress(originatingQueue.getAddress());

      if (bindings != null && bindings.allowRedistribute()) {
         // We have to copy the message and store it separately, otherwise we may lose remote bindings in case of restart before the message
         // arrived the target node
         // as described on https://issues.jboss.org/browse/JBPAPP-6130
         Message copyRedistribute = message.copy(storageManager.generateID());
         copyRedistribute.setAddress(originatingQueue.getAddress());

         if (tx != null) {
            tx.addOperation(new TransactionOperationAbstract() {
               @Override
               public void afterRollback(Transaction tx) {
                  try {
                     //this will cause large message file to be
                     //cleaned up
                     copyRedistribute.decrementRefCount();
                  } catch (Exception e) {
                     logger.warn("Failed to clean up message: " + copyRedistribute);
                  }
               }
            });
         }

         RoutingContext context = new RoutingContextImpl(tx);

         boolean routed = bindings.redistribute(copyRedistribute, originatingQueue, context);

         if (routed) {
            return new Pair<>(context, copyRedistribute);
         }
      }

      return null;
   }

   @Override
   public DuplicateIDCache getDuplicateIDCache(final SimpleString address) {
      DuplicateIDCache cache = duplicateIDCaches.get(address);

      if (cache == null) {
         cache = new DuplicateIDCacheImpl(address, idCacheSize, storageManager, persistIDCache);

         DuplicateIDCache oldCache = duplicateIDCaches.putIfAbsent(address, cache);

         if (oldCache != null) {
            cache = oldCache;
         }
      }

      return cache;
   }

   public ConcurrentMap<SimpleString, DuplicateIDCache> getDuplicateIDCaches() {
      return duplicateIDCaches;
   }

   @Override
   public Object getNotificationLock() {
      return notificationLock;
   }

   @Override
   public Set<SimpleString> getAddresses() {
      return addressManager.getAddresses();
   }

   @Override
   public void updateMessageLoadBalancingTypeForAddress(SimpleString  address, MessageLoadBalancingType messageLoadBalancingType) throws Exception {
      addressManager.updateMessageLoadBalancingTypeForAddress(address, messageLoadBalancingType);
   }

   @Override
   public SimpleString getMatchingQueue(SimpleString address, RoutingType routingType) throws Exception {
      return addressManager.getMatchingQueue(address, routingType);
   }

   @Override
   public SimpleString getMatchingQueue(SimpleString address,
                                        SimpleString queueName,
                                        RoutingType routingType) throws Exception {
      return addressManager.getMatchingQueue(address, queueName, routingType);
   }

   @Override
   public void sendQueueInfoToQueue(final SimpleString queueName, final SimpleString address) throws Exception {
      // We send direct to the queue so we can send it to the same queue that is bound to the notifications address -
      // this is crucial for ensuring
      // that queue infos and notifications are received in a contiguous consistent stream
      Binding binding = addressManager.getBinding(queueName);

      if (binding == null) {
         throw new IllegalStateException("Cannot find queue " + queueName);
      }

      if (logger.isDebugEnabled()) {
         logger.debug("PostOffice.sendQueueInfoToQueue on server=" + this.server + ", queueName=" + queueName + " and address=" + address);
      }

      Queue queue = (Queue) binding.getBindable();

      // Need to lock to make sure all queue info and notifications are in the correct order with no gaps
      synchronized (notificationLock) {
         // First send a reset message

         Message message = new CoreMessage(storageManager.generateID(), 50);

         message.setAddress(queueName);
         message.putBooleanProperty(PostOfficeImpl.HDR_RESET_QUEUE_DATA, true);
         routeQueueInfo(message, queue, false);

         for (QueueInfo info : queueInfos.values()) {
            if (logger.isTraceEnabled()) {
               logger.trace("QueueInfo on sendQueueInfoToQueue = " + info);
            }
            if (info.matchesAddress(address)) {
               message = createQueueInfoMessage(CoreNotificationType.BINDING_ADDED, queueName);

               message.putStringProperty(ManagementHelper.HDR_ADDRESS, info.getAddress());
               message.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, info.getClusterName());
               message.putStringProperty(ManagementHelper.HDR_ROUTING_NAME, info.getRoutingName());
               message.putLongProperty(ManagementHelper.HDR_BINDING_ID, info.getID());
               message.putStringProperty(ManagementHelper.HDR_FILTERSTRING, info.getFilterString());
               message.putIntProperty(ManagementHelper.HDR_DISTANCE, info.getDistance());

               routeQueueInfo(message, queue, true);

               int consumersWithFilters = info.getFilterStrings() != null ? info.getFilterStrings().size() : 0;

               for (int i = 0; i < info.getNumberOfConsumers() - consumersWithFilters; i++) {
                  message = createQueueInfoMessage(CoreNotificationType.CONSUMER_CREATED, queueName);

                  message.putStringProperty(ManagementHelper.HDR_ADDRESS, info.getAddress());
                  message.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, info.getClusterName());
                  message.putStringProperty(ManagementHelper.HDR_ROUTING_NAME, info.getRoutingName());
                  message.putIntProperty(ManagementHelper.HDR_DISTANCE, info.getDistance());

                  routeQueueInfo(message, queue, true);
               }

               if (info.getFilterStrings() != null) {
                  for (SimpleString filterString : info.getFilterStrings()) {
                     message = createQueueInfoMessage(CoreNotificationType.CONSUMER_CREATED, queueName);

                     message.putStringProperty(ManagementHelper.HDR_ADDRESS, info.getAddress());
                     message.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, info.getClusterName());
                     message.putStringProperty(ManagementHelper.HDR_ROUTING_NAME, info.getRoutingName());
                     message.putStringProperty(ManagementHelper.HDR_FILTERSTRING, filterString);
                     message.putIntProperty(ManagementHelper.HDR_DISTANCE, info.getDistance());

                     routeQueueInfo(message, queue, true);
                  }
               }
            }
         }
         Message completeMessage = new CoreMessage(storageManager.generateID(), 50);

         completeMessage.setAddress(queueName);
         completeMessage.putBooleanProperty(PostOfficeImpl.HDR_RESET_QUEUE_DATA_COMPLETE, true);
         routeQueueInfo(completeMessage, queue, false);
      }

   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString() {
      return "PostOfficeImpl [server=" + server + "]";
   }

   // Private -----------------------------------------------------------------

   private void setPagingStore(SimpleString address, Message message) throws Exception {
      PagingStore store = pagingManager.getPageStore(address);

      message.setContext(store);
   }

   private void routeQueueInfo(final Message message, final Queue queue, final boolean applyFilters) throws Exception {
      if (!applyFilters || queue.getFilter() == null || queue.getFilter().match(message)) {
         RoutingContext context = new RoutingContextImpl(null);

         queue.route(message, context);

         processRoute(message, context, false);
      }
   }

   private static class PageDelivery extends TransactionOperationAbstract {

      private final Set<Queue> queues = new HashSet<>();

      public void addQueues(List<Queue> queueList) {
         queues.addAll(queueList);
      }

      @Override
      public void afterCommit(Transaction tx) {
         // We need to try delivering async after paging, or nothing may start a delivery after paging since nothing is
         // going towards the queues
         // The queue will try to depage case it's empty
         for (Queue queue : queues) {
            queue.deliverAsync();
         }
      }

      @Override
      public List<MessageReference> getRelatedMessageReferences() {
         return Collections.emptyList();
      }

   }

   @Override
   public void processRoute(final Message message,
                            final RoutingContext context,
                            final boolean direct) throws Exception {
      final List<MessageReference> refs = new ArrayList<>();

      Transaction tx = context.getTransaction();

      Long deliveryTime = message.getScheduledDeliveryTime();

      for (Map.Entry<SimpleString, RouteContextList> entry : context.getContexListing().entrySet()) {
         PagingStore store = pagingManager.getPageStore(entry.getKey());

         if (storageManager.addToPage(store, message, context.getTransaction(), entry.getValue())) {
            if (message.isLargeMessage()) {
               confirmLargeMessageSend(tx, message);
            }

            // We need to kick delivery so the Queues may check for the cursors case they are empty
            schedulePageDelivery(tx, entry);
            continue;
         }

         for (Queue queue : entry.getValue().getNonDurableQueues()) {
            MessageReference reference = MessageReference.Factory.createReference(message, queue);

            if (deliveryTime != null) {
               reference.setScheduledDeliveryTime(deliveryTime);
            }
            refs.add(reference);

            message.incrementRefCount();
         }

         Iterator<Queue> iter = entry.getValue().getDurableQueues().iterator();

         while (iter.hasNext()) {
            Queue queue = iter.next();

            MessageReference reference = MessageReference.Factory.createReference(message, queue);

            if (context.isAlreadyAcked(context.getAddress(message), queue)) {
               reference.setAlreadyAcked();
               if (tx != null) {
                  queue.acknowledge(tx, reference);
               }
            }

            if (deliveryTime != null) {
               reference.setScheduledDeliveryTime(deliveryTime);
            }
            refs.add(reference);

            if (message.isDurable()) {
               int durableRefCount = message.incrementDurableRefCount();

               if (durableRefCount == 1) {
                  if (tx != null) {
                     storageManager.storeMessageTransactional(tx.getID(), message);
                  } else {
                     storageManager.storeMessage(message);
                  }

                  if (message.isLargeMessage()) {
                     confirmLargeMessageSend(tx, message);
                  }
               }

               if (tx != null) {
                  storageManager.storeReferenceTransactional(tx.getID(), queue.getID(), message.getMessageID());

                  tx.setContainsPersistent();
               } else {
                  storageManager.storeReference(queue.getID(), message.getMessageID(), !iter.hasNext());
               }

               if (deliveryTime > 0) {
                  if (tx != null) {
                     storageManager.updateScheduledDeliveryTimeTransactional(tx.getID(), reference);
                  } else {
                     storageManager.updateScheduledDeliveryTime(reference);
                  }
               }
            }

            message.incrementRefCount();
         }
      }

      if (tx != null) {
         tx.addOperation(new AddOperation(refs));
      } else {
         // This will use the same thread if there are no pending operations
         // avoiding a context switch on this case
         storageManager.afterCompleteOperations(new IOCallback() {
            @Override
            public void onError(final int errorCode, final String errorMessage) {
               ActiveMQServerLogger.LOGGER.ioErrorAddingReferences(errorCode, errorMessage);
            }

            @Override
            public void done() {
               addReferences(refs, direct);
            }
         });
      }
   }

   /**
    * @param tx
    * @param message
    * @throws Exception
    */
   private void confirmLargeMessageSend(Transaction tx, final Message message) throws Exception {
      LargeServerMessage largeServerMessage = (LargeServerMessage) message;
      if (largeServerMessage.getPendingRecordID() >= 0) {
         if (tx == null) {
            storageManager.confirmPendingLargeMessage(largeServerMessage.getPendingRecordID());
         } else {
            storageManager.confirmPendingLargeMessageTX(tx, largeServerMessage.getMessageID(), largeServerMessage.getPendingRecordID());
         }
         largeServerMessage.setPendingRecordID(-1);
      }
   }

   /**
    * This will kick a delivery async on the queue, so the queue may have a chance to depage messages
    *
    * @param tx
    * @param entry
    */
   private void schedulePageDelivery(Transaction tx, Map.Entry<SimpleString, RouteContextList> entry) {
      if (tx != null) {
         PageDelivery delivery = (PageDelivery) tx.getProperty(TransactionPropertyIndexes.PAGE_DELIVERY);
         if (delivery == null) {
            delivery = new PageDelivery();
            tx.putProperty(TransactionPropertyIndexes.PAGE_DELIVERY, delivery);
            tx.addOperation(delivery);
         }

         delivery.addQueues(entry.getValue().getDurableQueues());
         delivery.addQueues(entry.getValue().getNonDurableQueues());
      } else {

         List<Queue> durableQueues = entry.getValue().getDurableQueues();
         List<Queue> nonDurableQueues = entry.getValue().getNonDurableQueues();

         final List<Queue> queues = new ArrayList<>(durableQueues.size() + nonDurableQueues.size());

         queues.addAll(durableQueues);
         queues.addAll(nonDurableQueues);

         storageManager.afterCompleteOperations(new IOCallback() {

            @Override
            public void onError(int errorCode, String errorMessage) {
            }

            @Override
            public void done() {
               for (Queue queue : queues) {
                  // in case of paging, we need to kick asynchronous delivery to try delivering
                  queue.deliverAsync();
               }
            }
         });
      }
   }

   private boolean checkDuplicateID(final Message message,
                                    final RoutingContext context,
                                    boolean rejectDuplicates,
                                    AtomicBoolean startedTX) throws Exception {
      // Check the DuplicateCache for the Bridge first

      Object bridgeDup = message.removeExtraBytesProperty(Message.HDR_BRIDGE_DUPLICATE_ID);
      if (bridgeDup != null) {
         // if the message is being sent from the bridge, we just ignore the duplicate id, and use the internal one
         byte[] bridgeDupBytes = (byte[]) bridgeDup;

         DuplicateIDCache cacheBridge = getDuplicateIDCache(BRIDGE_CACHE_STR.concat(context.getAddress(message).toString()));

         if (context.getTransaction() == null) {
            context.setTransaction(new TransactionImpl(storageManager));
            startedTX.set(true);
         }

         if (!cacheBridge.atomicVerify(bridgeDupBytes, context.getTransaction())) {
            context.getTransaction().rollback();
            startedTX.set(false);
            message.decrementRefCount();
            return false;
         }
      } else {
         // if used BridgeDuplicate, it's not going to use the regular duplicate
         // since this will would break redistribution (re-setting the duplicateId)
         byte[] duplicateIDBytes = message.getDuplicateIDBytes();

         DuplicateIDCache cache = null;

         boolean isDuplicate = false;

         if (duplicateIDBytes != null) {
            cache = getDuplicateIDCache(context.getAddress(message));

            isDuplicate = cache.contains(duplicateIDBytes);

            if (rejectDuplicates && isDuplicate) {
               ActiveMQServerLogger.LOGGER.duplicateMessageDetected(message);

               String warnMessage = "Duplicate message detected - message will not be routed. Message information:" + message.toString();

               if (context.getTransaction() != null) {
                  context.getTransaction().markAsRollbackOnly(new ActiveMQDuplicateIdException(warnMessage));
               }

               message.decrementRefCount();

               return false;
            }
         }

         if (cache != null && !isDuplicate) {
            if (context.getTransaction() == null) {
               // We need to store the duplicate id atomically with the message storage, so we need to create a tx for this
               context.setTransaction(new TransactionImpl(storageManager));

               startedTX.set(true);
            }

            cache.addToCache(duplicateIDBytes, context.getTransaction(), false);
         }
      }

      return true;
   }

   /**
    * @param refs
    */
   private void addReferences(final List<MessageReference> refs, final boolean direct) {
      for (MessageReference ref : refs) {
         ref.getQueue().addTail(ref, direct);
      }
   }

   /**
    * The expiry scanner can't be started until the whole server has been started other wise you may get races
    */
   @Override
   public synchronized void startExpiryScanner() {
      if (reaperPeriod > 0) {
         if (reaperRunnable != null)
            reaperRunnable.stop();
         reaperRunnable = new Reaper(server.getScheduledPool(), server.getExecutorFactory().getExecutor(), reaperPeriod, TimeUnit.MILLISECONDS, false);

         reaperRunnable.start();
      }
   }

   private Message createQueueInfoMessage(final NotificationType type, final SimpleString queueName) {
      Message message = new CoreMessage().initBuffer(50).setMessageID(storageManager.generateID());

      message.setAddress(queueName);

      String uid = UUIDGenerator.getInstance().generateStringUUID();

      message.putStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE, new SimpleString(type.toString()));
      message.putLongProperty(ManagementHelper.HDR_NOTIFICATION_TIMESTAMP, System.currentTimeMillis());

      message.putStringProperty(new SimpleString("foobar"), new SimpleString(uid));

      return message;
   }

   private final class Reaper extends ActiveMQScheduledComponent {

      Reaper(ScheduledExecutorService scheduledExecutorService,
             Executor executor,
             long checkPeriod,
             TimeUnit timeUnit,
             boolean onDemand) {
         super(scheduledExecutorService, executor, checkPeriod, timeUnit, onDemand);
      }

      @Override
      public void run() {
         // The reaper thread should be finished case the PostOffice is gone
         // This is to avoid leaks on PostOffice between stops and starts
         Map<SimpleString, Binding> nameMap = addressManager.getBindings();

         List<Queue> queues = new ArrayList<>();

         for (Binding binding : nameMap.values()) {
            if (binding.getType() == BindingType.LOCAL_QUEUE) {
               Queue queue = (Queue) binding.getBindable();

               queues.add(queue);
            }
         }

         for (Queue queue : queues) {
            try {
               queue.expireReferences();
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorExpiringMessages(e);
            }
         }
      }
   }

   public static final class AddOperation implements TransactionOperation {

      private final List<MessageReference> refs;

      AddOperation(final List<MessageReference> refs) {
         this.refs = refs;
      }

      @Override
      public void afterCommit(final Transaction tx) {
         for (MessageReference ref : refs) {
            if (!ref.isAlreadyAcked()) {
               ref.getQueue().addTail(ref, false);
            }
         }
      }

      @Override
      public void afterPrepare(final Transaction tx) {
         for (MessageReference ref : refs) {
            if (ref.isAlreadyAcked()) {
               ref.getQueue().referenceHandled(ref);
               ref.getQueue().incrementMesssagesAdded();
            }
         }
      }

      @Override
      public void afterRollback(final Transaction tx) {
      }

      @Override
      public void beforeCommit(final Transaction tx) throws Exception {
      }

      @Override
      public void beforePrepare(final Transaction tx) throws Exception {
      }

      @Override
      public void beforeRollback(final Transaction tx) throws Exception {
         // Reverse the ref counts, and paging sizes

         for (MessageReference ref : refs) {
            Message message = ref.getMessage();

            if (message.isDurable() && ref.getQueue().isDurableMessage()) {
               message.decrementDurableRefCount();
            }

            message.decrementRefCount();
         }
      }

      @Override
      public List<MessageReference> getRelatedMessageReferences() {
         return refs;
      }

      @Override
      public List<MessageReference> getListOnConsumer(long consumerID) {
         return Collections.emptyList();
      }
   }

   @Override
   public Bindings createBindings(final SimpleString address) throws Exception {
      GroupingHandler groupingHandler = server.getGroupingHandler();
      BindingsImpl bindings = new BindingsImpl(address, groupingHandler, pagingManager.getPageStore(address));
      if (groupingHandler != null) {
         groupingHandler.addListener(bindings);
      }
      return bindings;
   }

   // For tests only
   public AddressManager getAddressManager() {
      return addressManager;
   }

   public ActiveMQServer getServer() {
      return server;
   }
}

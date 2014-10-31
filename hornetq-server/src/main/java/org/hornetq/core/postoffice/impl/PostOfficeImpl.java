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
package org.hornetq.core.postoffice.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hornetq.api.core.HornetQAddressFullException;
import org.hornetq.api.core.HornetQDuplicateIdException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.HornetQNonExistentQueueException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.AddressManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.BindingsFactory;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueInfo;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.RouteContextList;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.impl.RoutingContextImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationListener;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;
import org.hornetq.core.transaction.TransactionOperationAbstract;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUIDGenerator;

/**
 * A PostOfficeImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="csuconic@redhat.com">Clebert Suconic</a>
 */
public class PostOfficeImpl implements PostOffice, NotificationListener, BindingsFactory
{
   private static final boolean isTrace = HornetQServerLogger.LOGGER.isTraceEnabled();

   public static final SimpleString HDR_RESET_QUEUE_DATA = new SimpleString("_HQ_RESET_QUEUE_DATA");

   public static final SimpleString HDR_RESET_QUEUE_DATA_COMPLETE = new SimpleString("_HQ_RESET_QUEUE_DATA_COMPLETE");

   public static final SimpleString BRIDGE_CACHE_STR = new SimpleString("BRIDGE.");

   private final AddressManager addressManager;

   private final QueueFactory queueFactory;

   private final StorageManager storageManager;

   private final PagingManager pagingManager;

   private volatile boolean started;

   private final ManagementService managementService;

   private Reaper reaperRunnable;

   private volatile Thread reaperThread;

   private final long reaperPeriod;

   private final int reaperPriority;

   private final ConcurrentMap<SimpleString, DuplicateIDCache> duplicateIDCaches = new ConcurrentHashMap<SimpleString, DuplicateIDCache>();

   private final int idCacheSize;

   private final boolean persistIDCache;

   private final Map<SimpleString, QueueInfo> queueInfos = new HashMap<SimpleString, QueueInfo>();

   private final Object notificationLock = new Object();

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final HornetQServer server;

   public PostOfficeImpl(final HornetQServer server,
                         final StorageManager storageManager,
                         final PagingManager pagingManager,
                         final QueueFactory bindableFactory,
                         final ManagementService managementService,
                         final long reaperPeriod,
                         final int reaperPriority,
                         final boolean enableWildCardRouting,
                         final int idCacheSize,
                         final boolean persistIDCache,
                         final HierarchicalRepository<AddressSettings> addressSettingsRepository)

   {
      this.storageManager = storageManager;

      queueFactory = bindableFactory;

      this.managementService = managementService;

      this.pagingManager = pagingManager;

      this.reaperPeriod = reaperPeriod;

      this.reaperPriority = reaperPriority;

      if (enableWildCardRouting)
      {
         addressManager = new WildcardAddressManager(this);
      }
      else
      {
         addressManager = new SimpleAddressManager(this);
      }

      this.idCacheSize = idCacheSize;

      this.persistIDCache = persistIDCache;

      this.addressSettingsRepository = addressSettingsRepository;

      this.server = server;
   }

   // HornetQComponent implementation ---------------------------------------

   public synchronized void start() throws Exception
   {
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

   public synchronized void stop() throws Exception
   {
      started = false;

      managementService.removeNotificationListener(this);

      if (reaperRunnable != null)
         reaperRunnable.stop();

      if (reaperThread != null)
      {
         reaperThread.join();

         reaperThread = null;
      }

      addressManager.clear();

      queueInfos.clear();
   }

   public boolean isStarted()
   {
      return started;
   }

   // NotificationListener implementation -------------------------------------

   public void onNotification(final Notification notification)
   {
      if (isTrace)
      {
         HornetQServerLogger.LOGGER.trace("Receiving notification : " + notification + " on server " + this.server);
      }
      synchronized (notificationLock)
      {
         NotificationType type = notification.getType();

         switch (type)
         {
            case BINDING_ADDED:
            {
               TypedProperties props = notification.getProperties();

               if (!props.containsProperty(ManagementHelper.HDR_BINDING_TYPE))
               {
                  throw HornetQMessageBundle.BUNDLE.bindingTypeNotSpecified();
               }

               Integer bindingType = props.getIntProperty(ManagementHelper.HDR_BINDING_TYPE);

               if (bindingType == BindingType.DIVERT_INDEX)
               {
                  // We don't propagate diverts
                  return;
               }

               SimpleString routingName = props.getSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME);

               SimpleString clusterName = props.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

               SimpleString address = props.getSimpleStringProperty(ManagementHelper.HDR_ADDRESS);

               if (!props.containsProperty(ManagementHelper.HDR_BINDING_ID))
               {
                  throw HornetQMessageBundle.BUNDLE.bindingIdNotSpecified();
               }

               long id = props.getLongProperty(ManagementHelper.HDR_BINDING_ID);

               SimpleString filterString = props.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

               if (!props.containsProperty(ManagementHelper.HDR_DISTANCE))
               {
                  throw HornetQMessageBundle.BUNDLE.distancenotSpecified();
               }

               int distance = props.getIntProperty(ManagementHelper.HDR_DISTANCE);

               QueueInfo info = new QueueInfo(routingName, clusterName, address, filterString, id, distance);

               queueInfos.put(clusterName, info);

               break;
            }
            case BINDING_REMOVED:
            {
               TypedProperties props = notification.getProperties();

               if (!props.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
               {
                  throw new IllegalStateException("No cluster name");
               }

               SimpleString clusterName = props.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

               QueueInfo info = queueInfos.remove(clusterName);

               if (info == null)
               {
                  throw new IllegalStateException("Cannot find queue info for queue " + clusterName);
               }

               break;
            }
            case CONSUMER_CREATED:
            {
               TypedProperties props = notification.getProperties();

               if (!props.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
               {
                  throw new IllegalStateException("No cluster name");
               }

               SimpleString clusterName = props.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

               SimpleString filterString = props.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

               QueueInfo info = queueInfos.get(clusterName);

               if (info == null)
               {
                  throw new IllegalStateException("Cannot find queue info for queue " + clusterName);
               }

               info.incrementConsumers();

               if (filterString != null)
               {
                  List<SimpleString> filterStrings = info.getFilterStrings();

                  if (filterStrings == null)
                  {
                     filterStrings = new ArrayList<SimpleString>();

                     info.setFilterStrings(filterStrings);
                  }

                  filterStrings.add(filterString);
               }

               if (!props.containsProperty(ManagementHelper.HDR_DISTANCE))
               {
                  throw new IllegalStateException("No distance");
               }

               int distance = props.getIntProperty(ManagementHelper.HDR_DISTANCE);

               if (distance > 0)
               {
                  SimpleString queueName = props.getSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME);

                  if (queueName == null)
                  {
                     throw new IllegalStateException("No queue name");
                  }

                  Binding binding = getBinding(queueName);

                  if (binding != null)
                  {
                     // We have a local queue
                     Queue queue = (Queue) binding.getBindable();

                     AddressSettings addressSettings = addressSettingsRepository.getMatch(binding.getAddress()
                                                                                             .toString());

                     long redistributionDelay = addressSettings.getRedistributionDelay();

                     if (redistributionDelay != -1)
                     {
                        queue.addRedistributor(redistributionDelay);
                     }
                  }
               }

               break;
            }
            case CONSUMER_CLOSED:
            {
               TypedProperties props = notification.getProperties();

               SimpleString clusterName = props.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

               if (clusterName == null)
               {
                  throw new IllegalStateException("No cluster name");
               }

               SimpleString filterString = props.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

               QueueInfo info = queueInfos.get(clusterName);

               if (info == null)
               {
                  return;
               }

               info.decrementConsumers();

               if (filterString != null)
               {
                  List<SimpleString> filterStrings = info.getFilterStrings();

                  filterStrings.remove(filterString);
               }

               if (info.getNumberOfConsumers() == 0)
               {
                  if (!props.containsProperty(ManagementHelper.HDR_DISTANCE))
                  {
                     throw new IllegalStateException("No cluster name");
                  }

                  int distance = props.getIntProperty(ManagementHelper.HDR_DISTANCE);

                  if (distance == 0)
                  {
                     SimpleString queueName = props.getSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME);

                     if (queueName == null)
                     {
                        throw new IllegalStateException("No queue name");
                     }

                     Binding binding = getBinding(queueName);

                     if (binding == null)
                     {
                        throw new IllegalStateException("No queue " + queueName);
                     }

                     Queue queue = (Queue) binding.getBindable();

                     AddressSettings addressSettings = addressSettingsRepository.getMatch(binding.getAddress()
                                                                                             .toString());

                     long redistributionDelay = addressSettings.getRedistributionDelay();

                     if (redistributionDelay != -1)
                     {
                        queue.addRedistributor(redistributionDelay);
                     }
                  }
               }

               break;
            }
            default:
            {
               break;
            }
         }
      }
   }

   // PostOffice implementation -----------------------------------------------

   // TODO - needs to be synchronized to prevent happening concurrently with activate().
   // (and possible removeBinding and other methods)
   // Otherwise can have situation where createQueue comes in before failover, then failover occurs
   // and post office is activated but queue remains unactivated after failover so delivery never occurs
   // even though failover is complete
   public synchronized void addBinding(final Binding binding) throws Exception
   {
      addressManager.addBinding(binding);

      TypedProperties props = new TypedProperties();

      props.putIntProperty(ManagementHelper.HDR_BINDING_TYPE, binding.getType().toInt());

      props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

      props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

      props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

      props.putLongProperty(ManagementHelper.HDR_BINDING_ID, binding.getID());

      props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

      Filter filter = binding.getFilter();

      if (filter != null)
      {
         props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, filter.getFilterString());
      }

      String uid = UUIDGenerator.getInstance().generateStringUUID();

      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug("ClusterCommunication::Sending notification for addBinding " + binding + " from server " + server);
      }

      managementService.sendNotification(new Notification(uid, NotificationType.BINDING_ADDED, props));
   }

   public synchronized Binding removeBinding(final SimpleString uniqueName, Transaction tx) throws Exception
   {

      addressSettingsRepository.clearCache();

      Binding binding = addressManager.removeBinding(uniqueName, tx);

      if (binding == null)
      {
         throw new HornetQNonExistentQueueException();
      }

      if (addressManager.getBindingsForRoutingAddress(binding.getAddress()) == null)
      {
         pagingManager.deletePageStore(binding.getAddress());

         managementService.unregisterAddress(binding.getAddress());

         deleteDuplicateCache(binding.getAddress());
      }

      if (binding.getType() == BindingType.LOCAL_QUEUE)
      {
         managementService.unregisterQueue(uniqueName, binding.getAddress());
      }
      else if (binding.getType() == BindingType.DIVERT)
      {
         managementService.unregisterDivert(uniqueName);
      }

      if (binding.getType() != BindingType.DIVERT)
      {
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

         props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

         props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

         props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

         props.putLongProperty(ManagementHelper.HDR_BINDING_ID, binding.getID());

         if (binding.getFilter() == null)
         {
            props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, null);
         }
         else
         {
            props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, binding.getFilter().getFilterString());
         }

         managementService.sendNotification(new Notification(null, NotificationType.BINDING_REMOVED, props));
      }

      binding.close();

      return binding;
   }

   private void deleteDuplicateCache(SimpleString address) throws Exception
   {
      DuplicateIDCache cache = duplicateIDCaches.remove(address);

      if (cache != null)
      {
         cache.clear();
      }
   }

   @Override
   public boolean isAddressBound(final SimpleString address) throws Exception
   {
      Bindings bindings = getBindingsForAddress(address);
      return bindings != null && !bindings.getBindings().isEmpty();
   }


   public Bindings getBindingsForAddress(final SimpleString address) throws Exception
   {
      Bindings bindings = addressManager.getBindingsForRoutingAddress(address);

      if (bindings == null)
      {
         bindings = createBindings(address);
      }

      return bindings;
   }


   public Bindings lookupBindingsForAddress(final SimpleString address) throws Exception
   {
      return addressManager.getBindingsForRoutingAddress(address);
   }

   public Binding getBinding(final SimpleString name)
   {
      return addressManager.getBinding(name);
   }

   public Bindings getMatchingBindings(final SimpleString address) throws Exception
   {
      return addressManager.getMatchingBindings(address);
   }

   public Map<SimpleString, Binding> getAllBindings()
   {
      return addressManager.getBindings();
   }

   public void route(final ServerMessage message, final boolean direct) throws Exception
   {
      route(message, (Transaction) null, direct);
   }

   public void route(final ServerMessage message, final Transaction tx, final boolean direct) throws Exception
   {
      route(message, new RoutingContextImpl(tx), direct);
   }

   public void route(final ServerMessage message,
                     final Transaction tx,
                     final boolean direct,
                     final boolean rejectDuplicates) throws Exception
   {
      route(message, new RoutingContextImpl(tx), direct, rejectDuplicates);
   }

   public void route(final ServerMessage message, final RoutingContext context, final boolean direct) throws Exception
   {
      route(message, context, direct, true);
   }

   public void route(final ServerMessage message,
                     final RoutingContext context,
                     final boolean direct,
                     boolean rejectDuplicates) throws Exception
   {
      // Sanity check
      if (message.getRefCount() > 0)
      {
         throw new IllegalStateException("Message cannot be routed more than once");
      }

      SimpleString address = message.getAddress();

      setPagingStore(message);

      AtomicBoolean startedTX = new AtomicBoolean(false);

      applyExpiryDelay(message, address);

      if (!checkDuplicateID(message, context, rejectDuplicates, startedTX))
      {
         return;
      }

      if (message.hasInternalProperties())
      {
         // We need to perform some cleanup on internal properties,
         // but we don't do it every time, otherwise it wouldn't be optimal
         cleanupInternalPropertiesBeforeRouting(message);
      }

      Bindings bindings = addressManager.getBindingsForRoutingAddress(address);

      if (bindings != null)
      {
         bindings.route(message, context);
      }
      else
      {
         // this is a debug and not warn because this could be a regular scenario on publish-subscribe queues (or topic subscriptions on JMS)
         if (HornetQServerLogger.LOGGER.isDebugEnabled())
         {
            HornetQServerLogger.LOGGER.debug("Couldn't find any bindings for address=" + address + " on message=" + message);
         }
      }

      if (HornetQServerLogger.LOGGER.isTraceEnabled())
      {
         HornetQServerLogger.LOGGER.trace("Message after routed=" + message);
      }

      if (context.getQueueCount() == 0)
      {
         // Send to DLA if appropriate

         AddressSettings addressSettings = addressSettingsRepository.getMatch(address.toString());

         boolean sendToDLA = addressSettings.isSendToDLAOnNoRoute();

         if (sendToDLA)
         {
            // Send to the DLA for the address

            SimpleString dlaAddress = addressSettings.getDeadLetterAddress();

            if (HornetQServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQServerLogger.LOGGER.debug("sending message to dla address = " + dlaAddress + ", message=" + message);
            }

            if (dlaAddress == null)
            {
               HornetQServerLogger.LOGGER.noDLA(address);
            }
            else
            {
               message.setOriginalHeaders(message, null, false);

               message.setAddress(dlaAddress);

               route(message, context.getTransaction(), false);
            }
         }
         else
         {
            if (HornetQServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQServerLogger.LOGGER.debug("Message " + message + " is not going anywhere as it didn't have a binding on address:" + address);
            }

            if (message.isLargeMessage())
            {
               ((LargeServerMessage) message).deleteFile();
            }
         }
      }
      else
      {
         try
         {
            processRoute(message, context, direct);
         }
         catch (HornetQAddressFullException e)
         {
            if (startedTX.get())
            {
               context.getTransaction().rollback();
            }
            else if (context.getTransaction() != null)
            {
               context.getTransaction().markAsRollbackOnly(e);
            }
            throw e;
         }
      }

      if (startedTX.get())
      {
         context.getTransaction().commit();
      }
   }

   // HORNETQ-1029
   private void applyExpiryDelay(ServerMessage message, SimpleString address)
   {
      long expirationOverride = addressSettingsRepository.getMatch(address.toString()).getExpiryDelay();

      // A -1 <expiry-delay> means don't do anything
      if (expirationOverride >= 0)
      {
         // only override the exiration on messages where the expiration hasn't been set by the user
         if (message.getExpiration() == 0)
         {
            message.setExpiration(System.currentTimeMillis() + expirationOverride);
         }
      }
   }

   public MessageReference reroute(final ServerMessage message, final Queue queue, final Transaction tx) throws Exception
   {
      setPagingStore(message);

      MessageReference reference = message.createReference(queue);

      if (message.containsProperty(Message.HDR_SCHEDULED_DELIVERY_TIME))
      {
         Long scheduledDeliveryTime = message.getLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
         reference.setScheduledDeliveryTime(scheduledDeliveryTime);
      }

      message.incrementDurableRefCount();

      message.incrementRefCount();

      if (tx == null)
      {
         queue.reload(reference);
      }
      else
      {
         List<MessageReference> refs = new ArrayList<MessageReference>(1);

         refs.add(reference);

         tx.addOperation(new AddOperation(refs));
      }

      return reference;
   }

   /**
    * The redistribution can't process the route right away as we may be dealing with a large message which will need to be processed on a different thread
    */
   public Pair<RoutingContext, ServerMessage> redistribute(final ServerMessage message, final Queue originatingQueue, final Transaction tx) throws Exception
   {
      // We have to copy the message and store it separately, otherwise we may lose remote bindings in case of restart before the message
      // arrived the target node
      // as described on https://issues.jboss.org/browse/JBPAPP-6130
      ServerMessage copyRedistribute = message.copy(storageManager.generateUniqueID());

      Bindings bindings = addressManager.getBindingsForRoutingAddress(message.getAddress());

      if (bindings != null)
      {
         RoutingContext context = new RoutingContextImpl(tx);

         boolean routed = bindings.redistribute(copyRedistribute, originatingQueue, context);

         if (routed)
         {
            return new Pair<RoutingContext, ServerMessage>(context, copyRedistribute);
         }
      }

      return null;
   }

   public DuplicateIDCache getDuplicateIDCache(final SimpleString address)
   {
      DuplicateIDCache cache = duplicateIDCaches.get(address);

      if (cache == null)
      {
         cache = new DuplicateIDCacheImpl(address, idCacheSize, storageManager, persistIDCache);

         DuplicateIDCache oldCache = duplicateIDCaches.putIfAbsent(address, cache);

         if (oldCache != null)
         {
            cache = oldCache;
         }
      }

      return cache;
   }

   public ConcurrentMap<SimpleString, DuplicateIDCache> getDuplicateIDCaches()
   {
      return duplicateIDCaches;
   }

   public Object getNotificationLock()
   {
      return notificationLock;
   }

   public void sendQueueInfoToQueue(final SimpleString queueName, final SimpleString address) throws Exception
   {
      // We send direct to the queue so we can send it to the same queue that is bound to the notifications address -
      // this is crucial for ensuring
      // that queue infos and notifications are received in a contiguous consistent stream
      Binding binding = addressManager.getBinding(queueName);

      if (binding == null)
      {
         throw new IllegalStateException("Cannot find queue " + queueName);
      }

      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug("PostOffice.sendQueueInfoToQueue on server=" + this.server + ", queueName=" + queueName + " and address=" + address);
      }

      Queue queue = (Queue) binding.getBindable();

      // Need to lock to make sure all queue info and notifications are in the correct order with no gaps
      synchronized (notificationLock)
      {
         // First send a reset message

         ServerMessage message = new ServerMessageImpl(storageManager.generateUniqueID(), 50);

         message.setAddress(queueName);
         message.putBooleanProperty(PostOfficeImpl.HDR_RESET_QUEUE_DATA, true);
         routeQueueInfo(message, queue, false);

         for (QueueInfo info : queueInfos.values())
         {
            if (HornetQServerLogger.LOGGER.isTraceEnabled())
            {
               HornetQServerLogger.LOGGER.trace("QueueInfo on sendQueueInfoToQueue = " + info);
            }
            if (info.getAddress().startsWith(address))
            {
               message = createQueueInfoMessage(NotificationType.BINDING_ADDED, queueName);

               message.putStringProperty(ManagementHelper.HDR_ADDRESS, info.getAddress());
               message.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, info.getClusterName());
               message.putStringProperty(ManagementHelper.HDR_ROUTING_NAME, info.getRoutingName());
               message.putLongProperty(ManagementHelper.HDR_BINDING_ID, info.getID());
               message.putStringProperty(ManagementHelper.HDR_FILTERSTRING, info.getFilterString());
               message.putIntProperty(ManagementHelper.HDR_DISTANCE, info.getDistance());

               routeQueueInfo(message, queue, true);

               int consumersWithFilters = info.getFilterStrings() != null ? info.getFilterStrings().size() : 0;

               for (int i = 0; i < info.getNumberOfConsumers() - consumersWithFilters; i++)
               {
                  message = createQueueInfoMessage(NotificationType.CONSUMER_CREATED, queueName);

                  message.putStringProperty(ManagementHelper.HDR_ADDRESS, info.getAddress());
                  message.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, info.getClusterName());
                  message.putStringProperty(ManagementHelper.HDR_ROUTING_NAME, info.getRoutingName());
                  message.putIntProperty(ManagementHelper.HDR_DISTANCE, info.getDistance());

                  routeQueueInfo(message, queue, true);
               }

               if (info.getFilterStrings() != null)
               {
                  for (SimpleString filterString : info.getFilterStrings())
                  {
                     message = createQueueInfoMessage(NotificationType.CONSUMER_CREATED, queueName);

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
         ServerMessage completeMessage = new ServerMessageImpl(storageManager.generateUniqueID(), 50);

         completeMessage.setAddress(queueName);
         completeMessage.putBooleanProperty(PostOfficeImpl.HDR_RESET_QUEUE_DATA_COMPLETE, true);
         routeQueueInfo(completeMessage, queue, false);
      }

   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "PostOfficeImpl [server=" + server + "]";
   }

   // Private -----------------------------------------------------------------

   /**
    * @param message
    */
   protected void cleanupInternalPropertiesBeforeRouting(final ServerMessage message)
   {
      LinkedList<SimpleString> valuesToRemove = null;


      for (SimpleString name : message.getPropertyNames())
      {
         // We use properties to establish routing context on clustering.
         // However if the client resends the message after receiving, it needs to be removed
         if ((name.startsWith(MessageImpl.HDR_ROUTE_TO_IDS) && !name.equals(MessageImpl.HDR_ROUTE_TO_IDS)) ||
            name.equals(MessageImpl.HDR_ROUTE_TO_ACK_IDS))
         {
            if (valuesToRemove == null)
            {
               valuesToRemove = new LinkedList<SimpleString>();
            }
            valuesToRemove.add(name);
         }
      }

      if (valuesToRemove != null)
      {
         for (SimpleString removal : valuesToRemove)
         {
            message.removeProperty(removal);
         }
      }
   }


   private void setPagingStore(final ServerMessage message) throws Exception
   {
      PagingStore store = pagingManager.getPageStore(message.getAddress());

      message.setPagingStore(store);
   }

   private void routeQueueInfo(final ServerMessage message, final Queue queue, final boolean applyFilters) throws Exception
   {
      if (!applyFilters || queue.getFilter() == null || queue.getFilter().match(message))
      {
         RoutingContext context = new RoutingContextImpl(null);

         queue.route(message, context);

         processRoute(message, context, false);
      }
   }

   private static class PageDelivery extends TransactionOperationAbstract
   {
      private final Set<Queue> queues = new HashSet<Queue>();

      public void addQueues(List<Queue> queueList)
      {
         queues.addAll(queueList);
      }

      @Override
      public void afterCommit(Transaction tx)
      {
         // We need to try delivering async after paging, or nothing may start a delivery after paging since nothing is
         // going towards the queues
         // The queue will try to depage case it's empty
         for (Queue queue : queues)
         {
            queue.deliverAsync();
         }
      }

      @Override
      public List<MessageReference> getRelatedMessageReferences()
      {
         return Collections.emptyList();
      }

   }

   public void processRoute(final ServerMessage message, final RoutingContext context, final boolean direct) throws Exception
   {
      final List<MessageReference> refs = new ArrayList<MessageReference>();

      Transaction tx = context.getTransaction();

      for (Map.Entry<SimpleString, RouteContextList> entry : context.getContexListing().entrySet())
      {
         PagingStore store = pagingManager.getPageStore(entry.getKey());

         if (storageManager.addToPage(store, message, context.getTransaction(), entry.getValue()))
         {
            if (message.isLargeMessage())
            {
               confirmLargeMessageSend(tx, message);
            }

            // We need to kick delivery so the Queues may check for the cursors case they are empty
            schedulePageDelivery(tx, entry);
            continue;
         }

         for (Queue queue : entry.getValue().getNonDurableQueues())
         {
            MessageReference reference = message.createReference(queue);

            refs.add(reference);
            if (message.containsProperty(Message.HDR_SCHEDULED_DELIVERY_TIME))
            {
               Long scheduledDeliveryTime = message.getLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);

               reference.setScheduledDeliveryTime(scheduledDeliveryTime);
            }

            message.incrementRefCount();
         }

         Iterator<Queue> iter = entry.getValue().getDurableQueues().iterator();

         while (iter.hasNext())
         {
            Queue queue = iter.next();

            MessageReference reference = message.createReference(queue);

            if (context.isAlreadyAcked(message.getAddress(), queue))
            {
               reference.setAlreadyAcked();
               if (tx != null)
               {
                  queue.acknowledge(tx, reference);
               }
            }

            refs.add(reference);

            if (message.containsProperty(Message.HDR_SCHEDULED_DELIVERY_TIME))
            {
               Long scheduledDeliveryTime = message.getLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);

               reference.setScheduledDeliveryTime(scheduledDeliveryTime);
            }

            if (message.isDurable())
            {
               int durableRefCount = message.incrementDurableRefCount();

               if (durableRefCount == 1)
               {
                  if (tx != null)
                  {
                     storageManager.storeMessageTransactional(tx.getID(), message);
                  }
                  else
                  {
                     storageManager.storeMessage(message);
                  }

                  if (message.isLargeMessage())
                  {
                     confirmLargeMessageSend(tx, message);
                  }
               }

               if (tx != null)
               {
                  storageManager.storeReferenceTransactional(tx.getID(), queue.getID(), message.getMessageID());

                  tx.setContainsPersistent();
               }
               else
               {
                  storageManager.storeReference(queue.getID(), message.getMessageID(), !iter.hasNext());
               }

               if (message.containsProperty(Message.HDR_SCHEDULED_DELIVERY_TIME))
               {
                  if (tx != null)
                  {
                     storageManager.updateScheduledDeliveryTimeTransactional(tx.getID(), reference);
                  }
                  else
                  {
                     storageManager.updateScheduledDeliveryTime(reference);
                  }
               }
            }

            message.incrementRefCount();
         }
      }

      if (tx != null)
      {
         tx.addOperation(new AddOperation(refs));
      }
      else
      {
         // This will use the same thread if there are no pending operations
         // avoiding a context switch on this case
         storageManager.afterCompleteOperations(new IOAsyncTask()
         {
            public void onError(final int errorCode, final String errorMessage)
            {
               HornetQServerLogger.LOGGER.ioErrorAddingReferences(errorCode, errorMessage);
            }

            public void done()
            {
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
   private void confirmLargeMessageSend(Transaction tx, final ServerMessage message) throws Exception
   {
      LargeServerMessage largeServerMessage = (LargeServerMessage) message;
      if (largeServerMessage.getPendingRecordID() >= 0)
      {
         if (tx == null)
         {
            storageManager.confirmPendingLargeMessage(largeServerMessage.getPendingRecordID());
         }
         else
         {
            storageManager.confirmPendingLargeMessageTX(tx, largeServerMessage.getMessageID(),
                                                        largeServerMessage.getPendingRecordID());
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
   private void schedulePageDelivery(Transaction tx, Map.Entry<SimpleString, RouteContextList> entry)
   {
      if (tx != null)
      {
         PageDelivery delivery = (PageDelivery) tx.getProperty(TransactionPropertyIndexes.PAGE_DELIVERY);
         if (delivery == null)
         {
            delivery = new PageDelivery();
            tx.putProperty(TransactionPropertyIndexes.PAGE_DELIVERY, delivery);
            tx.addOperation(delivery);
         }

         delivery.addQueues(entry.getValue().getDurableQueues());
         delivery.addQueues(entry.getValue().getNonDurableQueues());
      }
      else
      {

         List<Queue> durableQueues = entry.getValue().getDurableQueues();
         List<Queue> nonDurableQueues = entry.getValue().getNonDurableQueues();

         final List<Queue> queues = new ArrayList<Queue>(durableQueues.size() + nonDurableQueues.size());

         queues.addAll(durableQueues);
         queues.addAll(nonDurableQueues);

         storageManager.afterCompleteOperations(new IOAsyncTask()
         {

            public void onError(int errorCode, String errorMessage)
            {
            }

            public void done()
            {
               for (Queue queue : queues)
               {
                  // in case of paging, we need to kick asynchronous delivery to try delivering
                  queue.deliverAsync();
               }
            }
         });
      }
   }

   private boolean checkDuplicateID(final ServerMessage message,
                                    final RoutingContext context,
                                    boolean rejectDuplicates,
                                    AtomicBoolean startedTX) throws Exception
   {
      // Check the DuplicateCache for the Bridge first

      Object bridgeDup = message.getObjectProperty(MessageImpl.HDR_BRIDGE_DUPLICATE_ID);
      if (bridgeDup != null)
      {
         // if the message is being sent from the bridge, we just ignore the duplicate id, and use the internal one
         byte[] bridgeDupBytes = (byte[]) bridgeDup;

         DuplicateIDCache cacheBridge = getDuplicateIDCache(BRIDGE_CACHE_STR.concat(message.getAddress()));

         if (cacheBridge.contains(bridgeDupBytes))
         {
            HornetQServerLogger.LOGGER.duplicateMessageDetectedThruBridge(message);

            if (context.getTransaction() != null)
            {
               context.getTransaction().markAsRollbackOnly(new HornetQDuplicateIdException());
            }

            message.decrementRefCount();

            return false;
         }
         else
         {
            if (context.getTransaction() == null)
            {
               context.setTransaction(new TransactionImpl(storageManager));
               startedTX.set(true);
            }
         }

         cacheBridge.addToCache(bridgeDupBytes, context.getTransaction());

         message.removeProperty(MessageImpl.HDR_BRIDGE_DUPLICATE_ID);

      }
      else
      {
         // if used BridgeDuplicate, it's not going to use the regular duplicate
         // since this will would break redistribution (re-setting the duplicateId)
         byte[] duplicateIDBytes = message.getDuplicateIDBytes();

         DuplicateIDCache cache = null;

         boolean isDuplicate = false;

         if (duplicateIDBytes != null)
         {
            cache = getDuplicateIDCache(message.getAddress());

            isDuplicate = cache.contains(duplicateIDBytes);

            if (rejectDuplicates && isDuplicate)
            {
               HornetQServerLogger.LOGGER.duplicateMessageDetected(message);

               String warnMessage = "Duplicate message detected - message will not be routed. Message information:" + message.toString();

               if (context.getTransaction() != null)
               {
                  context.getTransaction().markAsRollbackOnly(new HornetQDuplicateIdException(warnMessage));
               }

               message.decrementRefCount();

               return false;
            }
         }

         if (cache != null && !isDuplicate)
         {
            if (context.getTransaction() == null)
            {
               // We need to store the duplicate id atomically with the message storage, so we need to create a tx for this
               context.setTransaction(new TransactionImpl(storageManager));

               startedTX.set(true);
            }

            cache.addToCache(duplicateIDBytes, context.getTransaction());
         }
      }

      return true;
   }

   /**
    * @param refs
    */
   private void addReferences(final List<MessageReference> refs, final boolean direct)
   {
      for (MessageReference ref : refs)
      {
         ref.getQueue().addTail(ref, direct);
      }
   }

   /**
    * The expiry scanner can't be started until the whole server has been started other wise you may get races
    */
   public synchronized void startExpiryScanner()
   {
      if (reaperPeriod > 0)
      {
         if (reaperRunnable != null)
            reaperRunnable.stop();
         reaperRunnable = new Reaper();
         reaperThread = new Thread(reaperRunnable, "hornetq-expiry-reaper-thread");

         reaperThread.setPriority(reaperPriority);

         reaperThread.start();
      }
   }

   private ServerMessage createQueueInfoMessage(final NotificationType type, final SimpleString queueName)
   {
      ServerMessage message = new ServerMessageImpl(storageManager.generateUniqueID(), 50);

      message.setAddress(queueName);

      String uid = UUIDGenerator.getInstance().generateStringUUID();

      message.putStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE, new SimpleString(type.toString()));
      message.putLongProperty(ManagementHelper.HDR_NOTIFICATION_TIMESTAMP, System.currentTimeMillis());

      message.putStringProperty(new SimpleString("foobar"), new SimpleString(uid));

      return message;
   }

   private final class Reaper implements Runnable
   {
      private final CountDownLatch latch = new CountDownLatch(1);

      public void stop()
      {
         latch.countDown();
      }

      public void run()
      {
         // The reaper thread should be finished case the PostOffice is gone
         // This is to avoid leaks on PostOffice between stops and starts
         while (isStarted())
         {
            try
            {
               if (latch.await(reaperPeriod, TimeUnit.MILLISECONDS))
                  return;
            }
            catch (InterruptedException e1)
            {
               throw new HornetQInterruptedException(e1);
            }
            if (!isStarted())
               return;

            Map<SimpleString, Binding> nameMap = addressManager.getBindings();

            List<Queue> queues = new ArrayList<Queue>();

            for (Binding binding : nameMap.values())
            {
               if (binding.getType() == BindingType.LOCAL_QUEUE)
               {
                  Queue queue = (Queue) binding.getBindable();

                  queues.add(queue);
               }
            }

            for (Queue queue : queues)
            {
               try
               {
                  queue.expireReferences();
               }
               catch (Exception e)
               {
                  HornetQServerLogger.LOGGER.errorExpiringMessages(e);
               }
            }
         }
      }
   }

   public static final class AddOperation implements TransactionOperation
   {
      private final List<MessageReference> refs;

      AddOperation(final List<MessageReference> refs)
      {
         this.refs = refs;
      }

      public void afterCommit(final Transaction tx)
      {
         for (MessageReference ref : refs)
         {
            if (!ref.isAlreadyAcked())
            {
               ref.getQueue().addTail(ref, false);
            }
         }
      }

      public void afterPrepare(final Transaction tx)
      {
         for (MessageReference ref : refs)
         {
            if (ref.isAlreadyAcked())
            {
               ref.getQueue().referenceHandled();
               ref.getQueue().incrementMesssagesAdded();
            }
         }
      }

      public void afterRollback(final Transaction tx)
      {
      }

      public void beforeCommit(final Transaction tx) throws Exception
      {
      }

      public void beforePrepare(final Transaction tx) throws Exception
      {
      }

      public void beforeRollback(final Transaction tx) throws Exception
      {
         // Reverse the ref counts, and paging sizes

         for (MessageReference ref : refs)
         {
            ServerMessage message = ref.getMessage();

            if (message.isDurable() && ref.getQueue().isDurable())
            {
               message.decrementDurableRefCount();
            }

            message.decrementRefCount();
         }
      }

      public List<MessageReference> getRelatedMessageReferences()
      {
         return refs;
      }

      @Override
      public List<MessageReference> getListOnConsumer(long consumerID)
      {
         return Collections.emptyList();
      }
   }

   public Bindings createBindings(final SimpleString address) throws Exception
   {
      GroupingHandler groupingHandler = server.getGroupingHandler();
      BindingsImpl bindings = new BindingsImpl(address, groupingHandler, pagingManager.getPageStore(address));
      if (groupingHandler != null)
      {
         groupingHandler.addListener(bindings);
      }
      return bindings;
   }
}

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
package org.apache.activemq.artemis.core.server.impl;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.PagedReference;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.QueueStatus;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.HandleStatus;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.ScheduledDeliveryHandler;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.cluster.RemoteQueueBinding;
import org.apache.activemq.artemis.core.server.cluster.impl.Redistributor;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.HierarchicalRepositoryChangeListener;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.SlowConsumerPolicy;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.artemis.core.transaction.impl.BindingsTransactionImpl;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.Env;
import org.apache.activemq.artemis.utils.ReferenceCounter;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;
import org.apache.activemq.artemis.utils.collections.PriorityLinkedList;
import org.apache.activemq.artemis.utils.collections.PriorityLinkedListImpl;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.apache.activemq.artemis.utils.critical.CriticalComponentImpl;
import org.apache.activemq.artemis.utils.critical.EmptyCriticalAnalyzer;
import org.jboss.logging.Logger;

/**
 * Implementation of a Queue
 * <p>
 * Completely non blocking between adding to queue and delivering to consumers.
 */
public class QueueImpl extends CriticalComponentImpl implements Queue {

   protected static final int CRITICAL_PATHS = 4;
   protected static final int CRITICAL_PATH_ADD_TAIL = 0;
   protected static final int CRITICAL_PATH_ADD_HEAD = 1;
   protected static final int CRITICAL_DELIVER = 2;
   protected static final int CRITICAL_CONSUMER = 3;

   private static final Logger logger = Logger.getLogger(QueueImpl.class);

   public static final int REDISTRIBUTOR_BATCH_SIZE = 100;

   public static final int NUM_PRIORITIES = 10;

   public static final int MAX_DELIVERIES_IN_LOOP = 1000;

   public static final int CHECK_QUEUE_SIZE_PERIOD = 1000;

   /**
    * If The system gets slow for any reason, this is the maximum time a Delivery or
    * or depage executor should be hanging on
    */
   public static final int DELIVERY_TIMEOUT = 1000;

   private static final int FLUSH_TIMEOUT = 10000;

   public static final int DEFAULT_FLUSH_LIMIT = 500;

   private final long id;

   private final SimpleString name;

   private final SimpleString user;

   private volatile Filter filter;

   private final boolean propertyDurable;

   private final boolean temporary;

   private final boolean autoCreated;

   private final PostOffice postOffice;

   private volatile boolean queueDestroyed = false;

   private final PageSubscription pageSubscription;

   private ReferenceCounter refCountForConsumers;

   private final LinkedListIterator<PagedReference> pageIterator;

   private volatile boolean printErrorExpiring = false;

   // Messages will first enter intermediateMessageReferences
   // Before they are added to messageReferences
   // This is to avoid locking the queue on the producer
   private final ConcurrentLinkedQueue<MessageReference> intermediateMessageReferences = new ConcurrentLinkedQueue<>();

   // This is where messages are stored
   private final PriorityLinkedList<MessageReference> messageReferences = new PriorityLinkedListImpl<>(QueueImpl.NUM_PRIORITIES);

   // The quantity of pagedReferences on messageReferences priority list
   private final AtomicInteger pagedReferences = new AtomicInteger(0);

   // The estimate of memory being consumed by this queue. Used to calculate instances of messages to depage
   private final AtomicInteger queueMemorySize = new AtomicInteger(0);

   private final QueuePendingMessageMetrics pendingMetrics = new QueuePendingMessageMetrics(this);

   private final QueuePendingMessageMetrics deliveringMetrics = new QueuePendingMessageMetrics(this);

   // used to control if we should recalculate certain positions inside deliverAsync
   private volatile boolean consumersChanged = true;

   private final List<ConsumerHolder> consumerList = new CopyOnWriteArrayList<>();

   private final ScheduledDeliveryHandler scheduledDeliveryHandler;

   private AtomicLong messagesAdded = new AtomicLong(0);

   private AtomicLong messagesAcknowledged = new AtomicLong(0);

   private AtomicLong messagesExpired = new AtomicLong(0);

   private AtomicLong messagesKilled = new AtomicLong(0);

   private boolean paused;

   private long pauseStatusRecord = -1;

   private static final int MAX_SCHEDULED_RUNNERS = 2;

   // We don't ever need more than two DeliverRunner on the executor's list
   // that is getting the worse scenario possible when one runner is almost finishing before the second started
   // for that we keep a counter of scheduled instances
   private final AtomicInteger scheduledRunners = new AtomicInteger(0);

   private final Runnable deliverRunner = new DeliverRunner();

   private volatile boolean depagePending = false;

   private final StorageManager storageManager;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final ActiveMQServer server;

   private final ScheduledExecutorService scheduledExecutor;

   private final SimpleString address;

   private Redistributor redistributor;

   private ScheduledFuture<?> redistributorFuture;

   // We cache the consumers here since we don't want to include the redistributor

   private final AtomicInteger consumersCount = new AtomicInteger();

   private final Set<Consumer> consumerSet = new HashSet<>();

   private final Map<SimpleString, Consumer> groups = new HashMap<>();

   private volatile SimpleString expiryAddress;

   private int pos;

   private final ArtemisExecutor executor;

   private boolean internalQueue;

   private volatile long lastDirectDeliveryCheck = 0;

   private volatile boolean directDeliver = true;

   private volatile boolean supportsDirectDeliver = true;

   private AddressSettingsRepositoryListener addressSettingsRepositoryListener;

   private final ExpiryScanner expiryScanner = new ExpiryScanner();

   private final ReusableLatch deliveriesInTransit = new ReusableLatch(0);

   private volatile boolean caused = false;

   private final AtomicLong queueRateCheckTime = new AtomicLong(System.currentTimeMillis());

   private final AtomicLong messagesAddedSnapshot = new AtomicLong(0);

   private ScheduledFuture slowConsumerReaperFuture;

   private SlowConsumerReaperRunnable slowConsumerReaperRunnable;

   private volatile int maxConsumers;

   private volatile boolean exclusive;

   private volatile boolean purgeOnNoConsumers;

   private final AddressInfo addressInfo;

   private final AtomicInteger noConsumers = new AtomicInteger(0);

   private volatile RoutingType routingType;

   private final QueueFactory factory;

   /**
    * This is to avoid multi-thread races on calculating direct delivery,
    * to guarantee ordering will be always be correct
    */
   private final Object directDeliveryGuard = new Object();

   /**
    * For testing only
    */
   public List<SimpleString> getGroupsUsed() {
      final CountDownLatch flush = new CountDownLatch(1);
      executor.execute(new Runnable() {
         @Override
         public void run() {
            flush.countDown();
         }
      });
      try {
         flush.await(10, TimeUnit.SECONDS);
      } catch (Exception ignored) {
      }

      synchronized (this) {
         ArrayList<SimpleString> groupsUsed = new ArrayList<>();
         groupsUsed.addAll(groups.keySet());
         return groupsUsed;
      }
   }

   public String debug() {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      out.println("queueMemorySize=" + queueMemorySize);

      for (ConsumerHolder holder : consumerList) {
         out.println("consumer: " + holder.consumer.debug());
      }

      for (MessageReference reference : intermediateMessageReferences) {
         out.print("Intermediate reference:" + reference);
      }

      if (intermediateMessageReferences.isEmpty()) {
         out.println("No intermediate references");
      }

      boolean foundRef = false;

      synchronized (this) {
         Iterator<MessageReference> iter = messageReferences.iterator();
         while (iter.hasNext()) {
            foundRef = true;
            out.println("reference = " + iter.next());
         }
      }

      if (!foundRef) {
         out.println("No permanent references on queue");
      }

      System.out.println(str.toString());

      return str.toString();
   }

   public QueueImpl(final long id,
                    final SimpleString address,
                    final SimpleString name,
                    final Filter filter,
                    final SimpleString user,
                    final boolean durable,
                    final boolean temporary,
                    final boolean autoCreated,
                    final ScheduledExecutorService scheduledExecutor,
                    final PostOffice postOffice,
                    final StorageManager storageManager,
                    final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                    final ArtemisExecutor executor,
                    final ActiveMQServer server,
                    final QueueFactory factory) {
      this(id, address, name, filter, null, user, durable, temporary, autoCreated, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executor, server, factory);
   }

   public QueueImpl(final long id,
                    final SimpleString address,
                    final SimpleString name,
                    final Filter filter,
                    final PageSubscription pageSubscription,
                    final SimpleString user,
                    final boolean durable,
                    final boolean temporary,
                    final boolean autoCreated,
                    final ScheduledExecutorService scheduledExecutor,
                    final PostOffice postOffice,
                    final StorageManager storageManager,
                    final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                    final ArtemisExecutor executor,
                    final ActiveMQServer server,
                    final QueueFactory factory) {
      this(id, address, name, filter, pageSubscription, user, durable, temporary, autoCreated, RoutingType.MULTICAST, null, null, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executor, server, factory);
   }

   public QueueImpl(final long id,
                    final SimpleString address,
                    final SimpleString name,
                    final Filter filter,
                    final PageSubscription pageSubscription,
                    final SimpleString user,
                    final boolean durable,
                    final boolean temporary,
                    final boolean autoCreated,
                    final RoutingType routingType,
                    final Integer maxConsumers,
                    final Boolean purgeOnNoConsumers,
                    final ScheduledExecutorService scheduledExecutor,
                    final PostOffice postOffice,
                    final StorageManager storageManager,
                    final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                    final ArtemisExecutor executor,
                    final ActiveMQServer server,
                    final QueueFactory factory) {
      this(id, address, name, filter, pageSubscription, user, durable, temporary, autoCreated, routingType, maxConsumers, null, purgeOnNoConsumers, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executor, server, factory);
   }

   public QueueImpl(final long id,
                    final SimpleString address,
                    final SimpleString name,
                    final Filter filter,
                    final PageSubscription pageSubscription,
                    final SimpleString user,
                    final boolean durable,
                    final boolean temporary,
                    final boolean autoCreated,
                    final RoutingType routingType,
                    final Integer maxConsumers,
                    final Boolean exclusive,
                    final Boolean purgeOnNoConsumers,
                    final ScheduledExecutorService scheduledExecutor,
                    final PostOffice postOffice,
                    final StorageManager storageManager,
                    final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                    final ArtemisExecutor executor,
                    final ActiveMQServer server,
                    final QueueFactory factory) {
      super(server == null ? EmptyCriticalAnalyzer.getInstance() : server.getCriticalAnalyzer(), CRITICAL_PATHS);

      this.id = id;

      this.address = address;

      this.addressInfo = postOffice == null ? null : postOffice.getAddressInfo(address);

      this.routingType = routingType;

      this.name = name;

      this.filter = filter;

      this.pageSubscription = pageSubscription;

      this.propertyDurable = durable;

      this.temporary = temporary;

      this.autoCreated = autoCreated;

      this.maxConsumers = maxConsumers == null ? ActiveMQDefaultConfiguration.getDefaultMaxQueueConsumers() : maxConsumers;

      this.exclusive = exclusive == null ? ActiveMQDefaultConfiguration.getDefaultExclusive() : exclusive;

      this.purgeOnNoConsumers = purgeOnNoConsumers == null ? ActiveMQDefaultConfiguration.getDefaultPurgeOnNoConsumers() : purgeOnNoConsumers;

      this.postOffice = postOffice;

      this.storageManager = storageManager;

      this.addressSettingsRepository = addressSettingsRepository;

      this.scheduledExecutor = scheduledExecutor;

      this.server = server;

      scheduledDeliveryHandler = new ScheduledDeliveryHandlerImpl(scheduledExecutor, this);

      if (addressSettingsRepository != null) {
         addressSettingsRepositoryListener = new AddressSettingsRepositoryListener();
         addressSettingsRepository.registerListener(addressSettingsRepositoryListener);
      } else {
         expiryAddress = null;
      }

      if (pageSubscription != null) {
         pageSubscription.setQueue(this);
         this.pageIterator = pageSubscription.iterator();
      } else {
         this.pageIterator = null;
      }

      this.executor = executor;

      this.user = user;

      this.factory = factory;
   }

   // Bindable implementation -------------------------------------------------------------------------------------

   public SimpleString getRoutingName() {
      return name;
   }

   public SimpleString getUniqueName() {
      return name;
   }

   @Override
   public SimpleString getUser() {
      return user;
   }

   @Override
   public boolean isExclusive() {
      return exclusive;
   }

   @Override
   public synchronized void setExclusive(boolean exclusive) {
      this.exclusive = exclusive;
   }

   @Override
   public boolean isLastValue() {
      return false;
   }

   @Override
   public void route(final Message message, final RoutingContext context) throws Exception {
      if (purgeOnNoConsumers && getConsumerCount() == 0) {
         return;
      }
      context.addQueue(address, this);
   }

   @Override
   public void routeWithAck(Message message, RoutingContext context) {
      context.addQueueWithAck(address, this);
   }

   // Queue implementation ----------------------------------------------------------------------------------------
   @Override
   public synchronized void setConsumersRefCount(final ReferenceCounter referenceCounter) {
      if (refCountForConsumers == null) {
         this.refCountForConsumers = referenceCounter;
      }
   }

   @Override
   public ReferenceCounter getConsumersRefCount() {
      return refCountForConsumers;
   }

   @Override
   public boolean isDurable() {
      return propertyDurable;
   }

   @Override
   public boolean isDurableMessage() {
      return propertyDurable && !purgeOnNoConsumers;
   }

   @Override
   public boolean isTemporary() {
      return temporary;
   }

   @Override
   public boolean isAutoCreated() {
      return autoCreated;
   }

   @Override
   public boolean isPurgeOnNoConsumers() {
      return purgeOnNoConsumers;
   }

   @Override
   public synchronized void setPurgeOnNoConsumers(boolean value) {
      this.purgeOnNoConsumers = value;
   }

   @Override
   public int getMaxConsumers() {
      return maxConsumers;
   }

   @Override
   public synchronized void setMaxConsumer(int maxConsumers) {
      this.maxConsumers = maxConsumers;
   }

   @Override
   public SimpleString getName() {
      return name;
   }

   @Override
   public SimpleString getAddress() {
      return address;
   }

   @Override
   public long getID() {
      return id;
   }

   @Override
   public PageSubscription getPageSubscription() {
      return pageSubscription;
   }

   @Override
   public RoutingType getRoutingType() {
      return routingType;
   }

   @Override
   public void setRoutingType(RoutingType routingType) {
      if (addressInfo.getRoutingTypes().contains(routingType)) {
         this.routingType = routingType;
      }
   }

   @Override
   public Filter getFilter() {
      return filter;
   }

   @Override
   public void unproposed(final SimpleString groupID) {
      if (groupID.toString().endsWith("." + this.getName())) {
         // this means this unproposed belongs to this routing, so we will
         // remove this group

         // This is removing the name and a . added, giving us the original groupID used
         // this is because a groupID is stored per queue, and only this queue is expiring at this point
         final SimpleString groupIDToRemove = (SimpleString) groupID.subSequence(0, groupID.length() - getName().length() - 1);
         // using an executor so we don't want to hold anyone just because of this
         getExecutor().execute(new Runnable() {
            @Override
            public void run() {
               synchronized (QueueImpl.this) {
                  if (groups.remove(groupIDToRemove) != null) {
                     logger.debug("Removing group after unproposal " + groupID + " from queue " + QueueImpl.this);
                  } else {
                     logger.debug("Couldn't remove Removing group " + groupIDToRemove + " after unproposal on queue " + QueueImpl.this);
                  }
               }
            }
         });
      }
   }

   /* Called when a message is cancelled back into the queue */
   @Override
   public void addHead(final MessageReference ref, boolean scheduling) {
      enterCritical(CRITICAL_PATH_ADD_HEAD);
      synchronized (this) {
         try {
            if (!scheduling && scheduledDeliveryHandler.checkAndSchedule(ref, false)) {
               return;
            }

            internalAddHead(ref);

            directDeliver = false;
         } finally {
            leaveCritical(CRITICAL_PATH_ADD_HEAD);
         }
      }
   }

   /* Called when a message is cancelled back into the queue */
   @Override
   public void addHead(final List<MessageReference> refs, boolean scheduling) {
      enterCritical(CRITICAL_PATH_ADD_HEAD);
      synchronized (this) {
         try {
            for (MessageReference ref : refs) {
               addHead(ref, scheduling);
            }

            resetAllIterators();

            deliverAsync();
         } finally {
            leaveCritical(CRITICAL_PATH_ADD_HEAD);
         }
      }
   }

   @Override
   public synchronized void reload(final MessageReference ref) {
      queueMemorySize.addAndGet(ref.getMessageMemoryEstimate());
      if (!scheduledDeliveryHandler.checkAndSchedule(ref, true)) {
         internalAddTail(ref);
      }

      directDeliver = false;

      if (!ref.isPaged()) {
         messagesAdded.incrementAndGet();
      }
   }

   @Override
   public void addTail(final MessageReference ref) {
      addTail(ref, false);
   }

   @Override
   public void addTail(final MessageReference ref, final boolean direct) {
      enterCritical(CRITICAL_PATH_ADD_TAIL);
      try {
         if (scheduleIfPossible(ref)) {
            return;
         }

         if (supportsDirectDeliver && !directDeliver && direct && System.currentTimeMillis() - lastDirectDeliveryCheck > CHECK_QUEUE_SIZE_PERIOD) {
            if (logger.isTraceEnabled()) {
               logger.trace("Checking to re-enable direct deliver on queue " + this.getName());
            }
            lastDirectDeliveryCheck = System.currentTimeMillis();
            synchronized (directDeliveryGuard) {
               // The checkDirect flag is periodically set to true, if the delivery is specified as direct then this causes the
               // directDeliver flag to be re-computed resulting in direct delivery if the queue is empty
               // We don't recompute it on every delivery since executing isEmpty is expensive for a ConcurrentQueue

               if (deliveriesInTransit.getCount() == 0 && getExecutor().isFlushed() && intermediateMessageReferences.isEmpty() && messageReferences.isEmpty() && !pageIterator.hasNext() && !pageSubscription.isPaging()) {
                  // We must block on the executor to ensure any async deliveries have completed or we might get out of order
                  // deliveries
                  // Go into direct delivery mode
                  directDeliver = supportsDirectDeliver;
                  if (logger.isTraceEnabled()) {
                     logger.trace("Setting direct deliverer to " + supportsDirectDeliver);
                  }
               } else {
                  if (logger.isTraceEnabled()) {
                     logger.trace("Couldn't set direct deliver back");
                  }
               }
            }
         }

         if (direct && supportsDirectDeliver && directDeliver && deliveriesInTransit.getCount() == 0 && deliverDirect(ref)) {
            return;
         }

         // We only add queueMemorySize if not being delivered directly
         queueMemorySize.addAndGet(ref.getMessageMemoryEstimate());

         intermediateMessageReferences.add(ref);

         directDeliver = false;

         // Delivery async will both poll for intermediate reference and deliver to clients
         deliverAsync();
      } finally {
         leaveCritical(CRITICAL_PATH_ADD_TAIL);
      }
   }

   protected boolean scheduleIfPossible(MessageReference ref) {
      if (scheduledDeliveryHandler.checkAndSchedule(ref, true)) {
         synchronized (this) {
            if (!ref.isPaged()) {
               messagesAdded.incrementAndGet();
            }
         }

         return true;
      }
      return false;
   }

   /**
    * This will wait for any pending deliveries to finish
    */
   private boolean flushDeliveriesInTransit() {
      try {

         if (!deliveriesInTransit.await(100, TimeUnit.MILLISECONDS)) {
            caused = true;
            System.err.println("There are currently " + deliveriesInTransit.getCount() + " credits");
         }
         if (deliveriesInTransit.await(DELIVERY_TIMEOUT)) {
            return true;
         } else {
            ActiveMQServerLogger.LOGGER.timeoutFlushInTransit(getName().toString(), getAddress().toString());
            return false;
         }
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.unableToFlushDeliveries(e);
         return false;
      }
   }

   @Override
   public void forceDelivery() {
      if (pageSubscription != null && pageSubscription.isPaging()) {
         if (logger.isTraceEnabled()) {
            logger.trace("Force delivery scheduling depage");
         }
         scheduleDepage(false);
      }

      if (logger.isTraceEnabled()) {
         logger.trace("Force delivery delivering async");
      }

      deliverAsync();
   }

   @Override
   public void deliverAsync() {
      if (scheduledRunners.get() < MAX_SCHEDULED_RUNNERS) {
         scheduledRunners.incrementAndGet();
         try {
            getExecutor().execute(deliverRunner);
         } catch (RejectedExecutionException ignored) {
            // no-op
            scheduledRunners.decrementAndGet();
         }

         checkDepage();
      }

   }

   @Override
   public void close() throws Exception {
      getExecutor().execute(new Runnable() {
         @Override
         public void run() {
            try {
               cancelRedistributor();
            } catch (Exception e) {
               // nothing that could be done anyway.. just logging
               ActiveMQServerLogger.LOGGER.unableToCancelRedistributor(e);
            }
         }
      });

      if (addressSettingsRepository != null) {
         addressSettingsRepository.unRegisterListener(addressSettingsRepositoryListener);
      }
   }

   @Override
   public ArtemisExecutor getExecutor() {
      if (pageSubscription != null && pageSubscription.isPaging()) {
         // When in page mode, we don't want to have concurrent IO on the same PageStore
         return pageSubscription.getExecutor();
      } else {
         return executor;
      }
   }

   /* Only used on tests */
   public void deliverNow() {
      deliverAsync();

      flushExecutor();
   }

   @Override
   public boolean flushExecutor() {
      boolean ok = internalFlushExecutor(10000, true);

      if (!ok) {
         ActiveMQServerLogger.LOGGER.errorFlushingExecutorsOnQueue();
      }

      return ok;
   }

   private boolean internalFlushExecutor(long timeout, boolean log) {

      if (!getExecutor().flush(timeout, TimeUnit.MILLISECONDS)) {
         if (log) {
            ActiveMQServerLogger.LOGGER.queueBusy(this.name.toString(), timeout);
         }
         return false;
      } else {
         return true;
      }
   }

   @Override
   public void addConsumer(final Consumer consumer) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug(this + " adding consumer " + consumer);
      }

      enterCritical(CRITICAL_CONSUMER);
      try {
         synchronized (this) {

            if (maxConsumers != MAX_CONSUMERS_UNLIMITED && noConsumers.get() >= maxConsumers) {
               throw ActiveMQMessageBundle.BUNDLE.maxConsumerLimitReachedForQueue(address, name);
            }

            consumersChanged = true;

            if (!consumer.supportsDirectDelivery()) {
               this.supportsDirectDeliver = false;
            }

            cancelRedistributor();

            consumerList.add(new ConsumerHolder(consumer));

            if (consumerSet.add(consumer)) {
               consumersCount.incrementAndGet();
            }

            if (refCountForConsumers != null) {
               refCountForConsumers.increment();
            }

            noConsumers.incrementAndGet();
         }
      } finally {
         leaveCritical(CRITICAL_CONSUMER);
      }


   }

   @Override
   public void removeConsumer(final Consumer consumer) {

      enterCritical(CRITICAL_CONSUMER);
      try {
         synchronized (this) {
            consumersChanged = true;

            for (ConsumerHolder holder : consumerList) {
               if (holder.consumer == consumer) {
                  if (holder.iter != null) {
                     holder.iter.close();
                  }
                  consumerList.remove(holder);
                  break;
               }
            }

            this.supportsDirectDeliver = checkConsumerDirectDeliver();

            if (pos > 0 && pos >= consumerList.size()) {
               pos = consumerList.size() - 1;
            }

            if (consumerSet.remove(consumer)) {
               consumersCount.decrementAndGet();
            }

            LinkedList<SimpleString> groupsToRemove = null;

            for (SimpleString groupID : groups.keySet()) {
               if (consumer == groups.get(groupID)) {
                  if (groupsToRemove == null) {
                     groupsToRemove = new LinkedList<>();
                  }
                  groupsToRemove.add(groupID);
               }
            }

            // We use an auxiliary List here to avoid concurrent modification exceptions on the keySet
            // while the iteration is being done.
            // Since that's a simple HashMap there's no Iterator's support with a remove operation
            if (groupsToRemove != null) {
               for (SimpleString groupID : groupsToRemove) {
                  groups.remove(groupID);
               }
            }

            if (refCountForConsumers != null) {
               refCountForConsumers.decrement();
            }

            noConsumers.decrementAndGet();
         }
      } finally {
         leaveCritical(CRITICAL_CONSUMER);
      }
   }

   private boolean checkConsumerDirectDeliver() {
      boolean supports = true;
      for (ConsumerHolder consumerCheck : consumerList) {
         if (!consumerCheck.consumer.supportsDirectDelivery()) {
            supports = false;
         }
      }
      return supports;
   }

   @Override
   public synchronized void addRedistributor(final long delay) {
      clearRedistributorFuture();

      if (redistributor != null) {
         // Just prompt delivery
         deliverAsync();
      }

      if (delay > 0) {
         if (consumerSet.isEmpty()) {
            DelayedAddRedistributor dar = new DelayedAddRedistributor(executor);

            redistributorFuture = scheduledExecutor.schedule(dar, delay, TimeUnit.MILLISECONDS);
         }
      } else {
         internalAddRedistributor(executor);
      }
   }

   private void clearRedistributorFuture() {
      ScheduledFuture<?> future = redistributorFuture;
      redistributorFuture = null;
      if (future != null) {
         future.cancel(false);
      }
   }

   @Override
   public synchronized void cancelRedistributor() throws Exception {
      if (redistributor != null) {
         redistributor.stop();
         Redistributor redistributorToRemove = redistributor;
         redistributor = null;

         removeConsumer(redistributorToRemove);
      }

      clearRedistributorFuture();
   }

   @Override
   protected void finalize() throws Throwable {
      cancelRedistributor();

      super.finalize();
   }

   @Override
   public int getConsumerCount() {
      return consumersCount.get();
   }

   @Override
   public synchronized Set<Consumer> getConsumers() {
      return new HashSet<>(consumerSet);
   }

   @Override
   public synchronized Map<SimpleString, Consumer> getGroups() {
      return new HashMap<>(groups);
   }

   @Override
   public synchronized void resetGroup(SimpleString groupId) {
      groups.remove(groupId);
   }

   @Override
   public synchronized void resetAllGroups() {
      groups.clear();
   }

   @Override
   public synchronized int getGroupCount() {
      return groups.size();
   }

   @Override
   public boolean hasMatchingConsumer(final Message message) {
      for (ConsumerHolder holder : consumerList) {
         Consumer consumer = holder.consumer;

         if (consumer instanceof Redistributor) {
            continue;
         }

         Filter filter1 = consumer.getFilter();

         if (filter1 == null) {
            return true;
         } else {
            if (filter1.match(message)) {
               return true;
            }
         }
      }
      return false;
   }

   @Override
   public LinkedListIterator<MessageReference> iterator() {
      return new SynchronizedIterator(messageReferences.iterator());
   }

   @Override
   public QueueBrowserIterator browserIterator() {
      return new QueueBrowserIterator();
   }

   @Override
   public synchronized MessageReference removeReferenceWithID(final long id1) throws Exception {
      try (LinkedListIterator<MessageReference> iterator = iterator()) {

         MessageReference removed = null;

         while (iterator.hasNext()) {
            MessageReference ref = iterator.next();

            if (ref.getMessage().getMessageID() == id1) {
               iterator.remove();
               refRemoved(ref);

               removed = ref;

               break;
            }
         }

         if (removed == null) {
            // Look in scheduled deliveries
            removed = scheduledDeliveryHandler.removeReferenceWithID(id1);
         }

         return removed;
      }
   }

   @Override
   public synchronized MessageReference getReference(final long id1) throws ActiveMQException {
      try (LinkedListIterator<MessageReference> iterator = iterator()) {

         while (iterator.hasNext()) {
            MessageReference ref = iterator.next();

            if (ref.getMessage().getMessageID() == id1) {
               return ref;
            }
         }

         return null;
      }
   }

   @Override
   public long getMessageCount() {
      if (pageSubscription != null) {
         // messageReferences will have depaged messages which we need to discount from the counter as they are
         // counted on the pageSubscription as well
         return pendingMetrics.getMessageCount() + getScheduledCount() + getDeliveringCount() + pageSubscription.getMessageCount();
      } else {
         return pendingMetrics.getMessageCount() + getScheduledCount() + getDeliveringCount();
      }
   }

   @Override
   public long getPersistentSize() {
      if (pageSubscription != null) {
         // messageReferences will have depaged messages which we need to discount from the counter as they are
         // counted on the pageSubscription as well
         return pendingMetrics.getPersistentSize() + getScheduledSize() + getDeliveringSize() + pageSubscription.getPersistentSize();
      } else {
         return pendingMetrics.getPersistentSize() + getScheduledSize() + getDeliveringSize();
      }
   }

   @Override
   public long getDurableMessageCount() {
      if (isDurable()) {
         if (pageSubscription != null) {
            return pendingMetrics.getDurableMessageCount() + getDurableScheduledCount() + getDurableDeliveringCount() + pageSubscription.getMessageCount();
         } else {
            return pendingMetrics.getDurableMessageCount() + getDurableScheduledCount() + getDurableDeliveringCount();
         }
      }
      return 0;
   }

   @Override
   public long getDurablePersistentSize() {
      if (isDurable()) {
         if (pageSubscription != null) {
            return pendingMetrics.getDurablePersistentSize() + getDurableScheduledSize() + getDurableDeliveringSize() + pageSubscription.getPersistentSize();
         } else {
            return pendingMetrics.getDurablePersistentSize() + getDurableScheduledSize() + getDurableDeliveringSize();
         }
      }
      return 0;
   }

   @Override
   public synchronized int getScheduledCount() {
      return scheduledDeliveryHandler.getScheduledCount();
   }

   @Override
   public synchronized long getScheduledSize() {
      return scheduledDeliveryHandler.getScheduledSize();
   }

   @Override
   public synchronized int getDurableScheduledCount() {
      return scheduledDeliveryHandler.getDurableScheduledCount();
   }

   @Override
   public synchronized long getDurableScheduledSize() {
      return scheduledDeliveryHandler.getDurableScheduledSize();
   }

   @Override
   public synchronized List<MessageReference> getScheduledMessages() {
      return scheduledDeliveryHandler.getScheduledReferences();
   }

   @Override
   public Map<String, List<MessageReference>> getDeliveringMessages() {

      List<ConsumerHolder> consumerListClone = cloneConsumersList();

      Map<String, List<MessageReference>> mapReturn = new HashMap<>();

      for (ConsumerHolder holder : consumerListClone) {
         List<MessageReference> msgs = holder.consumer.getDeliveringMessages();
         if (msgs != null && msgs.size() > 0) {
            mapReturn.put(holder.consumer.toManagementString(), msgs);
         }
      }

      return mapReturn;
   }

   @Override
   public int getDeliveringCount() {
      return deliveringMetrics.getMessageCount();
   }

   @Override
   public long getDeliveringSize() {
      return deliveringMetrics.getPersistentSize();
   }

   @Override
   public int getDurableDeliveringCount() {
      return deliveringMetrics.getDurableMessageCount();
   }

   @Override
   public long getDurableDeliveringSize() {
      return deliveringMetrics.getDurablePersistentSize();
   }

   @Override
   public void acknowledge(final MessageReference ref) throws Exception {
      acknowledge(ref, null);
   }

   @Override
   public void acknowledge(final MessageReference ref, final ServerConsumer consumer) throws Exception {
      acknowledge(ref, AckReason.NORMAL, consumer);
   }

   @Override
   public void acknowledge(final MessageReference ref, final AckReason reason, final ServerConsumer consumer) throws Exception {
      if (ref.isPaged()) {
         pageSubscription.ack((PagedReference) ref);
         postAcknowledge(ref);
      } else {
         Message message = ref.getMessage();

         boolean durableRef = message.isDurable() && isDurableMessage();

         if (durableRef) {
            storageManager.storeAcknowledge(id, message.getMessageID());
         }
         postAcknowledge(ref);
      }

      if (reason == AckReason.EXPIRED) {
         messagesExpired.incrementAndGet();
      } else if (reason == AckReason.KILLED) {
         messagesKilled.incrementAndGet();
      } else {
         messagesAcknowledged.incrementAndGet();
      }

      if (server != null && server.hasBrokerPlugins()) {
         server.callBrokerPlugins(plugin -> plugin.messageAcknowledged(ref, reason, consumer));
      }
   }

   @Override
   public void acknowledge(final Transaction tx, final MessageReference ref) throws Exception {
      acknowledge(tx, ref, AckReason.NORMAL, null);
   }

   @Override
   public void acknowledge(final Transaction tx, final MessageReference ref, final AckReason reason, final ServerConsumer consumer) throws Exception {
      if (ref.isPaged()) {
         pageSubscription.ackTx(tx, (PagedReference) ref);

         getRefsOperation(tx).addAck(ref);
      } else {
         Message message = ref.getMessage();

         boolean durableRef = message.isDurable() && isDurableMessage();

         if (durableRef) {
            storageManager.storeAcknowledgeTransactional(tx.getID(), id, message.getMessageID());

            tx.setContainsPersistent();
         }

         getRefsOperation(tx).addAck(ref);
      }

      if (reason == AckReason.EXPIRED) {
         messagesExpired.incrementAndGet();
      } else if (reason == AckReason.KILLED) {
         messagesKilled.incrementAndGet();
      } else {
         messagesAcknowledged.incrementAndGet();
      }

      if (server != null && server.hasBrokerPlugins()) {
         server.callBrokerPlugins(plugin -> plugin.messageAcknowledged(ref, reason, consumer));
      }
   }

   @Override
   public void reacknowledge(final Transaction tx, final MessageReference ref) throws Exception {
      Message message = ref.getMessage();

      if (message.isDurable() && isDurableMessage()) {
         tx.setContainsPersistent();
      }

      getRefsOperation(tx).addAck(ref);

      // https://issues.jboss.org/browse/HORNETQ-609
      incDelivering(ref);

      messagesAcknowledged.incrementAndGet();
   }

   private RefsOperation getRefsOperation(final Transaction tx) {
      return getRefsOperation(tx, false);
   }

   private RefsOperation getRefsOperation(final Transaction tx, boolean ignoreRedlieveryCheck) {
      synchronized (tx) {
         RefsOperation oper = (RefsOperation) tx.getProperty(TransactionPropertyIndexes.REFS_OPERATION);

         if (oper == null) {
            oper = tx.createRefsOperation(this);

            tx.putProperty(TransactionPropertyIndexes.REFS_OPERATION, oper);

            tx.addOperation(oper);
         }

         if (ignoreRedlieveryCheck) {
            oper.setIgnoreRedeliveryCheck();
         }

         return oper;
      }
   }

   @Override
   public void cancel(final Transaction tx, final MessageReference reference) {
      cancel(tx, reference, false);
   }

   @Override
   public void cancel(final Transaction tx, final MessageReference reference, boolean ignoreRedeliveryCheck) {
      getRefsOperation(tx, ignoreRedeliveryCheck).addAck(reference);
   }

   @Override
   public synchronized void cancel(final MessageReference reference, final long timeBase) throws Exception {
      if (checkRedelivery(reference, timeBase, false)) {
         if (!scheduledDeliveryHandler.checkAndSchedule(reference, false)) {
            internalAddHead(reference);
         }

         resetAllIterators();
      } else {
         decDelivering(reference);
      }
   }

   @Override
   public void expire(final MessageReference ref) throws Exception {
      expire(ref, null);
   }

   @Override
   public void expire(final MessageReference ref, final ServerConsumer consumer) throws Exception {
      SimpleString messageExpiryAddress = expiryAddressFromMessageAddress(ref);
      if (messageExpiryAddress == null) {
         messageExpiryAddress = expiryAddressFromAddressSettings(ref);
      }

      if (messageExpiryAddress != null) {
         if (logger.isTraceEnabled()) {
            logger.trace("moving expired reference " + ref + " to address = " + messageExpiryAddress + " from queue=" + this.getName());
         }
         move(null, messageExpiryAddress, null, ref, false, AckReason.EXPIRED, consumer);
      } else {
         if (logger.isTraceEnabled()) {
            logger.trace("expiry is null, just acking expired message for reference " + ref + " from queue=" + this.getName());
         }
         acknowledge(ref, AckReason.EXPIRED, consumer);
      }

      if (server != null && server.hasBrokerPlugins()) {
         final SimpleString expiryAddress = messageExpiryAddress;
         server.callBrokerPlugins(plugin -> plugin.messageExpired(ref, expiryAddress, consumer));
      }
   }

   private SimpleString expiryAddressFromMessageAddress(MessageReference ref) {
      SimpleString messageAddress = extractAddress(ref.getMessage());
      SimpleString expiryAddress = null;

      if (messageAddress == null || messageAddress.equals(getAddress())) {
         expiryAddress = getExpiryAddress();
      }

      return expiryAddress;
   }

   private SimpleString expiryAddressFromAddressSettings(MessageReference ref) {
      SimpleString messageAddress = extractAddress(ref.getMessage());
      SimpleString expiryAddress = null;

      if (messageAddress != null) {
         AddressSettings addressSettings = addressSettingsRepository.getMatch(messageAddress.toString());

         expiryAddress = addressSettings.getExpiryAddress();
      }

      return expiryAddress;
   }

   private SimpleString extractAddress(Message message) {
      if (message.containsProperty(Message.HDR_ORIG_MESSAGE_ID.toString())) {
         return message.getSimpleStringProperty(Message.HDR_ORIGINAL_ADDRESS.toString());
      } else {
         return message.getAddressSimpleString();
      }
   }

   @Override
   public SimpleString getExpiryAddress() {
      return this.expiryAddress;
   }

   @Override
   public void referenceHandled(MessageReference ref) {
      incDelivering(ref);
   }

   @Override
   public void incrementMesssagesAdded() {
      messagesAdded.incrementAndGet();
   }

   @Override
   public void deliverScheduledMessages() throws ActiveMQException {
      List<MessageReference> scheduledMessages = scheduledDeliveryHandler.cancel(null);
      if (scheduledMessages != null && scheduledMessages.size() > 0) {
         for (MessageReference ref : scheduledMessages) {
            ref.getMessage().setScheduledDeliveryTime(ref.getScheduledDeliveryTime());
            ref.setScheduledDeliveryTime(0);
         }
         this.addHead(scheduledMessages, true);
      }
   }

   @Override
   public long getMessagesAdded() {
      if (pageSubscription != null) {
         return messagesAdded.get() + pageSubscription.getCounter().getValueAdded();
      } else {
         return messagesAdded.get();
      }
   }

   @Override
   public long getMessagesAcknowledged() {
      return messagesAcknowledged.get();
   }

   @Override
   public long getMessagesExpired() {
      return messagesExpired.get();
   }

   @Override
   public long getMessagesKilled() {
      return messagesKilled.get();
   }

   @Override
   public int deleteAllReferences() throws Exception {
      return deleteAllReferences(DEFAULT_FLUSH_LIMIT);
   }

   @Override
   public int deleteAllReferences(final int flushLimit) throws Exception {
      return deleteMatchingReferences(flushLimit, null);
   }

   @Override
   public int deleteMatchingReferences(Filter filter) throws Exception {
      return deleteMatchingReferences(DEFAULT_FLUSH_LIMIT, filter);
   }

   @Override
   public synchronized int deleteMatchingReferences(final int flushLimit, final Filter filter1, AckReason ackReason) throws Exception {
      return iterQueue(flushLimit, filter1, new QueueIterateAction() {
         @Override
         public void actMessage(Transaction tx, MessageReference ref) throws Exception {
            actMessage(tx, ref, true);
         }

         @Override
         public void actMessage(Transaction tx, MessageReference ref, boolean fromMessageReferences) throws Exception {
            incDelivering(ref);
            acknowledge(tx, ref, ackReason, null);
            if (fromMessageReferences) {
               refRemoved(ref);
            }
         }
      });
   }

   /**
    * This is a generic method for any method interacting on the Queue to move or delete messages
    * Instead of duplicate the feature we created an abstract class where you pass the logic for
    * each message.
    *
    * @param filter1
    * @param messageAction
    * @return
    * @throws Exception
    */
   private synchronized int iterQueue(final int flushLimit,
                                      final Filter filter1,
                                      QueueIterateAction messageAction) throws Exception {
      int count = 0;
      int txCount = 0;

      Transaction tx = new TransactionImpl(storageManager);

      try (LinkedListIterator<MessageReference> iter = iterator()) {

         while (iter.hasNext()) {
            MessageReference ref = iter.next();

            if (ref.isPaged() && queueDestroyed) {
               // this means the queue is being removed
               // hence paged references are just going away through
               // page cleanup
               continue;
            }

            if (filter1 == null || filter1.match(ref.getMessage())) {
               messageAction.actMessage(tx, ref);
               iter.remove();
               txCount++;
               count++;
            }
         }

         if (txCount > 0) {
            tx.commit();

            tx = new TransactionImpl(storageManager);

            txCount = 0;
         }

         List<MessageReference> cancelled = scheduledDeliveryHandler.cancel(filter1);
         for (MessageReference messageReference : cancelled) {
            messageAction.actMessage(tx, messageReference, false);
            count++;
            txCount++;
         }

         if (txCount > 0) {
            tx.commit();
            tx = new TransactionImpl(storageManager);
            txCount = 0;
         }

         if (pageIterator != null && !queueDestroyed) {
            while (pageIterator.hasNext()) {
               PagedReference reference = pageIterator.next();
               pageIterator.remove();

               if (filter1 == null || filter1.match(reference.getMessage())) {
                  count++;
                  txCount++;
                  messageAction.actMessage(tx, reference, false);
               } else {
                  addTail(reference, false);
               }

               if (txCount > 0 && txCount % flushLimit == 0) {
                  tx.commit();
                  tx = new TransactionImpl(storageManager);
                  txCount = 0;
               }
            }
         }

         if (txCount > 0) {
            tx.commit();
            tx = null;
         }

         if (filter != null && !queueDestroyed && pageSubscription != null) {
            scheduleDepage(false);
         }

         return count;
      }
   }

   @Override
   public void destroyPaging() throws Exception {
      // it could be null on embedded or certain unit tests
      if (pageSubscription != null) {
         pageSubscription.destroy();
         pageSubscription.cleanupEntries(true);
      }
   }

   @Override
   public synchronized boolean deleteReference(final long messageID) throws Exception {
      boolean deleted = false;

      Transaction tx = new TransactionImpl(storageManager);

      try (LinkedListIterator<MessageReference> iter = iterator()) {

         while (iter.hasNext()) {
            MessageReference ref = iter.next();
            if (ref.getMessage().getMessageID() == messageID) {
               incDelivering(ref);
               acknowledge(tx, ref);
               iter.remove();
               refRemoved(ref);
               deleted = true;
               break;
            }
         }

         if (!deleted) {
            // Look in scheduled deliveries
            deleted = scheduledDeliveryHandler.removeReferenceWithID(messageID) != null ? true : false;
         }

         tx.commit();

         return deleted;
      }
   }

   @Override
   public void deleteQueue() throws Exception {
      deleteQueue(false);
   }

   @Override
   public void deleteQueue(boolean removeConsumers) throws Exception {
      synchronized (this) {
         if (this.queueDestroyed)
            return;
         this.queueDestroyed = true;
      }

      Transaction tx = new BindingsTransactionImpl(storageManager);

      try {
         deleteAllReferences();

         destroyPaging();

         postOffice.removeBinding(name, tx, true);

         if (removeConsumers) {
            for (ConsumerHolder consumerHolder : consumerList) {
               consumerHolder.consumer.disconnect();
            }
         }

         if (isDurable()) {
            storageManager.deleteQueueBinding(tx.getID(), getID());
            tx.setContainsPersistent();
         }

         if (slowConsumerReaperFuture != null) {
            slowConsumerReaperFuture.cancel(false);
         }

         tx.commit();
      } catch (Exception e) {
         tx.rollback();
         throw e;
      } finally {
         if (factory != null) {
            factory.queueRemoved(this);
         }
      }
   }

   @Override
   public synchronized boolean expireReference(final long messageID) throws Exception {
      if (isExpirationRedundant()) {
         return false;
      }

      try (LinkedListIterator<MessageReference> iter = iterator()) {

         while (iter.hasNext()) {
            MessageReference ref = iter.next();
            if (ref.getMessage().getMessageID() == messageID) {
               incDelivering(ref);
               expire(ref);
               iter.remove();
               refRemoved(ref);
               return true;
            }
         }
         return false;
      }
   }

   @Override
   public synchronized int expireReferences(final Filter filter) throws Exception {
      if (isExpirationRedundant()) {
         return 0;
      }

      Transaction tx = new TransactionImpl(storageManager);

      int count = 0;

      try (LinkedListIterator<MessageReference> iter = iterator()) {

         while (iter.hasNext()) {
            MessageReference ref = iter.next();
            if (filter == null || filter.match(ref.getMessage())) {
               incDelivering(ref);
               expire(tx, ref);
               iter.remove();
               refRemoved(ref);
               count++;
            }
         }

         tx.commit();

         return count;
      }
   }

   @Override
   public void expireReferences() {
      if (isExpirationRedundant()) {
         return;
      }

      if (!queueDestroyed && expiryScanner.scannerRunning.get() == 0) {
         expiryScanner.scannerRunning.incrementAndGet();
         getExecutor().execute(expiryScanner);
      }
   }

   public boolean isExpirationRedundant() {
      if (expiryAddress != null && expiryAddress.equals(this.address)) {
         // check expire with itself would be silly (waste of time)
         if (logger.isTraceEnabled())
            logger.trace("Redundant expiration from " + address + " to " + expiryAddress);

         return true;
      }

      return false;
   }

   class ExpiryScanner implements Runnable {

      public AtomicInteger scannerRunning = new AtomicInteger(0);

      @Override
      public void run() {
         synchronized (QueueImpl.this) {
            if (queueDestroyed) {
               return;
            }
            logger.debug("Scanning for expires on " + QueueImpl.this.getName());

            LinkedListIterator<MessageReference> iter = iterator();

            boolean expired = false;
            boolean hasElements = false;

            int elementsExpired = 0;
            try {
               Transaction tx = null;

               while (postOffice.isStarted() && iter.hasNext()) {
                  hasElements = true;
                  MessageReference ref = iter.next();
                  try {
                     if (ref.getMessage().isExpired()) {
                        if (tx == null) {
                           tx = new TransactionImpl(storageManager);
                        }
                        incDelivering(ref);
                        expired = true;
                        expire(tx, ref);
                        iter.remove();
                        refRemoved(ref);

                        if (++elementsExpired >= MAX_DELIVERIES_IN_LOOP) {
                           logger.debug("Breaking loop of expiring");
                           scannerRunning.incrementAndGet();
                           getExecutor().execute(this);
                           break;
                        }
                     }

                  } catch (Exception e) {
                     ActiveMQServerLogger.LOGGER.errorExpiringReferencesOnQueue(e, ref);
                  }
               }

               logger.debug("Expired " + elementsExpired + " references");

               try {
                  if (tx != null) {
                     tx.commit();
                  }
               } catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.unableToCommitTransaction(e);
               }

               // If empty we need to schedule depaging to make sure we would depage expired messages as well
               if ((!hasElements || expired) && pageIterator != null && pageIterator.hasNext()) {
                  scheduleDepage(true);
               }
            } finally {
               try {
                  iter.close();
               } catch (Throwable ignored) {
               }
               scannerRunning.decrementAndGet();
               logger.debug("Scanning for expires on " + QueueImpl.this.getName() + " done");

            }
         }
      }
   }

   @Override
   public synchronized boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception {
      try (LinkedListIterator<MessageReference> iter = iterator()) {
         while (iter.hasNext()) {
            MessageReference ref = iter.next();
            if (ref.getMessage().getMessageID() == messageID) {
               incDelivering(ref);
               sendToDeadLetterAddress(null, ref);
               iter.remove();
               refRemoved(ref);
               return true;
            }
         }
         return false;
      }
   }

   @Override
   public synchronized int sendMessagesToDeadLetterAddress(Filter filter) throws Exception {
      int count = 0;

      try (LinkedListIterator<MessageReference> iter = iterator()) {
         while (iter.hasNext()) {
            MessageReference ref = iter.next();
            if (filter == null || filter.match(ref.getMessage())) {
               incDelivering(ref);
               sendToDeadLetterAddress(null, ref);
               iter.remove();
               refRemoved(ref);
               count++;
            }
         }
         return count;
      }
   }

   @Override
   public synchronized boolean moveReference(final long messageID,
                                             final SimpleString toAddress,
                                             final Binding binding,
                                             final boolean rejectDuplicate) throws Exception {
      try (LinkedListIterator<MessageReference> iter = iterator()) {
         while (iter.hasNext()) {
            MessageReference ref = iter.next();
            if (ref.getMessage().getMessageID() == messageID) {
               iter.remove();
               refRemoved(ref);
               incDelivering(ref);
               try {
                  move(null, toAddress, binding, ref, rejectDuplicate, AckReason.NORMAL, null);
               } catch (Exception e) {
                  decDelivering(ref);
                  throw e;
               }
               return true;
            }
         }
         return false;
      }
   }

   @Override
   public int moveReferences(final Filter filter, final SimpleString toAddress, Binding binding) throws Exception {
      return moveReferences(DEFAULT_FLUSH_LIMIT, filter, toAddress, false, binding);
   }

   @Override
   public synchronized int moveReferences(final int flushLimit,
                                          final Filter filter,
                                          final SimpleString toAddress,
                                          final boolean rejectDuplicates,
                                          final Binding binding) throws Exception {
      final DuplicateIDCache targetDuplicateCache = postOffice.getDuplicateIDCache(toAddress);

      return iterQueue(flushLimit, filter, new QueueIterateAction() {
         @Override
         public void actMessage(Transaction tx, MessageReference ref) throws Exception {
            boolean ignored = false;

            incDelivering(ref);

            if (rejectDuplicates) {
               byte[] duplicateBytes = ref.getMessage().getDuplicateIDBytes();
               if (duplicateBytes != null) {
                  if (targetDuplicateCache.contains(duplicateBytes)) {
                     ActiveMQServerLogger.LOGGER.messageWithDuplicateID(ref.getMessage().getDuplicateProperty(), toAddress, address, address);
                     acknowledge(tx, ref);
                     ignored = true;
                  }
               }
            }

            if (!ignored) {
               move(null, toAddress, binding, ref, rejectDuplicates, AckReason.NORMAL, null);
               refRemoved(ref);
               //move(toAddress, tx, ref, false, rejectDuplicates);
            }
         }
      });
   }

   public synchronized int moveReferencesBetweenSnFQueues(final SimpleString queueSuffix) throws Exception {
      return iterQueue(DEFAULT_FLUSH_LIMIT, null, new QueueIterateAction() {
         @Override
         public void actMessage(Transaction tx, MessageReference ref) throws Exception {
            moveBetweenSnFQueues(queueSuffix, tx, ref);
         }
      });
   }

   @Override
   public int retryMessages(Filter filter) throws Exception {

      final HashMap<String, Long> queues = new HashMap<>();

      return iterQueue(DEFAULT_FLUSH_LIMIT, filter, new QueueIterateAction() {
         @Override
         public void actMessage(Transaction tx, MessageReference ref) throws Exception {

            String originalMessageAddress = ref.getMessage().getAnnotationString(Message.HDR_ORIGINAL_ADDRESS);
            String originalMessageQueue = ref.getMessage().getAnnotationString(Message.HDR_ORIGINAL_QUEUE);

            if (originalMessageAddress != null) {

               incDelivering(ref);

               Long targetQueue = null;
               if (originalMessageQueue != null && !originalMessageQueue.equals(originalMessageAddress)) {
                  targetQueue = queues.get(originalMessageQueue);
                  if (targetQueue == null) {
                     Binding binding = postOffice.getBinding(SimpleString.toSimpleString(originalMessageQueue));

                     if (binding != null && binding instanceof LocalQueueBinding) {
                        targetQueue = ((LocalQueueBinding) binding).getID();
                        queues.put(originalMessageQueue, targetQueue);
                     }
                  }
               }

               if (targetQueue != null) {
                  move(SimpleString.toSimpleString(originalMessageAddress), tx, ref, false, false, targetQueue.longValue());
               } else {
                  move(SimpleString.toSimpleString(originalMessageAddress), tx, ref, false, false);
               }
               refRemoved(ref);
            }
         }
      });

   }

   @Override
   public synchronized boolean changeReferencePriority(final long messageID, final byte newPriority) throws Exception {
      try (LinkedListIterator<MessageReference> iter = iterator()) {

         while (iter.hasNext()) {
            MessageReference ref = iter.next();
            if (ref.getMessage().getMessageID() == messageID) {
               iter.remove();
               refRemoved(ref);
               ref.getMessage().setPriority(newPriority);
               addTail(ref, false);
               return true;
            }
         }

         return false;
      }
   }

   @Override
   public synchronized int changeReferencesPriority(final Filter filter, final byte newPriority) throws Exception {
      try (LinkedListIterator<MessageReference> iter = iterator()) {
         int count = 0;
         while (iter.hasNext()) {
            MessageReference ref = iter.next();
            if (filter == null || filter.match(ref.getMessage())) {
               count++;
               iter.remove();
               refRemoved(ref);
               ref.getMessage().setPriority(newPriority);
               addTail(ref, false);
            }
         }
         return count;
      }
   }

   @Override
   public synchronized void resetAllIterators() {
      for (ConsumerHolder holder : this.consumerList) {
         if (holder.iter != null) {
            holder.iter.close();
         }
         holder.iter = null;
      }
   }

   @Override
   public synchronized void pause() {
      pause(false);
   }

   @Override
   public synchronized void reloadPause(long recordID) {
      this.paused = true;
      if (pauseStatusRecord >= 0) {
         try {
            storageManager.deleteQueueStatus(pauseStatusRecord);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.unableToDeleteQueueStatus(e);
         }
      }
      this.pauseStatusRecord = recordID;
   }

   @Override
   public synchronized void pause(boolean persist) {
      try {
         this.flushDeliveriesInTransit();
         if (persist && isDurable()) {
            if (pauseStatusRecord >= 0) {
               storageManager.deleteQueueStatus(pauseStatusRecord);
            }
            pauseStatusRecord = storageManager.storeQueueStatus(this.id, QueueStatus.PAUSED);
         }
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.unableToPauseQueue(e);
      }
      paused = true;
   }

   @Override
   public synchronized void resume() {
      paused = false;

      if (pauseStatusRecord >= 0) {
         try {
            storageManager.deleteQueueStatus(pauseStatusRecord);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.unableToResumeQueue(e);
         }
         pauseStatusRecord = -1;
      }

      deliverAsync();
   }

   @Override
   public synchronized boolean isPaused() {
      return paused;
   }

   @Override
   public synchronized boolean isPersistedPause() {
      return this.pauseStatusRecord >= 0;
   }

   @Override
   public boolean isDirectDeliver() {
      return directDeliver;
   }

   /**
    * @return the internalQueue
    */
   @Override
   public boolean isInternalQueue() {
      return internalQueue;
   }

   /**
    * @param internalQueue the internalQueue to set
    */
   @Override
   public void setInternalQueue(boolean internalQueue) {
      this.internalQueue = internalQueue;
   }

   // Public
   // -----------------------------------------------------------------------------

   @Override
   public boolean equals(final Object other) {
      if (this == other) {
         return true;
      }
      if (!(other instanceof QueueImpl))
         return false;

      QueueImpl qother = (QueueImpl) other;

      return name.equals(qother.name);
   }

   @Override
   public int hashCode() {
      return name.hashCode();
   }

   @Override
   public String toString() {
      return "QueueImpl[name=" + name.toString() + ", postOffice=" + this.postOffice + ", temp=" + this.temporary + "]@" + Integer.toHexString(System.identityHashCode(this));
   }

   private synchronized void internalAddTail(final MessageReference ref) {
      refAdded(ref);
      messageReferences.addTail(ref, getPriority(ref));
      pendingMetrics.incrementMetrics(ref);
   }

   /**
    * The caller of this method requires synchronized on the queue.
    * I'm not going to add synchronized to this method just for a precaution,
    * as I'm not 100% sure this won't cause any extra runtime.
    *
    * @param ref
    */
   private void internalAddHead(final MessageReference ref) {
      queueMemorySize.addAndGet(ref.getMessageMemoryEstimate());
      pendingMetrics.incrementMetrics(ref);
      refAdded(ref);

      int priority = getPriority(ref);

      messageReferences.addHead(ref, priority);
   }

   private int getPriority(MessageReference ref) {
      try {
         return ref.getMessage().getPriority();
      } catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.unableToGetMessagePriority(e);
         return 4; // the default one in case of failure
      }
   }

   private synchronized void doInternalPoll() {

      int added = 0;
      MessageReference ref;

      while ((ref = intermediateMessageReferences.poll()) != null) {
         internalAddTail(ref);

         if (!ref.isPaged()) {
            messagesAdded.incrementAndGet();
         }

         if (added++ > MAX_DELIVERIES_IN_LOOP) {
            // if we just keep polling from the intermediate we could starve in case there's a sustained load
            deliverAsync();
            return;
         }
      }
   }

   /**
    * This method will deliver as many messages as possible until all consumers are busy or there
    * are no more matching or available messages.
    */
   private void deliver() {
      if (logger.isDebugEnabled()) {
         logger.debug(this + " doing deliver. messageReferences=" + messageReferences.size());
      }

      doInternalPoll();

      // Either the iterator is empty or the consumer is busy
      int noDelivery = 0;

      int size = 0;

      int endPos = -1;

      int handled = 0;

      long timeout = System.currentTimeMillis() + DELIVERY_TIMEOUT;

      while (true) {
         if (handled == MAX_DELIVERIES_IN_LOOP) {
            // Schedule another one - we do this to prevent a single thread getting caught up in this loop for too
            // long

            deliverAsync();

            return;
         }

         if (System.currentTimeMillis() > timeout) {
            if (logger.isTraceEnabled()) {
               logger.trace("delivery has been running for too long. Scheduling another delivery task now");
            }

            deliverAsync();

            return;
         }

         MessageReference ref;

         Consumer handledconsumer = null;

         synchronized (this) {

            // Need to do these checks inside the synchronized
            if (paused || consumerList.isEmpty()) {
               return;
            }

            if (messageReferences.size() == 0) {
               break;
            }

            if (endPos < 0 || consumersChanged) {
               consumersChanged = false;

               size = consumerList.size();

               endPos = pos - 1;

               if (endPos < 0) {
                  endPos = size - 1;
                  noDelivery = 0;
               }
            }

            ConsumerHolder holder = consumerList.get(pos);

            Consumer consumer = holder.consumer;
            Consumer groupConsumer = null;

            if (holder.iter == null) {
               holder.iter = messageReferences.iterator();
            }

            if (holder.iter.hasNext()) {
               ref = holder.iter.next();
            } else {
               ref = null;
            }
            if (ref == null) {
               noDelivery++;
            } else {
               if (checkExpired(ref)) {
                  if (logger.isTraceEnabled()) {
                     logger.trace("Reference " + ref + " being expired");
                  }
                  holder.iter.remove();

                  refRemoved(ref);

                  handled++;

                  continue;
               }

               if (logger.isTraceEnabled()) {
                  logger.trace("Queue " + this.getName() + " is delivering reference " + ref);
               }

               // If a group id is set, then this overrides the consumer chosen round-robin

               SimpleString groupID = extractGroupID(ref);

               if (groupID != null) {
                  groupConsumer = groups.get(groupID);

                  if (groupConsumer != null) {
                     consumer = groupConsumer;
                  }
               }

               if (exclusive) {
                  consumer = consumerList.get(0).consumer;
               }

               HandleStatus status = handle(ref, consumer);

               if (status == HandleStatus.HANDLED) {

                  deliveriesInTransit.countUp();

                  handledconsumer = consumer;

                  holder.iter.remove();

                  refRemoved(ref);

                  if (groupID != null && groupConsumer == null) {
                     groups.put(groupID, consumer);
                  }

                  handled++;
               } else if (status == HandleStatus.BUSY) {
                  try {
                     holder.iter.repeat();
                  } catch (NoSuchElementException e) {
                     // this could happen if there was an exception on the queue handling
                     // and it returned BUSY because of that exception
                     //
                     // We will just log it as there's nothing else we can do now.
                     logger.warn(e.getMessage(), e);
                  }

                  noDelivery++;
               } else if (status == HandleStatus.NO_MATCH) {
                  // nothing to be done on this case, the iterators will just jump next
               }
            }

            if (pos == endPos) {
               // Round robin'd all

               if (noDelivery == size) {
                  if (handledconsumer != null) {
                     // this shouldn't really happen,
                     // however I'm keeping this as an assertion case future developers ever change the logic here on this class
                     ActiveMQServerLogger.LOGGER.nonDeliveryHandled();
                  } else {
                     if (logger.isDebugEnabled()) {
                        logger.debug(this + "::All the consumers were busy, giving up now");
                     }
                     break;
                  }
               }

               noDelivery = 0;
            }

            // Only move onto the next position if the consumer on the current position was used.
            // When using group we don't need to load balance to the next position
            if (!exclusive && groupConsumer == null) {
               pos++;
            }

            if (pos >= size) {
               pos = 0;
            }
         }

         if (handledconsumer != null) {
            proceedDeliver(handledconsumer, ref);
         }
      }

      checkDepage();
   }

   private void checkDepage() {
      if (pageIterator != null && pageSubscription.isPaging() && !depagePending && needsDepage() && pageIterator.hasNext()) {
         scheduleDepage(false);
      }
   }

   /**
    * This is a common check we do before scheduling depaging.. or while depaging.
    * Before scheduling a depage runnable we verify if it fits / needs depaging.
    * We also check for while needsDepage While depaging.
    * This is just to avoid a copy & paste dependency
    *
    * @return
    */
   private boolean needsDepage() {
      return queueMemorySize.get() < pageSubscription.getPagingStore().getMaxSize();
   }

   private SimpleString extractGroupID(MessageReference ref) {
      if (internalQueue) {
         return null;
      } else {
         try {
            // But we don't use the groupID on internal queues (clustered queues) otherwise the group map would leak forever
            return ref.getMessage().getGroupID();
         } catch (Throwable e) {
            ActiveMQServerLogger.LOGGER.unableToExtractGroupID(e);
            return null;
         }
      }
   }

   protected void refRemoved(MessageReference ref) {
      queueMemorySize.addAndGet(-ref.getMessageMemoryEstimate());
      pendingMetrics.decrementMetrics(ref);
      if (ref.isPaged()) {
         pagedReferences.decrementAndGet();
      }
   }

   protected void refAdded(final MessageReference ref) {
      if (ref.isPaged()) {
         pagedReferences.incrementAndGet();
      }
   }

   private void scheduleDepage(final boolean scheduleExpiry) {
      if (!depagePending) {
         if (logger.isTraceEnabled()) {
            logger.trace("Scheduling depage for queue " + this.getName());
         }
         depagePending = true;
         pageSubscription.getExecutor().execute(new DepageRunner(scheduleExpiry));
      }
   }

   private void depage(final boolean scheduleExpiry) {
      depagePending = false;

      synchronized (this) {
         if (paused || pageIterator == null) {
            return;
         }
      }

      long maxSize = pageSubscription.getPagingStore().getPageSizeBytes();

      long timeout = System.currentTimeMillis() + DELIVERY_TIMEOUT;

      if (logger.isTraceEnabled()) {
         logger.trace("QueueMemorySize before depage on queue=" + this.getName() + " is " + queueMemorySize.get());
      }

      this.directDeliver = false;

      int depaged = 0;
      while (timeout > System.currentTimeMillis() && needsDepage() && pageIterator.hasNext()) {
         depaged++;
         PagedReference reference = pageIterator.next();
         if (logger.isTraceEnabled()) {
            logger.trace("Depaging reference " + reference + " on queue " + this.getName());
         }
         addTail(reference, false);
         pageIterator.remove();

         //We have to increment this here instead of in the iterator so we have access to the reference from next()
         pageSubscription.incrementDeliveredSize(getPersistentSize(reference));
      }

      if (logger.isDebugEnabled()) {
         if (depaged == 0 && queueMemorySize.get() >= maxSize) {
            logger.debug("Couldn't depage any message as the maxSize on the queue was achieved. " + "There are too many pending messages to be acked in reference to the page configuration");
         }

         if (logger.isDebugEnabled()) {
            logger.debug("Queue Memory Size after depage on queue=" + this.getName() + " is " + queueMemorySize.get() + " with maxSize = " + maxSize + ". Depaged " + depaged + " messages, pendingDelivery=" + messageReferences.size() + ", intermediateMessageReferences= " + intermediateMessageReferences.size() + ", queueDelivering=" + deliveringMetrics.getMessageCount());

         }
      }

      deliverAsync();

      if (depaged > 0 && scheduleExpiry) {
         // This will just call an executor
         expireReferences();
      }
   }

   private void internalAddRedistributor(final ArtemisExecutor executor) {
      // create the redistributor only once if there are no local consumers
      if (consumerSet.isEmpty() && redistributor == null) {
         if (logger.isTraceEnabled()) {
            logger.trace("QueueImpl::Adding redistributor on queue " + this.toString());
         }
         redistributor = new Redistributor(this, storageManager, postOffice, executor, QueueImpl.REDISTRIBUTOR_BATCH_SIZE);

         consumerList.add(new ConsumerHolder(redistributor));

         consumersChanged = true;

         redistributor.start();

         deliverAsync();
      }
   }

   @Override
   public boolean checkRedelivery(final MessageReference reference,
                                  final long timeBase,
                                  final boolean ignoreRedeliveryDelay) throws Exception {
      Message message = reference.getMessage();

      if (internalQueue) {
         if (logger.isTraceEnabled()) {
            logger.trace("Queue " + this.getName() + " is an internal queue, no checkRedelivery");
         }
         // no DLQ check on internal queues
         return true;
      }

      if (!internalQueue && message.isDurable() && isDurableMessage() && !reference.isPaged()) {
         storageManager.updateDeliveryCount(reference);
      }

      AddressSettings addressSettings = addressSettingsRepository.getMatch(address.toString());

      int maxDeliveries = addressSettings.getMaxDeliveryAttempts();
      long redeliveryDelay = addressSettings.getRedeliveryDelay();
      int deliveryCount = reference.getDeliveryCount();

      // First check DLA
      if (maxDeliveries > 0 && deliveryCount >= maxDeliveries) {
         if (logger.isTraceEnabled()) {
            logger.trace("Sending reference " + reference + " to DLA = " + addressSettings.getDeadLetterAddress() + " since ref.getDeliveryCount=" + reference.getDeliveryCount() + "and maxDeliveries=" + maxDeliveries + " from queue=" + this.getName());
         }
         sendToDeadLetterAddress(null, reference, addressSettings.getDeadLetterAddress());

         return false;
      } else {
         // Second check Redelivery Delay
         if (!ignoreRedeliveryDelay && redeliveryDelay > 0) {
            redeliveryDelay = calculateRedeliveryDelay(addressSettings, deliveryCount);

            if (logger.isTraceEnabled()) {
               logger.trace("Setting redeliveryDelay=" + redeliveryDelay + " on reference=" + reference);
            }

            reference.setScheduledDeliveryTime(timeBase + redeliveryDelay);

            if (!reference.isPaged() && message.isDurable() && isDurableMessage()) {
               storageManager.updateScheduledDeliveryTime(reference);
            }
         }

         decDelivering(reference);

         return true;
      }
   }

   /**
    * Used on testing only *
    */
   public int getNumberOfReferences() {
      return messageReferences.size();
   }

   private void move(final SimpleString toAddress,
                     final Transaction tx,
                     final MessageReference ref,
                     final boolean expiry,
                     final boolean rejectDuplicate,
                     final long... queueIDs) throws Exception {
      Message copyMessage = makeCopy(ref, expiry);

      copyMessage.setAddress(toAddress);

      if (queueIDs != null && queueIDs.length > 0) {
         ByteBuffer buffer = ByteBuffer.allocate(8 * queueIDs.length);
         for (long id : queueIDs) {
            buffer.putLong(id);
         }
         copyMessage.putBytesProperty(Message.HDR_ROUTE_TO_IDS.toString(), buffer.array());
      }

      postOffice.route(copyMessage, tx, false, rejectDuplicate);

      if (expiry) {
         acknowledge(tx, ref, AckReason.EXPIRED, null);
      } else {
         acknowledge(tx, ref);
      }

   }

   @SuppressWarnings({"ArrayToString", "ArrayToStringConcatenation"})
   private void moveBetweenSnFQueues(final SimpleString queueSuffix,
                                     final Transaction tx,
                                     final MessageReference ref) throws Exception {
      Message copyMessage = makeCopy(ref, false, false);

      byte[] oldRouteToIDs = null;
      String targetNodeID;
      Binding targetBinding;

      // remove the old route
      for (SimpleString propName : copyMessage.getPropertyNames()) {
         if (propName.startsWith(Message.HDR_ROUTE_TO_IDS)) {
            oldRouteToIDs = (byte[]) copyMessage.removeProperty(propName.toString());
            final String hashcodeToString = oldRouteToIDs.toString(); // don't use Arrays.toString(..) here
            logger.debug("Removed property from message: " + propName + " = " + hashcodeToString + " (" + ByteBuffer.wrap(oldRouteToIDs).getLong() + ")");

            // there should only be one of these properties so potentially save some loop iterations
            break;
         }
      }

      ByteBuffer oldBuffer = ByteBuffer.wrap(oldRouteToIDs);

      RoutingContext routingContext = new RoutingContextImpl(tx);

      /* this algorithm will look at the old route and find the new remote queue bindings where the messages should go
       * and route them there directly
       */
      while (oldBuffer.hasRemaining()) {
         long oldQueueID = oldBuffer.getLong();

         // look at all the bindings
         Pair<String, Binding> result = locateTargetBinding(queueSuffix, copyMessage, oldQueueID);
         targetBinding = result.getB();
         targetNodeID = result.getA();

         if (targetBinding == null) {
            ActiveMQServerLogger.LOGGER.unableToFindTargetQueue(targetNodeID);
         } else {
            logger.debug("Routing on binding: " + targetBinding);
            targetBinding.route(copyMessage, routingContext);
         }
      }

      postOffice.processRoute(copyMessage, routingContext, false);

      ref.handled();

      acknowledge(tx, ref);

      storageManager.afterCompleteOperations(new IOCallback() {

         @Override
         public void onError(final int errorCode, final String errorMessage) {
            ActiveMQServerLogger.LOGGER.ioErrorRedistributing(errorCode, errorMessage);
         }

         @Override
         public void done() {
            deliverAsync();
         }
      });
   }

   private Pair<String, Binding> locateTargetBinding(SimpleString queueSuffix, Message copyMessage, long oldQueueID) {
      String targetNodeID = null;
      Binding targetBinding = null;

      for (Map.Entry<SimpleString, Binding> entry : postOffice.getAllBindings().entrySet()) {
         Binding binding = entry.getValue();

         // we only care about the remote queue bindings
         if (binding instanceof RemoteQueueBinding) {
            RemoteQueueBinding remoteQueueBinding = (RemoteQueueBinding) binding;

            // does this remote queue binding point to the same queue as the message?
            if (oldQueueID == remoteQueueBinding.getRemoteQueueID()) {
               // get the name of this queue so we can find the corresponding remote queue binding pointing to the scale down target node
               SimpleString oldQueueName = remoteQueueBinding.getRoutingName();

               // parse the queue name of the remote queue binding to determine the node ID
               String temp = remoteQueueBinding.getQueue().getName().toString();
               targetNodeID = temp.substring(temp.lastIndexOf(".") + 1);
               logger.debug("Message formerly destined for " + oldQueueName + " with ID: " + oldQueueID + " on address " + copyMessage.getAddressSimpleString() + " on node " + targetNodeID);

               // now that we have the name of the queue we need to look through all the bindings again to find the new remote queue binding
               for (Map.Entry<SimpleString, Binding> entry2 : postOffice.getAllBindings().entrySet()) {
                  binding = entry2.getValue();

                  // again, we only care about the remote queue bindings
                  if (binding instanceof RemoteQueueBinding) {
                     remoteQueueBinding = (RemoteQueueBinding) binding;
                     temp = remoteQueueBinding.getQueue().getName().toString();
                     targetNodeID = temp.substring(temp.lastIndexOf(".") + 1);
                     if (oldQueueName.equals(remoteQueueBinding.getRoutingName()) && targetNodeID.equals(queueSuffix.toString())) {
                        targetBinding = remoteQueueBinding;
                        if (logger.isDebugEnabled()) {
                           logger.debug("Message now destined for " + remoteQueueBinding.getRoutingName() + " with ID: " + remoteQueueBinding.getRemoteQueueID() + " on address " + copyMessage.getAddress() + " on node " + targetNodeID);
                        }
                        break;
                     } else {
                        logger.debug("Failed to match: " + remoteQueueBinding);
                     }
                  }
               }
            }
         }
      }
      return new Pair<>(targetNodeID, targetBinding);
   }

   private Message makeCopy(final MessageReference ref, final boolean expiry) throws Exception {
      return makeCopy(ref, expiry, true);
   }

   private Message makeCopy(final MessageReference ref,
                            final boolean expiry,
                            final boolean copyOriginalHeaders) throws Exception {
      Message message = ref.getMessage();
      /*
       We copy the message and send that to the dla/expiry queue - this is
       because otherwise we may end up with a ref with the same message id in the
       queue more than once which would barf - this might happen if the same message had been
       expire from multiple subscriptions of a topic for example
       We set headers that hold the original message address, expiry time
       and original message id
      */

      long newID = storageManager.generateID();

      Message copy = message.copy(newID);

      if (copyOriginalHeaders) {
         copy.referenceOriginalMessage(message, ref != null ? ref.getQueue().getName().toString() : null);
      }

      copy.setExpiration(0);

      if (expiry) {
         copy.setAnnotation(Message.HDR_ACTUAL_EXPIRY_TIME, System.currentTimeMillis());
      }

      copy.reencode();

      return copy;
   }

   private void expire(final Transaction tx, final MessageReference ref) throws Exception {
      SimpleString expiryAddress = addressSettingsRepository.getMatch(address.toString()).getExpiryAddress();

      if (expiryAddress != null) {
         Bindings bindingList = postOffice.getBindingsForAddress(expiryAddress);

         if (bindingList.getBindings().isEmpty()) {
            ActiveMQServerLogger.LOGGER.errorExpiringReferencesNoBindings(expiryAddress);
            acknowledge(tx, ref, AckReason.EXPIRED, null);
         } else {
            move(expiryAddress, tx, ref, true, true);
         }
      } else {
         if (!printErrorExpiring) {
            printErrorExpiring = true;
            // print this only once
            ActiveMQServerLogger.LOGGER.errorExpiringReferencesNoQueue(name);
         }

         acknowledge(tx, ref, AckReason.EXPIRED, null);
      }
   }

   @Override
   public void sendToDeadLetterAddress(final Transaction tx, final MessageReference ref) throws Exception {
      sendToDeadLetterAddress(tx, ref, addressSettingsRepository.getMatch(address.toString()).getDeadLetterAddress());
   }

   private void sendToDeadLetterAddress(final Transaction tx,
                                        final MessageReference ref,
                                        final SimpleString deadLetterAddress) throws Exception {
      if (deadLetterAddress != null) {
         Bindings bindingList = postOffice.getBindingsForAddress(deadLetterAddress);

         if (bindingList.getBindings().isEmpty()) {
            ActiveMQServerLogger.LOGGER.messageExceededMaxDelivery(ref, deadLetterAddress);
            ref.acknowledge(tx, AckReason.KILLED, null);
         } else {
            ActiveMQServerLogger.LOGGER.messageExceededMaxDeliverySendtoDLA(ref, deadLetterAddress, name);
            move(tx, deadLetterAddress, null, ref, false, AckReason.KILLED, null);
         }
      } else {
         ActiveMQServerLogger.LOGGER.messageExceededMaxDeliveryNoDLA(ref, name);

         ref.acknowledge(tx, AckReason.KILLED, null);
      }
   }

   private void move(final Transaction originalTX,
                     final SimpleString address,
                     final Binding binding,
                     final MessageReference ref,
                     final boolean rejectDuplicate,
                     final AckReason reason,
                     final ServerConsumer consumer) throws Exception {
      Transaction tx;

      if (originalTX != null) {
         tx = originalTX;
      } else {
         // if no TX we create a new one to commit at the end
         tx = new TransactionImpl(storageManager);
      }

      Message copyMessage = makeCopy(ref, reason == AckReason.EXPIRED);

      copyMessage.setAddress(address);

      postOffice.route(copyMessage, tx, false, rejectDuplicate, binding);

      acknowledge(tx, ref, reason, consumer);

      if (originalTX == null) {
         tx.commit();
      }
   }

   /*
    * This method delivers the reference on the callers thread - this can give us better latency in the case there is nothing in the queue
    */
   private boolean deliverDirect(final MessageReference ref) {
      synchronized (this) {
         if (!supportsDirectDeliver) {
            // this should never happen, but who knows?
            // if someone ever change add and removeConsumer,
            // this would protect any eventual bug
            return false;
         }
         if (paused || consumerList.isEmpty()) {
            return false;
         }

         if (checkExpired(ref)) {
            return true;
         }

         int startPos = pos;

         int size = consumerList.size();

         while (true) {
            ConsumerHolder holder = consumerList.get(pos);

            Consumer consumer = holder.consumer;

            Consumer groupConsumer = null;

            // If a group id is set, then this overrides the consumer chosen round-robin

            SimpleString groupID = extractGroupID(ref);

            if (groupID != null) {
               groupConsumer = groups.get(groupID);

               if (groupConsumer != null) {
                  consumer = groupConsumer;
               }
            }

            if (exclusive) {
               consumer = consumerList.get(0).consumer;
            }

            // Only move onto the next position if the consumer on the current position was used.
            if (!exclusive && groupConsumer == null) {
               pos++;
            }

            if (pos == size) {
               pos = 0;
            }

            HandleStatus status = handle(ref, consumer);

            if (status == HandleStatus.HANDLED) {
               if (groupID != null && groupConsumer == null) {
                  groups.put(groupID, consumer);
               }

               messagesAdded.incrementAndGet();

               deliveriesInTransit.countUp();
               proceedDeliver(consumer, ref);
               return true;
            }

            if (pos == startPos) {
               // Tried them all
               break;
            }
         }
         return false;
      }
   }

   private void proceedDeliver(Consumer consumer, MessageReference reference) {
      try {
         consumer.proceedDeliver(reference);
      } catch (Throwable t) {
         ActiveMQServerLogger.LOGGER.removingBadConsumer(t, consumer, reference);

         synchronized (this) {
            // If the consumer throws an exception we remove the consumer
            try {
               removeConsumer(consumer);
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorRemovingConsumer(e);
            }

            // The message failed to be delivered, hence we try again
            addHead(reference, false);
         }
      } finally {
         deliveriesInTransit.countDown();
      }
   }

   private boolean checkExpired(final MessageReference reference) {
      try {
         if (reference.getMessage().isExpired()) {
            if (logger.isTraceEnabled()) {
               logger.trace("Reference " + reference + " is expired");
            }
            reference.handled();

            try {
               expire(reference);
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorExpiringRef(e);
            }

            return true;
         } else {
            return false;
         }
      } catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.unableToCheckIfMessageExpired(e);
         return false;
      }
   }

   private synchronized HandleStatus handle(final MessageReference reference, final Consumer consumer) {
      HandleStatus status;
      try {
         status = consumer.handle(reference);
      } catch (Throwable t) {
         ActiveMQServerLogger.LOGGER.removingBadConsumer(t, consumer, reference);

         // If the consumer throws an exception we remove the consumer
         try {
            removeConsumer(consumer);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorRemovingConsumer(e);
         }
         return HandleStatus.BUSY;
      }

      if (status == null) {
         throw new IllegalStateException("ClientConsumer.handle() should never return null");
      }

      return status;
   }

   private List<ConsumerHolder> cloneConsumersList() {
      List<ConsumerHolder> consumerListClone;

      synchronized (this) {
         consumerListClone = new ArrayList<>(consumerList);
      }
      return consumerListClone;
   }

   @Override
   public void postAcknowledge(final MessageReference ref) {
      QueueImpl queue = (QueueImpl) ref.getQueue();

      queue.decDelivering(ref);

      if (ref.isPaged()) {
         // nothing to be done
         return;
      }

      Message message;

      try {
         message = ref.getMessage();
      } catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.unableToPerformPostAcknowledge(e);
         message = null;
      }

      if (message == null)
         return;

      boolean durableRef = message.isDurable() && queue.isDurableMessage();

      try {
         message.decrementRefCount();
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorDecrementingRefCount(e);
      }

      if (durableRef) {
         int count = message.decrementDurableRefCount();

         if (count == 0) {
            // Note - we MUST store the delete after the preceding ack has been committed to storage, we cannot combine
            // the last ack and delete into a single delete.
            // This is because otherwise we could have a situation where the same message is being acked concurrently
            // from two different queues on different sessions.
            // One decrements the ref count, then the other stores a delete, the delete gets committed, but the first
            // ack isn't committed, then the server crashes and on
            // recovery the message is deleted even though the other ack never committed

            // also note then when this happens as part of a transaction it is the tx commit of the ack that is
            // important not this

            // Also note that this delete shouldn't sync to disk, or else we would build up the executor's queue
            // as we can't delete each messaging with sync=true while adding messages transactionally.
            // There is a startup check to remove non referenced messages case these deletes fail
            try {
               storageManager.deleteMessage(message.getMessageID());
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorRemovingMessage(e, message.getMessageID());
            }
         }
      }
   }

   void postRollback(final LinkedList<MessageReference> refs) {
      //if we have purged then ignore adding the messages back
      if (purgeOnNoConsumers && getConsumerCount() == 0) {
         purgeAfterRollback(refs);

         return;
      }
      addHead(refs, false);
   }

   private void purgeAfterRollback(LinkedList<MessageReference> refs) {
      try {
         Transaction transaction = new TransactionImpl(storageManager);
         for (MessageReference reference : refs) {
            incDelivering(reference); // post ack will decrement this, so need to inc
            acknowledge(transaction, reference, AckReason.KILLED, null);
         }
         transaction.commit();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      }
   }

   private long calculateRedeliveryDelay(final AddressSettings addressSettings, final int deliveryCount) {
      long redeliveryDelay = addressSettings.getRedeliveryDelay();
      long maxRedeliveryDelay = addressSettings.getMaxRedeliveryDelay();
      double redeliveryMultiplier = addressSettings.getRedeliveryMultiplier();

      int tmpDeliveryCount = deliveryCount > 0 ? deliveryCount - 1 : 0;
      long delay = (long) (redeliveryDelay * (Math.pow(redeliveryMultiplier, tmpDeliveryCount)));

      if (delay > maxRedeliveryDelay) {
         delay = maxRedeliveryDelay;
      }

      return delay;
   }

   @Override
   public synchronized void resetMessagesAdded() {
      messagesAdded.set(0);
   }

   @Override
   public synchronized void resetMessagesAcknowledged() {
      messagesAcknowledged.set(0);
   }

   @Override
   public synchronized void resetMessagesExpired() {
      messagesExpired.set(0);
   }

   @Override
   public synchronized void resetMessagesKilled() {
      messagesKilled.set(0);
   }

   @Override
   public float getRate() {
      long locaMessageAdded = getMessagesAdded();
      float timeSlice = ((System.currentTimeMillis() - queueRateCheckTime.getAndSet(System.currentTimeMillis())) / 1000.0f);
      if (timeSlice == 0) {
         messagesAddedSnapshot.getAndSet(locaMessageAdded);
         return 0.0f;
      }
      return BigDecimal.valueOf((locaMessageAdded - messagesAddedSnapshot.getAndSet(locaMessageAdded)) / timeSlice).setScale(2, BigDecimal.ROUND_UP).floatValue();
   }

   @Override
   public void recheckRefCount(OperationContext context) {
      ReferenceCounter refCount = refCountForConsumers;
      if (refCount != null) {
         context.executeOnCompletion(new IOCallback() {
            @Override
            public void done() {
               refCount.check();
            }

            @Override
            public void onError(int errorCode, String errorMessage) {

            }
         });
      }

   }

   // Inner classes
   // --------------------------------------------------------------------------

   private static class ConsumerHolder {

      ConsumerHolder(final Consumer consumer) {
         this.consumer = consumer;
      }

      final Consumer consumer;

      LinkedListIterator<MessageReference> iter;

   }

   private class DelayedAddRedistributor implements Runnable {

      private final ArtemisExecutor executor1;

      DelayedAddRedistributor(final ArtemisExecutor executor) {
         this.executor1 = executor;
      }

      @Override
      public void run() {
         synchronized (QueueImpl.this) {
            internalAddRedistributor(executor1);

            clearRedistributorFuture();
         }
      }
   }

   /**
    * There's no need of having multiple instances of this class. a Single instance per QueueImpl should be more than sufficient.
    * previous versions of this class were using a synchronized object. The current version is using the deliverRunner
    * instance, and to avoid confusion on the implementation I'm requesting to keep this single instanced per QueueImpl.
    */
   private final class DeliverRunner implements Runnable {

      @Override
      public void run() {
         try {
            // during the transition between paging and nonpaging, we could have this using a different executor
            // and at this short period we could have more than one delivery thread running in async mode
            // this will avoid that possibility
            // We will be using the deliverRunner instance as the guard object to avoid multiple threads executing
            // an asynchronous delivery
            enterCritical(CRITICAL_DELIVER);
            try {
               synchronized (QueueImpl.this.deliverRunner) {
                  deliver();
               }
            } finally {
               leaveCritical(CRITICAL_DELIVER);
            }
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorDelivering(e);
         } finally {
            scheduledRunners.decrementAndGet();
         }
      }
   }

   private final class DepageRunner implements Runnable {

      final boolean scheduleExpiry;

      private DepageRunner(boolean scheduleExpiry) {
         this.scheduleExpiry = scheduleExpiry;
      }

      @Override
      public void run() {
         try {
            depage(scheduleExpiry);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorDelivering(e);
         }
      }
   }

   /**
    * This will determine the actions that could be done while iterate the queue through iterQueue
    */
   abstract class QueueIterateAction {

      public abstract void actMessage(Transaction tx, MessageReference ref) throws Exception;

      public void actMessage(Transaction tx, MessageReference ref, boolean fromMessageReferences) throws Exception {
         actMessage(tx, ref);
      }
   }

   /* For external use we need to use a synchronized version since the list is not thread safe */
   private class SynchronizedIterator implements LinkedListIterator<MessageReference> {

      private final LinkedListIterator<MessageReference> iter;

      SynchronizedIterator(LinkedListIterator<MessageReference> iter) {
         this.iter = iter;
      }

      @Override
      public void close() {
         synchronized (QueueImpl.this) {
            iter.close();
         }
      }

      @Override
      public void repeat() {
         synchronized (QueueImpl.this) {
            iter.repeat();
         }
      }

      @Override
      public boolean hasNext() {
         synchronized (QueueImpl.this) {
            return iter.hasNext();
         }
      }

      @Override
      public MessageReference next() {
         synchronized (QueueImpl.this) {
            return iter.next();
         }
      }

      @Override
      public void remove() {
         synchronized (QueueImpl.this) {
            iter.remove();
         }
      }
   }

   //Readonly (no remove) iterator over the messages in the queue, in order of
   //paging store, intermediateMessageReferences and MessageReferences
   private class QueueBrowserIterator implements LinkedListIterator<MessageReference> {

      LinkedListIterator<PagedReference> pagingIterator = null;
      LinkedListIterator<MessageReference> messagesIterator = null;

      private LinkedListIterator<PagedReference> getPagingIterator() {
         if (pagingIterator == null) {
            pagingIterator = pageSubscription.iterator(true);
         }
         return pagingIterator;
      }

      Iterator lastIterator = null;

      MessageReference cachedNext = null;
      HashSet<PagePosition> previouslyBrowsed = new HashSet();

      private QueueBrowserIterator() {
         messagesIterator = new SynchronizedIterator(messageReferences.iterator());
      }

      @Override
      public boolean hasNext() {
         if (messagesIterator != null && messagesIterator.hasNext()) {
            lastIterator = messagesIterator;
            return true;
         }
         if (getPagingIterator() != null) {
            if (getPagingIterator().hasNext()) {
               lastIterator = getPagingIterator();
               return true;
            }
         }

         return false;
      }

      @Override
      public MessageReference next() {

         if (cachedNext != null) {
            try {
               return cachedNext;
            } finally {
               cachedNext = null;
            }

         }
         while (true) {
            if (messagesIterator != null && messagesIterator.hasNext()) {
               MessageReference msg = messagesIterator.next();
               if (msg.isPaged()) {
                  previouslyBrowsed.add(((PagedReference) msg).getPosition());
               }
               return msg;
            } else {
               break;
            }
         }
         if (getPagingIterator() != null) {
            while (getPagingIterator().hasNext()) {
               lastIterator = getPagingIterator();
               PagedReference ref = getPagingIterator().next();
               if (previouslyBrowsed.contains(ref.getPosition())) {
                  continue;
               }
               return ref;
            }
         }

         throw new NoSuchElementException();
      }

      @Override
      public void remove() {
         if (lastIterator != null) {
            lastIterator.remove();
         }
      }

      @Override
      public void repeat() {
      }

      @Override
      public void close() {
         if (getPagingIterator() != null) {
            getPagingIterator().close();
         }
         if (messagesIterator != null) {
            messagesIterator.close();
         }
      }
   }

   private void incDelivering(MessageReference ref) {
      deliveringMetrics.incrementMetrics(ref);
   }

   public void decDelivering(final MessageReference reference) {
      deliveringMetrics.decrementMetrics(reference);
   }

   private long getPersistentSize(final MessageReference reference) {
      long size = 0;

      try {
         size = reference.getPersistentSize() > 0 ? reference.getPersistentSize() : 0;
      } catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.errorCalculatePersistentSize(e);
      }

      return size;
   }

   private void configureExpiry(final AddressSettings settings) {
      this.expiryAddress = settings == null ? null : settings.getExpiryAddress();
   }

   private void configureSlowConsumerReaper(final AddressSettings settings) {
      if (settings == null || settings.getSlowConsumerThreshold() == AddressSettings.DEFAULT_SLOW_CONSUMER_THRESHOLD) {
         if (slowConsumerReaperFuture != null) {
            slowConsumerReaperFuture.cancel(false);
            slowConsumerReaperFuture = null;
            slowConsumerReaperRunnable = null;
            if (logger.isDebugEnabled()) {
               logger.debug("Cancelled slow-consumer-reaper thread for queue \"" + getName() + "\"");
            }
         }
      } else {
         if (slowConsumerReaperRunnable == null) {
            scheduleSlowConsumerReaper(settings);
         } else if (slowConsumerReaperRunnable.checkPeriod != settings.getSlowConsumerCheckPeriod() || slowConsumerReaperRunnable.threshold != settings.getSlowConsumerThreshold() || !slowConsumerReaperRunnable.policy.equals(settings.getSlowConsumerPolicy())) {
            slowConsumerReaperFuture.cancel(false);
            scheduleSlowConsumerReaper(settings);
         }
      }
   }

   void scheduleSlowConsumerReaper(AddressSettings settings) {
      slowConsumerReaperRunnable = new SlowConsumerReaperRunnable(settings.getSlowConsumerCheckPeriod(), settings.getSlowConsumerThreshold(), settings.getSlowConsumerPolicy());

      slowConsumerReaperFuture = scheduledExecutor.scheduleWithFixedDelay(slowConsumerReaperRunnable, settings.getSlowConsumerCheckPeriod(), settings.getSlowConsumerCheckPeriod(), TimeUnit.SECONDS);

      if (logger.isDebugEnabled()) {
         logger.debug("Scheduled slow-consumer-reaper thread for queue \"" + getName() + "\"; slow-consumer-check-period=" + settings.getSlowConsumerCheckPeriod() + ", slow-consumer-threshold=" + settings.getSlowConsumerThreshold() + ", slow-consumer-policy=" + settings.getSlowConsumerPolicy());
      }
   }

   private class AddressSettingsRepositoryListener implements HierarchicalRepositoryChangeListener {

      @Override
      public void onChange() {
         AddressSettings settings = addressSettingsRepository.getMatch(address.toString());
         configureExpiry(settings);
         checkDeadLetterAddressAndExpiryAddress(settings);
         configureSlowConsumerReaper(settings);
      }
   }

   private void checkDeadLetterAddressAndExpiryAddress(final AddressSettings settings) {
      if (!Env.isTestEnv() && !internalQueue && !address.equals(server.getConfiguration().getManagementNotificationAddress())) {
         if (settings.getDeadLetterAddress() == null) {
            ActiveMQServerLogger.LOGGER.AddressSettingsNoDLA(name);
         }
         if (settings.getExpiryAddress() == null) {
            ActiveMQServerLogger.LOGGER.AddressSettingsNoExpiryAddress(name);
         }
      }
   }

   private final class SlowConsumerReaperRunnable implements Runnable {

      private final SlowConsumerPolicy policy;
      private final float threshold;
      private final long checkPeriod;

      private SlowConsumerReaperRunnable(long checkPeriod, float threshold, SlowConsumerPolicy policy) {
         this.checkPeriod = checkPeriod;
         this.policy = policy;
         this.threshold = threshold;
      }

      @Override
      public void run() {
         float queueRate = getRate();
         if (logger.isDebugEnabled()) {
            logger.debug(getAddress() + ":" + getName() + " has " + getConsumerCount() + " consumer(s) and is receiving messages at a rate of " + queueRate + " msgs/second.");
         }

         Set<Consumer> consumersSet = getConsumers();

         if (consumersSet.size() == 0) {
            logger.debug("There are no consumers, no need to check slow consumer's rate");
            return;
         } else if (queueRate < (threshold * consumersSet.size())) {
            if (logger.isDebugEnabled()) {
               logger.debug("Insufficient messages received on queue \"" + getName() + "\" to satisfy slow-consumer-threshold. Skipping inspection of consumer.");
            }
            return;
         }

         for (Consumer consumer : consumersSet) {
            if (consumer instanceof ServerConsumerImpl) {
               ServerConsumerImpl serverConsumer = (ServerConsumerImpl) consumer;
               float consumerRate = serverConsumer.getRate();
               if (queueRate < threshold) {
                  if (logger.isDebugEnabled()) {
                     logger.debug("Insufficient messages received on queue \"" + getName() + "\" to satisfy slow-consumer-threshold. Skipping inspection of consumer.");
                  }
               } else if (consumerRate < threshold) {
                  RemotingConnection connection = null;
                  ActiveMQServer server = ((PostOfficeImpl) postOffice).getServer();
                  RemotingService remotingService = server.getRemotingService();

                  for (RemotingConnection potentialConnection : remotingService.getConnections()) {
                     if (potentialConnection.getID().toString().equals(serverConsumer.getConnectionID())) {
                        connection = potentialConnection;
                     }
                  }

                  serverConsumer.fireSlowConsumer();

                  if (connection != null) {
                     ActiveMQServerLogger.LOGGER.slowConsumerDetected(serverConsumer.getSessionID(), serverConsumer.getID(), getName().toString(), connection.getRemoteAddress(), threshold, consumerRate);
                     if (policy.equals(SlowConsumerPolicy.KILL)) {
                        connection.killMessage(server.getNodeID());
                        remotingService.removeConnection(connection.getID());
                        connection.fail(ActiveMQMessageBundle.BUNDLE.connectionsClosedByManagement(connection.getRemoteAddress()));
                     } else if (policy.equals(SlowConsumerPolicy.NOTIFY)) {
                        TypedProperties props = new TypedProperties();

                        props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT, getConsumerCount());

                        props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, address);

                        props.putSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS, SimpleString.toSimpleString(connection.getRemoteAddress()));

                        if (connection.getID() != null) {
                           props.putSimpleStringProperty(ManagementHelper.HDR_CONNECTION_NAME, SimpleString.toSimpleString(connection.getID().toString()));
                        }

                        props.putLongProperty(ManagementHelper.HDR_CONSUMER_NAME, serverConsumer.getID());

                        props.putSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME, SimpleString.toSimpleString(serverConsumer.getSessionID()));

                        Notification notification = new Notification(null, CoreNotificationType.CONSUMER_SLOW, props);

                        ManagementService managementService = ((PostOfficeImpl) postOffice).getServer().getManagementService();
                        try {
                           managementService.sendNotification(notification);
                        } catch (Exception e) {
                           ActiveMQServerLogger.LOGGER.failedToSendSlowConsumerNotification(notification, e);
                        }
                     }
                  }
               }
            }
         }
      }
   }
}

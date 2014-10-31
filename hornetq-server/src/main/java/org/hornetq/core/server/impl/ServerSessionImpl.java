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
/*
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

package org.hornetq.core.server.impl;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQNonExistentQueueException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.exception.HornetQXAException;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.SecurityStore;
import org.hornetq.core.server.BindingQueryResult;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueQueryResult;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerConsumer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.Transaction.State;
import org.hornetq.core.transaction.TransactionOperationAbstract;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUID;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

import static org.hornetq.api.core.management.NotificationType.CONSUMER_CREATED;

/**
 * Server side Session implementation
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 */
public class ServerSessionImpl implements ServerSession, FailureListener
{
   // Constants -----------------------------------------------------------------------------

   private static final boolean isTrace = HornetQServerLogger.LOGGER.isTraceEnabled();

   // Static -------------------------------------------------------------------------------

   // Attributes ----------------------------------------------------------------------------

   private final String username;

   private final String password;

   private final int minLargeMessageSize;

   private final boolean autoCommitSends;

   private final boolean autoCommitAcks;

   private final boolean preAcknowledge;

   private final boolean strictUpdateDeliveryCount;

   private final RemotingConnection remotingConnection;

   private final Map<Long, ServerConsumer> consumers = new ConcurrentHashMap<Long, ServerConsumer>();

   private Transaction tx;

   private final boolean xa;

   private final StorageManager storageManager;

   private final ResourceManager resourceManager;

   public final PostOffice postOffice;

   private final SecurityStore securityStore;

   private final ManagementService managementService;

   private volatile boolean started = false;

   private final Map<SimpleString, TempQueueCleanerUpper> tempQueueCleannerUppers = new HashMap<SimpleString, TempQueueCleanerUpper>();

   private final String name;

   private final HornetQServer server;

   private final SimpleString managementAddress;

   // The current currentLargeMessage being processed
   private volatile LargeServerMessage currentLargeMessage;

   private final RoutingContext routingContext = new RoutingContextImpl(null);

   private final SessionCallback callback;

   private volatile SimpleString defaultAddress;

   private volatile int timeoutSeconds;

   private Map<String, String> metaData;

   private final OperationContext context;

   // Session's usage should be by definition single threaded, hence it's not needed to use a concurrentHashMap here
   private final Map<SimpleString, Pair<UUID, AtomicLong>> targetAddressInfos = new HashMap<SimpleString, Pair<UUID, AtomicLong>>();

   private final long creationTime = System.currentTimeMillis();

   // to prevent session from being closed twice.
   // this can happen when a session close from client just
   // arrives while the connection failure is detected at the
   // server. Both the request and failure listener will
   // try to close one session from different threads
   // concurrently.
   private volatile boolean closed = false;

   // Constructors ---------------------------------------------------------------------------------

   public ServerSessionImpl(final String name,
                            final String username,
                            final String password,
                            final int minLargeMessageSize,
                            final boolean autoCommitSends,
                            final boolean autoCommitAcks,
                            final boolean preAcknowledge,
                            final boolean strictUpdateDeliveryCount,
                            final boolean xa,
                            final RemotingConnection remotingConnection,
                            final StorageManager storageManager,
                            final PostOffice postOffice,
                            final ResourceManager resourceManager,
                            final SecurityStore securityStore,
                            final ManagementService managementService,
                            final HornetQServer server,
                            final SimpleString managementAddress,
                            final SimpleString defaultAddress,
                            final SessionCallback callback,
                            final OperationContext context) throws Exception
   {
      this.username = username;

      this.password = password;

      this.minLargeMessageSize = minLargeMessageSize;

      this.autoCommitSends = autoCommitSends;

      this.autoCommitAcks = autoCommitAcks;

      this.preAcknowledge = preAcknowledge;

      this.remotingConnection = remotingConnection;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.resourceManager = resourceManager;

      this.securityStore = securityStore;

      timeoutSeconds = resourceManager.getTimeoutSeconds();
      this.xa = xa;

      this.strictUpdateDeliveryCount = strictUpdateDeliveryCount;

      this.managementService = managementService;

      this.name = name;

      this.server = server;

      this.managementAddress = managementAddress;

      this.callback = callback;

      this.defaultAddress = defaultAddress;

      remotingConnection.addFailureListener(this);
      this.context = context;
      if (!xa)
      {
         tx = newTransaction();
      }
   }

   // ServerSession implementation ----------------------------------------------------------------------------

   /**
    * @return the sessionContext
    */
   public OperationContext getSessionContext()
   {
      return context;
   }

   public String getUsername()
   {
      return username;
   }

   public String getPassword()
   {
      return password;
   }

   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   public String getName()
   {
      return name;
   }

   public Object getConnectionID()
   {
      return remotingConnection.getID();
   }

   public Set<ServerConsumer> getServerConsumers()
   {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());
      return Collections.unmodifiableSet(consumersClone);
   }

   public void removeConsumer(final long consumerID) throws Exception
   {
      if (consumers.remove(consumerID) == null)
      {
         throw new IllegalStateException("Cannot find consumer with id " + consumerID + " to remove");
      }
   }

   private void doClose(final boolean failed) throws Exception
   {
      synchronized (this)
      {
         if (closed) return;

         if (tx != null && tx.getXid() == null)
         {
            // We only rollback local txs on close, not XA tx branches

            try
            {
               rollback(failed, false);
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
            }
         }

         server.removeSession(name);

         remotingConnection.removeFailureListener(this);

         callback.closed();

         closed = true;
      }

      //putting closing of consumers outside the sync block
      //https://issues.jboss.org/browse/HORNETQ-1141
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone)
      {
         consumer.close(failed);
      }

      consumers.clear();

      if (currentLargeMessage != null)
      {
         try
         {
            currentLargeMessage.deleteFile();
         }
         catch (Throwable error)
         {
            HornetQServerLogger.LOGGER.errorDeletingLargeMessageFile(error);
         }
      }
   }

   public void createConsumer(final long consumerID,
                              final SimpleString queueName,
                              final SimpleString filterString,
                              final boolean browseOnly) throws Exception
   {
      this.createConsumer(consumerID, queueName, filterString, browseOnly, true, null);
   }

   public void createConsumer(final long consumerID,
                              final SimpleString queueName,
                              final SimpleString filterString,
                              final boolean browseOnly,
                              final boolean supportLargeMessage,
                              final Integer credits) throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null || binding.getType() != BindingType.LOCAL_QUEUE)
      {
         throw HornetQMessageBundle.BUNDLE.noSuchQueue(queueName);
      }

      securityStore.check(binding.getAddress(), CheckType.CONSUME, this);

      Filter filter = FilterImpl.createFilter(filterString);

      ServerConsumer consumer = new ServerConsumerImpl(consumerID,
                                                       this,
                                                       (QueueBinding) binding,
                                                       filter,
                                                       started,
                                                       browseOnly,
                                                       storageManager,
                                                       callback,
                                                       preAcknowledge,
                                                       strictUpdateDeliveryCount,
                                                       managementService,
                                                       supportLargeMessage,
                                                       credits);
      consumers.put(consumer.getID(), consumer);

      if (!browseOnly)
      {
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

         props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

         props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

         props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

         Queue theQueue = (Queue) binding.getBindable();

         props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT, theQueue.getConsumerCount());

         // HORNETQ-946
         props.putSimpleStringProperty(ManagementHelper.HDR_USER, SimpleString.toSimpleString(username));

         props.putSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS, SimpleString.toSimpleString(this.remotingConnection.getRemoteAddress()));

         props.putSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME, SimpleString.toSimpleString(name));

         if (filterString != null)
         {
            props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, filterString);
         }

         Notification notification = new Notification(null, CONSUMER_CREATED, props);

         if (HornetQServerLogger.LOGGER.isDebugEnabled())
         {
            HornetQServerLogger.LOGGER.debug("Session with user=" + username +
                                                ", connection=" + this.remotingConnection +
                                                " created a consumer on queue " + queueName +
                                                ", filter = " + filterString);
         }

         managementService.sendNotification(notification);
      }
   }

   public void createQueue(final SimpleString address,
                           final SimpleString name,
                           final SimpleString filterString,
                           final boolean temporary,
                           final boolean durable) throws Exception
   {
      if (durable)
      {
         // make sure the user has privileges to create this queue
         securityStore.check(address, CheckType.CREATE_DURABLE_QUEUE, this);
      }
      else
      {
         securityStore.check(address, CheckType.CREATE_NON_DURABLE_QUEUE, this);
      }

      Queue queue = server.createQueue(address, name, filterString, durable, temporary);

      if (temporary)
      {
         // Temporary queue in core simply means the queue will be deleted if
         // the remoting connection
         // dies. It does not mean it will get deleted automatically when the
         // session is closed.
         // It is up to the user to delete the queue when finished with it

         TempQueueCleanerUpper cleaner = new TempQueueCleanerUpper(server, name);

         remotingConnection.addCloseListener(cleaner);
         remotingConnection.addFailureListener(cleaner);

         tempQueueCleannerUppers.put(name, cleaner);
      }

      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug("Queue " + name + " created on address " + name +
                                             " with filter=" + filterString + " temporary = " +
                                             temporary + " durable=" + durable + " on session user=" + this.username + ", connection=" + this.remotingConnection);
      }

   }

   @Override
   public void createSharedQueue(final SimpleString address,
                                 final SimpleString name,
                                 boolean durable,
                                 final SimpleString filterString) throws Exception
   {
      securityStore.check(address, CheckType.CREATE_NON_DURABLE_QUEUE, this);

      server.createSharedQueue(address, name, filterString, durable);
   }

   public RemotingConnection getRemotingConnection()
   {
      return remotingConnection;
   }

   private static class TempQueueCleanerUpper implements CloseListener, FailureListener
   {
      private final SimpleString bindingName;

      private final HornetQServer server;

      TempQueueCleanerUpper(final HornetQServer server, final SimpleString bindingName)
      {
         this.server = server;

         this.bindingName = bindingName;
      }

      private void run()
      {
         try
         {
            if (HornetQServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQServerLogger.LOGGER.debug("deleting temporary queue " + bindingName);
            }
            try
            {
               server.destroyQueue(bindingName, null, false);
            }
            catch (HornetQException e)
            {
               // that's fine.. it can happen due to queue already been deleted
               HornetQServerLogger.LOGGER.debug(e.getMessage(), e);
            }
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorRemovingTempQueue(e, bindingName);
         }
      }

      public void connectionFailed(HornetQException exception, boolean failedOver)
      {
         run();
      }

      @Override
      public void connectionFailed(final HornetQException me, boolean failedOver, String scaleDownTargetNodeID)
      {
         connectionFailed(me, failedOver);
      }

      public void connectionClosed()
      {
         run();
      }

      @Override
      public String toString()
      {
         return "Temporary Cleaner for queue " + bindingName;
      }

   }

   public void deleteQueue(final SimpleString queueToDelete) throws Exception
   {
      Binding binding = postOffice.getBinding(queueToDelete);

      if (binding == null || binding.getType() != BindingType.LOCAL_QUEUE)
      {
         throw new HornetQNonExistentQueueException();
      }

      server.destroyQueue(queueToDelete, this, true);

      TempQueueCleanerUpper cleaner = this.tempQueueCleannerUppers.remove(queueToDelete);

      if (cleaner != null)
      {
         remotingConnection.removeCloseListener(cleaner);

         remotingConnection.removeFailureListener(cleaner);
      }
   }

   public QueueQueryResult executeQueueQuery(final SimpleString name) throws Exception
   {
      if (name == null)
      {
         throw HornetQMessageBundle.BUNDLE.queueNameIsNull();
      }

      QueueQueryResult response;

      Binding binding = postOffice.getBinding(name);

      if (binding != null && binding.getType() == BindingType.LOCAL_QUEUE)
      {
         Queue queue = (Queue) binding.getBindable();

         Filter filter = queue.getFilter();

         SimpleString filterString = filter == null ? null : filter.getFilterString();

         response = new QueueQueryResult(name,
                                         binding.getAddress(),
                                         queue.isDurable(),
                                         queue.isTemporary(),
                                         filterString,
                                         queue.getConsumerCount(),
                                         queue.getMessageCount(QueueImpl.DELIVERY_TIMEOUT));
      }
      // make an exception for the management address (see HORNETQ-29)
      else if (name.equals(managementAddress))
      {
         response = new QueueQueryResult(name, managementAddress, true, false, null, -1, -1);
      }
      else
      {
         response = new QueueQueryResult();
      }

      return response;
   }

   public BindingQueryResult executeBindingQuery(final SimpleString address) throws Exception
   {
      if (address == null)
      {
         throw HornetQMessageBundle.BUNDLE.addressIsNull();
      }

      List<SimpleString> names = new ArrayList<SimpleString>();

      // make an exception for the management address (see HORNETQ-29)
      if (address.equals(managementAddress))
      {
         return new BindingQueryResult(true, names);
      }

      Bindings bindings = postOffice.getMatchingBindings(address);

      for (Binding binding : bindings.getBindings())
      {
         if (binding.getType() == BindingType.LOCAL_QUEUE || binding.getType() == BindingType.REMOTE_QUEUE)
         {
            names.add(binding.getUniqueName());
         }
      }

      return new BindingQueryResult(!names.isEmpty(), names);
   }

   public void forceConsumerDelivery(final long consumerID, final long sequence) throws Exception
   {
      ServerConsumer consumer = consumers.get(consumerID);

      // this would be possible if the server consumer was closed by pings/pongs.. etc
      if (consumer != null)
      {
         consumer.forceDelivery(sequence);
      }
   }

   public void acknowledge(final long consumerID, final long messageID) throws Exception
   {
      ServerConsumer consumer = consumers.get(consumerID);

      if (consumer == null)
      {
         throw HornetQMessageBundle.BUNDLE.consumerDoesntExist(consumerID);
      }

      if (tx != null && tx.getState() == State.ROLLEDBACK)
      {
         // JBPAPP-8845 - if we let stuff to be acked on a rolled back TX, we will just
         // have these messages to be stuck on the limbo until the server is restarted
         // The tx has already timed out, so we need to ack and rollback immediately
         Transaction newTX = newTransaction();
         consumer.acknowledge(autoCommitAcks, newTX, messageID);
         newTX.rollback();
      }
      else
      {
         consumer.acknowledge(autoCommitAcks, tx, messageID);
      }
   }

   public void individualAcknowledge(final long consumerID, final long messageID) throws Exception
   {
      ServerConsumer consumer = consumers.get(consumerID);

      if (this.xa && tx == null)
      {
         throw new HornetQXAException(XAException.XAER_PROTO, "Invalid transaction state");
      }

      if (tx != null && tx.getState() == State.ROLLEDBACK)
      {
         // JBPAPP-8845 - if we let stuff to be acked on a rolled back TX, we will just
         // have these messages to be stuck on the limbo until the server is restarted
         // The tx has already timed out, so we need to ack and rollback immediately
         Transaction newTX = newTransaction();
         consumer.individualAcknowledge(autoCommitAcks, tx, messageID);
         newTX.rollback();
      }
      else
      {
         consumer.individualAcknowledge(autoCommitAcks, tx, messageID);
      }

   }

   public void individualCancel(final long consumerID, final long messageID, boolean failed) throws Exception
   {
      ServerConsumer consumer = consumers.get(consumerID);

      if (consumer != null)
      {
         consumer.individualCancel(messageID, failed);
      }

   }


   public void expire(final long consumerID, final long messageID) throws Exception
   {
      MessageReference ref = consumers.get(consumerID).removeReferenceByID(messageID);

      if (ref != null)
      {
         ref.getQueue().expire(ref);
      }
   }

   public synchronized void commit() throws Exception
   {
      if (isTrace)
      {
         HornetQServerLogger.LOGGER.trace("Calling commit");
      }
      try
      {
         tx.commit();
      }
      finally
      {
         tx = newTransaction();
      }
   }

   public void rollback(final boolean considerLastMessageAsDelivered) throws Exception
   {
      rollback(false, considerLastMessageAsDelivered);
   }

   /**
    * @param clientFailed                   If the client has failed, we can't decrease the delivery-counts, and the close may issue a rollback
    * @param considerLastMessageAsDelivered
    * @throws Exception
    */
   private synchronized void rollback(final boolean clientFailed, final boolean considerLastMessageAsDelivered) throws Exception
   {
      if (tx == null)
      {
         // Might be null if XA

         tx = newTransaction();
      }

      doRollback(clientFailed, considerLastMessageAsDelivered, tx);

      if (xa)
      {
         tx = null;
      }
      else
      {
         tx = newTransaction();
      }
   }

   /**
    * @return
    */
   private TransactionImpl newTransaction()
   {
      return new TransactionImpl(storageManager, timeoutSeconds);
   }

   /**
    * @param xid
    * @return
    */
   private TransactionImpl newTransaction(final Xid xid)
   {
      return new TransactionImpl(xid, storageManager, timeoutSeconds);
   }

   public synchronized void xaCommit(final Xid xid, final boolean onePhase) throws Exception
   {

      if (tx != null && tx.getXid().equals(xid))
      {
         final String msg = "Cannot commit, session is currently doing work in transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         Transaction theTx = resourceManager.removeTransaction(xid);


         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace("XAcommit into " + theTx + ", xid=" + xid);
         }

         if (theTx == null)
         {
            // checked heuristic committed transactions
            if (resourceManager.getHeuristicCommittedTransactions().contains(xid))
            {
               throw new HornetQXAException(XAException.XA_HEURCOM,
                                            "transaction has been heuristically committed: " + xid);
            }
            // checked heuristic rolled back transactions
            else if (resourceManager.getHeuristicRolledbackTransactions().contains(xid))
            {
               throw new HornetQXAException(XAException.XA_HEURRB,
                                            "transaction has been heuristically rolled back: " + xid);
            }
            else
            {
               if (isTrace)
               {
                  HornetQServerLogger.LOGGER.trace("XAcommit into " + theTx + ", xid=" + xid + " cannot find it");
               }

               throw new HornetQXAException(XAException.XAER_NOTA, "Cannot find xid in resource manager: " + xid);
            }
         }
         else
         {
            if (theTx.getState() == Transaction.State.SUSPENDED)
            {
               // Put it back
               resourceManager.putTransaction(xid, theTx);

               throw new HornetQXAException(XAException.XAER_PROTO, "Cannot commit transaction, it is suspended " + xid);
            }
            else
            {
               theTx.commit(onePhase);
            }
         }
      }
   }

   public synchronized void xaEnd(final Xid xid) throws Exception
   {
      if (tx != null && tx.getXid().equals(xid))
      {
         if (tx.getState() == Transaction.State.SUSPENDED)
         {
            final String msg = "Cannot end, transaction is suspended";

            throw new HornetQXAException(XAException.XAER_PROTO, msg);
         }
         else if (tx.getState() == Transaction.State.ROLLEDBACK)
         {
            final String msg = "Cannot end, transaction is rolled back";

            tx = null;

            throw new HornetQXAException(XAException.XAER_PROTO, msg);
         }
         else
         {
            tx = null;
         }
      }
      else
      {
         // It's also legal for the TM to call end for a Xid in the suspended
         // state
         // See JTA 1.1 spec 3.4.4 - state diagram
         // Although in practice TMs rarely do this.
         Transaction theTx = resourceManager.getTransaction(xid);

         if (theTx == null)
         {
            final String msg = "Cannot find suspended transaction to end " + xid;

            throw new HornetQXAException(XAException.XAER_NOTA, msg);
         }
         else
         {
            if (theTx.getState() != Transaction.State.SUSPENDED)
            {
               final String msg = "Transaction is not suspended " + xid;

               throw new HornetQXAException(XAException.XAER_PROTO, msg);
            }
            else
            {
               theTx.resume();
            }
         }
      }
   }

   public synchronized void xaForget(final Xid xid) throws Exception
   {
      long id = resourceManager.removeHeuristicCompletion(xid);

      if (id != -1)
      {
         try
         {
            storageManager.deleteHeuristicCompletion(id);
         }
         catch (Exception e)
         {
            e.printStackTrace();

            throw new HornetQXAException(XAException.XAER_RMERR);
         }
      }
      else
      {
         throw new HornetQXAException(XAException.XAER_NOTA);
      }
   }

   public synchronized void xaJoin(final Xid xid) throws Exception
   {
      Transaction theTx = resourceManager.getTransaction(xid);

      if (theTx == null)
      {
         final String msg = "Cannot find xid in resource manager: " + xid;

         throw new HornetQXAException(XAException.XAER_NOTA, msg);
      }
      else
      {
         if (theTx.getState() == Transaction.State.SUSPENDED)
         {
            throw new HornetQXAException(XAException.XAER_PROTO, "Cannot join tx, it is suspended " + xid);
         }
         else
         {
            tx = theTx;
         }
      }
   }

   public synchronized void xaResume(final Xid xid) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot resume, session is currently doing work in a transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         Transaction theTx = resourceManager.getTransaction(xid);

         if (theTx == null)
         {
            final String msg = "Cannot find xid in resource manager: " + xid;

            throw new HornetQXAException(XAException.XAER_NOTA, msg);
         }
         else
         {
            if (theTx.getState() != Transaction.State.SUSPENDED)
            {
               throw new HornetQXAException(XAException.XAER_PROTO,
                                            "Cannot resume transaction, it is not suspended " + xid);
            }
            else
            {
               tx = theTx;

               tx.resume();
            }
         }
      }
   }

   public synchronized void xaRollback(final Xid xid) throws Exception
   {
      if (tx != null && tx.getXid().equals(xid))
      {
         final String msg = "Cannot roll back, session is currently doing work in a transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         Transaction theTx = resourceManager.removeTransaction(xid);
         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace("xarollback into " + theTx);
         }

         if (theTx == null)
         {
            // checked heuristic committed transactions
            if (resourceManager.getHeuristicCommittedTransactions().contains(xid))
            {
               throw new HornetQXAException(XAException.XA_HEURCOM,
                                            "transaction has ben heuristically committed: " + xid);
            }
            // checked heuristic rolled back transactions
            else if (resourceManager.getHeuristicRolledbackTransactions().contains(xid))
            {
               throw new HornetQXAException(XAException.XA_HEURRB,
                                            "transaction has ben heuristically rolled back: " + xid);
            }
            else
            {
               if (isTrace)
               {
                  HornetQServerLogger.LOGGER.trace("xarollback into " + theTx + ", xid=" + xid + " forcing a rollback regular");
               }

               try
               {
                  // jbpapp-8845
                  // This could have happened because the TX timed out,
                  // at this point we would be better on rolling back this session as a way to prevent consumers from holding their messages
                  this.rollback(false);
               }
               catch (Exception e)
               {
                  HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
               }

               throw new HornetQXAException(XAException.XAER_NOTA, "Cannot find xid in resource manager: " + xid);
            }
         }
         else
         {
            if (theTx.getState() == Transaction.State.SUSPENDED)
            {
               if (isTrace)
               {
                  HornetQServerLogger.LOGGER.trace("xarollback into " + theTx + " sending tx back as it was suspended");
               }


               // Put it back
               resourceManager.putTransaction(xid, tx);

               throw new HornetQXAException(XAException.XAER_PROTO,
                                            "Cannot rollback transaction, it is suspended " + xid);
            }
            else
            {
               doRollback(false, false, theTx);
            }
         }
      }
   }

   public synchronized void xaStart(final Xid xid) throws Exception
   {
      if (tx != null)
      {
         HornetQServerLogger.LOGGER.xidReplacedOnXStart(tx.getXid().toString(), xid.toString());

         try
         {
            if (tx.getState() != Transaction.State.PREPARED)
            {
               // we don't want to rollback anything prepared here
               if (tx.getXid() != null)
               {
                  resourceManager.removeTransaction(tx.getXid());
               }
               tx.rollback();
            }
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.debug("An exception happened while we tried to debug the previous tx, we can ignore this exception", e);
         }
      }

      tx = newTransaction(xid);

      if (isTrace)
      {
         HornetQServerLogger.LOGGER.trace("xastart into tx= " + tx);
      }

      boolean added = resourceManager.putTransaction(xid, tx);

      if (!added)
      {
         final String msg = "Cannot start, there is already a xid " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_DUPID, msg);
      }
   }

   public synchronized void xaFailed(final Xid xid) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot start, session is already doing work in a transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {

         tx = newTransaction(xid);
         tx.markAsRollbackOnly(new HornetQException("Can't commit as a Failover happened during the operation"));

         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace("xastart into tx= " + tx);
         }

         boolean added = resourceManager.putTransaction(xid, tx);

         if (!added)
         {
            final String msg = "Cannot start, there is already a xid " + tx.getXid();

            throw new HornetQXAException(XAException.XAER_DUPID, msg);
         }
      }
   }

   public synchronized void xaSuspend() throws Exception
   {

      if (isTrace)
      {
         HornetQServerLogger.LOGGER.trace("xasuspend on " + this.tx);
      }

      if (tx == null)
      {
         final String msg = "Cannot suspend, session is not doing work in a transaction ";

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         if (tx.getState() == Transaction.State.SUSPENDED)
         {
            final String msg = "Cannot suspend, transaction is already suspended " + tx.getXid();

            throw new HornetQXAException(XAException.XAER_PROTO, msg);
         }
         else
         {
            tx.suspend();

            tx = null;
         }
      }
   }

   public synchronized void xaPrepare(final Xid xid) throws Exception
   {
      if (tx != null && tx.getXid().equals(xid))
      {
         final String msg = "Cannot commit, session is currently doing work in a transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         Transaction theTx = resourceManager.getTransaction(xid);

         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace("xaprepare into " + ", xid=" + xid + ", tx= " + tx);
         }

         if (theTx == null)
         {
            final String msg = "Cannot find xid in resource manager: " + xid;

            throw new HornetQXAException(XAException.XAER_NOTA, msg);
         }
         else
         {
            if (theTx.getState() == Transaction.State.SUSPENDED)
            {
               throw new HornetQXAException(XAException.XAER_PROTO,
                                            "Cannot prepare transaction, it is suspended " + xid);
            }
            else if (theTx.getState() == Transaction.State.PREPARED)
            {
               HornetQServerLogger.LOGGER.info("ignoring prepare on xid as already called :" + xid);
            }
            else
            {
               theTx.prepare();
            }
         }
      }
   }

   public List<Xid> xaGetInDoubtXids()
   {
      List<Xid> xids = new ArrayList<Xid>();

      xids.addAll(resourceManager.getPreparedTransactions());
      xids.addAll(resourceManager.getHeuristicCommittedTransactions());
      xids.addAll(resourceManager.getHeuristicRolledbackTransactions());

      return xids;
   }

   public int xaGetTimeout()
   {
      return resourceManager.getTimeoutSeconds();
   }

   public void xaSetTimeout(final int timeout)
   {
      timeoutSeconds = timeout;
      if (tx != null)
      {
         tx.setTimeout(timeout);
      }
   }

   public void start()
   {
      setStarted(true);
   }

   public void stop()
   {
      setStarted(false);
   }

   public void waitContextCompletion()
   {
      try
      {
         if (!context.waitCompletion(10000))
         {
            HornetQServerLogger.LOGGER.errorCompletingContext(new Exception("warning"));
         }
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
      }
   }

   public void close(final boolean failed)
   {
      if (closed) return;
      context.executeOnCompletion(new IOAsyncTask()
      {
         public void onError(int errorCode, String errorMessage)
         {
         }

         public void done()
         {
            try
            {
               doClose(failed);
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.errorClosingSession(e);
            }
         }
      });
   }

   public void closeConsumer(final long consumerID) throws Exception
   {
      final ServerConsumer consumer = consumers.get(consumerID);

      if (consumer != null)
      {
         consumer.close(false);
      }
      else
      {
         HornetQServerLogger.LOGGER.cannotFindConsumer(consumerID);
      }
   }

   public void receiveConsumerCredits(final long consumerID, final int credits) throws Exception
   {
      ServerConsumer consumer = consumers.get(consumerID);

      if (consumer == null)
      {
         HornetQServerLogger.LOGGER.debug("There is no consumer with id " + consumerID);

         return;
      }

      consumer.receiveCredits(credits);
   }

   @Override
   public Transaction getCurrentTransaction()
   {
      return tx;
   }

   public void sendLarge(final MessageInternal message) throws Exception
   {
      // need to create the LargeMessage before continue
      long id = storageManager.generateUniqueID();

      LargeServerMessage largeMsg = storageManager.createLargeMessage(id, message);

      if (HornetQServerLogger.LOGGER.isTraceEnabled())
      {
         HornetQServerLogger.LOGGER.trace("sendLarge::" + largeMsg);
      }

      if (currentLargeMessage != null)
      {
         HornetQServerLogger.LOGGER.replacingIncompleteLargeMessage(currentLargeMessage.getMessageID());
      }

      currentLargeMessage = largeMsg;
   }

   public void send(final ServerMessage message, final boolean direct) throws Exception
   {
      //large message may come from StompSession directly, in which
      //case the id header already generated.
      if (!message.isLargeMessage())
      {
         long id = storageManager.generateUniqueID();

         message.setMessageID(id);
         message.encodeMessageIDToBuffer();
      }

      SimpleString address = message.getAddress();

      if (defaultAddress == null && address != null)
      {
         defaultAddress = address;
      }

      if (address == null)
      {
         if (message.isDurable())
         {
            // We need to force a re-encode when the message gets persisted or when it gets reloaded
            // it will have no address
            message.setAddress(defaultAddress);
         }
         else
         {
            // We don't want to force a re-encode when the message gets sent to the consumer
            message.setAddressTransient(defaultAddress);
         }
      }

      if (isTrace)
      {
         HornetQServerLogger.LOGGER.trace("send(message=" + message + ", direct=" + direct + ") being called");
      }

      if (message.getAddress() == null)
      {
         // This could happen with some tests that are ignoring messages
         throw HornetQMessageBundle.BUNDLE.noAddress();
      }

      if (message.getAddress().equals(managementAddress))
      {
         // It's a management message

         handleManagementMessage(message, direct);
      }
      else
      {
         doSend(message, direct);
      }
   }

   public void sendContinuations(final int packetSize,
                                 final long messageBodySize,
                                 final byte[] body,
                                 final boolean continues) throws Exception
   {
      if (currentLargeMessage == null)
      {
         throw HornetQMessageBundle.BUNDLE.largeMessageNotInitialised();
      }

      // Immediately release the credits for the continuations- these don't contribute to the in-memory size
      // of the message

      currentLargeMessage.addBytes(body);

      if (!continues)
      {
         currentLargeMessage.releaseResources();

         if (messageBodySize >= 0)
         {
            currentLargeMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, messageBodySize);
         }

         doSend(currentLargeMessage, false);

         currentLargeMessage = null;
      }
   }

   public void requestProducerCredits(final SimpleString address, final int credits) throws Exception
   {
      PagingStore store = server.getPagingManager().getPageStore(address);

      if (!store.checkMemory(new Runnable()
      {
         public void run()
         {
            callback.sendProducerCreditsMessage(credits, address);
         }
      }))
      {
         callback.sendProducerCreditsFailMessage(credits, address);
      }
   }

   public void setTransferring(final boolean transferring)
   {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone)
      {
         consumer.setTransferring(transferring);
      }
   }

   public void addMetaData(String key, String data)
   {
      if (metaData == null)
      {
         metaData = new HashMap<String, String>();
      }
      metaData.put(key, data);
   }


   public boolean addUniqueMetaData(String key, String data)
   {
      ServerSession sessionWithMetaData = server.lookupSession(key, data);
      if (sessionWithMetaData != null && sessionWithMetaData != this)
      {
         // There is a duplication of this property
         return false;
      }
      else
      {
         addMetaData(key, data);
         return true;
      }
   }

   public String getMetaData(String key)
   {
      String data = null;
      if (metaData != null)
      {
         data = metaData.get(key);
      }
      return data;
   }

   public String[] getTargetAddresses()
   {
      Map<SimpleString, Pair<UUID, AtomicLong>> copy = cloneTargetAddresses();
      Iterator<SimpleString> iter = copy.keySet().iterator();
      int num = copy.keySet().size();
      String[] addresses = new String[num];
      int i = 0;
      while (iter.hasNext())
      {
         addresses[i] = iter.next().toString();
         i++;
      }
      return addresses;
   }

   public String getLastSentMessageID(String address)
   {
      Pair<UUID, AtomicLong> value = targetAddressInfos.get(SimpleString.toSimpleString(address));
      if (value != null)
      {
         return value.getA().toString();
      }
      else
      {
         return null;
      }
   }

   public long getCreationTime()
   {
      return this.creationTime;
   }

   public StorageManager getStorageManager()
   {
      return this.storageManager;
   }

   @Override
   public void describeProducersInfo(JSONArray array) throws Exception
   {
      Map<SimpleString, Pair<UUID, AtomicLong>> targetCopy = cloneTargetAddresses();

      for (Map.Entry<SimpleString, Pair<UUID, AtomicLong>> entry : targetCopy.entrySet())
      {
         JSONObject producerInfo = new JSONObject();
         producerInfo.put("connectionID", this.getConnectionID().toString());
         producerInfo.put("sessionID", this.getName());
         producerInfo.put("destination", entry.getKey().toString());
         producerInfo.put("lastUUIDSent", entry.getValue().getA());
         producerInfo.put("msgSent", entry.getValue().getB().longValue());
         array.put(producerInfo);
      }
   }

   @Override
   public String toString()
   {
      StringBuffer buffer = new StringBuffer();
      if (this.metaData != null)
      {
         for (Map.Entry<String, String> value : metaData.entrySet())
         {
            if (buffer.length() != 0)
            {
               buffer.append(",");
            }
            Object tmpValue = value.getValue();
            if (tmpValue == null || tmpValue.toString().isEmpty())
            {
               buffer.append(value.getKey() + "=*N/A*");
            }
            else
            {
               buffer.append(value.getKey() + "=" + tmpValue);
            }
         }
      }
      // This will actually appear on some management operations
      // so please don't clog this with debug objects
      // unless you provide a special way for management to translate sessions
      return "ServerSessionImpl(" + buffer.toString() + ")";
   }

   // FailureListener implementation
   // --------------------------------------------------------------------

   @Override
   public void connectionFailed(final HornetQException me, boolean failedOver)
   {
      try
      {
         HornetQServerLogger.LOGGER.clientConnectionFailed(name);

         close(true);

         HornetQServerLogger.LOGGER.clientConnectionFailedClearingSession(name);
      }
      catch (Throwable t)
      {
         HornetQServerLogger.LOGGER.errorClosingConnection(this);
      }
   }

   @Override
   public void connectionFailed(final HornetQException me, boolean failedOver, String scaleDownTargetNodeID)
   {
      connectionFailed(me, failedOver);
   }

   // Public
   // ----------------------------------------------------------------------------

   public void clearLargeMessage()
   {
      currentLargeMessage = null;
   }

   // Private
   // ----------------------------------------------------------------------------

   private Map<SimpleString, Pair<UUID, AtomicLong>> cloneTargetAddresses()
   {
      return new HashMap<SimpleString, Pair<UUID, AtomicLong>>(targetAddressInfos);
   }

   private void setStarted(final boolean s)
   {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone)
      {
         consumer.setStarted(s);
      }

      started = s;
   }

   private void handleManagementMessage(final ServerMessage message, final boolean direct) throws Exception
   {
      try
      {
         securityStore.check(message.getAddress(), CheckType.MANAGE, this);
      }
      catch (HornetQException e)
      {
         if (!autoCommitSends)
         {
            tx.markAsRollbackOnly(e);
         }
         throw e;
      }

      ServerMessage reply = managementService.handleMessage(message);

      SimpleString replyTo = message.getSimpleStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME);

      if (replyTo != null)
      {
         reply.setAddress(replyTo);

         doSend(reply, direct);
      }
   }

   private void doRollback(final boolean clientFailed, final boolean lastMessageAsDelived, final Transaction theTx) throws Exception
   {
      boolean wasStarted = started;

      List<MessageReference> toCancel = new ArrayList<MessageReference>();

      for (ServerConsumer consumer : consumers.values())
      {
         if (wasStarted)
         {
            consumer.setStarted(false);
         }

         toCancel.addAll(consumer.cancelRefs(clientFailed, lastMessageAsDelived, theTx));
      }

      for (MessageReference ref : toCancel)
      {
         ref.getQueue().cancel(theTx, ref);
      }
      //if we failed don't restart as an attempt to deliver messages may be made before we actually close the consumer
      if (wasStarted && !clientFailed)
      {
         theTx.addOperation(new TransactionOperationAbstract()
         {

            @Override
            public void afterRollback(Transaction tx)
            {
               for (ServerConsumer consumer : consumers.values())
               {
                  consumer.setStarted(true);
               }
            }

         });
      }

      theTx.rollback();
   }

   private void doSend(final ServerMessage msg, final boolean direct) throws Exception
   {
      // check the user has write access to this address.
      try
      {
         securityStore.check(msg.getAddress(), CheckType.SEND, this);
      }
      catch (HornetQException e)
      {
         if (!autoCommitSends)
         {
            tx.markAsRollbackOnly(e);
         }
         throw e;
      }

      if (tx == null || autoCommitSends)
      {
      }
      else
      {
         routingContext.setTransaction(tx);
      }

      try
      {
         postOffice.route(msg, routingContext, direct);

         Pair<UUID, AtomicLong> value = targetAddressInfos.get(msg.getAddress());

         if (value == null)
         {
            targetAddressInfos.put(msg.getAddress(), new Pair<UUID, AtomicLong>(msg.getUserID(), new AtomicLong(1)));
         }
         else
         {
            value.setA(msg.getUserID());
            value.getB().incrementAndGet();
         }
      }
      finally
      {
         routingContext.clear();
      }
   }


   @Override
   public List<MessageReference> getInTXMessagesForConsumer(long consumerId)
   {
      if (this.tx != null)
      {
         QueueImpl.RefsOperation oper = (QueueImpl.RefsOperation) tx.getProperty(TransactionPropertyIndexes.REFS_OPERATION);

         if (oper == null)
         {
            return Collections.emptyList();
         }
         else
         {
            return oper.getListOnConsumer(consumerId);
         }
      }
      else
      {
         return Collections.emptyList();
      }
   }

}

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
package org.apache.activemq.artemis.core.transaction.impl;

import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import io.netty.util.collection.IntObjectHashMap;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionTimeoutException;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.OperationConsistencyLevel;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.RefsOperation;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperation;
import org.apache.activemq.artemis.utils.ArtemisCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

public class TransactionImpl implements Transaction {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private List<TransactionOperation> operations;

   private List<TransactionOperation> storeOperations;

   private static final int INITIAL_NUM_PROPERTIES = 11;

   private IntObjectHashMap properties = null;

   protected final StorageManager storageManager;

   protected final Xid xid;

   protected final long id;

   protected volatile State state = State.ACTIVE;

   private ActiveMQException exception;

   private final Object timeoutLock = new Object();

   private final long createTime;

   protected volatile boolean containsPersistent;

   private int timeoutSeconds = -1;

   private Object protocolData;

   private boolean async;

   private Runnable afterWired;

   @Override
   public boolean isAsync() {
      return async;
   }

   @Override
   public TransactionImpl setAsync(boolean async) {
      this.async = async;
      return this;
   }

   @Override
   public Object getProtocolData() {
      return protocolData;
   }

   @Override
   public void setProtocolData(Object protocolData) {
      this.protocolData = protocolData;
   }

   public TransactionImpl(final StorageManager storageManager, final int timeoutSeconds) {
      this(storageManager.generateID(), null, storageManager, timeoutSeconds);
   }

   public TransactionImpl(final StorageManager storageManager) {
      this(storageManager.generateID(), null, storageManager,-1);
   }


   public TransactionImpl(final Xid xid, final StorageManager storageManager, final int timeoutSeconds) {
      this(storageManager.generateID(), xid, storageManager, timeoutSeconds);
   }

   public TransactionImpl(final long id, final Xid xid, final StorageManager storageManager) {
      this(id, xid, storageManager, -1);
   }

   private TransactionImpl(final long id, final Xid xid, final StorageManager storageManager, final int timeoutSeconds) {
      this.storageManager = storageManager;

      this.xid = xid;

      this.id = id;

      this.createTime = System.currentTimeMillis();

      this.timeoutSeconds = timeoutSeconds;
   }

   // Transaction implementation
   // -----------------------------------------------------------

   @Override
   public boolean isEffective() {
      return state == State.PREPARED || state == State.COMMITTED || state == State.ROLLEDBACK;
   }

   @Override
   public void setContainsPersistent() {
      containsPersistent = true;
   }

   @Override
   public boolean isContainsPersistent() {
      return containsPersistent;
   }

   @Override
   public void setTimeout(final int timeout) {
      this.timeoutSeconds = timeout;
   }

   @Override
   public RefsOperation createRefsOperation(Queue queue, AckReason reason) {
      return new RefsOperation(queue, reason, storageManager);
   }

   @Override
   public long getID() {
      return id;
   }

   @Override
   public long getCreateTime() {
      return createTime;
   }

   @Override
   public boolean hasTimedOut(final long currentTime, final int defaultTimeout) {
      synchronized (timeoutLock) {
         boolean timedout;
         if (timeoutSeconds == -1) {
            timedout = getState() != Transaction.State.PREPARED && currentTime > createTime + (long) defaultTimeout * 1000;
         } else {
            timedout = getState() != Transaction.State.PREPARED && currentTime > createTime + (long) timeoutSeconds * 1000;
         }

         if (timedout) {
            markAsRollbackOnly(new ActiveMQTransactionTimeoutException());
         }

         return timedout;
      }
   }

   @Override
   public boolean hasTimedOut() {
      return state == State.ROLLBACK_ONLY && exception != null && exception.getType() == ActiveMQExceptionType.TRANSACTION_TIMEOUT;
   }

   @Override
   public void prepare() throws Exception {
      logger.trace("TransactionImpl::prepare::{}", this);

      try (ArtemisCloseable lock = storageManager.closeableReadLock()) {
         synchronized (timeoutLock) {
            if (isEffective()) {
               logger.debug("TransactionImpl::prepare::{} is being ignored", this);
               return;
            }
            if (state == State.ROLLBACK_ONLY) {
               logger.trace("TransactionImpl::prepare::rollbackonly, rollingback {}", this);

               internalRollback();

               if (exception != null) {
                  throw exception;
               } else {
                  // Do nothing
                  return;
               }
            } else if (state != State.ACTIVE) {
               throw new IllegalStateException("Transaction is in invalid state " + state);
            }

            if (xid == null) {
               throw new IllegalStateException("Cannot prepare non XA transaction");
            }

            beforePrepare();

            storageManager.prepare(id, xid);

            state = State.PREPARED;
            // We use the Callback even for non persistence
            // If we are using non-persistence with replication, the replication manager will have
            // to execute this runnable in the correct order
            storageManager.afterCompleteOperations(new IOCallback() {

               @Override
               public void onError(final int errorCode, final String errorMessage) {
                  ActiveMQServerLogger.LOGGER.ioErrorOnTX("prepare", errorCode, errorMessage);
               }

               @Override
               public void done() {
                  afterPrepare();
               }
            });
         }
      }
   }

   @Override
   public void commit() throws Exception {
      commit(true);
   }

   @Override
   public void afterWired(Runnable callback) {
      this.afterWired = callback;
   }

   private void wired() {
      if (afterWired != null) {
         afterWired.run();
         afterWired = null;
      }
   }

   protected OperationConsistencyLevel getRequiredConsistency() {
      return OperationConsistencyLevel.FULL;
   }

   @Override
   public void commit(final boolean onePhase) throws Exception {
      logger.trace("TransactionImpl::commit::{}", this);

      synchronized (timeoutLock) {
         if (state == State.COMMITTED) {
            // I don't think this could happen, but just in case
            logger.debug("TransactionImpl::commit::{} is being ignored", this);
            return;
         }
         if (state == State.ROLLBACK_ONLY) {
            internalRollback();

            if (exception != null) {
               throw exception;
            } else {
               // Do nothing
               return;
            }
         }

         if (xid != null) {
            if (onePhase && state != State.ACTIVE || !onePhase && state != State.PREPARED) {
               throw new ActiveMQIllegalStateException("Transaction is in invalid state " + state);
            }
         } else {
            if (state != State.ACTIVE) {
               throw new ActiveMQIllegalStateException("Transaction is in invalid state " + state);
            }
         }

         beforeCommit();

         doCommit();

         // We want to make sure that nothing else gets done after the commit is issued
         // this will eliminate any possibility or races
         final List<TransactionOperation> operationsToComplete = this.operations;
         this.operations = null;

         // We use the Callback even for non persistence
         // If we are using non-persistence with replication, the replication manager will have
         // to execute this runnable in the correct order
         // This also will only use a different thread if there are any IO pending.
         // If the IO finished early by the time we got here, we won't need an executor
         storageManager.afterCompleteOperations(new IOCallback() {

            @Override
            public void onError(final int errorCode, final String errorMessage) {
               ActiveMQServerLogger.LOGGER.ioErrorOnTX("commit - afterComplete", errorCode, errorMessage);
            }

            @Override
            public void done() {
               afterCommit(operationsToComplete);
            }
         }, getRequiredConsistency());

         final List<TransactionOperation> storeOperationsToComplete = this.storeOperations;
         this.storeOperations = null;

         if (storeOperationsToComplete != null) {
            storageManager.afterStoreOperations(new IOCallback() {

               @Override
               public void onError(final int errorCode, final String errorMessage) {
                  ActiveMQServerLogger.LOGGER.ioErrorOnTX("commit - afterStore", errorCode, errorMessage);
               }

               @Override
               public void done() {
                  afterCommit(storeOperationsToComplete);
               }
            });
         }

         wired();
      }
   }

   /**
    * @throws Exception
    */
   protected void doCommit() throws Exception {
      if (containsPersistent || xid != null && state == State.PREPARED) {
         // ^^ These are the scenarios where we require a storage.commit
         // for anything else we won't use the journal
         if (async) {
            storageManager.asyncCommit(id);
         } else {
            storageManager.commit(id);
         }
      }

      state = State.COMMITTED;
   }

   @Override
   public boolean tryRollback() {
      synchronized (timeoutLock) {
         if (state == State.ROLLEDBACK) {
            // I don't think this could happen, but just in case
            logger.debug("TransactionImpl::rollbackIfPossible::{} is being ignored", this);
            return true;
         }
         if (state != State.PREPARED) {
            try {
               internalRollback();
               return true;
            } catch (Exception e) {
               // nothing we can do beyond logging
               // no need to special handler here as this was not even supposed to happen at this point
               // even if it happenes this would be the exception of the exception, so we just log here
               logger.warn(e.getMessage(), e);
            }
         }
      }
      return false;
   }

   @Override
   public void rollback() throws Exception {
      logger.trace("TransactionImpl::rollback::{}", this);

      synchronized (timeoutLock) {
         if (state == State.ROLLEDBACK) {
            // I don't think this could happen, but just in case
            logger.debug("TransactionImpl::rollback::{} is being ignored", this);
            return;
         }
         if (xid != null) {
            if (state != State.PREPARED && state != State.ACTIVE && state != State.ROLLBACK_ONLY) {
               throw new ActiveMQIllegalStateException("Transaction is in invalid state " + state);
            }
         } else {
            if (state != State.ACTIVE && state != State.ROLLBACK_ONLY) {
               throw new ActiveMQIllegalStateException("Transaction is in invalid state " + state);
            }
         }

         internalRollback();
      }
   }

   private void internalRollback() throws Exception {
      logger.trace("TransactionImpl::internalRollback {}", this);

      beforeRollback();

      try {
         doRollback();
         state = State.ROLLEDBACK;
      } catch (IllegalStateException e) {
         // Something happened before and the TX didn't make to the Journal / Storage
         // We will like to execute afterRollback and clear anything pending
         ActiveMQServerLogger.LOGGER.failedToPerformRollback(e);
      }
      // We want to make sure that nothing else gets done after the rollback is issued
      final List<TransactionOperation> operationsToComplete = this.operations;
      this.operations = null;

      final List<TransactionOperation> storeOperationsToComplete = this.storeOperations;
      this.storeOperations = null;

      // We use the Callback even for non persistence
      // If we are using non-persistence with replication, the replication manager will have
      // to execute this runnable in the correct order
      storageManager.afterCompleteOperations(new IOCallback() {

         @Override
         public void onError(final int errorCode, final String errorMessage) {
            ActiveMQServerLogger.LOGGER.ioErrorOnTX("rollback - afterComplete", errorCode, errorMessage);
         }

         @Override
         public void done() {
            afterRollback(operationsToComplete);
         }
      });

      if (storeOperationsToComplete != null) {
         storageManager.afterStoreOperations(new IOCallback() {

            @Override
            public void onError(final int errorCode, final String errorMessage) {
               ActiveMQServerLogger.LOGGER.ioErrorOnTX("rollback - afterStore", errorCode, errorMessage);
            }

            @Override
            public void done() {
               afterRollback(storeOperationsToComplete);
            }
         });
      }
   }

   @Override
   public void suspend() {
      synchronized (timeoutLock) {
         if (state != State.ACTIVE) {
            throw new IllegalStateException("Can only suspend active transaction");
         }
         state = State.SUSPENDED;
      }
   }

   @Override
   public void resume() {
      synchronized (timeoutLock) {
         if (state != State.SUSPENDED) {
            throw new IllegalStateException("Can only resume a suspended transaction");
         }
         state = State.ACTIVE;
      }
   }

   @Override
   public Transaction.State getState() {
      return state;
   }

   @Override
   public void setState(final State state) {
      this.state = state;
   }

   @Override
   public Xid getXid() {
      return xid;
   }

   @Override
   public void markAsRollbackOnly(final ActiveMQException exception) {
      synchronized (timeoutLock) {
         if (logger.isTraceEnabled()) {
            logger.trace("TransactionImpl::{} marking rollbackOnly for {}, msg={}", this, exception.toString(), exception.getMessage());
         }

         if (isEffective()) {
            if (logger.isDebugEnabled()) {
               logger.debug("Trying to mark transaction {} xid={} as rollbackOnly but it was already effective (prepared, committed or rolledback!)", id, xid);
            }
            return;
         }

         if (logger.isDebugEnabled()) {
            logger.debug("Marking Transaction {} as rollback only", id);
         }
         state = State.ROLLBACK_ONLY;

         this.exception = exception;
      }
   }

   @Override
   public synchronized void addOperation(final TransactionOperation operation) {
      // We do this check, because in the case of XA Transactions and paging,
      // the commit could happen while the counters are being rebuilt.
      // if the state is commited we should execute it right away.
      // this is just to avoid a race.
      switch (state) {
         case COMMITTED:
            operation.afterCommit(this);
            return;
         case ROLLEDBACK:
            operation.afterRollback(this);
            return;
         default:
            checkCreateOperations();
            operations.add(operation);
      }
   }

   @Override
   public synchronized void afterStore(TransactionOperation sync) {
      if (storeOperations == null) {
         storeOperations = new LinkedList<>();
      }
      storeOperations.add(sync);
   }

   private int getOperationsCount() {
      checkCreateOperations();

      return operations.size();
   }

   @Override
   public synchronized List<TransactionOperation> getAllOperations() {

      if (operations != null) {
         return new ArrayList<>(operations);
      } else {
         return new ArrayList<>();
      }
   }

   @Override
   public void putProperty(final int index, final Object property) {

      if (properties == null) {
         properties = new IntObjectHashMap();
      }

      properties.put(index, property);
   }

   @Override
   public Object getProperty(final int index) {
      return properties == null ? null : properties.get(index);
   }

   // Private
   // -------------------------------------------------------------------

   protected void doRollback() throws Exception {
      if (containsPersistent || xid != null && state == State.PREPARED) {
         storageManager.rollback(id);
      }
   }

   private void checkCreateOperations() {
      if (operations == null) {
         operations = new LinkedList<>();
      }
   }

   protected synchronized void afterCommit(List<TransactionOperation> operationsToComplete) {
      if (operationsToComplete != null) {
         for (TransactionOperation operation : operationsToComplete) {
            operation.afterCommit(this);
         }
         // Help out GC here
         operationsToComplete.clear();
      }
   }

   private synchronized void afterRollback(List<TransactionOperation> operationsToComplete) {
      if (operationsToComplete != null) {
         for (TransactionOperation operation : operationsToComplete) {
            operation.afterRollback(this);
         }
         // Help out GC here
         operationsToComplete.clear();
      }
   }

   private synchronized void beforeCommit() throws Exception {
      if (operations != null) {
         for (TransactionOperation operation : operations) {
            operation.beforeCommit(this);
         }
      }
      if (storeOperations != null) {
         for (TransactionOperation operation : storeOperations) {
            operation.beforeCommit(this);
         }
      }
   }

   private synchronized void beforePrepare() throws Exception {
      if (operations != null) {
         for (TransactionOperation operation : operations) {
            operation.beforePrepare(this);
         }
      }
      if (storeOperations != null) {
         for (TransactionOperation operation : storeOperations) {
            operation.beforePrepare(this);
         }
      }
   }

   private synchronized void beforeRollback() throws Exception {
      if (operations != null) {
         for (TransactionOperation operation : operations) {
            operation.beforeRollback(this);
         }
      }
      if (storeOperations != null) {
         for (TransactionOperation operation : storeOperations) {
            operation.beforeRollback(this);
         }
      }
   }

   private synchronized void afterPrepare() {
      if (operations != null) {
         for (TransactionOperation operation : operations) {
            operation.afterPrepare(this);
         }
      }
      if (storeOperations != null) {
         for (TransactionOperation operation : storeOperations) {
            operation.afterPrepare(this);
         }
      }
   }

   @Override
   public String toString() {
      Date dt = new Date(this.createTime);
      return "TransactionImpl [xid=" + xid +
         ", txID=" +
         id +
         ", xid=" + xid +
         ", state=" +
         state +
         ", createTime=" +
         createTime + "(" + dt + ")" +
         ", timeoutSeconds=" +
         timeoutSeconds +
         ", nr operations = " + getOperationsCount() +
         "]@" +
         Integer.toHexString(hashCode());
   }
}

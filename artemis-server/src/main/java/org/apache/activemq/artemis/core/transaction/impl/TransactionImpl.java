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
import java.util.List;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.RefsOperation;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperation;

public class TransactionImpl implements Transaction {

   private static final boolean isTrace = ActiveMQServerLogger.LOGGER.isTraceEnabled();

   private List<TransactionOperation> operations;

   private static final int INITIAL_NUM_PROPERTIES = 10;

   private Object[] properties = new Object[TransactionImpl.INITIAL_NUM_PROPERTIES];

   protected final StorageManager storageManager;

   private final Xid xid;

   private final long id;

   private volatile State state = State.ACTIVE;

   private ActiveMQException exception;

   private final Object timeoutLock = new Object();

   private final long createTime;

   private volatile boolean containsPersistent;

   private int timeoutSeconds = -1;

   public TransactionImpl(final StorageManager storageManager, final int timeoutSeconds) {
      this.storageManager = storageManager;

      xid = null;

      id = storageManager.generateID();

      createTime = System.currentTimeMillis();

      this.timeoutSeconds = timeoutSeconds;
   }

   public TransactionImpl(final StorageManager storageManager) {
      this.storageManager = storageManager;

      xid = null;

      id = storageManager.generateID();

      createTime = System.currentTimeMillis();
   }

   public TransactionImpl(final Xid xid, final StorageManager storageManager, final int timeoutSeconds) {
      this.storageManager = storageManager;

      this.xid = xid;

      id = storageManager.generateID();

      createTime = System.currentTimeMillis();

      this.timeoutSeconds = timeoutSeconds;
   }

   public TransactionImpl(final long id, final Xid xid, final StorageManager storageManager) {
      this.storageManager = storageManager;

      this.xid = xid;

      this.id = id;

      createTime = System.currentTimeMillis();
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
   public RefsOperation createRefsOperation(Queue queue) {
      return new RefsOperation(queue, storageManager);
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
            timedout = getState() != Transaction.State.PREPARED && currentTime > createTime + defaultTimeout * 1000;
         }
         else {
            timedout = getState() != Transaction.State.PREPARED && currentTime > createTime + timeoutSeconds * 1000;
         }

         if (timedout) {
            markAsRollbackOnly(new ActiveMQException("TX Timeout"));
         }

         return timedout;
      }
   }

   @Override
   public void prepare() throws Exception {
      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("TransactionImpl::prepare::" + this);
      }
      storageManager.readLock();
      try {
         synchronized (timeoutLock) {
            if (isEffective()) {
               ActiveMQServerLogger.LOGGER.debug("TransactionImpl::prepare::" + this + " is being ignored");
               return;
            }
            if (state == State.ROLLBACK_ONLY) {
               if (isTrace) {
                  ActiveMQServerLogger.LOGGER.trace("TransactionImpl::prepare::rollbackonly, rollingback " + this);
               }

               internalRollback();

               if (exception != null) {
                  throw exception;
               }
               else {
                  // Do nothing
                  return;
               }
            }
            else if (state != State.ACTIVE) {
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
                  ActiveMQServerLogger.LOGGER.ioErrorOnTX(errorCode, errorMessage);
               }

               @Override
               public void done() {
                  afterPrepare();
               }
            });
         }
      }
      finally {
         storageManager.readUnLock();
      }
   }

   @Override
   public void commit() throws Exception {
      commit(true);
   }

   @Override
   public void commit(final boolean onePhase) throws Exception {
      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("TransactionImpl::commit::" + this);
      }
      synchronized (timeoutLock) {
         if (state == State.COMMITTED) {
            // I don't think this could happen, but just in case
            ActiveMQServerLogger.LOGGER.debug("TransactionImpl::commit::" + this + " is being ignored");
            return;
         }
         if (state == State.ROLLBACK_ONLY) {
            internalRollback();

            if (exception != null) {
               throw exception;
            }
            else {
               // Do nothing
               return;
            }
         }

         if (xid != null) {
            if (onePhase && state != State.ACTIVE || !onePhase && state != State.PREPARED) {
               throw new ActiveMQIllegalStateException("Transaction is in invalid state " + state);
            }
         }
         else {
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
               ActiveMQServerLogger.LOGGER.ioErrorOnTX(errorCode, errorMessage);
            }

            @Override
            public void done() {
               afterCommit(operationsToComplete);
            }
         });

      }
   }

   /**
    * @throws Exception
    */
   protected void doCommit() throws Exception {
      if (containsPersistent || xid != null && state == State.PREPARED) {
         // ^^ These are the scenarios where we require a storage.commit
         // for anything else we won't use the journal
         storageManager.commit(id);
      }

      state = State.COMMITTED;
   }

   @Override
   public void rollback() throws Exception {
      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("TransactionImpl::rollback::" + this);
      }

      synchronized (timeoutLock) {
         if (state == State.ROLLEDBACK) {
            // I don't think this could happen, but just in case
            ActiveMQServerLogger.LOGGER.debug("TransactionImpl::rollback::" + this + " is being ignored");
            return;
         }
         if (xid != null) {
            if (state != State.PREPARED && state != State.ACTIVE && state != State.ROLLBACK_ONLY) {
               throw new ActiveMQIllegalStateException("Transaction is in invalid state " + state);
            }
         }
         else {
            if (state != State.ACTIVE && state != State.ROLLBACK_ONLY) {
               throw new ActiveMQIllegalStateException("Transaction is in invalid state " + state);
            }
         }

         internalRollback();
      }
   }

   private void internalRollback() throws Exception {
      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("TransactionImpl::internalRollback " + this);
      }

      beforeRollback();

      try {
         doRollback();
         state = State.ROLLEDBACK;
      }
      catch (IllegalStateException e) {
         // Something happened before and the TX didn't make to the Journal / Storage
         // We will like to execute afterRollback and clear anything pending
         ActiveMQServerLogger.LOGGER.warn(e);
      }
      // We want to make sure that nothing else gets done after the commit is issued
      // this will eliminate any possibility or races
      final List<TransactionOperation> operationsToComplete = this.operations;
      this.operations = null;

      // We use the Callback even for non persistence
      // If we are using non-persistence with replication, the replication manager will have
      // to execute this runnable in the correct order
      storageManager.afterCompleteOperations(new IOCallback() {

         public void onError(final int errorCode, final String errorMessage) {
            ActiveMQServerLogger.LOGGER.ioErrorOnTX(errorCode, errorMessage);
         }

         public void done() {
            afterRollback(operationsToComplete);
         }
      });
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
         if (isTrace) {
            ActiveMQServerLogger.LOGGER.trace("TransactionImpl::" + this + " marking rollbackOnly for " + exception.toString() + ", msg=" + exception.getMessage());
         }

         if (isEffective()) {
            ActiveMQServerLogger.LOGGER.debug("Trying to mark transaction " + this.id + " xid=" + this.xid + " as rollbackOnly but it was already effective (prepared, committed or rolledback!)");
            return;
         }

         if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
            ActiveMQServerLogger.LOGGER.debug("Marking Transaction " + this.id + " as rollback only");
         }
         state = State.ROLLBACK_ONLY;

         this.exception = exception;
      }
   }

   @Override
   public synchronized void addOperation(final TransactionOperation operation) {
      checkCreateOperations();

      operations.add(operation);
   }

   private int getOperationsCount() {
      checkCreateOperations();

      return operations.size();
   }

   @Override
   public synchronized List<TransactionOperation> getAllOperations() {

      if (operations != null) {
         return new ArrayList<>(operations);
      }
      else {
         return new ArrayList<>();
      }
   }

   @Override
   public void putProperty(final int index, final Object property) {
      if (index >= properties.length) {
         Object[] newProperties = new Object[index];

         System.arraycopy(properties, 0, newProperties, 0, properties.length);

         properties = newProperties;
      }

      properties[index] = property;
   }

   @Override
   public Object getProperty(final int index) {
      return properties[index];
   }

   // Private
   // -------------------------------------------------------------------

   private void doRollback() throws Exception {
      if (containsPersistent || xid != null && state == State.PREPARED) {
         storageManager.rollback(id);
      }
   }

   private void checkCreateOperations() {
      if (operations == null) {
         operations = new ArrayList<>();
      }
   }

   private synchronized void afterCommit(List<TransactionOperation> oeprationsToComplete) {
      if (oeprationsToComplete != null) {
         for (TransactionOperation operation : oeprationsToComplete) {
            operation.afterCommit(this);
         }
         // Help out GC here
         oeprationsToComplete.clear();
      }
   }

   private synchronized void afterRollback(List<TransactionOperation> oeprationsToComplete) {
      if (oeprationsToComplete != null) {
         for (TransactionOperation operation : oeprationsToComplete) {
            operation.afterRollback(this);
         }
         // Help out GC here
         oeprationsToComplete.clear();
      }
   }

   private synchronized void beforeCommit() throws Exception {
      if (operations != null) {
         for (TransactionOperation operation : operations) {
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
   }

   private synchronized void beforeRollback() throws Exception {
      if (operations != null) {
         for (TransactionOperation operation : operations) {
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
   }

   @Override
   public String toString() {
      Date dt = new Date(this.createTime);
      return "TransactionImpl [xid=" + xid +
         ", id=" +
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

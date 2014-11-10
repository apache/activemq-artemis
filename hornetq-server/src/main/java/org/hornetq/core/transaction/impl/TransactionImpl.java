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
60 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.transaction.impl;

import javax.transaction.xa.Xid;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.impl.RefsOperation;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;

/**
 * A TransactionImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 */
public class TransactionImpl implements Transaction
{
   private List<TransactionOperation> operations;

   private static final int INITIAL_NUM_PROPERTIES = 10;

   private Object[] properties = new Object[TransactionImpl.INITIAL_NUM_PROPERTIES];

   protected final StorageManager storageManager;

   private final Xid xid;

   private final long id;


   private volatile State state = State.ACTIVE;

   private HornetQException exception;

   private final Object timeoutLock = new Object();

   private final long createTime;

   private volatile boolean containsPersistent;

   private int timeoutSeconds = -1;

   public TransactionImpl(final StorageManager storageManager, final int timeoutSeconds)
   {
      this.storageManager = storageManager;

      xid = null;

      id = storageManager.generateID();

      createTime = System.currentTimeMillis();

      this.timeoutSeconds = timeoutSeconds;
   }

   public TransactionImpl(final StorageManager storageManager)
   {
      this.storageManager = storageManager;

      xid = null;

      id = storageManager.generateID();

      createTime = System.currentTimeMillis();
   }

   public TransactionImpl(final Xid xid, final StorageManager storageManager, final int timeoutSeconds)
   {
      this.storageManager = storageManager;

      this.xid = xid;

      id = storageManager.generateID();

      createTime = System.currentTimeMillis();

      this.timeoutSeconds = timeoutSeconds;
   }

   public TransactionImpl(final long id, final Xid xid, final StorageManager storageManager)
   {
      this.storageManager = storageManager;

      this.xid = xid;

      this.id = id;

      createTime = System.currentTimeMillis();
   }

   // Transaction implementation
   // -----------------------------------------------------------

   public void setContainsPersistent()
   {
      containsPersistent = true;
   }

   public boolean isContainsPersistent()
   {
      return containsPersistent;
   }

   public void setTimeout(final int timeout)
   {
      this.timeoutSeconds = timeout;
   }

   @Override
   public RefsOperation createRefsOperation(Queue queue)
   {
      return new RefsOperation(queue, storageManager);
   }

   public long getID()
   {
      return id;
   }

   public long getCreateTime()
   {
      return createTime;
   }

   public boolean hasTimedOut(final long currentTime, final int defaultTimeout)
   {
      if (timeoutSeconds == -1)
      {
         return getState() != Transaction.State.PREPARED && currentTime > createTime + defaultTimeout * 1000;
      }
      else
      {
         return getState() != Transaction.State.PREPARED && currentTime > createTime + timeoutSeconds * 1000;
      }
   }

   public void prepare() throws Exception
   {
      storageManager.readLock();
      try
      {
         synchronized (timeoutLock)
         {
            if (state == State.ROLLBACK_ONLY)
            {
               if (exception != null)
               {
                  // this TX will never be rolled back,
                  // so we reset it now
                  beforeRollback();
                  afterRollback();
                  operations.clear();
                  throw exception;
               }
               else
               {
                  // Do nothing
                  return;
               }
            }
            else if (state != State.ACTIVE)
            {
               throw new IllegalStateException("Transaction is in invalid state " + state);
            }

            if (xid == null)
            {
               throw new IllegalStateException("Cannot prepare non XA transaction");
            }

            beforePrepare();

            storageManager.prepare(id, xid);

            state = State.PREPARED;
            // We use the Callback even for non persistence
            // If we are using non-persistence with replication, the replication manager will have
            // to execute this runnable in the correct order
            storageManager.afterCompleteOperations(new IOAsyncTask()
            {

               public void onError(final int errorCode, final String errorMessage)
               {
                  HornetQServerLogger.LOGGER.ioErrorOnTX(errorCode, errorMessage);
               }

               public void done()
               {
                  afterPrepare();
               }
            });
         }
      }
      finally
      {
         storageManager.readUnLock();
      }
   }

   public void commit() throws Exception
   {
      commit(true);
   }

   public void commit(final boolean onePhase) throws Exception
   {
      synchronized (timeoutLock)
      {
         if (state == State.ROLLBACK_ONLY)
         {
            rollback();

            if (exception != null)
            {
               throw exception;
            }
            else
            {
               // Do nothing
               return;
            }
         }

         if (xid != null)
         {
            if (onePhase && state != State.ACTIVE || !onePhase && state != State.PREPARED)
            {
               throw new IllegalStateException("Transaction is in invalid state " + state);
            }
         }
         else
         {
            if (state != State.ACTIVE)
            {
               throw new IllegalStateException("Transaction is in invalid state " + state);
            }
         }

         beforeCommit();

         doCommit();

         // We use the Callback even for non persistence
         // If we are using non-persistence with replication, the replication manager will have
         // to execute this runnable in the correct order
         // This also will only use a different thread if there are any IO pending.
         // If the IO finished early by the time we got here, we won't need an executor
         storageManager.afterCompleteOperations(new IOAsyncTask()
         {

            public void onError(final int errorCode, final String errorMessage)
            {
               HornetQServerLogger.LOGGER.ioErrorOnTX(errorCode, errorMessage);
            }

            public void done()
            {
               afterCommit();
            }
         });

      }
   }

   /**
    * @throws Exception
    */
   protected void doCommit() throws Exception
   {
      if (containsPersistent || xid != null && state == State.PREPARED)
      {

         storageManager.commit(id);

         state = State.COMMITTED;
      }
   }

   public void rollback() throws Exception
   {
      synchronized (timeoutLock)
      {
         if (xid != null)
         {
            if (state != State.PREPARED && state != State.ACTIVE && state != State.ROLLBACK_ONLY)
            {
               throw new IllegalStateException("Transaction is in invalid state " + state);
            }
         }
         else
         {
            if (state != State.ACTIVE && state != State.ROLLBACK_ONLY)
            {
               throw new IllegalStateException("Transaction is in invalid state " + state);
            }
         }

         beforeRollback();

         doRollback();

         // We use the Callback even for non persistence
         // If we are using non-persistence with replication, the replication manager will have
         // to execute this runnable in the correct order
         storageManager.afterCompleteOperations(new IOAsyncTask()
         {

            public void onError(final int errorCode, final String errorMessage)
            {
               HornetQServerLogger.LOGGER.ioErrorOnTX(errorCode, errorMessage);
            }

            public void done()
            {
               afterRollback();
               state = State.ROLLEDBACK;
            }
         });
      }
   }

   public void suspend()
   {
      if (state != State.ACTIVE)
      {
         throw new IllegalStateException("Can only suspend active transaction");
      }
      state = State.SUSPENDED;
   }

   public void resume()
   {
      if (state != State.SUSPENDED)
      {
         throw new IllegalStateException("Can only resume a suspended transaction");
      }
      state = State.ACTIVE;
   }

   public Transaction.State getState()
   {
      return state;
   }

   public void setState(final State state)
   {
      this.state = state;
   }

   public Xid getXid()
   {
      return xid;
   }

   public void markAsRollbackOnly(final HornetQException exception1)
   {
      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug("Marking Transaction " + this.id + " as rollback only");
      }
      state = State.ROLLBACK_ONLY;

      this.exception = exception1;
   }

   public synchronized void addOperation(final TransactionOperation operation)
   {
      checkCreateOperations();

      operations.add(operation);
   }

   private int getOperationsCount()
   {
      checkCreateOperations();

      return operations.size();
   }

   public synchronized List<TransactionOperation> getAllOperations()
   {
      return new ArrayList<TransactionOperation>(operations);
   }

   public void putProperty(final int index, final Object property)
   {
      if (index >= properties.length)
      {
         Object[] newProperties = new Object[index];

         System.arraycopy(properties, 0, newProperties, 0, properties.length);

         properties = newProperties;
      }

      properties[index] = property;
   }

   public Object getProperty(final int index)
   {
      return properties[index];
   }

   // Private
   // -------------------------------------------------------------------

   private void doRollback() throws Exception
   {
      if (containsPersistent || xid != null && state == State.PREPARED)
      {
         storageManager.rollback(id);
      }
   }

   private void checkCreateOperations()
   {
      if (operations == null)
      {
         operations = new ArrayList<TransactionOperation>();
      }
   }

   private synchronized void afterCommit()
   {
      if (operations != null)
      {
         for (TransactionOperation operation : operations)
         {
            operation.afterCommit(this);
         }
      }
   }

   private synchronized void afterRollback()
   {
      if (operations != null)
      {
         for (TransactionOperation operation : operations)
         {
            operation.afterRollback(this);
         }
      }
   }

   private synchronized void beforeCommit() throws Exception
   {
      if (operations != null)
      {
         for (TransactionOperation operation : operations)
         {
            operation.beforeCommit(this);
         }
      }
   }

   private synchronized void beforePrepare() throws Exception
   {
      if (operations != null)
      {
         for (TransactionOperation operation : operations)
         {
            operation.beforePrepare(this);
         }
      }
   }

   private synchronized void beforeRollback() throws Exception
   {
      if (operations != null)
      {
         for (TransactionOperation operation : operations)
         {
            operation.beforeRollback(this);
         }
      }
   }

   private synchronized void afterPrepare()
   {
      if (operations != null)
      {
         for (TransactionOperation operation : operations)
         {
            operation.afterPrepare(this);
         }
      }
   }

   @Override
   public String toString()
   {
      Date dt = new Date(this.createTime);
      return "TransactionImpl [xid=" + xid +
         ", id=" +
         id +
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

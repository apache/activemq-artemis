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
package org.apache.activemq.artemis.tests.extras.jms.bridge;

import com.arjuna.ats.arjuna.common.Uid;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

public class FailingTransactionManager implements TransactionManager {

   private final TransactionManager tm;
   private int calls;
   private final int limit;
   private final AtomicInteger failures = new AtomicInteger(0);
   private final Map<Uid, FailingTransaction> transactions = Collections.synchronizedMap(new HashMap<>(10));

   public FailingTransactionManager(TransactionManager tm, int limit) {
      this.tm = tm;
      this.calls = 0;
      this.limit = limit;
   }

   @Override
   public void begin() throws NotSupportedException, SystemException {
      tm.begin();
   }

   @Override
   public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
      transactions.remove(((com.arjuna.ats.jta.transaction.Transaction) tm.getTransaction()).get_uid()).commit();
   }

   @Override
   public void rollback() throws IllegalStateException, SecurityException, SystemException {
      transactions.remove(((com.arjuna.ats.jta.transaction.Transaction) tm.getTransaction()).get_uid()).rollback();
   }

   @Override
   public void setRollbackOnly() throws IllegalStateException, SystemException {
      tm.setRollbackOnly();
   }

   @Override
   public int getStatus() throws SystemException {
      return tm.getStatus();
   }

   @Override
   public Transaction getTransaction() throws SystemException {
      com.arjuna.ats.jta.transaction.Transaction real =  (com.arjuna.ats.jta.transaction.Transaction) tm.getTransaction();
      if (transactions.containsKey(real.get_uid())) {
         return transactions.get(real.get_uid());
      }
      FailingTransaction tx = new FailingTransaction(real, calls++);
      transactions.put(real.get_uid(), tx);
      return tx;
   }

   @Override
   public void setTransactionTimeout(int i) throws SystemException {
      tm.setTransactionTimeout(i);
   }

   @Override
   public Transaction suspend() throws SystemException {
      Transaction real = tm.suspend();
      if (real == null) {
         return null;
      }
      return transactions.get(((com.arjuna.ats.jta.transaction.Transaction) real).get_uid());
   }

   public int getFailures() {
      return failures.get();
   }

   @Override
   public void resume(Transaction transaction) throws InvalidTransactionException, IllegalStateException, SystemException {
      tm.resume(((FailingTransaction)transaction).transaction);
   }

   private final class FailingTransaction implements Transaction {

      private final com.arjuna.ats.jta.transaction.Transaction transaction;
      private final int number;

      private FailingTransaction(com.arjuna.ats.jta.transaction.Transaction transaction, int number) {
         this.transaction = transaction;
         this.number = number;
      }

      @Override
      public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
         if (number < limit) {
            transaction.commit();
            transactions.remove(transaction.get_uid());
         } else {
            int fails = failures.incrementAndGet();
            RollbackException ex = new RollbackException("Expected rollback for test");
            throw ex;
         }
      }


      @Override
      public boolean delistResource(XAResource arg0, int arg1) throws IllegalStateException, SystemException {
         return transaction.delistResource(arg0, arg1);
      }

      @Override
      public boolean enlistResource(XAResource arg0) throws RollbackException, IllegalStateException, SystemException {
         return transaction.enlistResource(arg0);
      }

      @Override
      public int getStatus() throws SystemException {
         return transaction.getStatus();
      }

      @Override
      public void registerSynchronization(Synchronization arg0) throws RollbackException, IllegalStateException, SystemException {
         transaction.registerSynchronization(arg0);
      }

      @Override
      public void rollback() throws IllegalStateException, SystemException {
         transaction.rollback();
         transactions.remove(transaction.get_uid());
      }

      @Override
      public void setRollbackOnly() throws IllegalStateException, SystemException {
         transaction.setRollbackOnly();
      }

      @Override
      public String toString() {
         return "FailingTransaction{" + "transaction=" + transaction.get_uid() + ", number=" + number + '}';
      }
   }

}

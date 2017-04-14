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
package org.apache.activemq.artemis.core.journal.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.core.journal.impl.dataformat.JournalInternalRecord;
import org.jboss.logging.Logger;

public class JournalTransaction {

   private static final Logger logger = Logger.getLogger(JournalTransaction.class);

   private JournalRecordProvider journal;

   private List<JournalUpdate> pos;

   private List<JournalUpdate> neg;

   private final long id;

   // All the files this transaction is touching on.
   // We can't have those files being reclaimed if there is a pending transaction
   private Set<JournalFile> pendingFiles;

   private TransactionCallback currentCallback;

   private boolean compacting = false;

   private final Map<JournalFile, TransactionCallback> callbackList = Collections.synchronizedMap(new HashMap<JournalFile, TransactionCallback>());

   private JournalFile lastFile = null;

   private final AtomicInteger counter = new AtomicInteger();

   private CountDownLatch firstCallbackLatch;

   public JournalTransaction(final long id, final JournalRecordProvider journal) {
      this.id = id;
      this.journal = journal;
   }

   public void replaceRecordProvider(final JournalRecordProvider provider) {
      journal = provider;
   }

   /**
    * @return the id
    */
   public long getId() {
      return id;
   }

   public int getCounter(final JournalFile file) {
      return internalgetCounter(file).intValue();
   }

   public void incCounter(final JournalFile file) {
      internalgetCounter(file).incrementAndGet();
   }

   public long[] getPositiveArray() {
      if (pos == null) {
         return new long[0];
      } else {
         int i = 0;
         long[] ids = new long[pos.size()];
         for (JournalUpdate el : pos) {
            ids[i++] = el.getId();
         }
         return ids;
      }
   }

   public void setCompacting() {
      compacting = true;

      // Everything is cleared on the transaction...
      // since we are compacting, everything is at the compactor's level
      clear();
   }

   /**
    * This is used to merge transactions from compacting
    */
   public void merge(final JournalTransaction other) {
      if (other.pos != null) {
         if (pos == null) {
            pos = new ArrayList<>();
         }

         pos.addAll(other.pos);
      }

      if (other.neg != null) {
         if (neg == null) {
            neg = new ArrayList<>();
         }

         neg.addAll(other.neg);
      }

      if (other.pendingFiles != null) {
         if (pendingFiles == null) {
            pendingFiles = new HashSet<>();
         }

         pendingFiles.addAll(other.pendingFiles);
      }

      compacting = false;
   }

   /**
    *
    */
   public void clear() {
      // / Compacting is recreating all the previous files and everything
      // / so we just clear the list of previous files, previous pos and previous adds
      // / The transaction may be working at the top from now

      if (pendingFiles != null) {
         pendingFiles.clear();
      }

      callbackList.clear();

      if (pos != null) {
         pos.clear();
      }

      if (neg != null) {
         neg.clear();
      }

      counter.set(0);

      lastFile = null;

      currentCallback = null;

      firstCallbackLatch = null;
   }

   /**
    * @param currentFile
    * @param data
    */
   public void fillNumberOfRecords(final JournalFile currentFile, final JournalInternalRecord data) {
      data.setNumberOfRecords(getCounter(currentFile));
   }

   public TransactionCallback getCurrentCallback() {
      return currentCallback;
   }

   public TransactionCallback getCallback(final JournalFile file) throws Exception {
      if (firstCallbackLatch != null && callbackList.isEmpty()) {
         firstCallbackLatch.countDown();
      }

      currentCallback = callbackList.get(file);

      if (currentCallback == null) {
         currentCallback = new TransactionCallback();
         callbackList.put(file, currentCallback);
      }

      currentCallback.countUp();

      return currentCallback;
   }

   public void checkErrorCondition() throws Exception {
      if (currentCallback != null) {
         if (currentCallback.getErrorMessage() != null) {
            throw ActiveMQExceptionType.createException(currentCallback.getErrorCode(), currentCallback.getErrorMessage());
         }
      }
   }

   public void addPositive(final JournalFile file, final long id, final int size) {
      incCounter(file);

      addFile(file);

      if (pos == null) {
         pos = new ArrayList<>();
      }

      pos.add(new JournalUpdate(file, id, size));
   }

   public void addNegative(final JournalFile file, final long id) {
      incCounter(file);

      addFile(file);

      if (neg == null) {
         neg = new ArrayList<>();
      }

      neg.add(new JournalUpdate(file, id, 0));
   }

   /**
    * The caller of this method needs to guarantee appendLock.lock at the journal. (unless this is being called from load what is a single thread process).
    */
   public void commit(final JournalFile file) {
      JournalCompactor compactor = journal.getCompactor();

      // https://issues.apache.org/jira/browse/ARTEMIS-1114
      //   There was a race once where compacting was not set
      //   because the Journal was missing a readLock and compacting was starting
      //   without setting this properly...
      if (compacting && compactor != null) {
         if (logger.isTraceEnabled()) {
            logger.trace("adding tx " + this.id + " into compacting");
         }
         compactor.addCommandCommit(this, file);
      } else {

         if (logger.isTraceEnabled()) {
            logger.trace("no compact commit " + this.id);
         }
         if (pos != null) {
            for (JournalUpdate trUpdate : pos) {
               JournalRecord posFiles = journal.getRecords().get(trUpdate.id);

               if (compactor != null && compactor.lookupRecord(trUpdate.id)) {
                  // This is a case where the transaction was opened after compacting was started,
                  // but the commit arrived while compacting was working
                  // We need to cache the counter update, so compacting will take the correct files when it is done
                  compactor.addCommandUpdate(trUpdate.id, trUpdate.file, trUpdate.size);
               } else if (posFiles == null) {
                  posFiles = new JournalRecord(trUpdate.file, trUpdate.size);

                  journal.getRecords().put(trUpdate.id, posFiles);
               } else {
                  posFiles.addUpdateFile(trUpdate.file, trUpdate.size);
               }
            }
         }

         if (neg != null) {
            for (JournalUpdate trDelete : neg) {
               if (compactor != null) {
                  compactor.addCommandDelete(trDelete.id, trDelete.file);
               } else {
                  JournalRecord posFiles = journal.getRecords().remove(trDelete.id);

                  if (posFiles != null) {
                     posFiles.delete(trDelete.file);
                  }
               }
            }
         }

         // Now add negs for the pos we added in each file in which there were
         // transactional operations

         for (JournalFile jf : pendingFiles) {
            file.incNegCount(jf);
         }
      }
   }

   public void waitCallbacks() throws InterruptedException {
      waitFirstCallback();
      synchronized (callbackList) {
         for (TransactionCallback callback : callbackList.values()) {
            callback.waitCompletion();
         }
      }
   }

   /**
    * Wait completion at the latest file only
    */
   public void waitCompletion() throws Exception {
      waitFirstCallback();
      currentCallback.waitCompletion();
   }

   private void waitFirstCallback() throws InterruptedException {
      if (currentCallback == null) {
         firstCallbackLatch = new CountDownLatch(1);
         firstCallbackLatch.await();
         firstCallbackLatch = null;
      }
   }

   /**
    * The caller of this method needs to guarantee appendLock.lock before calling this method if being used outside of the lock context.
    * or else potFilesMap could be affected
    */
   public void rollback(final JournalFile file) {
      JournalCompactor compactor = journal.getCompactor();

      if (compacting && compactor != null) {
         compactor.addCommandRollback(this, file);
      } else {
         // Now add negs for the pos we added in each file in which there were
         // transactional operations
         // Note that we do this on rollback as we do on commit, since we need
         // to ensure the file containing
         // the rollback record doesn't get deleted before the files with the
         // transactional operations are deleted
         // Otherwise we may run into problems especially with XA where we are
         // just left with a prepare when the tx
         // has actually been rolled back

         for (JournalFile jf : pendingFiles) {
            file.incNegCount(jf);
         }
      }
   }

   /**
    * The caller of this method needs to guarantee appendLock.lock before calling this method if being used outside of the lock context.
    * or else potFilesMap could be affected
    */
   public void prepare(final JournalFile file) {
      // We don't want the prepare record getting deleted before time

      addFile(file);
   }

   /**
    * Used by load, when the transaction was not loaded correctly
    */
   public void forget() {
      // The transaction was not committed or rolled back in the file, so we
      // reverse any pos counts we added
      for (JournalFile jf : pendingFiles) {
         jf.decPosCount();
      }

   }

   @Override
   public String toString() {
      return "JournalTransaction(" + id + ")";
   }

   private AtomicInteger internalgetCounter(final JournalFile file) {
      if (lastFile != file) {
         lastFile = file;
         counter.set(0);
      }
      return counter;
   }

   private void addFile(final JournalFile file) {
      if (pendingFiles == null) {
         pendingFiles = new HashSet<>();
      }

      if (!pendingFiles.contains(file)) {
         pendingFiles.add(file);

         // We add a pos for the transaction itself in the file - this
         // prevents any transactional operations
         // being deleted before a commit or rollback is written
         file.incPosCount();
      }
   }

   private static class JournalUpdate {

      private final JournalFile file;

      long id;

      int size;

      /**
       * @param file
       * @param id
       * @param size
       */
      private JournalUpdate(final JournalFile file, final long id, final int size) {
         super();
         this.file = file;
         this.id = id;
         this.size = size;
      }

      /**
       * @return the id
       */
      public long getId() {
         return id;
      }
   }
}

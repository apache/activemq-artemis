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

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.io.DummyCallback;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.impl.dataformat.ByteArrayEncoding;

abstract class JournalBase implements Journal
{

   protected final int fileSize;
   private final boolean supportsCallback;

   public JournalBase(boolean supportsCallback, int fileSize)
   {
      if (fileSize < JournalImpl.MIN_FILE_SIZE)
      {
         throw new IllegalArgumentException("File size cannot be less than " + JournalImpl.MIN_FILE_SIZE + " bytes");
      }
      this.supportsCallback = supportsCallback;
      this.fileSize = fileSize;
   }

   public abstract void appendAddRecord(final long id, final byte recordType, final EncodingSupport record,
                                        final boolean sync, final IOCompletion callback) throws Exception;

   public abstract void appendAddRecordTransactional(final long txID, final long id, final byte recordType,
                                                     final EncodingSupport record) throws Exception;

   public abstract void appendCommitRecord(final long txID, final boolean sync, final IOCompletion callback,
                                           boolean lineUpContext) throws Exception;

   public abstract void appendDeleteRecord(final long id, final boolean sync, final IOCompletion callback) throws Exception;

   public abstract void appendDeleteRecordTransactional(final long txID, final long id, final EncodingSupport record) throws Exception;

   public abstract void appendPrepareRecord(final long txID, final EncodingSupport transactionData, final boolean sync,
                                            final IOCompletion callback) throws Exception;

   public abstract void appendUpdateRecord(final long id, final byte recordType, final EncodingSupport record,
                                           final boolean sync, final IOCompletion callback) throws Exception;

   public abstract void appendUpdateRecordTransactional(final long txID, final long id, final byte recordType,
                                                        final EncodingSupport record) throws Exception;

   public abstract void appendRollbackRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception;


   public void appendAddRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception
   {
      appendAddRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   public void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception
   {
      SyncIOCompletion callback = getSyncCallback(sync);

      appendAddRecord(id, recordType, record, sync, callback);

      if (callback != null)
      {
         callback.waitCompletion();
      }
   }

   public void appendCommitRecord(final long txID, final boolean sync) throws Exception
   {
      SyncIOCompletion syncCompletion = getSyncCallback(sync);

      appendCommitRecord(txID, sync, syncCompletion, true);

      if (syncCompletion != null)
      {
         syncCompletion.waitCompletion();
      }
   }

   public void appendCommitRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception
   {
      appendCommitRecord(txID, sync, callback, true);
   }

   public void appendUpdateRecord(final long id, final byte recordType, final byte[] record, final boolean sync) throws Exception
   {
      appendUpdateRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   public void appendUpdateRecordTransactional(final long txID, final long id, final byte recordType,
                                               final byte[] record) throws Exception
   {
      appendUpdateRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   public void appendAddRecordTransactional(final long txID, final long id, final byte recordType, final byte[] record) throws Exception
   {
      appendAddRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   public void appendDeleteRecordTransactional(final long txID, final long id) throws Exception
   {
      appendDeleteRecordTransactional(txID, id, NullEncoding.instance);
   }

   public void appendPrepareRecord(final long txID, final byte[] transactionData, final boolean sync) throws Exception
   {
      appendPrepareRecord(txID, new ByteArrayEncoding(transactionData), sync);
   }

   public void appendPrepareRecord(final long txID, final EncodingSupport transactionData, final boolean sync) throws Exception
   {
      SyncIOCompletion syncCompletion = getSyncCallback(sync);

      appendPrepareRecord(txID, transactionData, sync, syncCompletion);

      if (syncCompletion != null)
      {
         syncCompletion.waitCompletion();
      }
   }

   public void appendDeleteRecordTransactional(final long txID, final long id, final byte[] record) throws Exception
   {
      appendDeleteRecordTransactional(txID, id, new ByteArrayEncoding(record));
   }

   public void
   appendUpdateRecord(final long id, final byte recordType, final EncodingSupport record, final boolean sync) throws Exception
   {
      SyncIOCompletion callback = getSyncCallback(sync);

      appendUpdateRecord(id, recordType, record, sync, callback);

      if (callback != null)
      {
         callback.waitCompletion();
      }
   }

   public void appendRollbackRecord(final long txID, final boolean sync) throws Exception
   {
      SyncIOCompletion syncCompletion = getSyncCallback(sync);

      appendRollbackRecord(txID, sync, syncCompletion);

      if (syncCompletion != null)
      {
         syncCompletion.waitCompletion();
      }

   }

   public void appendDeleteRecord(final long id, final boolean sync) throws Exception
   {
      SyncIOCompletion callback = getSyncCallback(sync);

      appendDeleteRecord(id, sync, callback);

      if (callback != null)
      {
         callback.waitCompletion();
      }
   }

   abstract void scheduleReclaim();

   protected SyncIOCompletion getSyncCallback(final boolean sync)
   {
      if (supportsCallback)
      {
         if (sync)
         {
            return new SimpleWaitIOCallback();
         }
         return DummyCallback.getInstance();
      }
      return null;
   }

   private static final class NullEncoding implements EncodingSupport
   {

      private static NullEncoding instance = new NullEncoding();

      public void decode(final ActiveMQBuffer buffer)
      {
         // no-op
      }

      public void encode(final ActiveMQBuffer buffer)
      {
         // no-op
      }

      public int getEncodeSize()
      {
         return 0;
      }
   }

   public int getFileSize()
   {
      return fileSize;
   }
}

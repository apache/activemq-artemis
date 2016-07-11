/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.extras.benchmarks.journal.gcfree;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;

final class GcFreeJournal extends JournalImpl {

   private final AddJournalRecordEncoder addJournalRecordEncoder = new AddJournalRecordEncoder();
   //TODO replace with thread local pools if not single threaded!
   private ByteBuffer journalRecordBytes = null;

   GcFreeJournal(final int fileSize,
                 final int minFiles,
                 final int poolSize,
                 final int compactMinFiles,
                 final int compactPercentage,
                 final SequentialFileFactory fileFactory,
                 final String filePrefix,
                 final String fileExtension,
                 final int maxAIO) {
      super(fileSize, minFiles, poolSize, compactMinFiles, compactPercentage, fileFactory, filePrefix, fileExtension, maxAIO, 0);
   }

   public static int align(final int value, final int alignment) {
      return (value + (alignment - 1)) & ~(alignment - 1);
   }

   public void appendAddRecord(final long id,
                               final int recordType,
                               final ByteBuffer encodedRecord,
                               final int offset,
                               final int length,
                               final boolean sync) throws Exception {
      final int expectedLength = JournalRecordHeader.BYTES + AddJournalRecordEncoder.expectedSize(length);
      final int alignedLength = align(expectedLength, 8);
      switchFileIfNecessary(alignedLength);
      final JournalFile currentFile = getCurrentFile();
      final int fileId = currentFile.getRecordID();
      if (this.journalRecordBytes == null || this.journalRecordBytes.capacity() < alignedLength) {
         final int newPooledLength = align(alignedLength, 4096);
         //TODO ADD LIMITS OR WARNS IN CASE OF TOO MUCH BIGGER SIZE
         this.journalRecordBytes = ByteBuffer.allocateDirect(newPooledLength);
         this.journalRecordBytes.order(ByteOrder.nativeOrder());
      }
      final long journalRecordHeader = JournalRecordHeader.makeHeader(JournalRecordTypes.ADD_JOURNAL, expectedLength);
      this.journalRecordBytes.putLong(0, journalRecordHeader);
      //use natural stride while encoding: FileId<CompactCount<Id<RecordType<RecordBytes
      this.addJournalRecordEncoder.on(this.journalRecordBytes, JournalRecordHeader.BYTES).fileId(fileId).compactCount(0).id(id).recordType(recordType).record(encodedRecord, offset, length);
      final SequentialFile sequentialFile = currentFile.getFile();
      try {
         this.journalRecordBytes.limit(alignedLength);
         sequentialFile.writeDirect(this.journalRecordBytes, sync);
      }
      finally {
         this.journalRecordBytes.clear();
      }
      //TODO AVOID INDEXING WITH CONCURRENT MAP!
   }

}
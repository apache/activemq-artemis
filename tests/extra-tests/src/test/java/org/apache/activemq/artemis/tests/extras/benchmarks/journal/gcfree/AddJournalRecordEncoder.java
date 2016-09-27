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

import io.netty.util.internal.PlatformDependent;

/**
 * IT IS NOT A FLYWEIGHT BUT AN ENCODER: NEED TO RESPECT THE SEQUENCE OF WRITE:
 * FileId<CompactCount<Id<RecordType<RecordBytes
 */
final class AddJournalRecordEncoder {

   private static final int FILE_ID_OFFSET = 0;
   private static final int COMPACT_COUNT_OFFSET = FILE_ID_OFFSET + 4;
   private static final int ID_OFFSET = COMPACT_COUNT_OFFSET + 4;
   private static final int RECORD_TYPE_OFFSET = ID_OFFSET + 8;
   public static final int BLOCK_SIZE = RECORD_TYPE_OFFSET + 4;

   private ByteBuffer bytes;
   private int offset;
   private int limit;

   public static int expectedSize(int recordBytes) {
      return BLOCK_SIZE + 4 + recordBytes;
   }

   public ByteBuffer bytes() {
      return bytes;
   }

   public int offset() {
      return this.offset;
   }

   public int limit() {
      return this.limit;
   }

   public void limit(int limit) {
      this.limit = limit;
   }

   public AddJournalRecordEncoder on(ByteBuffer bytes, int offset) {
      this.bytes = bytes;
      this.offset = offset;
      this.limit = offset + BLOCK_SIZE;
      return this;
   }

   public AddJournalRecordEncoder fileId(int value) {
      this.bytes.putInt(offset + FILE_ID_OFFSET, value);
      return this;
   }

   public AddJournalRecordEncoder compactCount(int value) {
      this.bytes.putInt(offset + COMPACT_COUNT_OFFSET, value);
      return this;
   }

   public AddJournalRecordEncoder id(long value) {
      this.bytes.putLong(offset + ID_OFFSET, value);
      return this;
   }

   public AddJournalRecordEncoder recordType(int value) {
      this.bytes.putLong(offset + RECORD_TYPE_OFFSET, value);
      return this;
   }

   public AddJournalRecordEncoder noRecord() {
      this.bytes.putInt(this.limit, 0);
      this.limit += 4;
      return this;
   }

   public AddJournalRecordEncoder record(final ByteBuffer recordBytes, final int recordOffset, final int recordLength) {
      this.bytes.putInt(this.limit, recordLength);
      final long dstAddr = PlatformDependent.directBufferAddress(bytes) + this.limit + 4;
      final long srcAddr = PlatformDependent.directBufferAddress(recordBytes) + recordOffset;
      PlatformDependent.copyMemory(srcAddr, dstAddr, recordLength);
      this.limit += (4 + recordLength);
      return this;
   }

   public int encodedLength() {
      return this.limit - this.offset;
   }

}
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

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.io.SequentialFile;
import org.jboss.logging.Logger;

public class JournalFileImpl implements JournalFile {

   private final SequentialFile file;

   private final long fileID;

   private final int recordID;

   private long offset;

   private final AtomicInteger posCount = new AtomicInteger(0);

   private final AtomicInteger liveBytes = new AtomicInteger(0);

   // Flags to be used by determine if the journal file can be reclaimed
   private boolean posReclaimCriteria = false;
   private boolean negReclaimCriteria = false;

   private final AtomicInteger totalNegativeToOthers = new AtomicInteger(0);

   private final int version;

   private final ConcurrentMap<JournalFile, AtomicInteger> negCounts = new ConcurrentHashMap<>();

   private static final Logger logger = Logger.getLogger(JournalFileImpl.class);

   public JournalFileImpl(final SequentialFile file, final long fileID, final int version) {
      this.file = file;

      this.fileID = fileID;

      this.version = version;

      recordID = (int) (fileID & Integer.MAX_VALUE);
   }

   @Override
   public int getPosCount() {
      return posCount.intValue();
   }

   @Override
   public boolean isPosReclaimCriteria() {
      return posReclaimCriteria;
   }

   @Override
   public void setPosReclaimCriteria() {
      this.posReclaimCriteria = true;
   }

   @Override
   public boolean isNegReclaimCriteria() {
      return negReclaimCriteria;
   }

   @Override
   public void setNegReclaimCriteria() {
      this.negReclaimCriteria = true;
   }

   @Override
   public boolean isCanReclaim() {
      return posReclaimCriteria && negReclaimCriteria && !file.isPending();
   }

   @Override
   public void incNegCount(final JournalFile file) {
      incNegCount(file, 1);
   }

   @Override
   public void incNegCount(final JournalFile file, int delta) {
      if (delta <= 0) {
         throw new IllegalArgumentException("delta must be > 0");
      }
      if (file != this) {
         totalNegativeToOthers.addAndGet(delta);
      }
      // GC-free path: including capturing lambdas
      AtomicInteger previous = negCounts.get(file);
      if (previous != null) {
         previous.addAndGet(delta);
         return;
      }
      // no counter yet: slow path, allocating
      previous = negCounts.putIfAbsent(file, new AtomicInteger(delta));
      // racy attempt to create the counter
      if (previous != null) {
         previous.addAndGet(delta);
      }
   }

   @Override
   public int getNegCount(final JournalFile file) {
      AtomicInteger count = negCounts.get(file);

      if (count == null) {
         return 0;
      } else {
         return count.intValue();
      }
   }

   @Override
   public int getJournalVersion() {
      return version;
   }

   @Override
   public void incPosCount() {
      posCount.incrementAndGet();
   }

   @Override
   public void decPosCount() {
      posCount.decrementAndGet();
   }

   public long getOffset() {
      return offset;
   }

   @Override
   public long getFileID() {
      return fileID;
   }

   @Override
   public int getRecordID() {
      return recordID;
   }

   public void setOffset(final long offset) {
      this.offset = offset;
   }

   @Override
   public SequentialFile getFile() {
      return file;
   }

   @Override
   public String toString() {
      try {
         return "JournalFileImpl: (" + file.getFileName() + " id = " + fileID + ", recordID = " + recordID + ")";
      } catch (Exception e) {
         logger.warn("Error during method invocation", e.getMessage(), e);
         return "Error:" + e.toString();
      }
   }

   /**
    * Receive debug information about the journal
    */
   public String debug() {
      StringBuilder builder = new StringBuilder();

      for (Entry<JournalFile, AtomicInteger> entry : negCounts.entrySet()) {
         builder.append(" file = " + entry.getKey() + " negcount value = " + entry.getValue() + "\n");
      }

      return builder.toString();
   }

   @Override
   public void addSize(final int bytes) {
      liveBytes.addAndGet(bytes);
   }

   @Override
   public void decSize(final int bytes) {
      liveBytes.addAndGet(-bytes);
   }

   @Override
   public int getLiveSize() {
      return liveBytes.get();
   }

   @Override
   public int getTotalNegativeToOthers() {
      return totalNegativeToOthers.get();
   }

}

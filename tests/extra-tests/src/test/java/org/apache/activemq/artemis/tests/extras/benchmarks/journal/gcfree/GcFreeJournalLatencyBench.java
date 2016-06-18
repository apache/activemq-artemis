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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;

import net.openhft.chronicle.core.jlbh.JLBH;
import net.openhft.chronicle.core.jlbh.JLBHOptions;
import net.openhft.chronicle.core.jlbh.JLBHTask;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.mapped.MappedSequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;

public class GcFreeJournalLatencyBench implements JLBHTask {

   private static final int FILE_SIZE = JournalImpl.SIZE_HEADER + (1024 * 1024 * 1024);
   private static final JournalType JOURNAL_TYPE = JournalType.MAPPED;
   private static final int ITERATIONS = 100_000;
   private static final int WARMUP_ITERATIONS = 20_000;
   private static final int TARGET_THROUGHPUT = 500_000;
   private static final int TESTS = 5;
   private static int TOTAL_MESSAGES = (ITERATIONS * TESTS + WARMUP_ITERATIONS);
   private static int ENCODED_SIZE = 8;
   private static int CHUNK_BYTES = FILE_SIZE;
   private static int OVERLAP_BYTES = CHUNK_BYTES / 4;
   private final SequentialFileFactory sequentialFileFactory;
   private GcFreeJournal journal;
   private JLBH jlbh;
   private long id;
   private ByteBuffer encodedRecord;

   public GcFreeJournalLatencyBench(SequentialFileFactory sequentialFileFactory) {
      this.sequentialFileFactory = sequentialFileFactory;
   }

   public static void main(String[] args) throws IOException {
      final File journalDir = Files.createTempDirectory("seq_files").toFile();
      journalDir.deleteOnExit();
      final boolean buffered = false;
      final int bufferSize = 4096;
      final int bufferTimeout = 0;
      final int maxIO = -1;
      final boolean logRates = false;
      final IOCriticalErrorListener criticalErrorListener = null;
      final SequentialFileFactory sequentialFileFactory;
      switch (JOURNAL_TYPE) {
         case MAPPED:
            sequentialFileFactory = new MappedSequentialFileFactory(journalDir, criticalErrorListener).chunkBytes(CHUNK_BYTES).overlapBytes(OVERLAP_BYTES);
            break;
         case NIO:
            sequentialFileFactory = new NIOSequentialFileFactory(journalDir, buffered, bufferSize, bufferTimeout, maxIO, logRates, criticalErrorListener);
            break;

         default:
            throw new AssertionError("!?");
      }
      final JLBHOptions lth = new JLBHOptions().warmUpIterations(WARMUP_ITERATIONS).iterations(ITERATIONS).throughput(TARGET_THROUGHPUT).runs(TESTS).recordOSJitter(true).accountForCoordinatedOmmission(true).jlbhTask(new GcFreeJournalLatencyBench(sequentialFileFactory));
      new JLBH(lth).start();
   }

   @Override
   public void init(JLBH jlbh) {
      id = 0;
      this.jlbh = jlbh;
      final int expectedMaxSize = GcFreeJournal.align(JournalRecordHeader.BYTES + AddJournalRecordEncoder.expectedSize(ENCODED_SIZE), 8);
      int numFiles = (int) ((TOTAL_MESSAGES * expectedMaxSize + 512) / FILE_SIZE * 1.3);
      if (numFiles < 2) {
         numFiles = 2;
      }
      this.encodedRecord = ByteBuffer.allocateDirect(ENCODED_SIZE);
      this.encodedRecord.order(ByteOrder.nativeOrder());
      this.journal = new GcFreeJournal(FILE_SIZE, numFiles, numFiles, 0, 0, sequentialFileFactory, "activemq-data", "amq", Integer.MAX_VALUE);
      try {
         journal.start();
         journal.load(new ArrayList<RecordInfo>(), null, null);
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }

   }

   @Override
   public void run(long startTimeNS) {
      id++;
      try {
         journal.appendAddRecord(id, (byte) 0, encodedRecord, 0, ENCODED_SIZE, false);
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
      jlbh.sample(System.nanoTime() - startTimeNS);
   }

   @Override
   public void complete() {
      try {
         journal.stop();
         for (File journalFile : sequentialFileFactory.getDirectory().listFiles()) {
            journalFile.deleteOnExit();
         }
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private enum JournalType {
      MAPPED,
      NIO
   }
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.extras.benchmarks.sequentialfile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;

import net.openhft.chronicle.core.jlbh.JLBH;
import net.openhft.chronicle.core.jlbh.JLBHOptions;
import net.openhft.chronicle.core.jlbh.JLBHTask;
import org.apache.activemq.artemis.core.io.DummyCallback;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.mapped.MappedSequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;

public final class SequentialFileLatencyBench implements JLBHTask {

   private static final JournalType JOURNAL_TYPE = JournalType.MAPPED;
   //NOTE: SUPPORTED ONLY ON *NIX
   private static final boolean SHM = false;
   private static final int JOURNAL_RECORD_SIZE = 8;
   private static final int ITERATIONS = 100_000;
   private static final int WARMUP_ITERATIONS = 20_000;
   private static final int TARGET_THROUGHPUT = 500_000;
   private static final int TESTS = 5;
   private static int CHUNK_BYTES = 4096 * 1024 * 16;
   private static int OVERLAP_BYTES = CHUNK_BYTES / 4;
   private final SequentialFileFactory sequentialFileFactory;
   private SequentialFile sequentialFile;
   private ByteBuffer message;
   private JLBH jlbh;
   public SequentialFileLatencyBench(SequentialFileFactory sequentialFileFactory) {
      this.sequentialFileFactory = sequentialFileFactory;
   }

   public static void main(String[] args) throws IOException {
      final File journalDir;
      if (SHM) {
         journalDir = Files.createDirectory(Paths.get("/dev/shm/seq_files")).toFile();
      }
      else {
         journalDir = Files.createTempDirectory("seq_files").toFile();
      }
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
            sequentialFileFactory = new MappedSequentialFileFactory(journalDir).chunkBytes(CHUNK_BYTES).overlapBytes(OVERLAP_BYTES);
            break;
         case NIO:
            sequentialFileFactory = new NIOSequentialFileFactory(journalDir, buffered, bufferSize, bufferTimeout, maxIO, logRates, criticalErrorListener);
            break;
         default:
            throw new AssertionError("!?");
      }
      final JLBHOptions lth = new JLBHOptions().warmUpIterations(WARMUP_ITERATIONS).iterations(ITERATIONS).throughput(TARGET_THROUGHPUT).runs(TESTS).recordOSJitter(true).accountForCoordinatedOmmission(true).jlbhTask(new SequentialFileLatencyBench(sequentialFileFactory));
      new JLBH(lth).start();
   }

   @Override
   public void init(JLBH jlbh) {
      this.jlbh = jlbh;
      this.sequentialFile = this.sequentialFileFactory.createSequentialFile(Long.toString(System.nanoTime()));
      try {
         this.sequentialFile.open(-1, false);
         final File file = this.sequentialFile.getJavaFile();
         file.deleteOnExit();
         System.out.println("sequentialFile: " + file);
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
      this.message = this.sequentialFileFactory.allocateDirectBuffer(JOURNAL_RECORD_SIZE).order(ByteOrder.nativeOrder());

   }

   @Override
   public void run(long startTimeNS) {
      message.position(0);
      try {
         sequentialFile.writeDirect(message, false, DummyCallback.getInstance());
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
      jlbh.sample(System.nanoTime() - startTimeNS);
   }

   @Override
   public void complete() {
      sequentialFileFactory.releaseDirectBuffer(message);
      try {
         sequentialFile.close();
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
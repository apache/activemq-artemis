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

package org.apache.activemq.artemis.tests.extras.benchmarks.journal;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import io.netty.util.internal.shaded.org.jctools.util.UnsafeAccess;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.io.DummyCallback;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;

public class JournalImplAppendTptBench {

   private static final int FILE_SIZE = 1024 * 1024 * 1024;
   private static final int ITERATIONS = 100_000;
   private static final int WARMUP_ITERATIONS = 20_000;
   private static final int TESTS = 10;
   private static int TOTAL_MESSAGES = (ITERATIONS * TESTS + WARMUP_ITERATIONS);
   private static int ENCODED_SIZE = 8;
   private static final EncodingSupport encodingSupport = new EncodingSupport() {
      @Override
      public int getEncodeSize() {
         return ENCODED_SIZE;
      }

      @Override
      public void encode(ActiveMQBuffer buffer) {
         buffer.writerIndex(buffer.writerIndex() + ENCODED_SIZE);
      }

      @Override
      public void decode(ActiveMQBuffer buffer) {
         throw new UnsupportedOperationException();
      }
   };
   private static byte RECORD_TYPE = (byte) 1;

   public static void main(String[] args) throws Exception {
      int numFiles = (int) ((TOTAL_MESSAGES * 1024 + 512) / FILE_SIZE * 1.3);
      if (numFiles < 2) {
         numFiles = 2;
      }
      final File journalDir = new File(System.getProperty("java.io.tmpdir"));
      final SequentialFileFactory sequentialFileFactory = new NIOSequentialFileFactory(journalDir, false, 0, 0, 1, false, (code, message, file) -> {
      }) {

         private final ByteBuffer buffer = ByteBuffer.allocateDirect(UnsafeAccess.UNSAFE.pageSize());

         @Override
         public ByteBuffer newBuffer(int size) {
            //little trick to reduce GC pressure
            if (size > buffer.capacity())
               throw new IllegalStateException("IMPOSSIBLE!");
            this.buffer.clear();
            this.buffer.limit(size);
            return this.buffer;
         }

         @Override
         public boolean isSupportsCallbacks() {
            return true;
         }
      };
      final JournalImpl journal = new JournalImpl(() -> Runnable::run, FILE_SIZE, numFiles, numFiles, 0, 0, sequentialFileFactory, "activemq-data", "amq", 1, 0);
      journal.start();
      journal.load(new ArrayList<>(), null, null);
      try {
         long id = 0;
         for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            journal.appendAddRecord(id, RECORD_TYPE, encodingSupport, false, DummyCallback.getInstance());
            id++;
         }
         for (int t = 0; t < TESTS; t++) {
            System.gc();
            final long start = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
               journal.appendAddRecord(id, RECORD_TYPE, encodingSupport, false, DummyCallback.getInstance());
               id++;
            }
            final long elapsed = System.nanoTime() - start;
            System.out.println((ITERATIONS * 1000_000_000L) / elapsed);
         }
      } finally {
         journal.stop();
         for (File file : journalDir.listFiles()) {
            file.deleteOnExit();
         }
      }
   }

}

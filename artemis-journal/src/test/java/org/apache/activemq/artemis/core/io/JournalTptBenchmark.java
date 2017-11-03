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

package org.apache.activemq.artemis.core.io;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;
import org.apache.activemq.artemis.ArtemisConstants;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.io.mapped.MappedSequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;

/**
 * To benchmark Type.Aio you need to define -Djava.library.path=${project-root}/native/src/.libs when calling the JVM
 */
public class JournalTptBenchmark {

   public static void main(String[] args) throws Exception {
      final boolean useDefaultIoExecutor = true;
      final int fileSize = 10 * 1024 * 1024;
      final boolean dataSync = false;
      final Type type = Type.Mapped;
      final int tests = 10;
      final int warmup = 20_000;
      final int measurements = 100_000;
      final int msgSize = 100;
      final byte[] msgContent = new byte[msgSize];
      Arrays.fill(msgContent, (byte) 1);
      final int totalMessages = (measurements * tests + warmup);
      final File tmpDirectory = new File("./");
      //using the default configuration when the broker starts!
      final SequentialFileFactory factory;
      switch (type) {

         case Mapped:
            factory = new MappedSequentialFileFactory(tmpDirectory, fileSize, true, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, null)
               .setDatasync(dataSync);
            break;
         case Nio:
            factory = new NIOSequentialFileFactory(tmpDirectory, true, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO, 1, false, null, null).setDatasync(dataSync);
            break;
         case Aio:
            factory = new AIOSequentialFileFactory(tmpDirectory, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, 500, false, null, null).setDatasync(dataSync);
            //disable it when using directly the same buffer: ((AIOSequentialFileFactory)factory).disableBufferReuse();
            if (!LibaioContext.isLoaded()) {
               throw new IllegalStateException("lib AIO not loaded!");
            }
            break;
         default:
            throw new AssertionError("unsupported case");
      }

      int numFiles = (int) (totalMessages * factory.calculateBlockSize(msgSize)) / fileSize;
      if (numFiles < 2) {
         numFiles = 2;
      }
      ExecutorService service = null;
      final Journal journal;
      if (useDefaultIoExecutor) {
         journal = new JournalImpl(fileSize, numFiles, numFiles, Integer.MAX_VALUE, 100, factory, "activemq-data", "amq", factory.getMaxIO());
         journal.start();
      } else {
         final ArrayList<MpscArrayQueue<Runnable>> tasks = new ArrayList<>();
         service = Executors.newSingleThreadExecutor();
         journal = new JournalImpl(() -> new ArtemisExecutor() {

            private final MpscArrayQueue<Runnable> taskQueue = new MpscArrayQueue<>(1024);

            {
               tasks.add(taskQueue);
            }

            @Override
            public void execute(Runnable command) {
               while (!taskQueue.offer(command)) {
                  LockSupport.parkNanos(1L);
               }
            }
         }, fileSize, numFiles, numFiles, Integer.MAX_VALUE, 100, factory, "activemq-data", "amq", factory.getMaxIO(), 0);
         journal.start();
         service.execute(() -> {
            final int size = tasks.size();
            final int capacity = 1024;
            while (!Thread.currentThread().isInterrupted()) {
               for (int i = 0; i < size; i++) {
                  final MpscArrayQueue<Runnable> runnables = tasks.get(i);
                  for (int j = 0; j < capacity; j++) {
                     final Runnable task = runnables.poll();
                     if (task == null) {
                        break;
                     }
                     try {
                        task.run();
                     } catch (Throwable t) {
                        System.err.println(t);
                     }
                  }
               }
            }

         });
      }
      try {
         journal.load(new ArrayList<RecordInfo>(), null, null);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      try {
         final EncodingSupport encodingSupport = new EncodingSupport() {
            @Override
            public int getEncodeSize() {
               return msgSize;
            }

            @Override
            public void encode(ActiveMQBuffer buffer) {
               final int writerIndex = buffer.writerIndex();
               buffer.setBytes(writerIndex, msgContent);
               buffer.writerIndex(writerIndex + msgSize);
            }

            @Override
            public void decode(ActiveMQBuffer buffer) {

            }
         };
         long id = 1;
         {
            final long elapsed = writeMeasurements(id, journal, encodingSupport, warmup);
            id += warmup;
            System.out.println("warmup:" + (measurements * 1000_000_000L) / elapsed + " ops/sec");
         }
         for (int t = 0; t < tests; t++) {
            final long elapsed = writeMeasurements(id, journal, encodingSupport, measurements);
            System.out.println((measurements * 1000_000_000L) / elapsed + " ops/sec");
            id += warmup;
         }

      } finally {
         journal.stop();
         if (service != null) {
            service.shutdown();
         }
         final File[] fileToDeletes = tmpDirectory.listFiles();
         System.out.println("Files to deletes" + Arrays.toString(fileToDeletes));
         Stream.of(fileToDeletes).forEach(File::delete);
      }
   }

   private static long writeMeasurements(long id,
                                         Journal journal,
                                         EncodingSupport encodingSupport,
                                         int measurements) throws Exception {
      System.gc();
      TimeUnit.SECONDS.sleep(2);

      final long start = System.nanoTime();
      for (int i = 0; i < measurements; i++) {
         write(id, journal, encodingSupport);
         id++;
      }
      final long elapsed = System.nanoTime() - start;
      return elapsed;
   }

   private static void write(long id, Journal journal, EncodingSupport encodingSupport) throws Exception {
      journal.appendAddRecord(id, (byte) 1, encodingSupport, false);
      journal.appendUpdateRecord(id, (byte) 1, encodingSupport, true);
   }

   private enum Type {

      Mapped, Nio, Aio

   }
}

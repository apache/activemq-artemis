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

package org.apache.activemq.artemis.cli.commands.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.io.mapped.MappedSequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.utils.ReusableLatch;

/**
 * It will perform a simple test to evaluate how many syncs a disk can make per second
 * * *
 */
public class SyncCalculation {

   //uses nanoseconds to avoid any conversion cost: useful when performing operation on mapped journal without fsync or with RAM-like devices
   private static final long MAX_FLUSH_NANOS = TimeUnit.SECONDS.toNanos(5);

   /**
    * It will perform {@code tries} write tests of {@code blockSize * blocks} bytes and returning the lowest elapsed time to perform a try.
    *
    * <p>
    * Please configure {@code blocks >= -XX:CompileThreshold} (ie by default on most JVMs is 10000) to favour the best JIT/OSR compilation (ie: Just In Time/On Stack Replacement)
    * if the test is running on a temporary file-system (eg: tmpfs on Linux) or without {@code fsync}.
    * <p>
    * NOTE: The write latencies are provided only if {@code verbose && !(journalType == JournalType.ASYNCIO && !syncWrites)} (ie are used effective synchronous writes).
    *
    * @param datafolder  the folder where the journal files will be stored
    * @param blockSize   the size in bytes of each write on the journal
    * @param blocks      the number of {@code blockSize} writes performed on each try
    * @param tries       the number of tests
    * @param verbose     {@code true} to make the output verbose, {@code false} otherwise
    * @param fsync       if {@code true} the test is performing full durable writes, {@code false} otherwise
    * @param syncWrites  if {@code true} each write is performed only if the previous one is completed, {@code false} otherwise (ie each try will wait only the last write)
    * @param fileName    the name of the journal file used for the test
    * @param maxAIO      the max number of in-flight IO requests (if {@code journalType} will support it)
    * @param journalType the {@link JournalType} used for the tests
    * @return the lowest elapsed time (in {@link TimeUnit#MILLISECONDS}) to perform a try
    * @throws Exception
    */
   public static long syncTest(File datafolder,
                               int blockSize,
                               int blocks,
                               int tries,
                               boolean verbose,
                               boolean fsync,
                               boolean syncWrites,
                               String fileName,
                               int maxAIO,
                               JournalType journalType) throws Exception {
      SequentialFileFactory factory = newFactory(datafolder, fsync, journalType, blockSize * blocks, maxAIO);
      final boolean asyncWrites = journalType == JournalType.ASYNCIO && !syncWrites;
      //the write latencies could be taken only when writes are effectively synchronous
      final Histogram writeLatencies = (verbose && !asyncWrites) ? new Histogram(MAX_FLUSH_NANOS, 2) : null;

      if (journalType == JournalType.ASYNCIO && syncWrites) {
         System.out.println();
         System.out.println("*******************************************************************************************");
         System.out.println("*** Notice: The recommendation for AsyncIO journal is to not use --sync-writes          ***");
         System.out.println("***         The measures here will be useful to understand your device                  ***");
         System.out.println("***         however the result here won't represent the best configuration option       ***");
         System.out.println("*******************************************************************************************");
         System.out.println();
      }

      if (verbose) {
         System.out.println("Using " + factory.getClass().getName() + " to calculate sync times, alignment=" + factory.getAlignment());
         if (writeLatencies == null) {
            System.out.println("*** Use --sync-writes if you want to see a histogram for each write performed ***");
         }
      }
      SequentialFile file = factory.createSequentialFile(fileName);
      //to be sure that a process/thread crash won't leave the dataFolder with garbage files
      file.getJavaFile().deleteOnExit();
      try {
         final ByteBuffer bufferBlock = allocateAlignedBlock(blockSize, factory);

         // making sure the blockSize matches the device
         blockSize = bufferBlock.remaining();

         file.delete();
         file.open();

         file.fill(blockSize * blocks);

         file.close();

         long[] result = new long[tries];

         final ReusableLatch latch = new ReusableLatch(0);

         IOCallback callback = new IOCallback() {
            @Override
            public void done() {
               latch.countDown();
            }

            @Override
            public void onError(int errorCode, String errorMessage) {

            }
         };

         DecimalFormat dcformat = new DecimalFormat("###.##");
         for (int ntry = 0; ntry < tries; ntry++) {

            if (verbose) {
               System.out.println("**************************************************");
               System.out.println(ntry + " of " + tries + " calculation");
            }
            file.open();
            file.position(0);
            long start = System.currentTimeMillis();
            for (int i = 0; i < blocks; i++) {
               bufferBlock.position(0);
               latch.countUp();
               long startWrite = 0;
               if (writeLatencies != null) {
                  startWrite = System.nanoTime();
               }
               file.writeDirect(bufferBlock, true, callback);

               if (syncWrites) {
                  flushLatch(latch);
               }
               if (writeLatencies != null) {
                  final long elapsedWriteNanos = System.nanoTime() - startWrite;
                  writeLatencies.recordValue(elapsedWriteNanos);
               }
            }

            if (!syncWrites) flushLatch(latch);

            long end = System.currentTimeMillis();

            result[ntry] = (end - start);

            if (verbose) {
               double writesPerMillisecond = (double) blocks / (double) result[ntry];
               System.out.println("Time = " + result[ntry] + " milliseconds");
               System.out.println("Writes / millisecond = " + dcformat.format(writesPerMillisecond));
               System.out.println("bufferTimeout = " + toNanos(result[ntry], blocks, verbose));
               System.out.println("**************************************************");
            }
            file.close();

            if (ntry == 0 && writeLatencies != null) {
               writeLatencies.reset(); // discarding the first one.. some warmup time
            }
         }

         factory.releaseDirectBuffer(bufferBlock);

         if (writeLatencies != null) {
            System.out.println("Write Latencies Percentile Distribution in microseconds");
            //print latencies in us -> (ns * 1000d)

            System.out.println("*****************************************************************");
            writeLatencies.outputPercentileDistribution(System.out, 1000d);
            System.out.println();
            System.out.println("*****************************************************************");
            System.out.println("*** this may be useful to generate charts if you like charts: ***");
            System.out.println("*** http://hdrhistogram.github.io/HdrHistogram/plotFiles.html ***");
            System.out.println("*****************************************************************");
            System.out.println();

            writeLatencies.reset();
         }

         long totalTime = Long.MAX_VALUE;
         for (int i = 0; i < tries; i++) {
            if (result[i] < totalTime) {
               totalTime = result[i];
            }
         }

         return totalTime;
      } finally {
         try {
            file.close();
         } catch (Exception e) {
         }
         try {
            file.delete();
         } catch (Exception e) {
         }
         try {
            factory.stop();
         } catch (Exception e) {
         }
      }
   }

   private static ByteBuffer allocateAlignedBlock(int blockSize, SequentialFileFactory factory) {
      final ByteBuffer bufferBlock = factory.newBuffer(blockSize);
      final byte[] block = new byte[bufferBlock.remaining()];
      Arrays.fill(block, (byte) 't');
      bufferBlock.put(block);
      bufferBlock.position(0);
      return bufferBlock;
   }

   private static void flushLatch(ReusableLatch latch) throws InterruptedException, IOException {
      if (!latch.await(MAX_FLUSH_NANOS, TimeUnit.NANOSECONDS)) {
         throw new IOException("Timed out on receiving IO callback");
      }
   }

   public static long toNanos(long time, long blocks, boolean verbose) {

      double blocksPerMillisecond = (double) blocks / (double) (time);

      if (verbose) {
         System.out.println("Blocks per millisecond::" + blocksPerMillisecond);
      }

      long nanoSeconds = TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS);

      long timeWait = (long) (nanoSeconds / blocksPerMillisecond);

      if (verbose) {
         System.out.println("your system could make a sync every " + timeWait + " nanoseconds, and this will be your timeout");
      }

      return timeWait;
   }

   private static SequentialFileFactory newFactory(File datafolder, boolean datasync, JournalType journalType, int fileSize, int maxAIO) {
      SequentialFileFactory factory;

      if (journalType == JournalType.ASYNCIO && !LibaioContext.isLoaded()) {
         journalType = JournalType.NIO;
      }

      switch (journalType) {

         case NIO:
            factory = new NIOSequentialFileFactory(datafolder, 1).setDatasync(datasync);
            ((NIOSequentialFileFactory) factory).disableBufferReuse();
            factory.start();
            return factory;
         case ASYNCIO:
            factory = new AIOSequentialFileFactory(datafolder, maxAIO).setDatasync(datasync);
            factory.start();
            ((AIOSequentialFileFactory) factory).disableBufferReuse();
            return factory;
         case MAPPED:
            factory = new MappedSequentialFileFactory(datafolder, fileSize, false, 0, 0, null)
               .setDatasync(datasync)
               .disableBufferReuse();
            factory.start();
            return factory;
         default:
            throw ActiveMQMessageBundle.BUNDLE.invalidJournalType2(journalType);
      }
   }
}

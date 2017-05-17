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

package org.apache.activemq.artemis.cli.commands.tools.journal;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.Configurable;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.io.buffer.counters.FlushSampleFlyweight;
import org.apache.activemq.artemis.core.io.buffer.counters.Profiler;
import org.apache.activemq.artemis.utils.collections.MulticastBuffer;

@Command(name = "profile-journal", description = "Provide sampling of performance counters of a profiled journal")
public class ProfileJournal extends Configurable {

   @Option(name = "--in", description = "The input counter file to be used to sample profiled data")
   public String fileIn;

   @Option(name = "--out", description = "Print the CSV output in a text file or stdout if not defined")
   public String fileOut;

   @Option(name = "--freq", description = "Profile at this frequency (Hz) or will use 1/100 of the configured Journal timeout")
   public int freq = -1;

   @Option(name = "--separator", description = "The column separator, whitespace if not specified")
   public String separator = " ";

   @Option(name = "--raw-time", description = "Uses absolute nanoseconds timestamps, false if not defined")
   public boolean rawTimeStamp = false;

   @Option(name = "--bytes", description = "Add bytes flushed column in the CSV output, false if not defined")
   public boolean addBytes = false;

   @Option(name = "--no-refresh", description = "Disable console refresh while printing profiler statistics on STDERR, false if not defined")
   public boolean noRefresh = false;

   private final AtomicLong samplesCounters = new AtomicLong(0);
   private long totalMissedSamples = 0;
   private long totalSyncs = 0;
   private long totalBytes = 0;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      try {
         final FileConfiguration fileConfiguration = getFileConfiguration();
         final long nanoPeriod;
         if (freq > 0) {
            nanoPeriod = freq > 0 ? (TimeUnit.SECONDS.toNanos(1) / freq) : 0;
         } else {
            switch (fileConfiguration.getJournalType()) {

               case NIO:
                  nanoPeriod = fileConfiguration.getJournalBufferTimeout_NIO() / 100;
                  break;
               case ASYNCIO:
                  nanoPeriod = fileConfiguration.getJournalBufferTimeout_AIO() / 100;
                  break;
               case MAPPED:
                  nanoPeriod = fileConfiguration.getJournalBufferTimeout_NIO() / 100;
                  break;
               default:
                  throw new AssertionError("unsupported journal type!");
            }
         }
         final File counterFile;
         if (fileIn == null) {
            counterFile = findLastModifiedFlushPerformanceCountersFile();
         } else {
            counterFile = new File(fileIn);
         }
         final MulticastBuffer.Reader sampleReader = MulticastBuffer.reader(counterFile, FlushSampleFlyweight.maxSize());
         final PrintStream outStream = fileOut == null ? this.context.out : new PrintStream(new FileOutputStream(fileOut), false);

         final Thread samplerProfilerStatTask = new Thread(() -> {
            try {
               printJournalStats(nanoPeriod, rawTimeStamp, outStream, sampleReader);
            } finally {
               outStream.close();
            }
         });
         final Thread printCommandStats = new Thread(this::printCommandStats);
         Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            //request interrupt
            samplerProfilerStatTask.interrupt();
            try {
               samplerProfilerStatTask.join();
            } catch (Throwable t) {
               //silent
            } finally {
               printCommandStats.interrupt();
               try {
                  printCommandStats.join();
               } catch (Throwable t1) {
                  //silent
               }
            }
         }));
         samplerProfilerStatTask.start();
         printCommandStats.start();
      } catch (Exception e) {
         treatError(e, "data", "stat");
      }

      return null;
   }

   private static File findLastModifiedFlushPerformanceCountersFile() {
      final File[] countersFiles = Profiler.listFlushInstrumentationCountersFiles();
      if (countersFiles.length == 0) {
         throw new IllegalStateException("need to enable flush journal profiling adding <journal-profiler>true</journal-profiler> on broker.xml under <core> element!");
      }
      if (countersFiles.length > 1) {
         Arrays.sort(countersFiles, Comparator.comparingLong(File::lastModified));
         //the last file is the one last modified more recently -> the biggest lastModified value
         return countersFiles[countersFiles.length - 1];
      } else {
         return countersFiles[0];
      }
   }

   private void printJournalStats(long nanoPeriod,
                                  boolean rawTimestamp,
                                  PrintStream out,
                                  MulticastBuffer.Reader sampleReader) {
      final boolean verbose = this.verbose;
      final boolean addBytes = this.addBytes;
      final String separator = this.separator;
      long firstTimestamp = Long.MAX_VALUE;
      long totalValidSamples = 0;
      final FlushSampleFlyweight flushSample = new FlushSampleFlyweight();
      flushSample.wrap(sampleReader.buffer(), 0);
      //drop the first read sample because probably it is an old one
      sampleReader.hasNext();
      final long lostInThePast = sampleReader.lost();
      final StringBuilder lineBuilder = new StringBuilder();
      //force first resizing to avoid jitters during sampling
      lineBuilder.append(Long.MAX_VALUE).append(separator).append(Long.MAX_VALUE);
      if (addBytes) {
         lineBuilder.append(separator).append(Long.MAX_VALUE);
      }
      lineBuilder.append('\n').setLength(0);
      final Thread thread = Thread.currentThread();
      while (!thread.isInterrupted()) {
         while (sampleReader.hasNext()) {
            //read samples
            final long timeStamp = flushSample.timeStamp();
            final long latency = flushSample.latency();
            //statistics collection
            long flushedBytes = 0;
            boolean sync = false;
            if (verbose) {
               flushedBytes = flushSample.flushedBytes();
               sync = flushSample.sync();
            } else if (addBytes) {
               flushedBytes = flushSample.flushedBytes();
            }

            //are the collected samples still valid?
            if (sampleReader.validateMessage()) {
               //normalize the read timestamp
               final long normalizedTimestamp;
               if (!rawTimestamp && firstTimestamp == Long.MAX_VALUE) {
                  firstTimestamp = timeStamp;
                  normalizedTimestamp = 0;
               } else {
                  if (!rawTimestamp) {
                     normalizedTimestamp = timeStamp - firstTimestamp;
                  } else {
                     normalizedTimestamp = timeStamp;
                  }
               }
               totalValidSamples++;
               if (verbose) {
                  this.totalBytes += flushedBytes;
                  if (sync) {
                     this.totalSyncs++;
                  }
               }
               //append the validated samples
               lineBuilder.setLength(0);
               lineBuilder.append(normalizedTimestamp).append(separator).append(latency);
               if (addBytes) {
                  lineBuilder.append(separator).append(flushedBytes);
               }
               lineBuilder.append('\n');
               append8bit(out, lineBuilder);
            }
            this.totalMissedSamples = sampleReader.lost() - lostInThePast;
            //write release statistics
            this.samplesCounters.lazySet(totalValidSamples);
         }
         this.totalMissedSamples = sampleReader.lost() - lostInThePast;
         //write release statistics
         this.samplesCounters.lazySet(totalValidSamples);
         precisePark(nanoPeriod);
      }

   }

   private static void precisePark(long nanoPeriod) {
      if (nanoPeriod > 100_000) {
         //take it easy
         LockSupport.parkNanos(nanoPeriod);
      } else if (nanoPeriod > 1000) {
         //yield wait
         final long deadLine = System.nanoTime() + nanoPeriod;
         while (System.nanoTime() < deadLine) {
            Thread.yield();
         }
      } else if (nanoPeriod > 0) {
         //spin wait
         final long deadLine = System.nanoTime() + nanoPeriod;
         while (System.nanoTime() < deadLine) {
            //burn CPU burn!!! :)
         }
      }
   }

   //Zero garbage print implementation that avoid any encoding performance issues using a simple 8bit ASCII encoding
   private static void append8bit(PrintStream out, CharSequence charSequence) {
      final int length = charSequence.length();
      for (int i = 0; i < length; i++) {
         final int c = charSequence.charAt(i);
         out.write(c);
      }
   }

   private void printCommandStats() {
      final PrintStream printStream = this.context.err;
      final boolean refresh = !this.noRefresh;
      final boolean verbose = this.verbose;
      final long start = System.currentTimeMillis();
      long lastTimeStamp = start;
      long lastPoll = samplesCounters.get();
      long lastLost = this.totalMissedSamples;
      long lastSyncs = 0;
      long lastBytes = 0;
      if (verbose) {
         lastSyncs = this.totalSyncs;
         lastBytes = this.totalBytes;
      }

      final long TEN_kB = 10_000L;
      final long TEN_MB = 10_000_000L;
      final Thread thread = Thread.currentThread();
      while (!thread.isInterrupted()) {
         LockSupport.parkNanos(1_000_000_000L);
         final long newTimeStamp = System.currentTimeMillis();
         final long newPoll = samplesCounters.get();
         //read acquire samples -> they could be overwritten by the newer ones, but with no tearing
         final long newLost = this.totalMissedSamples;
         long newSyncs = 0;
         long newBytes = 0;
         if (verbose) {
            newSyncs = this.totalSyncs;
            newBytes = this.totalBytes;
         }
         final long lost = newLost - lastLost;
         final long poll = newPoll - lastPoll;
         final long duration = newTimeStamp - lastTimeStamp;
         if (refresh) {
            printStream.print("\033[H\033[2J");
         }
         if (!verbose) {
            printStream.format("Duration %dms - %,d samples - %,d missed%n", duration, poll, lost);
         } else {
            final long syncs = newSyncs - lastSyncs;
            final long bytes = newBytes - lastBytes;
            if (bytes >= TEN_MB) {
               final long megaBytes = bytes / 1000_000L;
               printStream.format("Duration %dms - %,d samples - %,d missed - %,d MB - %,d sync%n", duration, poll, lost, megaBytes, syncs);
            } else if (bytes >= TEN_kB) {
               final long kiloBytes = bytes / 1000L;
               printStream.format("Duration %dms - %,d samples - %,d missed - %,d kB - %,d sync%n", duration, poll, lost, kiloBytes, syncs);
            } else {
               printStream.format("Duration %dms - %,d samples - %,d missed - %,d bytes - %,d sync%n", duration, poll, lost, bytes, syncs);
            }
            lastSyncs = newSyncs;
            lastBytes = newBytes;
         }
         lastTimeStamp = newTimeStamp;
         lastLost = newLost;
         lastPoll = newPoll;
      }
      final long elapsedMillis = lastTimeStamp - start;
      if (refresh) {
         printStream.print("\033[H\033[2J");
      }
      printStream.format("Total Duration %dms - %,d total samples - %,d total missed%n", elapsedMillis, lastPoll, lastLost);
   }
}

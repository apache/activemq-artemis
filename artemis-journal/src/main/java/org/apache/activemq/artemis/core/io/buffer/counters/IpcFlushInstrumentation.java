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

package org.apache.activemq.artemis.core.io.buffer.counters;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.activemq.artemis.utils.Env;
import org.apache.activemq.artemis.utils.collections.MulticastBuffer;

final class IpcFlushInstrumentation implements FlushProfiler {

   private static final File INSTRUMENTATION_COUNTERS_DIR;

   static {
      File chosenDir = null;
      if (Env.isLinuxOs()) {
         final File devShmDir = new File("/dev/shm");
         if (devShmDir.exists()) {
            chosenDir = devShmDir;
         }
      }
      if (chosenDir == null) {
         chosenDir = new File(System.getProperty("java.io.tmpdir"));
      }
      INSTRUMENTATION_COUNTERS_DIR = chosenDir;
   }

   private final MulticastBuffer.Writer sampleWriter;
   private final FlushSampleFlyweight flushSampleFlyweight;
   private long startFlush;
   private int bytes;
   private boolean sync;

   private IpcFlushInstrumentation(MulticastBuffer.Writer sampleWriter) {
      this.sampleWriter = sampleWriter;
      this.flushSampleFlyweight = new FlushSampleFlyweight();
      this.flushSampleFlyweight.wrap(this.sampleWriter.buffer(), 0);
   }

   @Override
   public void onStartFlush(int bytes, boolean sync) {
      this.startFlush = System.nanoTime();
      this.bytes = bytes;
      this.sync = sync;
   }

   @Override
   public void onCompletedFlush() {
      final MulticastBuffer.Writer sampleWriter = this.sampleWriter;
      final FlushSampleFlyweight flushSampleFlyweight = this.flushSampleFlyweight;
      final long startFlush = this.startFlush;
      final int bytes = this.bytes;
      final boolean sync = this.sync;
      final long elapsed = System.nanoTime() - startFlush;
      assert elapsed >= 0 : "Missed a completed sample or non monotonic timestamp!";
      final long version = sampleWriter.claim();
      try {
         flushSampleFlyweight.timeStamp(startFlush).latency(elapsed).flushedBytes(bytes).sync(sync);
      } finally {
         sampleWriter.commit(version);
      }
   }

   private static final String prefix = "journal";
   private static final String suffix = ".bin";
   private static final String defaultInstrumentationDataFileName = "journal.%date.%pid.bin";

   private static String fillPidAndDate(String logFileName) {
      final String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
      final String processID = processName.split("@")[0];
      final SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd.HHmm");
      final String formattedDate = formatter.format(new Date());

      logFileName = logFileName.replaceAll("%pid", processID);
      logFileName = logFileName.replaceAll("%date", formattedDate);
      return logFileName;
   }

   public static File[] listCountersFiles() {
      return INSTRUMENTATION_COUNTERS_DIR.listFiles((dir, name) -> name.startsWith(prefix) && name.endsWith(suffix));
   }

   public static IpcFlushInstrumentation create() throws IOException {
      final String fileName = fillPidAndDate(defaultInstrumentationDataFileName);
      final File file = new File(INSTRUMENTATION_COUNTERS_DIR, fileName);
      file.deleteOnExit();
      final MulticastBuffer.Writer writer = MulticastBuffer.writer(file, FlushSampleFlyweight.maxSize());
      return new IpcFlushInstrumentation(writer);
   }
}

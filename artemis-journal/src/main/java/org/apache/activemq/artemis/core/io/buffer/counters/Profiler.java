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

import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.apache.activemq.artemis.utils.collections.MulticastBuffer;

/**
 * Factory class to instantiate {@link FlushProfiler}s classes.
 */
public final class Profiler {

   private static final FlushProfiler BLACK_HOLE_PROFILER = new FlushProfiler() {
      @Override
      public void onStartFlush(int bytes, boolean sync) {

      }

      @Override
      public void onCompletedFlush() {

      }
   };

   private Profiler() {

   }

   /**
    * Returns a {@link FlushProfiler} that is not recording any profile sample.
    */
   public static FlushProfiler none() {
      return BLACK_HOLE_PROFILER;
   }

   /**
    * Returns the list of files with profile samples that could be read using {@link MulticastBuffer#reader(File, int)}.
    */
   public static File[] listFlushInstrumentationCountersFiles() {
      return IpcFlushInstrumentation.listCountersFiles();
   }

   /**
    * Returns a {@link FlushProfiler} instance that records profile samples in the form of {@link FlushSampleFlyweight}.
    * The profiled samples are written into {@link #listFlushInstrumentationCountersFiles()}.
    */
   public static FlushProfiler instrumented() {
      try {
         return IpcFlushInstrumentation.create();
      } catch (IOException e) {
         ActiveMQJournalLogger.LOGGER.warn("Can't instantiate a flush profiler!", e);
         return none();
      }
   }
}

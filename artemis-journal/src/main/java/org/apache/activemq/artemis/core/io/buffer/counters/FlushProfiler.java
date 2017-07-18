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

/**
 * A profiler that can collect journal's flush operation samples (ie: {@link FlushSampleFlyweight}).
 *
 * {@link Profiler} is a utility class that provides not thread-safe implementations of this interface.
 * <p>It is recommended practice to follow this usage pattern:
 *
 * <pre> {@code
 *    // ...
 *    profiler.onStartFlush(bytes, sync);
 *    try {
 *       // ... performs the flush operation
 *    } finally {
 *       profiler.onCompletedFlush();
 *    }
 *    // ...
 * }</pre>
 */
public interface FlushProfiler {

   /**
    * Marks the start of a flush operation. MUST be followed by a {@link #onCompletedFlush} call.
    *
    * @param bytes is the size in bytes of flushed data into the journal
    * @param sync  if {@code true} it requires the journal to perform a durable write, {@code false} otherwise
    */
   void onStartFlush(int bytes, boolean sync);

   /**
    * Marks the end of the started flush operation.
    */
   void onCompletedFlush();
}

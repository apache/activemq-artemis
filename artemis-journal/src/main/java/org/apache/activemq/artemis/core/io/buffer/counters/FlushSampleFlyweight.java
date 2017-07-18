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

import java.nio.ByteBuffer;

/**
 * Struct-like flyweight that represent the binary form of a flush sample collected by a {@link FlushProfiler}.
 */
public final class FlushSampleFlyweight {

   //each field is field-size-aligned for performance reasons
   private static final int TIMESTAMP_OFFSET = 0;
   private static final int TIMESTAMP_BYTES = 8;
   private static final int LATENCY_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_BYTES;
   private static final int LATENCY_BYTES = 8;
   private static final int FLUSHED_BYTES_OFFSET = LATENCY_OFFSET + LATENCY_BYTES;
   private static final int FLUSHED_BYTES_BYTES = 4;

   private static final int SYNC_OFFSET = FLUSHED_BYTES_OFFSET + FLUSHED_BYTES_BYTES;
   private static final int SYNC_BYTES = 1;

   private static final int MAX_SIZE = SYNC_OFFSET + SYNC_BYTES;

   private ByteBuffer byteBuffer = null;
   private int index = -1;

   public ByteBuffer buffer() {
      return this.byteBuffer;
   }

   public int index() {
      return this.index;
   }

   public FlushSampleFlyweight wrap(ByteBuffer byteBuffer, int index) {
      this.byteBuffer = byteBuffer;
      this.index = index;
      return this;
   }

   public FlushSampleFlyweight reset() {
      this.byteBuffer = null;
      this.index = -1;
      return this;
   }

   public long timeStamp() {
      return this.byteBuffer.getLong(this.index + TIMESTAMP_OFFSET);
   }

   FlushSampleFlyweight timeStamp(long value) {
      this.byteBuffer.putLong(this.index + TIMESTAMP_OFFSET, value);
      return this;
   }

   public long latency() {
      return this.byteBuffer.getLong(this.index + LATENCY_OFFSET);
   }

   FlushSampleFlyweight latency(long value) {
      this.byteBuffer.putLong(this.index + LATENCY_OFFSET, value);
      return this;
   }

   public int flushedBytes() {
      return this.byteBuffer.getInt(this.index + FLUSHED_BYTES_OFFSET);
   }

   FlushSampleFlyweight flushedBytes(int value) {
      this.byteBuffer.putInt(this.index + FLUSHED_BYTES_OFFSET, value);
      return this;
   }

   public boolean sync() {
      final int value = this.byteBuffer.get(this.index + SYNC_OFFSET);
      final boolean sync = value != 0;
      return sync;
   }

   FlushSampleFlyweight sync(boolean value) {
      final byte booleanValue = (byte) (value ? 1 : 0);
      this.byteBuffer.put(this.index + SYNC_OFFSET, booleanValue);
      return this;
   }

   public static int maxSize() {
      return MAX_SIZE;
   }

}

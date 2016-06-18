/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.io.mapped;

import java.nio.ByteBuffer;

import io.netty.util.internal.PlatformDependent;

final class BytesUtils {

   private BytesUtils() {
   }

   public static long align(final long value, final long alignment) {
      return (value + (alignment - 1)) & ~(alignment - 1);
   }

   public static void zerosDirect(final ByteBuffer buffer) {
      //TODO When PlatformDependent will be replaced by VarHandle or Unsafe, replace with safepoint-fixed setMemory
      //DANGEROUS!! erases bound-checking using directly addresses -> safe only if it use counted loops
      int remaining = buffer.capacity();
      long address = PlatformDependent.directBufferAddress(buffer);
      while (remaining >= 8) {
         PlatformDependent.putLong(address, 0L);
         address += 8;
         remaining -= 8;
      }
      while (remaining > 0) {
         PlatformDependent.putByte(address, (byte) 0);
         address++;
         remaining--;
      }
   }

}

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

package org.apache.activemq.artemis.utils.pools;

import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.netty.util.internal.PlatformDependent;
import org.jboss.logging.Logger;

/**
 * A simple encapsulation of Netty MpscQueue to provide a pool of objects.
 * Use this pool only when the borrowing of object (consume) is done on a single thread.
 * This is using a Multi Producer Single Consumer queue (MPSC).
 * If you need other uses you may create different strategies for ObjectPooling.
 * @param <T>
 */
public class MpscPool<T> extends Pool<T> {

   private static final Logger logger = Logger.getLogger(MpscPool.class);

   public MpscPool(int maxSize, Consumer<T> cleaner, Supplier<T> supplier) {
      super(maxSize, cleaner, supplier);
   }

   @Override
   protected Queue<T> createQueue(int maxSize) {
      if (logger.isTraceEnabled()) {
         logger.trace("Creating Pool with size=" + maxSize + " for classes for " + supplier.get().getClass());
      }
      final Queue<T> internalPool;
      if (maxSize > 0) {
         internalPool = PlatformDependent.newFixedMpscQueue(maxSize);
      } else {
         internalPool = null;
      }
      return internalPool;
   }

}

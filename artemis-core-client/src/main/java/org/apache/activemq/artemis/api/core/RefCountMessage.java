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

package org.apache.activemq.artemis.api.core;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public abstract class RefCountMessage implements Message {

   private static final AtomicIntegerFieldUpdater<RefCountMessage> DURABLE_REF_COUNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(RefCountMessage.class, "durableRefCount");
   private static final AtomicIntegerFieldUpdater<RefCountMessage> REF_COUNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(RefCountMessage.class, "refCount");

   private volatile int durableRefCount = 0;

   private volatile int refCount = 0;

   private RefCountMessageListener context;

   @Override
   public Message setContext(RefCountMessageListener context) {
      this.context = context;
      return this;
   }

   @Override
   public RefCountMessageListener getContext() {
      return context;
   }

   @Override
   public int getRefCount() {
      return refCount;
   }

   @Override
   public int incrementRefCount() throws Exception {
      int count = REF_COUNT_UPDATER.incrementAndGet(this);
      if (context != null) {
         context.nonDurableUp(this, count);
      }
      return count;
   }

   @Override
   public int incrementDurableRefCount() {
      int count = DURABLE_REF_COUNT_UPDATER.incrementAndGet(this);
      if (context != null) {
         context.durableUp(this, count);
      }
      return count;
   }

   @Override
   public int decrementDurableRefCount() {
      int count = DURABLE_REF_COUNT_UPDATER.decrementAndGet(this);
      if (context != null) {
         context.durableDown(this, count);
      }
      return count;
   }

   @Override
   public int decrementRefCount() throws Exception {
      int count = REF_COUNT_UPDATER.decrementAndGet(this);
      if (context != null) {
         context.nonDurableDown(this, count);
      }
      return count;
   }
}

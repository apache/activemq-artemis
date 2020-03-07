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

// import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet; -- #ifdef DEBUG

public class RefCountMessage {

   private static final AtomicIntegerFieldUpdater<RefCountMessage> DURABLE_REF_COUNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(RefCountMessage.class, "durableRefCount");
   private static final AtomicIntegerFieldUpdater<RefCountMessage> REF_COUNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(RefCountMessage.class, "refCount");
   private static final AtomicIntegerFieldUpdater<RefCountMessage> REF_USAGE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(RefCountMessage.class, "usageCount");

   private volatile int durableRefCount = 0;

   private volatile int refCount = 0;

   private volatile int usageCount = 0;

   private volatile boolean fired = false;

   public int getRefCount() {
      return REF_COUNT_UPDATER.get(this);
   }

   public int getUsage() {
      return REF_USAGE_UPDATER.get(this);
   }

   public int getDurableCount() {
      return DURABLE_REF_COUNT_UPDATER.get(this);
   }

   /**
    * in certain cases the large message is copied from another LargeServerMessage
    * and the ref count needs to be on that place.
    */
   private volatile RefCountMessage parentRef;

   public RefCountMessage getParentRef() {
      return parentRef;
   }
   // I am usually against keeping commented out code
   // However this is very useful for me to debug referencing counting.
   // Uncomment out anything between #ifdef DEBUG and #endif

   // #ifdef DEBUG -- comment out anything before endif if you want to debug REFERENCE COUNTS
   //final ConcurrentHashSet<Exception> upSet = new ConcurrentHashSet<>();
   // #endif

   private void onUp() {
      // #ifdef DEBUG -- comment out anything before endif if you want to debug REFERENCE COUNTS
      // upSet.add(new Exception("upEvent(" + debugString() + ")"));
      // #endif
   }

   private void onDown() {
      // #ifdef DEBUG -- comment out anything before endif if you want to debug REFERENCE COUNTS
      // upSet.add(new Exception("upEvent(" + debugString() + ")"));
      // #endif
      if (getRefCount() <= 0 && getUsage() <= 0 && getDurableCount() <= 0 && !fired) {

         debugRefs();
         fired = true;
         releaseComplete();
      }
   }
   /**
    *
    * This method will be useful if you remove commented out code around #ifdef AND #endif COMMENTS
    * */
   public final void debugRefs() {
      // #ifdef DEBUG -- comment out anything before endif if you want to debug REFERENCE COUNTS
      //   try {
      //      System.err.println("************************************************************************************************************************");
      //      System.err.println("Printing refcounts for " + debugString() + " this = " + this);
      //      for (Exception e : upSet) {
      //         e.printStackTrace();
      //      }
      //      System.err.println("************************************************************************************************************************");
      //   } catch (Throwable e) {
      //      e.printStackTrace();
      //   }
      // #ifdef DEBUG -- comment out anything before endif if you want to debug REFERENCE COUNTS
   }



   public String debugString() {
      return "refCount=" + getRefCount() + ", durableRefCount=" + getDurableCount() + ", usageCount=" + getUsage() + ", parentRef=" + this.parentRef;
   }
   public void setParentRef(RefCountMessage origin) {
      // if copy of a copy.. just go to the parent:
      if (origin.getParentRef() != null) {
         this.parentRef = origin.getParentRef();
      } else {
         this.parentRef = origin;
      }
   }

   protected void releaseComplete() {
   }

   public int usageUp() {
      if (parentRef != null) {
         return parentRef.usageUp();
      }
      int count = REF_USAGE_UPDATER.incrementAndGet(this);
      onUp();
      return count;
   }
   public int usageDown() {
      if (parentRef != null) {
         return parentRef.usageDown();
      }
      int count = REF_USAGE_UPDATER.decrementAndGet(this);
      onDown();
      return count;
   }

   public int durableUp() {
      if (parentRef != null) {
         return parentRef.durableUp();
      }
      int count = DURABLE_REF_COUNT_UPDATER.incrementAndGet(this);
      onUp();
      return count;
   }

   public int durableDown() {
      if (parentRef != null) {
         return parentRef.durableDown();
      }
      int count = DURABLE_REF_COUNT_UPDATER.decrementAndGet(this);
      onDown();
      return count;
   }

   public int refDown() {
      if (parentRef != null) {
         return parentRef.refDown();
      }
      int count = REF_COUNT_UPDATER.decrementAndGet(this);
      onDown();
      return count;
   }

   public int refUp() {
      if (parentRef != null) {
         return parentRef.refUp();
      }
      int count = REF_COUNT_UPDATER.incrementAndGet(this);
      onUp();
      return count;
   }

}

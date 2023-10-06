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
package org.apache.activemq.artemis.core.client.impl;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.spi.core.remoting.SessionContext;

public class ClientProducerCreditManagerImpl implements ClientProducerCreditManager {

   public static final int MAX_UNREFERENCED_CREDITS_CACHE_SIZE = 1000;

   private final Map<SimpleString, ClientProducerCredits> producerCredits = new LinkedHashMap<>();

   private final Map<SimpleString, ClientProducerCredits> unReferencedCredits = new LinkedHashMap<>();

   private final ClientSessionInternal session;

   private int windowSize;

   private ClientProducerFlowCallback callback;

   private final ScheduledExecutorService scheduledThreadPool;

   private ScheduledFuture future;

   public ClientProducerCreditManagerImpl(final ClientSessionInternal session, final int windowSize, ScheduledExecutorService scheduledThreadPool) {
      this.session = session;

      this.windowSize = windowSize;

      this.scheduledThreadPool = scheduledThreadPool;
   }


   /** This will determine the flow control as asynchronous,
    *  no actual block should happen instead a callback will be sent whenever blockages change  */
   @Override
   public void setCallback(ClientProducerFlowCallback callback) {
      this.callback = callback;
   }

   @Override
   public synchronized ClientProducerCredits getCredits(final SimpleString address,
                                                        final boolean anon,
                                                        SessionContext context) {
      if (windowSize == -1) {
         return ClientProducerCreditsNoFlowControl.instance;
      } else {
         boolean needInit = false;
         ClientProducerCredits credits;

         synchronized (this) {
            credits = producerCredits.get(address);

            if (credits == null) {
               // Doesn't need to be fair since session is single threaded
               credits = build(address);
               needInit = true;

               producerCredits.put(address, credits);
            }

            if (!anon) {
               credits.incrementRefCount();

               // Remove from anon credits (if there)
               unReferencedCredits.remove(address);
            } else {
               addToUnReferencedCache(address, credits);
            }
         }

         // The init is done outside of the lock
         // otherwise packages may arrive with flow control
         // while this is still sending requests causing a dead lock
         if (needInit) {
            credits.init(context);
         }

         return credits;
      }
   }

   private ClientProducerCredits build(SimpleString address) {
      if (callback != null) {
         return new AsynchronousProducerCreditsImpl(session, address, windowSize, callback);
      } else {
         return new ClientProducerCreditsImpl(session, address, windowSize);
      }
   }

   @Override
   public synchronized void returnCredits(final SimpleString address) {
      ClientProducerCredits credits = producerCredits.get(address);

      if (credits != null && credits.decrementRefCount() == 0) {
         addToUnReferencedCache(address, credits);
      }
   }

   @Override
   public synchronized void receiveCredits(final SimpleString address, final int credits) {
      ClientProducerCredits cr = producerCredits.get(address);

      if (cr != null) {
         cr.receiveCredits(credits);
      }
   }

   @Override
   public synchronized void receiveFailCredits(final SimpleString address, int credits) {
      ClientProducerCredits cr = producerCredits.get(address);

      if (cr != null) {
         cr.receiveFailCredits(credits);
      }
   }

   @Override
   public synchronized void reset() {
      for (ClientProducerCredits credits : producerCredits.values()) {
         credits.reset();
      }
   }

   @Override
   public synchronized void close() {
      windowSize = -1;

      for (ClientProducerCredits credits : producerCredits.values()) {
         credits.close();
      }

      producerCredits.clear();

      unReferencedCredits.clear();

      if (future != null) {
         future.cancel(false);
      }
   }

   @Override
   public synchronized int creditsMapSize() {
      return producerCredits.size();
   }

   @Override
   public synchronized int unReferencedCreditsSize() {
      return unReferencedCredits.size();
   }

   private void addToUnReferencedCache(final SimpleString address, final ClientProducerCredits credits) {
      unReferencedCredits.put(address, credits);

      if (unReferencedCredits.size() > MAX_UNREFERENCED_CREDITS_CACHE_SIZE) {
         // if we've exceeded our limit then try to clean up
         if (future == null) {
            future = scheduledThreadPool.scheduleWithFixedDelay(() -> {
               synchronized (this) {
                  Iterator<Map.Entry<SimpleString, ClientProducerCredits>> iter = unReferencedCredits.entrySet().iterator();
                  while (iter.hasNext()) {
                     Map.Entry<SimpleString, ClientProducerCredits> entry = iter.next();
                     if (entry.getValue().getBalance() == 0) {
                        iter.remove();
                        producerCredits.remove(entry.getKey());
                        entry.getValue().close();
                     }
                  }
               }
            }, 0, 30, TimeUnit.SECONDS);
         }
      } else {
         // if we're below our limit make sure we're not trying to clean up
         if (future != null) {
            future.cancel(false);
         }
      }
   }

   static class ClientProducerCreditsNoFlowControl implements ClientProducerCredits {

      static ClientProducerCreditsNoFlowControl instance = new ClientProducerCreditsNoFlowControl();

      @Override
      public void acquireCredits(int credits) {
      }

      @Override
      public void receiveCredits(int credits) {
      }

      @Override
      public void receiveFailCredits(int credits) {
      }

      @Override
      public boolean isBlocked() {
         return false;
      }

      @Override
      public void init(SessionContext ctx) {
      }

      @Override
      public void reset() {
      }

      @Override
      public void close() {
      }

      @Override
      public void incrementRefCount() {
      }

      @Override
      public int decrementRefCount() {
         return 1;
      }

      @Override
      public SimpleString getAddress() {
         return SimpleString.toSimpleString("");
      }

      @Override
      public int getBalance() {
         return 0;
      }
   }

}

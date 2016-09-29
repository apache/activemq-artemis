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
package org.apache.activemq.artemis.rest.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.artemis.rest.ActiveMQRestLogger;

public class TimeoutTask implements Runnable {

   protected boolean running = true;
   protected int interval = 10;
   protected final Lock callbacksLock = new ReentrantLock();
   protected Map<String, Callback> callbacks = new HashMap<>();
   protected final Lock pendingCallbacksLock = new ReentrantLock();
   protected Map<String, Callback> pendingCallbacks = new HashMap<>();
   protected Thread thread;

   public TimeoutTask(int interval) {
      this.interval = interval;
   }

   public interface Callback {

      boolean testTimeout(String token, boolean autoShutdown);

      void shutdown(String token);
   }

   public synchronized void add(Callback callback, String token) {
      if (callbacksLock.tryLock()) {
         try {
            callbacks.put(token, callback);
         } finally {
            callbacksLock.unlock();
         }
      } else {
         pendingCallbacksLock.lock();
         try {
            pendingCallbacks.put(token, callback);
         } finally {
            pendingCallbacksLock.unlock();
         }
      }
   }

   public synchronized void remove(String token) {
      callbacksLock.lock();
      try {
         callbacks.remove(token);
      } finally {
         callbacksLock.unlock();
      }
   }

   public synchronized void stop() {
      running = false;
      if (thread != null) {
         thread.interrupt();
      }
   }

   public synchronized int getInterval() {
      return interval;
   }

   public synchronized void setInterval(int interval) {
      this.interval = interval;
   }

   public void start() {
      thread = new Thread(this);
      thread.start();
   }

   @Override
   public void run() {
      while (running) {
         try {
            Thread.sleep(interval * 1000);
         } catch (InterruptedException e) {
            running = false;
            break;
         }

         // First, test all known callbacks for timeouts.
         // If the timeout is true, then move it to a separate map.
         Map<String, Callback> expiredCallbacks = new HashMap<>();

         int liveConsumers = 0;
         int deadConsumers = 0;

         callbacksLock.lock();
         try {
            long startTime = System.currentTimeMillis();
            List<String> tokens = new ArrayList<>(callbacks.size());
            for (String token : callbacks.keySet()) {
               tokens.add(token);
            }
            for (String token : tokens) {
               Callback callback = callbacks.get(token);
               if (callback.testTimeout(token, false)) {
                  deadConsumers += 1;
                  expiredCallbacks.put(token, callback);
                  callbacks.remove(token);
               } else {
                  liveConsumers += 1;
               }
            }
            ActiveMQRestLogger.LOGGER.debug("Finished testing callbacks for timeouts in " +
                                               (System.currentTimeMillis() - startTime) + "ms. " +
                                               "(Live: " + liveConsumers + ", Expired: " + deadConsumers + ")");

            // Next, move any pending callback additions to the main callbacks map.
            pendingCallbacksLock.lock();
            try {
               if (pendingCallbacks.size() > 0) {
                  ActiveMQRestLogger.LOGGER.debug("Found " + pendingCallbacks.size() + " callbacks to add.");
                  callbacks.putAll(pendingCallbacks);
                  pendingCallbacks.clear();
               }
            } finally {
               pendingCallbacksLock.unlock();
            }
         } finally {
            callbacksLock.unlock();
         }

         // Finally, freely shutdown all expired consumers.
         if (expiredCallbacks.size() > 0) {
            List<String> tokens = new ArrayList<>(expiredCallbacks.size());
            for (String token : expiredCallbacks.keySet()) {
               tokens.add(token);
            }
            for (String token : tokens) {
               Callback expired = expiredCallbacks.get(token);
               expired.shutdown(token);
            }
         }
      }
   }
}

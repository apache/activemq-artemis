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

package org.apache.activemq.artemis.core.server.reload;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.jboss.logging.Logger;

public class ReloadManagerImpl implements ReloadManager {
   private static final Logger logger = Logger.getLogger(ReloadManagerImpl.class);

   private final ScheduledExecutorService scheduledExecutorService;
   private final long checkPeriod;
   private ScheduledFuture future;
   private volatile Runnable tick;

   private Map<URL, ReloadRegistry> registry = new HashMap<>();

   public ReloadManagerImpl(ScheduledExecutorService scheduledExecutorService, long checkPeriod) {
      this.scheduledExecutorService = scheduledExecutorService;
      this.checkPeriod = checkPeriod;
   }

   @Override
   public synchronized void start() {
      if (future != null) {
         return;
      }
      future = scheduledExecutorService.scheduleWithFixedDelay(new ConfigurationFileReloader(), checkPeriod, checkPeriod, TimeUnit.MILLISECONDS);
   }

   @Override
   public synchronized void setTick(Runnable tick) {
      this.tick = tick;
   }

   @Override
   public synchronized void stop() {
      if (future == null) {
         return; // no big deal
      }

      future.cancel(false);
      future = null;

   }

   @Override
   public synchronized boolean isStarted() {
      return future != null;
   }

   @Override
   public synchronized void addCallback(URL uri, ReloadCallback callback) {
      if (future == null) {
         start();
      }
      ReloadRegistry uriRegistry = getRegistry(uri);
      uriRegistry.add(callback);
   }

   private synchronized void tick() {
      for (ReloadRegistry item : registry.values()) {
         item.check();
      }

      if (tick != null) {
         tick.run();
         tick = null;
      }
   }

   private ReloadRegistry getRegistry(URL uri) {
      ReloadRegistry uriRegistry = registry.get(uri);
      if (uriRegistry == null) {
         uriRegistry = new ReloadRegistry(uri);
         registry.put(uri, uriRegistry);
      }

      return uriRegistry;
   }



   private final class ConfigurationFileReloader implements Runnable {
      @Override
      public void run() {
         try {
            tick();
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.configurationReloadFailed(e);
         }
      }
   }

   class ReloadRegistry {
      private final File file;
      private final URL uri;
      private volatile long lastModified;

      private final List<ReloadCallback> callbacks = new LinkedList<>();

      ReloadRegistry(URL uri) {
         this.file = new File(uri.getPath());
         this.uri = uri;
      }

      public void check()  {

         long fileModified = file.lastModified();

         if (logger.isDebugEnabled()) {
            logger.debug("Validating lastModified " + lastModified + " modified = " + fileModified + " on " + uri);
         }

         if (lastModified > 0 && fileModified > lastModified) {

            for (ReloadCallback callback : callbacks) {
               try {
                  callback.reload(uri);
               }
               catch (Throwable e) {
                  ActiveMQServerLogger.LOGGER.configurationReloadFailed(e);
               }
            }
         }

         this.lastModified = fileModified;
      }


      public List<ReloadCallback> getCallbacks() {
         return callbacks;
      }

      public void add(ReloadCallback callback) {
         callbacks.add(callback);
      }
   }
}

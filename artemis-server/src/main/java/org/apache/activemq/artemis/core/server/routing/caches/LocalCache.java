/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.routing.caches;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.config.PersistedKeyValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class LocalCache implements Cache, RemovalListener<String, String> {
   private static final Logger logger = LoggerFactory.getLogger(LocalCache.class);

   private String id;
   private boolean persisted;
   private int timeout;
   private StorageManager storageManager;
   private com.google.common.cache.Cache<String, String> cache;
   private Map<String, PersistedKeyValuePair> persistedCacheEntries;

   private volatile boolean running;

   public String getId() {
      return id;
   }

   public boolean isPersisted() {
      return persisted;
   }

   public int getTimeout() {
      return timeout;
   }

   public LocalCache(String id, boolean persisted, int timeout, StorageManager storageManager) {
      this.id = id;
      this.persisted = persisted;
      this.timeout = timeout;
      this.storageManager = storageManager;

      if (timeout == 0) {
         cache = CacheBuilder.newBuilder().build();
      } else {
         cache = CacheBuilder.newBuilder().removalListener(this).expireAfterAccess(timeout, TimeUnit.MILLISECONDS).build();
      }
   }


   @Override
   public void start() {
      if (persisted) {
         persistedCacheEntries = storageManager.getPersistedKeyValuePairs(id);

         if (persistedCacheEntries != null) {
            for (Map.Entry<String, PersistedKeyValuePair> cacheEntry : persistedCacheEntries.entrySet()) {
               cache.put(cacheEntry.getKey(), cacheEntry.getValue().getValue());
               logger.info(cacheEntry.toString());
            }
         }
      }

      running = true;
   }

   @Override
   public void stop() {
      cache.cleanUp();

      if (persistedCacheEntries != null) {
         persistedCacheEntries.clear();
      }

      running = false;
   }

   @Override
   public String get(String key) {
      return cache.getIfPresent(key);
   }

   @Override
   public void put(String key, String nodeId) {
      if (persisted) {
         PersistedKeyValuePair persistedKeyValuePair = persistedCacheEntries.get(key);

         if (persistedKeyValuePair == null || !Objects.equals(nodeId, persistedKeyValuePair.getValue())) {
            persistedKeyValuePair = new PersistedKeyValuePair(id, key, nodeId);

            try {
               storageManager.storeKeyValuePair(persistedKeyValuePair);
            } catch (Exception e) {
               throw new RuntimeException(e);
            }

            persistedCacheEntries.put(key, persistedKeyValuePair);
         }
      }

      cache.put(key, nodeId);
   }

   @Override
   public void onRemoval(RemovalNotification<String, String> notification) {
      if (running && persisted) {
         PersistedKeyValuePair persistedKeyValuePair = persistedCacheEntries.remove(notification.getKey());

         if (persistedKeyValuePair != null) {
            try {
               storageManager.deleteKeyValuePair(persistedKeyValuePair.getMapId(), persistedKeyValuePair.getKey());
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      }
   }
}
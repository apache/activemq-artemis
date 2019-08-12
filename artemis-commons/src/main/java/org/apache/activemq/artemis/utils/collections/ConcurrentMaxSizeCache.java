/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.activemq.artemis.utils.collections;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Super simple thread-safe cache which evicts the oldest entry when maxSize is reached
 *
 * @param <K>
 */
public class ConcurrentMaxSizeCache<K> {

   private final int maxSize;

   private ConcurrentLinkedQueue<K> queue;

   public ConcurrentMaxSizeCache(final int maxSize) {
      this.maxSize = maxSize;
      queue = new ConcurrentLinkedQueue<K>();
   }

   public void add(final K key) {
      while (queue.size() >= maxSize) {
         queue.poll();
      }

      queue.add(key);
   }

   public boolean contains(final K key) {
      return queue.contains(key);
   }

   public boolean remove(final K key) {
      return queue.remove(key);
   }

   public int size() {
      return queue.size();
   }
}
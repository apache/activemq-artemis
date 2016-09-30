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
package org.apache.activemq.artemis.utils;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.logging.Logger;

public class SoftValueHashMap<K, V extends SoftValueHashMap.ValueCache> implements Map<K, V> {

   private static final Logger logger = Logger.getLogger(SoftValueHashMap.class);

   // The soft references that are already good.
   // too bad there's no way to override the queue method on ReferenceQueue, so I wouldn't need this
   private final ReferenceQueue<V> refQueue = new ReferenceQueue<>();

   private final Map<K, AggregatedSoftReference> mapDelegate = new HashMap<>();

   private final AtomicLong usedCounter = new AtomicLong(0);

   private int maxElements;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public interface ValueCache {

      boolean isLive();
   }

   // Constructors --------------------------------------------------

   public SoftValueHashMap(final int maxElements) {
      this.maxElements = maxElements;
   }

   // Public --------------------------------------------------------

   public void setMaxElements(final int maxElements) {
      this.maxElements = maxElements;
      checkCacheSize();
   }

   public int getMaxEelements() {
      return this.maxElements;
   }

   /**
    * @see java.util.Map#size()
    */
   @Override
   public int size() {
      processQueue();
      return mapDelegate.size();
   }

   /**
    * @see java.util.Map#isEmpty()
    */
   @Override
   public boolean isEmpty() {
      processQueue();
      return mapDelegate.isEmpty();
   }

   /**
    * @param key
    * @see java.util.Map#containsKey(java.lang.Object)
    */
   @Override
   public boolean containsKey(final Object key) {
      processQueue();
      return mapDelegate.containsKey(key);
   }

   /**
    * @param value
    * @see java.util.Map#containsValue(java.lang.Object)
    */
   @Override
   public boolean containsValue(final Object value) {
      processQueue();
      for (AggregatedSoftReference valueIter : mapDelegate.values()) {
         V valueElement = valueIter.get();
         if (valueElement != null && value.equals(valueElement)) {
            return true;
         }

      }
      return false;
   }

   /**
    * @param key
    * @see java.util.Map#get(java.lang.Object)
    */
   @Override
   public V get(final Object key) {
      processQueue();
      AggregatedSoftReference value = mapDelegate.get(key);
      if (value != null) {
         value.used();
         return value.get();
      } else {
         return null;
      }
   }

   /**
    * @param key
    * @param value
    * @see java.util.Map#put(java.lang.Object, java.lang.Object)
    */
   @Override
   public V put(final K key, final V value) {
      processQueue();
      AggregatedSoftReference newRef = createReference(key, value);
      AggregatedSoftReference oldRef = mapDelegate.put(key, newRef);
      checkCacheSize();
      newRef.used();
      if (oldRef != null) {
         return oldRef.get();
      } else {
         return null;
      }
   }

   private void checkCacheSize() {
      if (maxElements > 0 && mapDelegate.size() > maxElements) {
         TreeSet<AggregatedSoftReference> usedReferences = new TreeSet<>(new ComparatorAgregated());

         for (AggregatedSoftReference ref : mapDelegate.values()) {
            V v = ref.get();

            if (v != null && !v.isLive()) {
               usedReferences.add(ref);
            }
         }

         for (AggregatedSoftReference ref : usedReferences) {
            if (ref.used > 0) {
               Object removed = mapDelegate.remove(ref.key);

               if (logger.isTraceEnabled()) {
                  logger.trace("Removing " + removed + " with id = " + ref.key + " from SoftValueHashMap");
               }

               if (mapDelegate.size() <= maxElements) {
                  break;
               }
            }
         }
      }
   }

   class ComparatorAgregated implements Comparator<AggregatedSoftReference> {

      @Override
      public int compare(AggregatedSoftReference o1, AggregatedSoftReference o2) {
         long k = o1.used - o2.used;

         if (k > 0) {
            return 1;
         } else if (k < 0) {
            return -1;
         }

         k = o1.hashCode() - o2.hashCode();

         if (k > 0) {
            return 1;
         } else if (k < 0) {
            return -1;
         } else {
            return 0;
         }
      }
   }

   /**
    * @param key
    * @see java.util.Map#remove(java.lang.Object)
    */
   @Override
   public V remove(final Object key) {
      processQueue();
      AggregatedSoftReference ref = mapDelegate.remove(key);
      if (ref != null) {
         return ref.get();
      } else {
         return null;
      }
   }

   /**
    * @param m
    * @see java.util.Map#putAll(java.util.Map)
    */
   @Override
   public void putAll(final Map<? extends K, ? extends V> m) {
      processQueue();
      for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
         put(e.getKey(), e.getValue());
      }
   }

   /**
    * @see java.util.Map#clear()
    */
   @Override
   public void clear() {
      mapDelegate.clear();
   }

   /**
    * @see java.util.Map#keySet()
    */
   @Override
   public Set<K> keySet() {
      processQueue();
      return mapDelegate.keySet();
   }

   /**
    * @see java.util.Map#values()
    */
   @Override
   public Collection<V> values() {
      processQueue();
      ArrayList<V> list = new ArrayList<>();

      for (AggregatedSoftReference refs : mapDelegate.values()) {
         V value = refs.get();
         if (value != null) {
            list.add(value);
         }
      }

      return list;
   }

   /**
    * @see java.util.Map#entrySet()
    */
   @Override
   public Set<java.util.Map.Entry<K, V>> entrySet() {
      processQueue();
      HashSet<Map.Entry<K, V>> set = new HashSet<>();
      for (Map.Entry<K, AggregatedSoftReference> pair : mapDelegate.entrySet()) {
         V value = pair.getValue().get();
         if (value != null) {
            set.add(new EntryElement<>(pair.getKey(), value));
         }
      }
      return set;
   }

   /**
    * @param o
    * @see java.util.Map#equals(java.lang.Object)
    */
   @Override
   public boolean equals(final Object o) {
      processQueue();
      return mapDelegate.equals(o);
   }

   /**
    * @see java.util.Map#hashCode()
    */
   @Override
   public int hashCode() {
      return mapDelegate.hashCode();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   @SuppressWarnings("unchecked")
   private void processQueue() {
      AggregatedSoftReference ref = null;
      while ((ref = (AggregatedSoftReference) this.refQueue.poll()) != null) {
         logger.tracef("Removing reference through processQueue:: %s", ref.get());
         mapDelegate.remove(ref.key);
      }
   }

   private AggregatedSoftReference createReference(final K key, final V value) {
      AggregatedSoftReference ref = new AggregatedSoftReference(key, value);
      return ref;
   }

   // Inner classes -------------------------------------------------

   class AggregatedSoftReference extends SoftReference<V> {

      final K key;

      long used = 0;

      public long getUsed() {
         return used;
      }

      public void used() {
         used = usedCounter.incrementAndGet();
      }

      AggregatedSoftReference(final K key, final V referent) {
         super(referent, refQueue);
         this.key = key;
      }

      @Override
      public String toString() {
         return "AggregatedSoftReference [key=" + key + ", used=" + used + "]";
      }
   }

   static final class EntryElement<K, V> implements Map.Entry<K, V> {

      final K key;

      volatile V value;

      EntryElement(final K key, final V value) {
         this.key = key;
         this.value = value;
      }

      /* (non-Javadoc)
       * @see java.util.Map.Entry#getKey()
       */
      @Override
      public K getKey() {
         return key;
      }

      /* (non-Javadoc)
       * @see java.util.Map.Entry#getValue()
       */
      @Override
      public V getValue() {
         return value;
      }

      /* (non-Javadoc)
       * @see java.util.Map.Entry#setValue(java.lang.Object)
       */
      @Override
      public V setValue(final V value) {
         this.value = value;
         return value;
      }
   }

}

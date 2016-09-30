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
package org.apache.activemq.artemis.core.postoffice.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.artemis.api.core.ActiveMQDuplicateIdException;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.jboss.logging.Logger;

/**
 * A DuplicateIDCacheImpl
 *
 * A fixed size rotating cache of last X duplicate ids.
 */
public class DuplicateIDCacheImpl implements DuplicateIDCache {

   private static final Logger logger = Logger.getLogger(DuplicateIDCacheImpl.class);

   // ByteHolder, position
   private final Map<ByteArrayHolder, Integer> cache = new ConcurrentHashMap<>();

   private final SimpleString address;

   // Note - deliberately typed as ArrayList since we want to ensure fast indexed
   // based array access
   private final ArrayList<Pair<ByteArrayHolder, Long>> ids;

   private int pos;

   private final int cacheSize;

   private final StorageManager storageManager;

   private final boolean persist;

   public DuplicateIDCacheImpl(final SimpleString address,
                               final int size,
                               final StorageManager storageManager,
                               final boolean persist) {
      this.address = address;

      cacheSize = size;

      ids = new ArrayList<>(size);

      this.storageManager = storageManager;

      this.persist = persist;
   }

   @Override
   public void load(final List<Pair<byte[], Long>> theIds) throws Exception {
      long txID = -1;

      // If we have more IDs than cache size, we shrink the first ones
      int deleteCount = theIds.size() - cacheSize;
      if (deleteCount < 0) {
         deleteCount = 0;
      }

      for (Pair<byte[], Long> id : theIds) {
         if (deleteCount > 0) {
            if (txID == -1) {
               txID = storageManager.generateID();
            }
            if (logger.isTraceEnabled()) {
               logger.trace("DuplicateIDCacheImpl::load deleting id=" + describeID(id.getA(), id.getB()));
            }

            storageManager.deleteDuplicateIDTransactional(txID, id.getB());
            deleteCount--;
         } else {
            ByteArrayHolder bah = new ByteArrayHolder(id.getA());

            Pair<ByteArrayHolder, Long> pair = new Pair<>(bah, id.getB());

            cache.put(bah, ids.size());

            ids.add(pair);
            if (logger.isTraceEnabled()) {
               logger.trace("DuplicateIDCacheImpl::load loading id=" + describeID(id.getA(), id.getB()));
            }
         }

      }

      if (txID != -1) {
         storageManager.commit(txID);
      }

      pos = ids.size();

      if (pos == cacheSize) {
         pos = 0;
      }

   }

   @Override
   public void deleteFromCache(byte[] duplicateID) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("DuplicateIDCacheImpl::deleteFromCache deleting id=" + describeID(duplicateID, 0));
      }

      ByteArrayHolder bah = new ByteArrayHolder(duplicateID);

      Integer posUsed = cache.remove(bah);

      if (posUsed != null) {
         Pair<ByteArrayHolder, Long> id;

         synchronized (this) {
            id = ids.get(posUsed.intValue());

            if (id.getA().equals(bah)) {
               id.setA(null);
               storageManager.deleteDuplicateID(id.getB());
               if (logger.isTraceEnabled()) {
                  logger.trace("DuplicateIDCacheImpl(" + this.address + ")::deleteFromCache deleting id=" + describeID(duplicateID, id.getB()));
               }
               id.setB(null);
            }
         }
      }

   }

   private String describeID(byte[] duplicateID, long id) {
      if (id != 0) {
         return ByteUtil.bytesToHex(duplicateID, 4) + ", simpleString=" + ByteUtil.toSimpleString(duplicateID);
      } else {
         return ByteUtil.bytesToHex(duplicateID, 4) + ", simpleString=" + ByteUtil.toSimpleString(duplicateID) + ", id=" + id;
      }
   }

   @Override
   public boolean contains(final byte[] duplID) {
      boolean contains = cache.get(new ByteArrayHolder(duplID)) != null;

      if (contains) {
         logger.trace("DuplicateIDCacheImpl(" + this.address + ")::constains found a duplicate " + describeID(duplID, 0));
      }
      return contains;
   }

   @Override
   public void addToCache(final byte[] duplID) throws Exception {
      addToCache(duplID, null, false);
   }

   @Override
   public void addToCache(final byte[] duplID, final Transaction tx) throws Exception {
      addToCache(duplID, tx, false);
   }

   @Override
   public synchronized boolean atomicVerify(final byte[] duplID, final Transaction tx) throws Exception {

      if (contains(duplID)) {
         if (tx != null) {
            tx.markAsRollbackOnly(new ActiveMQDuplicateIdException());
         }
         return false;
      } else {
         addToCache(duplID, tx, true);
         return true;
      }

   }

   @Override
   public synchronized void addToCache(final byte[] duplID, final Transaction tx, boolean instantAdd) throws Exception {
      long recordID = -1;

      if (tx == null) {
         if (persist) {
            recordID = storageManager.generateID();
            storageManager.storeDuplicateID(address, duplID, recordID);
         }

         addToCacheInMemory(duplID, recordID);
      } else {
         if (persist) {
            recordID = storageManager.generateID();
            storageManager.storeDuplicateIDTransactional(tx.getID(), address, duplID, recordID);

            tx.setContainsPersistent();
         }

         if (instantAdd) {
            addToCacheInMemory(duplID, recordID);
         } else {
            if (logger.isTraceEnabled()) {
               logger.trace("DuplicateIDCacheImpl(" + this.address + ")::addToCache Adding duplicateID TX operation for " + describeID(duplID, recordID) + ", tx=" + tx);
            }
            // For a tx, it's important that the entry is not added to the cache until commit
            // since if the client fails then resends them tx we don't want it to get rejected
            tx.afterStore(new AddDuplicateIDOperation(duplID, recordID));
         }
      }
   }

   @Override
   public void load(final Transaction tx, final byte[] duplID) {
      tx.addOperation(new AddDuplicateIDOperation(duplID, tx.getID()));
   }

   private synchronized void addToCacheInMemory(final byte[] duplID, final long recordID) {
      if (logger.isTraceEnabled()) {
         logger.trace("DuplicateIDCacheImpl(" + this.address + ")::addToCacheInMemory Adding " + describeID(duplID, recordID));
      }

      ByteArrayHolder holder = new ByteArrayHolder(duplID);

      cache.put(holder, pos);

      Pair<ByteArrayHolder, Long> id;

      if (pos < ids.size()) {
         // Need fast array style access here -hence ArrayList typing
         id = ids.get(pos);

         // The id here might be null if it was explicit deleted
         if (id.getA() != null) {
            if (logger.isTraceEnabled()) {
               logger.trace("DuplicateIDCacheImpl(" + this.address + ")::addToCacheInMemory removing excess duplicateDetection " + describeID(id.getA().bytes, id.getB()));
            }

            cache.remove(id.getA());

            // Record already exists - we delete the old one and add the new one
            // Note we can't use update since journal update doesn't let older records get
            // reclaimed

            if (id.getB() != null) {
               try {
                  storageManager.deleteDuplicateID(id.getB());
               } catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.errorDeletingDuplicateCache(e);
               }
            }
         }

         id.setA(holder);

         // The recordID could be negative if the duplicateCache is configured to not persist,
         // -1 would mean null on this case
         id.setB(recordID >= 0 ? recordID : null);

         if (logger.isTraceEnabled()) {
            logger.trace("DuplicateIDCacheImpl(" + this.address + ")::addToCacheInMemory replacing old duplicateID by " + describeID(id.getA().bytes, id.getB()));
         }

         holder.pos = pos;
      } else {
         id = new Pair<>(holder, recordID >= 0 ? recordID : null);

         if (logger.isTraceEnabled()) {
            logger.trace("DuplicateIDCacheImpl(" + this.address + ")::addToCacheInMemory Adding new duplicateID " + describeID(id.getA().bytes, id.getB()));
         }

         ids.add(id);

         holder.pos = pos;
      }

      if (pos++ == cacheSize - 1) {
         pos = 0;
      }
   }

   @Override
   public void clear() throws Exception {
      logger.debug("DuplicateIDCacheImpl(" + this.address + ")::clear removing duplicate ID data");
      synchronized (this) {
         if (ids.size() > 0) {
            long tx = storageManager.generateID();
            for (Pair<ByteArrayHolder, Long> id : ids) {
               storageManager.deleteDuplicateIDTransactional(tx, id.getB());
            }
            storageManager.commit(tx);
         }

         ids.clear();
         cache.clear();
         pos = 0;
      }
   }

   @Override
   public List<Pair<byte[], Long>> getMap() {
      List<Pair<byte[], Long>> list = new ArrayList<>();
      for (Pair<ByteArrayHolder, Long> id : ids) {
         list.add(new Pair<>(id.getA().bytes, id.getB()));
      }
      return list;
   }

   private final class AddDuplicateIDOperation extends TransactionOperationAbstract {

      final byte[] duplID;

      final long recordID;

      volatile boolean done;

      AddDuplicateIDOperation(final byte[] duplID, final long recordID) {
         this.duplID = duplID;
         this.recordID = recordID;
      }

      private void process() {
         if (!done) {
            addToCacheInMemory(duplID, recordID);

            done = true;
         }
      }

      @Override
      public void afterCommit(final Transaction tx) {
         process();
      }

      @Override
      public List<MessageReference> getRelatedMessageReferences() {
         return null;
      }
   }

   private static final class ByteArrayHolder {

      ByteArrayHolder(final byte[] bytes) {
         this.bytes = bytes;
      }

      final byte[] bytes;

      int hash;

      int pos;

      @Override
      public boolean equals(final Object other) {
         if (other instanceof ByteArrayHolder) {
            ByteArrayHolder s = (ByteArrayHolder) other;

            if (bytes.length != s.bytes.length) {
               return false;
            }

            for (int i = 0; i < bytes.length; i++) {
               if (bytes[i] != s.bytes[i]) {
                  return false;
               }
            }

            return true;
         } else {
            return false;
         }
      }

      @Override
      public int hashCode() {
         if (hash == 0) {
            for (byte b : bytes) {
               hash = 31 * hash + b;
            }
         }

         return hash;
      }
   }
}

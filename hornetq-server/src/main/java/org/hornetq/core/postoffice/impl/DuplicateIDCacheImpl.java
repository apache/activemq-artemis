/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.core.postoffice.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperationAbstract;

/**
 * A DuplicateIDCacheImpl
 *
 * A fixed size rotating cache of last X duplicate ids.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *         Created 8 Dec 2008 16:35:55
 */
public class DuplicateIDCacheImpl implements DuplicateIDCache
{
   // ByteHolder, position
   private final Map<ByteArrayHolder, Integer> cache = new ConcurrentHashMap<ByteArrayHolder, Integer>();

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
                               final boolean persist)
   {
      this.address = address;

      cacheSize = size;

      ids = new ArrayList<Pair<ByteArrayHolder, Long>>(size);

      this.storageManager = storageManager;

      this.persist = persist;
   }

   public void load(final List<Pair<byte[], Long>> theIds) throws Exception
   {
      int count = 0;

      long txID = -1;

      for (Pair<byte[], Long> id : theIds)
      {
         if (count < cacheSize)
         {
            ByteArrayHolder bah = new ByteArrayHolder(id.getA());

            Pair<ByteArrayHolder, Long> pair = new Pair<ByteArrayHolder, Long>(bah, id.getB());

            cache.put(bah, ids.size());

            ids.add(pair);
         }
         else
         {
            // cache size has been reduced in config - delete the extra records
            if (txID == -1)
            {
               txID = storageManager.generateID();
            }

            storageManager.deleteDuplicateIDTransactional(txID, id.getB());
         }

         count++;
      }

      if (txID != -1)
      {
         storageManager.commit(txID);
      }

      pos = ids.size();

      if (pos == cacheSize)
      {
         pos = 0;
      }

   }


   public void deleteFromCache(byte[] duplicateID) throws Exception
   {
      ByteArrayHolder bah = new ByteArrayHolder(duplicateID);

      Integer posUsed = cache.remove(bah);

      if (posUsed != null)
      {
         Pair<ByteArrayHolder, Long> id;

         synchronized (this)
         {
            id = ids.get(posUsed.intValue());

            if (id.getA().equals(bah))
            {
               id.setA(null);
               storageManager.deleteDuplicateID(id.getB());
               id.setB(null);
            }
         }
      }

   }


   public boolean contains(final byte[] duplID)
   {
      return cache.get(new ByteArrayHolder(duplID)) != null;
   }

   public synchronized void addToCache(final byte[] duplID, final Transaction tx) throws Exception
   {
      long recordID = -1;

      if (tx == null)
      {
         if (persist)
         {
            recordID = storageManager.generateID();
            storageManager.storeDuplicateID(address, duplID, recordID);
         }

         addToCacheInMemory(duplID, recordID);
      }
      else
      {
         if (persist)
         {
            recordID = storageManager.generateID();
            storageManager.storeDuplicateIDTransactional(tx.getID(), address, duplID, recordID);

            tx.setContainsPersistent();
         }

         // For a tx, it's important that the entry is not added to the cache until commit
         // since if the client fails then resends them tx we don't want it to get rejected
         tx.addOperation(new AddDuplicateIDOperation(duplID, recordID));
      }
   }

   public void load(final Transaction tx, final byte[] duplID)
   {
      tx.addOperation(new AddDuplicateIDOperation(duplID, tx.getID()));
   }

   private synchronized void addToCacheInMemory(final byte[] duplID, final long recordID)
   {
      ByteArrayHolder holder = new ByteArrayHolder(duplID);

      cache.put(holder, pos);

      Pair<ByteArrayHolder, Long> id;

      if (pos < ids.size())
      {
         // Need fast array style access here -hence ArrayList typing
         id = ids.get(pos);

         // The id here might be null if it was explicit deleted
         if (id.getA() != null)
         {
            cache.remove(id.getA());

            // Record already exists - we delete the old one and add the new one
            // Note we can't use update since journal update doesn't let older records get
            // reclaimed

            if (id.getB() != null)
            {
               try
               {
                  storageManager.deleteDuplicateID(id.getB());
               }
               catch (Exception e)
               {
                  HornetQServerLogger.LOGGER.errorDeletingDuplicateCache(e);
               }
            }
         }

         id.setA(holder);

         // The recordID could be negative if the duplicateCache is configured to not persist,
         // -1 would mean null on this case
         id.setB(recordID >= 0 ? recordID : null);

         holder.pos = pos;
      }
      else
      {
         id = new Pair<ByteArrayHolder, Long>(holder, recordID >= 0 ? recordID : null);

         ids.add(id);

         holder.pos = pos;
      }

      if (pos++ == cacheSize - 1)
      {
         pos = 0;
      }
   }

   public void clear() throws Exception
   {
      synchronized (this)
      {
         if (ids.size() > 0)
         {
            long tx = storageManager.generateID();
            for (Pair<ByteArrayHolder, Long> id : ids)
            {
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
   public List<Pair<byte[], Long>> getMap()
   {
      List<Pair<byte[], Long>> list = new ArrayList<>();
      for (Pair<ByteArrayHolder, Long> id : ids)
      {
         list.add(new Pair<>(id.getA().bytes, id.getB()));
      }
      return list;
   }

   private final class AddDuplicateIDOperation extends TransactionOperationAbstract
   {
      final byte[] duplID;

      final long recordID;

      volatile boolean done;

      AddDuplicateIDOperation(final byte[] duplID, final long recordID)
      {
         this.duplID = duplID;
         this.recordID = recordID;
      }

      private void process()
      {
         if (!done)
         {
            addToCacheInMemory(duplID, recordID);

            done = true;
         }
      }

      @Override
      public void afterCommit(final Transaction tx)
      {
         process();
      }

      @Override
      public List<MessageReference> getRelatedMessageReferences()
      {
         return null;
      }
   }

   private static final class ByteArrayHolder
   {
      ByteArrayHolder(final byte[] bytes)
      {
         this.bytes = bytes;
      }

      final byte[] bytes;

      int hash;

      int pos;

      @Override
      public boolean equals(final Object other)
      {
         if (other instanceof ByteArrayHolder)
         {
            ByteArrayHolder s = (ByteArrayHolder) other;

            if (bytes.length != s.bytes.length)
            {
               return false;
            }

            for (int i = 0; i < bytes.length; i++)
            {
               if (bytes[i] != s.bytes[i])
               {
                  return false;
               }
            }

            return true;
         }
         else
         {
            return false;
         }
      }

      @Override
      public int hashCode()
      {
         if (hash == 0)
         {
            for (byte b : bytes)
            {
               hash = 31 * hash + b;
            }
         }

         return hash;
      }
   }
}

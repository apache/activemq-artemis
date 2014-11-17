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
package org.apache.activemq.core.journal.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.core.journal.SequentialFile;

/**
 *
 * A JournalFileImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class JournalFileImpl implements JournalFile
{
   private final SequentialFile file;

   private final long fileID;

   private final int recordID;

   private long offset;

   private final AtomicInteger posCount = new AtomicInteger(0);

   private final AtomicInteger liveBytes = new AtomicInteger(0);

   private boolean canReclaim;

   private final AtomicInteger totalNegativeToOthers = new AtomicInteger(0);

   private final int version;

   private final Map<JournalFile, AtomicInteger> negCounts = new ConcurrentHashMap<JournalFile, AtomicInteger>();

   public JournalFileImpl(final SequentialFile file, final long fileID, final int version)
   {
      this.file = file;

      this.fileID = fileID;

      this.version = version;

      recordID = (int)(fileID & Integer.MAX_VALUE);
   }

   public int getPosCount()
   {
      return posCount.intValue();
   }

   @Override
   public boolean isCanReclaim()
   {
      return canReclaim;
   }

   @Override
   public void setCanReclaim(final boolean canReclaim)
   {
      this.canReclaim = canReclaim;
   }

   public void incNegCount(final JournalFile file)
   {
      if (file != this)
      {
         totalNegativeToOthers.incrementAndGet();
      }
      getOrCreateNegCount(file).incrementAndGet();
   }

   public int getNegCount(final JournalFile file)
   {
      AtomicInteger count = negCounts.get(file);

      if (count == null)
      {
         return 0;
      }
      else
      {
         return count.intValue();
      }
   }

   public int getJournalVersion()
   {
      return version;
   }

   public void incPosCount()
   {
      posCount.incrementAndGet();
   }

   public void decPosCount()
   {
      posCount.decrementAndGet();
   }

   public long getOffset()
   {
      return offset;
   }

   public long getFileID()
   {
      return fileID;
   }

   public int getRecordID()
   {
      return recordID;
   }

   public void setOffset(final long offset)
   {
      this.offset = offset;
   }

   public SequentialFile getFile()
   {
      return file;
   }

   @Override
   public String toString()
   {
      try
      {
         return "JournalFileImpl: (" + file.getFileName() + " id = " + fileID + ", recordID = " + recordID + ")";
      }
      catch (Exception e)
      {
         e.printStackTrace();
         return "Error:" + e.toString();
      }
   }

   /** Receive debug information about the journal */
   public String debug()
   {
      StringBuilder builder = new StringBuilder();

      for (Entry<JournalFile, AtomicInteger> entry : negCounts.entrySet())
      {
         builder.append(" file = " + entry.getKey() + " negcount value = " + entry.getValue() + "\n");
      }

      return builder.toString();
   }

   private synchronized AtomicInteger getOrCreateNegCount(final JournalFile file)
   {
      AtomicInteger count = negCounts.get(file);

      if (count == null)
      {
         count = new AtomicInteger();
         negCounts.put(file, count);
      }

      return count;
   }

   @Override
   public void addSize(final int bytes)
   {
      liveBytes.addAndGet(bytes);
   }

   @Override
   public void decSize(final int bytes)
   {
      liveBytes.addAndGet(-bytes);
   }

   @Override
   public int getLiveSize()
   {
      return liveBytes.get();
   }

   public int getTotalNegativeToOthers()
   {
      return totalNegativeToOthers.get();
   }

}

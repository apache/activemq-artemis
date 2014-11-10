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

package org.proton.plug.util;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * @author Clebert Suconic
 */

public class CreditsSemaphore
{

   @SuppressWarnings("serial")
   private static class Sync extends AbstractQueuedSynchronizer
   {
      public Sync(int initial)
      {
         setState(initial);
      }

      public int getCredits()
      {
         return getState();
      }

      @Override
      public int tryAcquireShared(final int numberOfAqcquires)
      {
         for (;;)
         {
            int actualSize = getState();
            int newValue = actualSize - numberOfAqcquires;

            if (newValue < 0)
            {
               if (actualSize == getState())
               {
                  return -1;
               }
            }
            else if (compareAndSetState(actualSize, newValue))
            {
               return newValue;
            }
         }
      }

      @Override
      public boolean tryReleaseShared(final int numberOfReleases)
      {
         for (;;)
         {
            int actualSize = getState();
            int newValue = actualSize + numberOfReleases;

            if (compareAndSetState(actualSize, newValue))
            {
               return true;
            }

         }
      }

      public void setCredits(final int credits)
      {
         for (;;)
         {
            int actualState = getState();
            if (compareAndSetState(actualState, credits))
            {
               // This is to wake up any pending threads that could be waiting on queued
               releaseShared(0);
               return;
            }
         }
      }
   }

   private final Sync sync;


   public CreditsSemaphore(int initialCredits)
   {
      sync = new Sync(initialCredits);
   }

   public void acquire() throws InterruptedException
   {
      sync.acquireSharedInterruptibly(1);
   }

   public boolean tryAcquire()
   {
      return sync.tryAcquireShared(1) >= 0;
   }

   public void release() throws InterruptedException
   {
      sync.releaseShared(1);
   }

   public void release(int credits) throws InterruptedException
   {
      sync.releaseShared(credits);
   }

   public void setCredits(int credits)
   {
      sync.setCredits(credits);
   }

   public int getCredits()
   {
      return sync.getCredits();
   }

   public boolean hasQueuedThreads()
   {
      return sync.hasQueuedThreads();
   }

}
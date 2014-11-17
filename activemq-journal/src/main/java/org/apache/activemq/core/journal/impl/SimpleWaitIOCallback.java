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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.HornetQExceptionType;
import org.apache.activemq.journal.HornetQJournalLogger;

/**
 * A SimpleWaitIOCallback
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public final class SimpleWaitIOCallback extends SyncIOCompletion
{
   private final CountDownLatch latch = new CountDownLatch(1);

   private volatile String errorMessage;

   private volatile int errorCode = 0;

   @Override
   public String toString()
   {
      return SimpleWaitIOCallback.class.getName();
   }

   public void done()
   {
      latch.countDown();
   }

   public void onError(final int errorCode1, final String errorMessage1)
   {
      this.errorCode = errorCode1;

      this.errorMessage = errorMessage1;

      HornetQJournalLogger.LOGGER.errorOnIOCallback(errorMessage1);

      latch.countDown();
   }

   @Override
   public void waitCompletion() throws InterruptedException, HornetQException
   {
      while (true)
      {
         if (latch.await(2, TimeUnit.SECONDS))
            break;
      }

      if (errorMessage != null)
      {
         throw HornetQExceptionType.createException(errorCode, errorMessage);
      }

      return;
   }

   public boolean waitCompletion(final long timeout) throws InterruptedException, HornetQException
   {
      boolean retValue = latch.await(timeout, TimeUnit.MILLISECONDS);

      if (errorMessage != null)
      {
         throw HornetQExceptionType.createException(errorCode, errorMessage);
      }

      return retValue;
   }

   @Override
   public void storeLineUp()
   {
   }
}

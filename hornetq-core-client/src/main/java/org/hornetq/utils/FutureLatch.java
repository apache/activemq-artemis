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
package org.hornetq.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A Future
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class FutureLatch implements Runnable
{
   private final CountDownLatch latch;

   public FutureLatch()
   {
      super();
      latch = new CountDownLatch(1);
   }

   public FutureLatch(int latches)
   {
      super();
      latch =  new CountDownLatch(latches);
   }

   public boolean await(final long timeout)
   {
      try
      {
         return latch.await(timeout, TimeUnit.MILLISECONDS);
      }
      catch (InterruptedException e)
      {
         return false;
      }
   }

   public void run()
   {
      latch.countDown();
   }

   @Override
   public String toString()
   {
      return FutureLatch.class.getSimpleName() + "(latch=" + latch + ")";
   }
}

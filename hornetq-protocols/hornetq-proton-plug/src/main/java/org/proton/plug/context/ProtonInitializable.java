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

package org.proton.plug.context;

import java.util.concurrent.TimeUnit;

import org.proton.plug.exceptions.HornetQAMQPException;
import org.proton.plug.exceptions.HornetQAMQPIllegalStateException;
import org.proton.plug.exceptions.HornetQAMQPTimeoutException;
import org.proton.plug.util.FutureRunnable;

/**
 * @author Clebert Suconic
 */

public class ProtonInitializable
{

   private Runnable afterInit;

   private boolean initialized = false;

   public void afterInit(Runnable afterInit)
   {
      this.afterInit = afterInit;
   }


   public boolean isInitialized()
   {
      return initialized;
   }


   public void initialise() throws Exception
   {
      if (!initialized)
      {
         initialized = false;
         try
         {
            if (afterInit != null)
            {
               afterInit.run();
            }
         }
         finally
         {
            afterInit = null;
         }
      }
   }


   public void waitWithTimeout(FutureRunnable latch) throws HornetQAMQPException
   {
      try
      {
         // TODO Configure this
         if (!latch.await(30, TimeUnit.SECONDS))
         {
            throw new HornetQAMQPTimeoutException("Timed out waiting for response");
         }
      }
      catch (InterruptedException e)
      {
         Thread.currentThread().interrupt();
         throw new HornetQAMQPIllegalStateException(e.getMessage());
      }
   }

}

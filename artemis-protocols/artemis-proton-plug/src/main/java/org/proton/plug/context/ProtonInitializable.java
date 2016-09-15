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
package org.proton.plug.context;

import java.util.concurrent.TimeUnit;

import org.proton.plug.exceptions.ActiveMQAMQPException;
import org.proton.plug.exceptions.ActiveMQAMQPIllegalStateException;
import org.proton.plug.exceptions.ActiveMQAMQPTimeoutException;
import org.proton.plug.util.FutureRunnable;

public class ProtonInitializable {

   private Runnable afterInit;

   private boolean initialized = false;

   public void afterInit(Runnable afterInit) {
      this.afterInit = afterInit;
   }

   public boolean isInitialized() {
      return initialized;
   }

   public void initialise() throws Exception {
      if (!initialized) {
         initialized = true;
         try {
            if (afterInit != null) {
               afterInit.run();
            }
         }
         finally {
            afterInit = null;
         }
      }
   }

   public void waitWithTimeout(FutureRunnable latch) throws ActiveMQAMQPException {
      try {
         // TODO Configure this
         if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new ActiveMQAMQPTimeoutException("Timed out waiting for response");
         }
      }
      catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new ActiveMQAMQPIllegalStateException(e.getMessage());
      }
   }

}

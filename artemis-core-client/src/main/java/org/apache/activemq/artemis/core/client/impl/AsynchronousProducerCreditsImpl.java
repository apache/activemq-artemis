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

package org.apache.activemq.artemis.core.client.impl;

import org.apache.activemq.artemis.api.core.SimpleString;

public class AsynchronousProducerCreditsImpl extends AbstractProducerCreditsImpl {

   int balance;

   final ClientProducerFlowCallback callback;

   public AsynchronousProducerCreditsImpl(ClientSessionInternal session, SimpleString address, int windowSize,
                                          ClientProducerFlowCallback callback) {
      super(session, address, windowSize);
      balance = windowSize;
      this.callback = callback;
   }

   @Override
   protected synchronized void actualAcquire(int credits) {
      synchronized (this) {
         balance -= credits;
         if (balance <= 0) {
            callback.onCreditsFlow(true, this);
         }
      }

   }

   @Override
   public int getBalance() {
      return balance;
   }

   @Override
   public void receiveCredits(int credits) {
      synchronized (this) {
         super.receiveCredits(credits);
         balance += credits;
         callback.onCreditsFlow(balance <= 0, this);
      }

   }


   @Override
   public void receiveFailCredits(final int credits) {
      super.receiveFailCredits(credits);
      callback.onCreditsFail(this);
   }

   @Override
   public void releaseOutstanding() {
      synchronized (this) {
         balance = 0;
         callback.onCreditsFlow(true, this);
      }

   }
}

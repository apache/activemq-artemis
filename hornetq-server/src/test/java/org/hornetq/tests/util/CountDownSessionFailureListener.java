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
package org.hornetq.tests.util;

import java.util.concurrent.CountDownLatch;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.SessionFailureListener;

public final class CountDownSessionFailureListener implements SessionFailureListener
{
   private final CountDownLatch latch;
   private final ClientSession session;

   public CountDownSessionFailureListener(ClientSession session)
   {
      this(1, session);
   }

   public CountDownSessionFailureListener(int n, ClientSession session)
   {
      latch = new CountDownLatch(n);
      this.session = session;
   }

   public CountDownSessionFailureListener(CountDownLatch latch, ClientSession session)
   {
      this.latch = latch;
      this.session = session;
   }

   @Override
   public void connectionFailed(HornetQException exception, boolean failedOver, String scaleDownTargetNodeID)
   {
      connectionFailed(exception, failedOver);
   }

   @Override
   public void connectionFailed(HornetQException exception, boolean failedOver)
   {
      latch.countDown();
      session.removeFailureListener(this);

   }

   public CountDownLatch getLatch()
   {
      return latch;
   }

   @Override
   public void beforeReconnect(HornetQException exception)
   {
      // No-op
   }

}

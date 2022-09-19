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

import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.spi.core.remoting.SessionContext;
import org.jboss.logging.Logger;

public abstract class AbstractProducerCreditsImpl implements ClientProducerCredits {

   private static final Logger logger = Logger.getLogger(AbstractProducerCreditsImpl.class);

   protected int pendingCredits;

   protected final int windowSize;

   protected volatile boolean closed;

   protected boolean blocked;

   protected final SimpleString address;

   private final ClientSessionInternal session;

   protected int arriving;

   private int refCount;

   protected boolean serverRespondedWithFail;

   protected SessionContext sessionContext;

   public AbstractProducerCreditsImpl(final ClientSessionInternal session,
                                      final SimpleString address,
                                      final int windowSize) {
      this.session = session;

      this.address = address;

      this.windowSize = windowSize / 2;
   }

   @Override
   public SimpleString getAddress() {
      return address;
   }

   @Override
   public void init(SessionContext sessionContext) {
      // We initial request twice as many credits as we request in subsequent requests
      // This allows the producer to keep sending as more arrive, minimising pauses
      checkCredits(windowSize);

      this.sessionContext = sessionContext;

      this.sessionContext.linkFlowControl(address, this);
   }

   @Override
   public void acquireCredits(final int credits) throws ActiveMQException {
      checkCredits(credits);

      actualAcquire(credits);

      afterAcquired(credits);

   }

   protected void afterAcquired(int credits) throws ActiveMQAddressFullException {
      if (logger.isDebugEnabled()) {
         logger.debugf("AfterAcquire %s credits on address %s", credits, address);
      }
      synchronized (this) {
         pendingCredits -= credits;
      }
   }

   protected abstract void actualAcquire(int credits);

   @Override
   public boolean isBlocked() {
      return blocked;
   }

   @Override
   public void receiveFailCredits(final int credits) {
      serverRespondedWithFail = true;
      // receive credits like normal to keep the sender from blocking
      receiveCredits(credits);
   }


   @Override
   public void receiveCredits(final int credits) {
      synchronized (this) {
         arriving -= credits;
      }
   }


   @Override
   public synchronized void reset() {
      logger.debugf("reset credits on address %s", address);
      // Any pendingCredits credits from before failover won't arrive, so we re-initialise

      int beforeFailure = pendingCredits;

      pendingCredits = 0;
      arriving = 0;

      // If we are waiting for more credits than what's configured, then we need to use what we tried before
      // otherwise the client may starve as the credit will never arrive
      checkCredits(Math.max(windowSize * 2, beforeFailure));
   }

   @Override
   public void close() {
      // Closing a producer that is blocking should make it return
      closed = true;
   }

   @Override
   public synchronized void incrementRefCount() {
      refCount++;
   }

   @Override
   public synchronized int decrementRefCount() {
      return --refCount;
   }

   public abstract int getBalance();

   protected void checkCredits(final int credits) {
      int needed = Math.max(credits, windowSize);
      if (logger.isTraceEnabled()) {
         logger.tracef("CheckCredits %s on address %s, needed=%s, credits=%s, window=%s", credits, address, needed, credits, windowSize);
      }

      int toRequest = -1;

      synchronized (this) {
         if (getBalance() + arriving < needed) {
            toRequest = needed - arriving;

            if (logger.isTraceEnabled()) {
               logger.tracef("CheckCredits on Address %s, requesting=%s, arriving=%s, balance=%s", address, toRequest, arriving, getBalance());
            }
         } else {
            if (logger.isTraceEnabled()) {
               logger.tracef("CheckCredits did not need it, balance=%s, arriving=%s,  needed=%s, getbalance + arriving < needed=%s", getBalance(), arriving, needed, (boolean)(getBalance() + arriving < needed));
            }
         }
      }

      if (toRequest > 0) {
         if (logger.isDebugEnabled()) {
            logger.debugf("Requesting %s credits on address %s, needed = %s, arriving = %s", toRequest, address, needed, arriving);
         }
         requestCredits(toRequest);
      } else {
         logger.debugf("not asking for %s credits on %s", toRequest, address);
      }
   }

   protected void requestCredits(final int credits) {
      logger.debugf("Request %s credits on address %s", credits, address);
      synchronized (this) {
         pendingCredits += credits;
         arriving += credits;
      }
      session.sendProducerCreditsMessage(credits, address);
   }
}

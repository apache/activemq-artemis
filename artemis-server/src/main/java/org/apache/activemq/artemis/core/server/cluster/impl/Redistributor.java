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
package org.apache.activemq.artemis.core.server.cluster.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.HandleStatus;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.utils.ReusableLatch;

public class Redistributor implements Consumer {

   private boolean active;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final Executor executor;

   private final int batchSize;

   private final Queue queue;

   private int count;

   private final long sequentialID;

   // a Flush executor here is happening inside another executor.
   // what may cause issues under load. Say you are running out of executors for cases where you don't need to wait at all.
   // So, instead of using a future we will use a plain ReusableLatch here
   private ReusableLatch pendingRuns = new ReusableLatch();

   public Redistributor(final Queue queue,
                        final StorageManager storageManager,
                        final PostOffice postOffice,
                        final Executor executor,
                        final int batchSize) {
      this.queue = queue;

      this.sequentialID = storageManager.generateID();

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.executor = executor;

      this.batchSize = batchSize;
   }

   @Override
   public long sequentialID() {
      return sequentialID;
   }

   @Override
   public Filter getFilter() {
      return null;
   }

   @Override
   public String debug() {
      return toString();
   }

   @Override
   public String toManagementString() {
      return "Redistributor[" + queue.getName() + "/" + queue.getID() + "]";
   }

   @Override
   public void disconnect() {
      //noop
   }

   public synchronized void start() {
      active = true;
   }

   public synchronized void stop() throws Exception {
      active = false;

      boolean ok = flushExecutor();

      if (!ok) {
         ActiveMQServerLogger.LOGGER.errorStoppingRedistributor();
      }
   }

   public synchronized void close() {
      boolean ok = flushExecutor();

      if (!ok) {
         throw new IllegalStateException("Timed out waiting for executor to complete");
      }

      active = false;
   }

   private boolean flushExecutor() {
      try {
         boolean ok = pendingRuns.await(10000);
         return ok;
      } catch (InterruptedException e) {
         ActiveMQServerLogger.LOGGER.failedToFlushExecutor(e);
         return false;
      }
   }

   @Override
   public synchronized HandleStatus handle(final MessageReference reference) throws Exception {
      if (!active) {
         return HandleStatus.BUSY;
      } else if (reference.getMessage().getGroupID() != null) {
         //we shouldn't redistribute with message groups return NO_MATCH so other messages can be delivered
         return HandleStatus.NO_MATCH;
      }

      final Transaction tx = new TransactionImpl(storageManager);

      final Pair<RoutingContext, Message> routingInfo = postOffice.redistribute(reference.getMessage(), queue, tx);

      if (routingInfo == null) {
         tx.rollback();
         return HandleStatus.BUSY;
      }

      if (!reference.getMessage().isLargeMessage()) {

         postOffice.processRoute(routingInfo.getB(), routingInfo.getA(), false);

         ackRedistribution(reference, tx);
      } else {
         active = false;
         executor.execute(new Runnable() {
            @Override
            public void run() {
               try {

                  postOffice.processRoute(routingInfo.getB(), routingInfo.getA(), false);

                  ackRedistribution(reference, tx);

                  synchronized (Redistributor.this) {
                     active = true;

                     count++;

                     queue.deliverAsync();
                  }
               } catch (Exception e) {
                  try {
                     tx.rollback();
                  } catch (Exception e2) {
                     // Nothing much we can do now
                     ActiveMQServerLogger.LOGGER.failedToRollback(e2);
                  }
               }
            }
         });
      }

      return HandleStatus.HANDLED;
   }

   @Override
   public void proceedDeliver(MessageReference ref) {
      // no op
   }

   private void internalExecute(final Runnable runnable) {
      pendingRuns.countUp();
      executor.execute(new Runnable() {
         @Override
         public void run() {
            try {
               runnable.run();
            } finally {
               pendingRuns.countDown();
            }
         }
      });
   }

   private void ackRedistribution(final MessageReference reference, final Transaction tx) throws Exception {
      reference.handled();

      queue.acknowledge(tx, reference);

      tx.commit();

      storageManager.afterCompleteOperations(new IOCallback() {

         @Override
         public void onError(final int errorCode, final String errorMessage) {
            ActiveMQServerLogger.LOGGER.ioErrorRedistributing(errorCode, errorMessage);
         }

         @Override
         public void done() {
            execPrompter();
         }
      });
   }

   private void execPrompter() {
      count++;

      // We use >= as the large message redistribution will set count to max_int
      // so we are use the prompter will get called
      if (count >= batchSize) {
         // We continue the next batch on a different thread, so as not to keep the delivery thread busy for a very
         // long time in the case there are many messages in the queue
         active = false;

         executor.execute(new Prompter());

         count = 0;
      }

   }

   private class Prompter implements Runnable {

      @Override
      public void run() {
         synchronized (Redistributor.this) {
            active = true;

            queue.deliverAsync();
         }
      }
   }

   /* (non-Javadoc)
    * @see org.apache.activemq.artemis.core.server.Consumer#getDeliveringMessages()
    */
   @Override
   public List<MessageReference> getDeliveringMessages() {
      return Collections.emptyList();
   }

}

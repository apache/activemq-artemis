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
package org.apache.activemq.artemis.core.protocol.openwire.amq;

import java.util.List;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageConverter;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.HandleStatus;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;

public class AMQServerConsumer extends ServerConsumerImpl {

   public AMQServerConsumer(long consumerID,
                            AMQServerSession serverSession,
                            QueueBinding binding,
                            Filter filter,
                            boolean started,
                            boolean browseOnly,
                            StorageManager storageManager,
                            SessionCallback callback,
                            boolean preAcknowledge,
                            boolean strictUpdateDeliveryCount,
                            ManagementService managementService,
                            boolean supportLargeMessage,
                            Integer credits) throws Exception {
      super(consumerID, serverSession, binding, filter, started, browseOnly, storageManager, callback, preAcknowledge, strictUpdateDeliveryCount, managementService, supportLargeMessage, credits);
   }

   public void setBrowserListener(BrowserListener listener) {
      AMQBrowserDeliverer newBrowserDeliverer = new AMQBrowserDeliverer(this.browserDeliverer);
      newBrowserDeliverer.listener = listener;
      this.browserDeliverer = newBrowserDeliverer;
   }

   private class AMQBrowserDeliverer extends BrowserDeliverer {

      private BrowserListener listener = null;

      public AMQBrowserDeliverer(final BrowserDeliverer other) {
         super(other.iterator);
      }

      @Override
      public synchronized void run() {
         // if the reference was busy during the previous iteration, handle it now
         if (current != null) {
            try {
               HandleStatus status = handle(current);

               if (status == HandleStatus.BUSY) {
                  return;
               }

               if (status == HandleStatus.HANDLED) {
                  proceedDeliver(current);
               }

               current = null;
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorBrowserHandlingMessage(e, current);
               return;
            }
         }

         MessageReference ref = null;
         HandleStatus status;

         while (true) {
            try {
               ref = null;
               synchronized (messageQueue) {
                  if (!iterator.hasNext()) {
                     //here we need to send a null for amq browsers
                     if (listener != null) {
                        listener.browseFinished();
                     }
                     break;
                  }

                  ref = iterator.next();

                  status = handle(ref);
               }

               if (status == HandleStatus.HANDLED) {
                  proceedDeliver(ref);
               }
               else if (status == HandleStatus.BUSY) {
                  // keep a reference on the current message reference
                  // to handle it next time the browser deliverer is executed
                  current = ref;
                  break;
               }
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorBrowserHandlingMessage(e, ref);
               break;
            }
         }
      }
   }

   public void amqPutBackToDeliveringList(final List<MessageReference> refs) {
      try {
         synchronized (this.deliveringRefs) {
            for (MessageReference ref : refs) {
               ref.incrementDeliveryCount();
               deliveringRefs.add(ref);
            }
            //adjust the order. Suppose deliveringRefs has 2 existing
            //refs m1, m2, and refs has 3 m3, m4, m5
            //new order must be m3, m4, m5, m1, m2
            if (refs.size() > 0) {
               long first = refs.get(0).getMessage().getMessageID();
               MessageReference m = deliveringRefs.peek();
               while (m.getMessage().getMessageID() != first) {
                  deliveringRefs.poll();
                  deliveringRefs.add(m);
                  m = deliveringRefs.peek();
               }
            }
         }
      }
      catch (ActiveMQException e) {
         // TODO: what to do here?
         throw new IllegalStateException(e.getMessage(), e);
      }
   }

   public void moveToDeadLetterAddress(long mid, Throwable cause) throws Exception {
      MessageReference ref = removeReferenceByID(mid);

      if (ref == null) {
         throw new IllegalStateException("Cannot find ref to ack " + mid);
      }

      ServerMessage coreMsg = ref.getMessage();
      coreMsg.putStringProperty(OpenWireMessageConverter.AMQ_MSG_DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY, cause.toString());

      QueueImpl queue = (QueueImpl) ref.getQueue();
      synchronized (queue) {
         queue.sendToDeadLetterAddress(ref);
         queue.decDelivering();
      }
   }

}

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

import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageConverter;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;

public class AMQServerConsumer extends ServerConsumerImpl {

   // TODO-NOW: remove this once unified
   AMQConsumer amqConsumer;

   public AMQConsumer getAmqConsumer() {
      return amqConsumer;
   }

   /** TODO-NOW: remove this once unified */
   public void setAmqConsumer(AMQConsumer amqConsumer) {
      this.amqConsumer = amqConsumer;
   }

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
                            Integer credits,
                            final ActiveMQServer server) throws Exception {
      super(consumerID, serverSession, binding, filter, started, browseOnly, storageManager, callback, preAcknowledge, strictUpdateDeliveryCount, managementService, supportLargeMessage, credits, server);
   }

   public void amqPutBackToDeliveringList(final List<MessageReference> refs) {
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

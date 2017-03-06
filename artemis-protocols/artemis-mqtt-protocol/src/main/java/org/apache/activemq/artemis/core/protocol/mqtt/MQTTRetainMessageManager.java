/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.mqtt;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.BindingQueryResult;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.LinkedListIterator;

public class MQTTRetainMessageManager {

   private MQTTSession session;

   public MQTTRetainMessageManager(MQTTSession session) {
      this.session = session;
   }

   /**
    * FIXME
    * Retained messages should be handled in the core API.  There is currently no support for retained messages
    * at the time of writing.  Instead we handle retained messages here.  This method will create a new queue for
    * every address that is used to store retained messages.  THere should only ever be one message in the retained
    * message queue.  When a new subscription is created the queue should be browsed and the message copied onto
    * the subscription queue for the consumer.  When a new retained message is received the message will be sent to
    * the retained queue and the previous retain message consumed to remove it from the queue.
    */
   void handleRetainedMessage(Message message, String address, boolean reset, Transaction tx) throws Exception {
      SimpleString retainAddress = new SimpleString(MQTTUtil.convertMQTTAddressFilterToCoreRetain(address, session.getWildcardConfiguration()));

      Queue queue = session.getServer().locateQueue(retainAddress);
      if (queue == null) {
         queue = session.getServerSession().createQueue(retainAddress, retainAddress, null, false, true);
      }


      try (LinkedListIterator<MessageReference> iterator = queue.iterator()) {
         synchronized (queue) {
            if (iterator.hasNext()) {
               MessageReference ref = iterator.next();
               iterator.remove();
               queue.acknowledge(tx, ref);
            }

            if (!reset) {
               sendToQueue(message.copy(session.getServer().getStorageManager().generateID()), queue, tx);
            }
         }
      }
   }

   // SEND to Queue.
   void addRetainedMessagesToQueue(Queue queue, String address) throws Exception {
      // The address filter that matches all retained message queues.
      String retainAddress = MQTTUtil.convertMQTTAddressFilterToCoreRetain(address, session.getWildcardConfiguration());
      BindingQueryResult bindingQueryResult = session.getServerSession().executeBindingQuery(new SimpleString(retainAddress));

      // Iterate over all matching retain queues and add the queue
      Transaction tx = session.getServerSession().newTransaction();
      try {
         synchronized (queue) {
            for (SimpleString retainedQueueName : bindingQueryResult.getQueueNames()) {
               Queue retainedQueue = session.getServer().locateQueue(retainedQueueName);
               try (LinkedListIterator<MessageReference> i = retainedQueue.iterator()) {
                  if (i.hasNext()) {
                     Message message = i.next().getMessage().copy(session.getServer().getStorageManager().generateID());
                     sendToQueue(message, queue, tx);
                  }
               }
            }
         }
      } catch (Throwable t) {
         tx.rollback();
         throw t;
      }
      tx.commit();
   }

   private void sendToQueue(Message message, Queue queue, Transaction tx) throws Exception {
      RoutingContext context = new RoutingContextImpl(tx);
      queue.route(message, context);
      session.getServer().getPostOffice().processRoute(message, context, false);
   }
}

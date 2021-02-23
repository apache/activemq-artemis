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
package org.apache.activemq.artemis.core.server;


import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.MessageReferenceImpl;
import org.apache.activemq.artemis.core.transaction.Transaction;

/**
 * A reference to a message.
 *
 * Channels store message references rather than the messages themselves.
 */
public interface MessageReference {

   final class Factory {
      public static MessageReference createReference(Message encode, final Queue queue) {
         return new MessageReferenceImpl(encode, queue);
      }
   }
   boolean isPaged();

   Message getMessage();

   long getMessageID();

   boolean isDurable();

   SimpleString getLastValueProperty();

   /**
    * This is to be used in cases where a message delivery happens on an executor.
    * Most MessageReference implementations will allow execution, and if it does,
    * and the protocol requires an execution per message, this callback may be used.
    *
    * At the time of this implementation only AMQP was used.
    */
   void onDelivery(Consumer<? super MessageReference> callback);

   /**
    * We define this method aggregation here because on paging we need to hold the original estimate,
    * so we need to perform some extra steps on paging.
    *
    * @return
    */
   int getMessageMemoryEstimate();

   /**
    * To be used on holding protocol specific data during the delivery.
    * This will be only valid while the message is on the delivering queue at the consumer
    */
   Object getProtocolData();

   /**
    * To be used on holding protocol specific data during the delivery.
    * This will be only valid while the message is on the delivering queue at the consumer
    */
   void setProtocolData(Object data);

   MessageReference copy(Queue queue);

   /**
    * @return The time in the future that delivery will be delayed until, or zero if
    * no scheduled delivery will occur
    */
   long getScheduledDeliveryTime();

   void setScheduledDeliveryTime(long scheduledDeliveryTime);

   int getDeliveryCount();

   void setDeliveryCount(int deliveryCount);

   void setPersistedCount(int deliveryCount);

   int getPersistedCount();

   void incrementDeliveryCount();

   void decrementDeliveryCount();

   Queue getQueue();

   void acknowledge() throws Exception;

   void acknowledge(Transaction tx) throws Exception;

   void acknowledge(Transaction tx, ServerConsumer consumer) throws Exception;

   void acknowledge(Transaction tx, AckReason reason, ServerConsumer consumer) throws Exception;

   void emptyConsumerID();

   void setConsumerId(long consumerID);

   boolean hasConsumerId();

   long getConsumerId();

   void handled();

   void setInDelivery(boolean alreadyDelivered);

   boolean isInDelivery();

   void setAlreadyAcked();

   boolean isAlreadyAcked();

   /**
    * This is the size of the message when persisted on disk which is used for metrics tracking
    * Note that even if the message itself is not persisted on disk (ie non-durable) this value is
    * still used for metrics tracking for the amount of data on a queue
    *
    * @return
    * @throws ActiveMQException
    */
   long getPersistentSize() throws ActiveMQException;

}

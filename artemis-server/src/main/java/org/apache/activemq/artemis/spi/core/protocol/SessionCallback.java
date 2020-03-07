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
package org.apache.activemq.artemis.spi.core.protocol;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;

public interface SessionCallback {

   /** A requirement to do direct delivery is:
    *  no extra locking required at the protocol layer.
    *  which cannot be guaranteed at AMQP as proton will need the locking.
    *  So, disable this on AMQP or any other protocol requiring extra lock.
    * @return
    */
   default boolean supportsDirectDelivery() {
      return true;
   }


   /**
    * This one gives a chance for Proton to have its own flow control.
    */
   boolean hasCredits(ServerConsumer consumerID);

   /**
    * This can be used to complete certain operations outside of the lock,
    * like acks or other operations.
    */
   void afterDelivery() throws Exception;

   /**
    * Use this to updates specifics on the message after a redelivery happened.
    * Return true if there was specific logic applied on the protocol, so the ServerConsumer won't make any adjustments.
    *
    * @param consumer
    * @param ref
    * @param failed
    */
   boolean updateDeliveryCountAfterCancel(ServerConsumer consumer, MessageReference ref, boolean failed);

   void sendProducerCreditsMessage(int credits, SimpleString address);

   void sendProducerCreditsFailMessage(int credits, SimpleString address);

   // Note: don't be tempted to remove the parameter message
   //       Even though ref will contain the message in certain cases
   //       such as paging the message could be a SoftReference or WeakReference
   //       and I wanted to avoid re-fetching paged data in case of GCs on this specific case.
   //
   //       Future developments may change this, but beware why I have chosen to keep the parameter separated here
   int sendMessage(MessageReference ref, Message message, ServerConsumer consumerID, int deliveryCount);

   int sendLargeMessage(MessageReference reference,
                        Message message,
                        ServerConsumer consumerID,
                        long bodySize,
                        int deliveryCount);

   int sendLargeMessageContinuation(ServerConsumer consumerID,
                                    byte[] body,
                                    boolean continues,
                                    boolean requiresResponse);

   void closed();

   void disconnect(ServerConsumer consumerId, SimpleString queueName);

   boolean isWritable(ReadyListener callback, Object protocolContext);

   /**
    * Some protocols (Openwire) needs a special message with the browser is finished.
    */
   void browserFinished(ServerConsumer consumer);

   default void close(boolean failed) {

   }

   default Transaction getCurrentTransaction() {
      return null;
   }
}

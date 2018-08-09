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

package org.apache.activemq.artemis.core.server.plugin;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.transaction.Transaction;

/**
 *
 */
public interface ActiveMQServerMessagePlugin extends ActiveMQServerBasePlugin {

   /**
    * Before a message is sent
    *
    * @param session the session that sends the message
    * @param tx
    * @param message
    * @param direct
    * @param noAutoCreateQueue
    * @throws ActiveMQException
    */
   default void beforeSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue) throws ActiveMQException {
      //by default call the old method for backwards compatibility
      this.beforeSend(tx, message, direct, noAutoCreateQueue);
   }

   /**
    * After a message is sent
    *
    * @param session the session that sends the message
    * @param tx
    * @param message
    * @param direct
    * @param noAutoCreateQueue
    * @param result
    * @throws ActiveMQException
    */
   default void afterSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue,
                          RoutingStatus result) throws ActiveMQException {
      //by default call the old method for backwards compatibility
      this.afterSend(tx, message, direct, noAutoCreateQueue, result);
   }


   /**
    * Before a message is sent
    *
    * @param tx
    * @param message
    * @param direct
    * @param noAutoCreateQueue
    * @throws ActiveMQException
    *
    * @deprecated use {@link #beforeSend(ServerSession, Transaction, Message, boolean, boolean)}
    */
   @Deprecated
   default void beforeSend(Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue) throws ActiveMQException {

   }

   /**
    * After a message is sent
    *
    * @param tx
    * @param message
    * @param direct
    * @param noAutoCreateQueue
    * @param result
    * @throws ActiveMQException
    *
    * @deprecated use {@link #afterSend(ServerSession, Transaction, Message, boolean, boolean, RoutingStatus)}
    */
   @Deprecated
   default void afterSend(Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue,
                          RoutingStatus result) throws ActiveMQException {

   }

   /**
    * Before a message is routed
    *
    * @param message
    * @param context
    * @param direct
    * @param rejectDuplicates
    * @throws ActiveMQException
    */
   default void beforeMessageRoute(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates) throws ActiveMQException {

   }

   /**
    * After a message is routed
    *
    * @param message
    * @param context
    * @param direct
    * @param rejectDuplicates
    * @param result
    * @throws ActiveMQException
    */
   default void afterMessageRoute(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates,
                                  RoutingStatus result) throws ActiveMQException {

   }

   /**
    * Before a message is delivered to a client consumer
    *
    * @param consumer the consumer the message will be delivered to
    * @param reference message reference
    * @throws ActiveMQException
    */
   default void beforeDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
      //by default call the old method for backwards compatibility
      this.beforeDeliver(reference);
   }

   /**
    * After a message is delivered to a client consumer
    *
    * @param consumer the consumer the message was delivered to
    * @param reference message reference
    * @throws ActiveMQException
    */
   default void afterDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
      //by default call the old method for backwards compatibility
      this.afterDeliver(reference);
   }

   /**
    * Before a message is delivered to a client consumer
    *
    * @param reference
    * @throws ActiveMQException
    *
    * @deprecated use throws ActiveMQException {@link #beforeDeliver(ServerConsumer, MessageReference)}
    */
   @Deprecated
   default void beforeDeliver(MessageReference reference) throws ActiveMQException {

   }

   /**
    * After a message is delivered to a client consumer
    *
    * @param reference
    * @throws ActiveMQException
    *
    * @deprecated use {@link #afterDeliver(ServerConsumer, MessageReference)}
    */
   @Deprecated
   default void afterDeliver(MessageReference reference) throws ActiveMQException {

   }

   /**
    * A message has been expired
    *
    * @param message The expired message
    * @param messageExpiryAddress The message expiry address if exists
    * @throws ActiveMQException
    *
    * @deprecated use {@link #messageExpired(MessageReference, SimpleString, ServerConsumer)}
    */
   @Deprecated
   default void messageExpired(MessageReference message, SimpleString messageExpiryAddress) throws ActiveMQException {

   }

   /**
    * A message has been expired
    *
    * @param message The expired message
    * @param messageExpiryAddress The message expiry address if exists
    * @param consumer the Consumer that acknowledged the message - this field is optional
    * and can be null
    * @throws ActiveMQException
    */
   default void messageExpired(MessageReference message, SimpleString messageExpiryAddress, ServerConsumer consumer) throws ActiveMQException {
      messageExpired(message, messageExpiryAddress);
   }

   /**
    * A message has been acknowledged
    *
    * @param ref The acked message
    * @param reason The ack reason
    * @throws ActiveMQException
    *
    * @deprecated use {@link #messageAcknowledged(MessageReference, AckReason, ServerConsumer)}
    */
   @Deprecated
   default void messageAcknowledged(MessageReference ref, AckReason reason) throws ActiveMQException {

   }

   /**
    * A message has been acknowledged
    *
    * @param ref The acked message
    * @param reason The ack reason
    * @param consumer the Consumer that acknowledged the message - this field is optional
    * and can be null
    * @throws ActiveMQException
    *
    */
   default void messageAcknowledged(MessageReference ref, AckReason reason, ServerConsumer consumer) throws ActiveMQException {
      //by default call the old method for backwards compatibility
      this.messageAcknowledged(ref, reason);
   }
}

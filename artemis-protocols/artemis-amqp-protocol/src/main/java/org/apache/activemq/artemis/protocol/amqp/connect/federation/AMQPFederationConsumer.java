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
package org.apache.activemq.artemis.protocol.amqp.connect.federation;

import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.DETACH_FORCED;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.NOT_FOUND;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.RESOURCE_DELETED;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.internal.FederationConsumerInternal;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;

/**
 * Base class for AMQP Federation consumers that implements some of the common functionality.
 */
public abstract class AMQPFederationConsumer implements FederationConsumerInternal {

   protected static final Symbol[] OUTCOMES = new Symbol[]{Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL,
                                                           Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL};

   protected static final Modified DEFAULT_OUTCOME;
   static {
      DEFAULT_OUTCOME = new Modified();
      DEFAULT_OUTCOME.setDeliveryFailed(true);
   }

   protected final AMQPFederation federation;
   protected final AMQPFederationConsumerConfiguration configuration;
   protected final FederationConsumerInfo consumerInfo;
   protected final AMQPConnectionContext connection;
   protected final AMQPSessionContext session;
   protected final Predicate<Link> remoteCloseInterceptor = this::remoteLinkClosedInterceptor;
   protected final BiConsumer<FederationConsumerInfo, Message> messageObserver;
   protected final AtomicLong messageCount = new AtomicLong();

   protected Receiver protonReceiver;
   protected boolean started;
   protected volatile boolean closed;
   protected Consumer<FederationConsumerInternal> remoteCloseHandler;

   public AMQPFederationConsumer(AMQPFederation federation, AMQPFederationConsumerConfiguration configuration,
                                 AMQPSessionContext session, FederationConsumerInfo consumerInfo,
                                 BiConsumer<FederationConsumerInfo, Message> messageObserver) {
      this.federation = federation;
      this.consumerInfo = consumerInfo;
      this.connection = session.getAMQPConnectionContext();
      this.session = session;
      this.configuration = configuration;
      this.messageObserver = messageObserver;
   }

   /**
    * @return the number of messages this consumer has received from the remote during its lifetime.
    */
   public final long getMessagesReceived() {
      return messageCount.get();
   }

   @Override
   public final AMQPFederation getFederation() {
      return federation;
   }

   @Override
   public final FederationConsumerInfo getConsumerInfo() {
      return consumerInfo;
   }

   @Override
   public final synchronized void start() {
      if (!started && !closed) {
         started = true;
         asyncCreateReceiver();
      }
   }

   @Override
   public final synchronized void close() {
      if (!closed) {
         closed = true;
         if (started) {
            started = false;
            asyncCloseReceiver();
         }
      }
   }

   @Override
   public final AMQPFederationConsumer setRemoteClosedHandler(Consumer<FederationConsumerInternal> handler) {
      if (started) {
         throw new IllegalStateException("Cannot set a remote close handler after the consumer is started");
      }

      this.remoteCloseHandler = handler;
      return this;
   }

   protected final boolean remoteLinkClosedInterceptor(Link link) {
      if (link == protonReceiver && link.getRemoteCondition() != null && link.getRemoteCondition().getCondition() != null) {
         final Symbol errorCondition = link.getRemoteCondition().getCondition();

         // Cases where remote link close is not considered terminal, additional checks
         // should be added as needed for cases where the remote has closed the link either
         // during the attach or at some point later.

         if (RESOURCE_DELETED.equals(errorCondition)) {
            // Remote side manually deleted this queue.
            return true;
         } else if (NOT_FOUND.equals(errorCondition)) {
            // Remote did not have a queue that matched.
            return true;
         } else if (DETACH_FORCED.equals(errorCondition)) {
            // Remote operator forced the link to detach.
            return true;
         }
      }

      return false;
   }

   /**
    * Called from a subclass upon handling an incoming federated message from the remote.
    *
    * @param message
    *    The original message that arrived from the remote.
    */
   protected final void recordFederatedMessageReceived(Message message) {
      messageCount.incrementAndGet();

      if (messageObserver != null) {
         messageObserver.accept(consumerInfo, message);
      }
   }

   /**
    * Called during the start of the consumer to trigger an asynchronous link attach
    * of the underlying AMQP receiver that backs this federation consumer.
    */
   protected abstract void asyncCreateReceiver();

   /**
    * Called during the close of the consumer to trigger an asynchronous link detach
    * of the underlying AMQP receiver that backs this federation consumer.
    */
   protected abstract void asyncCloseReceiver();

}

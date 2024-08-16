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

import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederation.FEDERATION_INSTANCE_RECORD;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPLargeMessageWriter;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPMessageWriter;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPTunneledCoreLargeMessageWriter;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPTunneledCoreMessageWriter;
import org.apache.activemq.artemis.protocol.amqp.proton.MessageWriter;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerSenderContext;
import org.apache.activemq.artemis.protocol.amqp.proton.SenderController;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Sender;

/**
 * A base class abstract {@link SenderController} implementation for use by federation address and
 * queue senders that provides some common functionality used between both.
 */
public abstract class AMQPFederationBaseSenderController implements SenderController {

   protected final AMQPSessionContext session;
   protected final AMQPSessionCallback sessionSPI;
   protected final AMQPFederation federation;
   protected final String controllerId = UUID.randomUUID().toString();

   protected AMQPMessageWriter standardMessageWriter;
   protected AMQPLargeMessageWriter largeMessageWriter;
   protected AMQPTunneledCoreMessageWriter coreMessageWriter;
   protected AMQPTunneledCoreLargeMessageWriter coreLargeMessageWriter;

   protected boolean tunnelCoreMessages; // only enabled if remote offers support.

   protected Consumer<ErrorCondition> resourceDeletedAction;

   public AMQPFederationBaseSenderController(AMQPSessionContext session) throws ActiveMQAMQPException {
      final Connection protonConnection = session.getSession().getConnection();
      final org.apache.qpid.proton.engine.Record attachments = protonConnection.attachments();

      this.federation = attachments.get(FEDERATION_INSTANCE_RECORD, AMQPFederation.class);
      this.session = session;
      this.sessionSPI = session.getSessionSPI();
   }

   public AMQPSessionContext getSessionContext() {
      return session;
   }

   public AMQPSessionCallback getSessionCallback() {
      return sessionSPI;
   }

   @Override
   public final void close() throws Exception {
      if (federation != null) {
         federation.removeLinkClosedInterceptor(controllerId);
      }

      handleLinkRemotelyClosed();
   }

   protected void handleLinkRemotelyClosed() {
      // Default does nothing.
   }

   @Override
   public final void close(ErrorCondition error) {
      if (error != null && AmqpError.RESOURCE_DELETED.equals(error.getCondition())) {
         if (resourceDeletedAction != null) {
            resourceDeletedAction.accept(error);
         }
      }

      if (federation != null) {
         federation.removeLinkClosedInterceptor(controllerId);
      }

      handleLinkLocallyClosed(error);
   }

   protected void handleLinkLocallyClosed(ErrorCondition error) {
      // Default does nothing.
   }

   @Override
   public MessageWriter selectOutgoingMessageWriter(ProtonServerSenderContext sender, MessageReference reference) {
      final MessageWriter selected;
      final Message message = reference.getMessage();

      if (message instanceof AMQPMessage) {
         if (message.isLargeMessage()) {
            selected = largeMessageWriter != null ? largeMessageWriter :
               (largeMessageWriter = new AMQPLargeMessageWriter(sender));
         } else {
            selected = standardMessageWriter != null ? standardMessageWriter :
               (standardMessageWriter = new AMQPMessageWriter(sender));
         }
      } else if (tunnelCoreMessages) {
         if (message.isLargeMessage()) {
            selected = coreLargeMessageWriter != null ? coreLargeMessageWriter :
               (coreLargeMessageWriter = new AMQPTunneledCoreLargeMessageWriter(sender));
         } else {
            selected = coreMessageWriter != null ? coreMessageWriter :
               (coreMessageWriter = new AMQPTunneledCoreMessageWriter(sender));
         }
      } else {
         selected = standardMessageWriter != null ? standardMessageWriter :
            (standardMessageWriter = new AMQPMessageWriter(sender));
      }

      return selected;
   }

   protected final void registerRemoteLinkClosedInterceptor(Sender protonSender) {
      Objects.requireNonNull(federation, "Subclass should have validated federation state before adding an interceptor");

      federation.addLinkClosedInterceptor(controllerId, (link) -> {
         // Normal close from remote due to demand being removed is handled here but remote close with an error is left
         // to the parent federation instance to decide on how it should be handled.
         if (link == protonSender && (link.getRemoteCondition() == null || link.getRemoteCondition().getCondition() == null)) {
            return true;
         }

         return false;
      });
   }
}

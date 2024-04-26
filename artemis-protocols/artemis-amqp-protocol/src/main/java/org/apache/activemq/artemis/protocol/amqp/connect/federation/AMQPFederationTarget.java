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

import java.util.Objects;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConstants;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;

/**
 * This is the receiving side of an AMQP broker federation that occurs over an
 * inbound connection from a remote peer. The federation target only comes into
 * existence once a remote peer connects and successfully authenticates against
 * a control link validation address. Only one federation target is allowed per
 * connection.
 */
public class AMQPFederationTarget extends AMQPFederation {

   private final AMQPConnectionContext connection;
   private final AMQPFederationConfiguration configuration;

   public AMQPFederationTarget(String name, AMQPFederationConfiguration configuration, AMQPSessionContext session, ActiveMQServer server) {
      super(name, server);

      Objects.requireNonNull(session, "Provided session instance cannot be null");

      this.session = session;
      this.connection = session.getAMQPConnectionContext();
      this.connection.addLinkRemoteCloseListener(getName(), this::handleLinkRemoteClose);
      this.configuration = configuration;
   }

   @Override
   public AMQPConnectionContext getConnectionContext() {
      return connection;
   }

   @Override
   public AMQPSessionContext getSessionContext() {
      return session;
   }

   @Override
   public synchronized AMQPFederationConfiguration getConfiguration() {
      return configuration;
   }

   @Override
   protected void handleFederationStarted() throws ActiveMQException {
      // Tag the session with Federation metadata which will allow local federation policies sent by
      // the remote to apply checks when seeing local demand to determine if a federation consumer
      // should cause remote receivers to be created.
      //
      // This currently is a session global tag which means any consumer created from this session in
      // response to remote attach of said receiver is going to get caught by the filtering but as of
      // now we shouldn't be creating consumers other than federation consumers but if that were to
      // change we'd either need single new session for this federation instance or a session per
      // consumer at the extreme which then requires that the protocol handling code add the metadata
      // during the receiver attach on the remote.
      try {
         session.getSessionSPI().addMetaData(FederationConstants.FEDERATION_NAME, getName());
      } catch (ActiveMQAMQPException e) {
         throw e;
      } catch (Exception e) {
         throw new ActiveMQAMQPInternalErrorException("Error while configuring interal session metadata");
      }

      super.handleFederationStarted();
   }

   private void handleLinkRemoteClose(Link link) {
      // If the connection has already closed then we can ignore this event.
      final Connection protonConnection = link.getSession().getConnection();
      if (protonConnection.getLocalState() != EndpointState.ACTIVE) {
         return;
      }

      // If the link is locally closed then we closed it intentionally and
      // we can continue as normal otherwise we need to check on why it closed.
      if (link.getLocalState() != EndpointState.ACTIVE) {
         return;
      }

      // Did the federation links handle this so that we can ignore it?
      // If not then we consider this a terminal outcome and close the connection.
      if (!invokeLinkClosedInterceptors(link)) {
         signalError(new ActiveMQAMQPInternalErrorException("Federation link closed unexpectedly: " + link.getName()));
      }
   }

   @Override
   protected void signalResourceCreateError(Exception cause) {
      signalError(cause);
   }

   @Override
   protected void signalError(Exception cause) {
      final Symbol condition;
      final String description = cause.getMessage();

      if (cause instanceof ActiveMQAMQPException) {
         condition = ((ActiveMQAMQPException) cause).getAmqpError();
      } else {
         condition = AmqpError.INTERNAL_ERROR;
      }

      connection.close(new ErrorCondition(condition, description));
   }
}

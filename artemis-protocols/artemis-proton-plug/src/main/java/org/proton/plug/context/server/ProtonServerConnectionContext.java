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
package org.proton.plug.context.server;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.proton.plug.AMQPConnectionCallback;
import org.proton.plug.AMQPServerConnectionContext;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.context.AbstractConnectionContext;
import org.proton.plug.context.AbstractProtonSessionContext;
import org.proton.plug.exceptions.ActiveMQAMQPException;
import org.proton.plug.handler.ExtCapability;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

public class ProtonServerConnectionContext extends AbstractConnectionContext implements AMQPServerConnectionContext {

   public ProtonServerConnectionContext(AMQPConnectionCallback connectionSP, Executor dispatchExecutor, ScheduledExecutorService scheduledPool) {
      super(connectionSP, dispatchExecutor, scheduledPool);
   }

   public ProtonServerConnectionContext(AMQPConnectionCallback connectionSP,
                                        String containerId,
                                        int idleTimeout,
                                        int maxFrameSize,
                                        int channelMax,
                                        Executor dispatchExecutor,
                                        ScheduledExecutorService scheduledPool) {
      super(connectionSP, containerId, idleTimeout, maxFrameSize, channelMax, dispatchExecutor, scheduledPool);
   }

   @Override
   protected AbstractProtonSessionContext newSessionExtension(Session realSession) throws ActiveMQAMQPException {
      AMQPSessionCallback sessionSPI = connectionCallback.createSessionCallback(this);
      AbstractProtonSessionContext protonSession = new ProtonServerSessionContext(sessionSPI, this, realSession);

      return protonSession;
   }

   @Override
   protected boolean validateConnection(Connection connection) {
      return connectionCallback.validateConnection(connection, handler.getSASLResult());
   }

   @Override
   protected void initInternal() throws Exception {
      connectionCallback.init();
   }

   @Override
   protected void remoteLinkOpened(Link link) throws Exception {

      ProtonServerSessionContext protonSession = (ProtonServerSessionContext) getSessionExtension(link.getSession());

      link.setSource(link.getRemoteSource());
      link.setTarget(link.getRemoteTarget());
      if (link instanceof Receiver) {
         Receiver receiver = (Receiver) link;
         if (link.getRemoteTarget() instanceof Coordinator) {
            Coordinator coordinator = (Coordinator) link.getRemoteTarget();
            protonSession.addTransactionHandler(coordinator, receiver);
         }
         else {
            protonSession.addReceiver(receiver);
         }
      }
      else {
         Sender sender = (Sender) link;
         protonSession.addSender(sender);
         sender.offer(1);
      }
   }

   public Symbol[] getConnectionCapabilitiesOffered() {
      return ExtCapability.getCapabilities();
   }
}

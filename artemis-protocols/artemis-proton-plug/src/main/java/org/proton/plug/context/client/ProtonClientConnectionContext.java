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
package org.proton.plug.context.client;

import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Session;
import org.proton.plug.AMQPClientConnectionContext;
import org.proton.plug.AMQPClientSessionContext;
import org.proton.plug.ClientSASL;
import org.proton.plug.AMQPConnectionCallback;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.context.AbstractConnectionContext;
import org.proton.plug.context.AbstractProtonSessionContext;
import org.proton.plug.exceptions.ActiveMQAMQPException;
import org.proton.plug.context.ProtonInitializable;
import org.proton.plug.util.FutureRunnable;

public class ProtonClientConnectionContext extends AbstractConnectionContext implements AMQPClientConnectionContext {

   public ProtonClientConnectionContext(AMQPConnectionCallback connectionCallback) {
      super(connectionCallback);
   }

   public ProtonClientConnectionContext(AMQPConnectionCallback connectionCallback,
                                        int idleTimeout,
                                        int maxFrameSize,
                                        int channelMax) {
      super(connectionCallback, idleTimeout, maxFrameSize, channelMax);
   }

   // Maybe a client interface?
   @Override
   public void clientOpen(ClientSASL sasl) throws Exception {
      FutureRunnable future = new FutureRunnable(1);
      synchronized (handler.getLock()) {
         this.afterInit(future);
         if (sasl != null) {
            handler.createClientSasl(sasl);
         }
         handler.getConnection().open();
      }

      flush();

      waitWithTimeout(future);
   }

   @Override
   public AMQPClientSessionContext createClientSession() throws ActiveMQAMQPException {

      FutureRunnable futureRunnable = new FutureRunnable(1);
      ProtonClientSessionContext sessionImpl;
      synchronized (handler.getLock()) {
         Session session = handler.getConnection().session();
         sessionImpl = (ProtonClientSessionContext) getSessionExtension(session);
         sessionImpl.afterInit(futureRunnable);
         session.open();
      }

      flush();
      waitWithTimeout(futureRunnable);

      return sessionImpl;
   }

   @Override
   protected AbstractProtonSessionContext newSessionExtension(Session realSession) throws ActiveMQAMQPException {
      AMQPSessionCallback sessionSPI = connectionCallback.createSessionCallback(this);
      AbstractProtonSessionContext protonSession = new ProtonClientSessionContext(sessionSPI, this, realSession);

      return protonSession;

   }

   @Override
   protected void remoteLinkOpened(Link link) throws Exception {
      Object context = link.getContext();
      if (context != null && context instanceof ProtonInitializable) {
         ((ProtonInitializable) context).initialise();
      }
   }
}

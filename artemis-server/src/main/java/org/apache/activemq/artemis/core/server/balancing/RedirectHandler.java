/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.balancing;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.spi.core.remoting.Connection;

public abstract class RedirectHandler<T extends RedirectContext> {
   private final ActiveMQServer server;


   public ActiveMQServer getServer() {
      return server;
   }


   protected RedirectHandler(ActiveMQServer server) {
      this.server = server;
   }

   protected abstract void cannotRedirect(T context) throws Exception;

   protected abstract void redirectTo(T context) throws Exception;

   protected boolean redirect(T context) throws Exception {
      Connection transportConnection = context.getConnection().getTransportConnection();

      BrokerBalancer brokerBalancer = getServer().getBalancerManager().getBalancer(transportConnection.getRedirectTo());

      if (brokerBalancer == null) {
         ActiveMQServerLogger.LOGGER.brokerBalancerNotFound(transportConnection.getRedirectTo());

         cannotRedirect(context);

         return true;
      }

      context.setTarget(brokerBalancer.getTarget(transportConnection, context.getClientID(), context.getUsername()));

      if (context.getTarget() == null) {
         ActiveMQServerLogger.LOGGER.cannotRedirectClientConnection(transportConnection);

         cannotRedirect(context);

         return true;
      }

      ActiveMQServerLogger.LOGGER.redirectClientConnection(transportConnection, context.getTarget());

      if (!context.getTarget().isLocal()) {
         redirectTo(context);

         return true;
      }

      return false;
   }
}

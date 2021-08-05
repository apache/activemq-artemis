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

package org.apache.activemq.artemis.protocol.amqp.proton;

import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancing.RedirectHandler;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.qpid.proton.amqp.transport.ConnectionError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;

import java.util.HashMap;
import java.util.Map;

public class AMQPRedirectHandler extends RedirectHandler<AMQPRedirectContext> {

   public AMQPRedirectHandler(ActiveMQServer server) {
      super(server);
   }


   public boolean redirect(AMQPConnectionContext connectionContext, Connection protonConnection) throws Exception {
      return redirect(new AMQPRedirectContext(connectionContext, protonConnection));
   }

   @Override
   protected void cannotRedirect(AMQPRedirectContext context) throws Exception {
      ErrorCondition error = new ErrorCondition();
      error.setCondition(ConnectionError.CONNECTION_FORCED);
      error.setDescription(String.format("Broker balancer %s is not ready to redirect", context.getConnection().getTransportConnection().getRedirectTo()));
      context.getProtonConnection().setCondition(error);
   }

   @Override
   protected void redirectTo(AMQPRedirectContext context) throws Exception {
      String host = ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME, TransportConstants.DEFAULT_HOST, context.getTarget().getConnector().getParams());
      int port = ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT, context.getTarget().getConnector().getParams());

      ErrorCondition error = new ErrorCondition();
      error.setCondition(ConnectionError.REDIRECT);
      error.setDescription(String.format("Connection redirected to %s:%d by broker balancer %s", host, port, context.getConnection().getTransportConnection().getRedirectTo()));
      Map info = new HashMap();
      info.put(AmqpSupport.NETWORK_HOST, host);
      info.put(AmqpSupport.PORT, port);
      error.setInfo(info);
      context.getProtonConnection().setCondition(error);
   }
}

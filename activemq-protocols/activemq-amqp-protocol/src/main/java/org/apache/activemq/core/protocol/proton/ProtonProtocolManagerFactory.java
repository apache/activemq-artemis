/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.activemq6.core.protocol.proton;

import org.apache.activemq6.api.core.Interceptor;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.spi.core.protocol.ProtocolManager;
import org.apache.activemq6.spi.core.protocol.ProtocolManagerFactory;

import java.util.List;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ProtonProtocolManagerFactory implements ProtocolManagerFactory
{
   private static final String AMQP_PROTOCOL_NAME = "AMQP";

   private static String[] SUPPORTED_PROTOCOLS = {AMQP_PROTOCOL_NAME};

   @Override
   public ProtocolManager createProtocolManager(HornetQServer server, List<Interceptor> incomingInterceptors, List<Interceptor> outgoingInterceptors)
   {
      return new ProtonProtocolManager(server);
   }

   @Override
   public String[] getProtocols()
   {
      return SUPPORTED_PROTOCOLS;
   }
}

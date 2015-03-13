/**
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
package org.apache.activemq.core.protocol.stomp;

import java.util.List;

import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.spi.core.protocol.ProtocolManager;
import org.apache.activemq.spi.core.protocol.ProtocolManagerFactory;

public class StompProtocolManagerFactory implements ProtocolManagerFactory
{
   public static final String STOMP_PROTOCOL_NAME = "STOMP";

   private static String[] SUPPORTED_PROTOCOLS = {STOMP_PROTOCOL_NAME};

   public ProtocolManager createProtocolManager(final ActiveMQServer server, final List<Interceptor> incomingInterceptors, List<Interceptor> outgoingInterceptors)
   {
      return new StompProtocolManager(server, incomingInterceptors, outgoingInterceptors);
   }

   @Override
   public String[] getProtocols()
   {
      return SUPPORTED_PROTOCOLS;
   }

}

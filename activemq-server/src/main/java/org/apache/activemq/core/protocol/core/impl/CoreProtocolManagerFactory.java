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
package org.apache.activemq.core.protocol.core.impl;

import java.util.List;

import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.api.core.client.HornetQClient;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.spi.core.protocol.ProtocolManager;
import org.apache.activemq.spi.core.protocol.ProtocolManagerFactory;

/**
 * A CoreProtocolManagerFactory
 *
 * @author Tim Fox
 *
 *
 */
public class CoreProtocolManagerFactory implements ProtocolManagerFactory
{
   private static String[] SUPPORTED_PROTOCOLS = {HornetQClient.DEFAULT_CORE_PROTOCOL};

   public ProtocolManager createProtocolManager(final HornetQServer server, final List<Interceptor> incomingInterceptors, List<Interceptor> outgoingInterceptors)
   {
      return new CoreProtocolManager(server, incomingInterceptors, outgoingInterceptors);
   }

   @Override
   public String[] getProtocols()
   {
      return SUPPORTED_PROTOCOLS;
   }
}

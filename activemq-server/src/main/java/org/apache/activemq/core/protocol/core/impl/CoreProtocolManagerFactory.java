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
package org.apache.activemq.core.protocol.core.impl;

import java.util.List;

import org.apache.activemq.api.core.BaseInterceptor;
import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.spi.core.protocol.AbstractProtocolManagerFactory;
import org.apache.activemq.spi.core.protocol.ProtocolManager;

public class CoreProtocolManagerFactory extends AbstractProtocolManagerFactory<Interceptor>
{
   private static String[] SUPPORTED_PROTOCOLS = {ActiveMQClient.DEFAULT_CORE_PROTOCOL};

   /**
    * {@inheritDoc} *
    * @param server
    * @param incomingInterceptors
    * @param outgoingInterceptors
    * @return
    */
   public ProtocolManager createProtocolManager(final ActiveMQServer server, final List<Interceptor> incomingInterceptors, List<Interceptor> outgoingInterceptors)
   {
      return new CoreProtocolManager(this, server, incomingInterceptors, outgoingInterceptors);
   }

   @Override
   public List<Interceptor> filterInterceptors(List<BaseInterceptor> interceptors)
   {
      // This is using this tool method
      // it wouldn't be possible to write a generic method without this class parameter
      // and I didn't want to bloat the cllaers for this
      return filterInterceptors(Interceptor.class, interceptors);
   }

   @Override
   public String[] getProtocols()
   {
      return SUPPORTED_PROTOCOLS;
   }
}

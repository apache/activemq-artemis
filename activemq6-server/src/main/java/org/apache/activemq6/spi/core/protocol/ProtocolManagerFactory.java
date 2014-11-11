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
package org.apache.activemq6.spi.core.protocol;

import java.util.List;

import org.apache.activemq6.api.core.Interceptor;
import org.apache.activemq6.core.server.HornetQServer;

/**
 * A ProtocolManagerFactory
 *
 * @author tim
 *
 *
 */
public interface ProtocolManagerFactory
{
   ProtocolManager createProtocolManager(HornetQServer server, List<Interceptor> incomingInterceptors, List<Interceptor> outgoingInterceptors);

   String[] getProtocols();
}

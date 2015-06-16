/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server;

import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.artemis.core.server.cluster.Transformer;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A holder for common services leveraged by the broker.
 */
public interface ServiceRegistry
{
   ExecutorService getExecutorService();

   void setExecutorService(ExecutorService executorService);

   ScheduledExecutorService getScheduledExecutorService();

   void setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService);

   void addConnectorService(ConnectorServiceFactory connectorServiceFactory, ConnectorServiceConfiguration configuration);

   void removeConnectorService(ConnectorServiceConfiguration configuration);

   Collection<Pair<ConnectorServiceFactory, ConnectorServiceConfiguration>> getConnectorServices();

   void addIncomingInterceptor(String name, Interceptor interceptor);

   void removeIncomingInterceptor(String name);

   Collection<Interceptor> getIncomingInterceptors();

   Interceptor getIncomingInterceptor(String name);

   void addOutgoingInterceptor(String name, Interceptor interceptor);

   Interceptor getOutgoingInterceptor(String name);

   void removeOutgoingInterceptor(String name);

   Collection<Interceptor> getOutgoingInterceptors();

   Transformer getDivertTransformer(String name);

   Transformer getBridgeTransformer(String name);
}

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

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.artemis.core.server.cluster.Transformer;

/**
 * A holder for common services leveraged by the broker.
 */
public interface ServiceRegistry
{
   ExecutorService getExecutorService();

   ScheduledExecutorService getScheduledExecutorService();

   Collection<Pair<ConnectorServiceFactory, ConnectorServiceConfiguration>> getConnectorServices();

   Collection<Interceptor> getIncomingInterceptors();

   Collection<Interceptor> getOutgoingInterceptors();

   Transformer getDivertTransformer(String name);

   Transformer getBridgeTransformer(String name);
}

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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.artemis.core.server.cluster.Transformer;
import org.apache.activemq.artemis.spi.core.remoting.AcceptorFactory;

/**
 * A holder for common services leveraged by the broker.
 */
public interface ServiceRegistry {

   ExecutorService getExecutorService();

   void setExecutorService(ExecutorService executorService);

   ScheduledExecutorService getScheduledExecutorService();

   void setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService);

   void addConnectorService(ConnectorServiceFactory connectorServiceFactory,
                            ConnectorServiceConfiguration configuration);

   void removeConnectorService(ConnectorServiceConfiguration configuration);

   /**
    * Get a collection of paired org.apache.activemq.artemis.core.server.ConnectorServiceFactory and
    * org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration instances.
    *
    * @param configs
    * @return
    */
   Collection<Pair<ConnectorServiceFactory, ConnectorServiceConfiguration>> getConnectorServices(List<ConnectorServiceConfiguration> configs);

   void addIncomingInterceptor(BaseInterceptor interceptor);

   /**
    * Get a list of org.apache.activemq.artemis.api.core.BaseInterceptor instances
    *
    * @param classNames
    * @return
    */
   List<BaseInterceptor> getIncomingInterceptors(List<String> classNames);

   void addOutgoingInterceptor(BaseInterceptor interceptor);

   /**
    * Get a list of org.apache.activemq.artemis.api.core.BaseInterceptor instances
    *
    * @param classNames
    * @return
    */
   List<BaseInterceptor> getOutgoingInterceptors(List<String> classNames);

   /**
    * Get an instance of org.apache.activemq.artemis.core.server.cluster.Transformer for a divert
    *
    * @param name      the name of divert for which the transformer will be used
    * @param className the fully qualified name of the transformer implementation (can be null)
    * @return
    */
   Transformer getDivertTransformer(String name, String className);

   void addDivertTransformer(String name, Transformer transformer);

   /**
    * Get an instance of org.apache.activemq.artemis.core.server.cluster.Transformer for a bridge
    *
    * @param name      the name of bridge for which the transformer will be used
    * @param className the fully qualified name of the transformer implementation (can be null)
    * @return
    */
   Transformer getBridgeTransformer(String name, String className);

   void addBridgeTransformer(String name, Transformer transformer);

   /**
    * Get an instance of org.apache.activemq.artemis.spi.core.remoting.AcceptorFactory
    *
    * @param name      the name of acceptor for which the factory will be used
    * @param className the fully qualified name of the factory implementation (can be null)
    * @return
    */
   AcceptorFactory getAcceptorFactory(String name, String className);

   void addAcceptorFactory(String name, AcceptorFactory acceptorFactory);
}

/*
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
package org.apache.activemq.artemis.core.server.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.artemis.core.server.ConnectorServiceFactory;

public class ServiceRegistry
{
   private ExecutorService executorService;

   private ScheduledExecutorService scheduledExecutorService;

   /* We are using a List rather than HashMap here as ActiveMQ Artemis allows multiple instances of the same class to be added
   * to the interceptor list
   */
   private Map<String, Interceptor> incomingInterceptors;

   private Map<String, Interceptor> outgoingInterceptors;

   private Map<String, Pair<ConnectorServiceFactory, ConnectorServiceConfiguration>> connectorServices;

   public ServiceRegistry()
   {
      this.incomingInterceptors = new ConcurrentHashMap<String, Interceptor>();
      this.outgoingInterceptors = new ConcurrentHashMap<String, Interceptor>();
      this.connectorServices = new ConcurrentHashMap<String, Pair<ConnectorServiceFactory, ConnectorServiceConfiguration>>();
   }

   public ExecutorService getExecutorService()
   {
      return executorService;
   }

   public void setExecutorService(ExecutorService executorService)
   {
      this.executorService = executorService;
   }

   public ScheduledExecutorService getScheduledExecutorService()
   {
      return scheduledExecutorService;
   }

   public void setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService)
   {
      this.scheduledExecutorService = scheduledExecutorService;
   }

   public void addConnectorService(ConnectorServiceFactory connectorServiceFactory, ConnectorServiceConfiguration configuration)
   {
      connectorServices.put(configuration.getConnectorName(), new Pair<>(connectorServiceFactory, configuration));
   }

   public void removeConnectorService(ConnectorServiceConfiguration configuration)
   {
      connectorServices.remove(configuration.getConnectorName());
   }

   public Collection<Pair<ConnectorServiceFactory, ConnectorServiceConfiguration>> getConnectorServices()
   {
      return connectorServices.values();
   }

   public void addIncomingInterceptor(String name, Interceptor interceptor)
   {
      incomingInterceptors.put(name, interceptor);
   }

   public void removeIncomingInterceptor(String name)
   {
      incomingInterceptors.remove(name);
   }

   public Collection<Interceptor> getIncomingInterceptors()
   {
      return Collections.unmodifiableCollection(incomingInterceptors.values());
   }

   public Interceptor getIncomingInterceptor(String name)
   {
      return incomingInterceptors.get(name);
   }

   public void addOutgoingInterceptor(String name, Interceptor interceptor)
   {
      outgoingInterceptors.put(name, interceptor);
   }

   public Interceptor getOutgoingInterceptor(String name)
   {
      return outgoingInterceptors.get(name);
   }

   public void removeOutgoingInterceptor(String name)
   {
      outgoingInterceptors.remove(name);
   }

   public Collection<Interceptor> getOutgoingInterceptors()
   {
      return Collections.unmodifiableCollection(outgoingInterceptors.values());
   }
}

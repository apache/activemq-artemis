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
package org.apache.activemq6.spi.core.remoting;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq6.api.core.TransportConfigurationHelper;

/**
 * A ConnectorFactory is used by the client for creating connectors.
 * <p>
 * A Connector is used to connect to an {@link org.apache.activemq6.spi.core.remoting.Acceptor}.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface ConnectorFactory extends TransportConfigurationHelper
{
   /**
    * creates a new instance of a connector.
    *
    * @param configuration       the configuration
    * @param handler             the handler
    * @param listener            the listener
    * @param closeExecutor       the close executor
    * @param threadPool          the thread pool
    * @param scheduledThreadPool the scheduled thread pool
    * @return a new connector
    */
   Connector createConnector(Map<String, Object> configuration,
                             BufferHandler handler,
                             ConnectionLifeCycleListener listener,
                             Executor closeExecutor,
                             Executor threadPool,
                             ScheduledExecutorService scheduledThreadPool,
                             ClientProtocolManager protocolManager);

   /**
    * Returns the allowable properties for this connector.
    * <p>
    * This will differ between different connector implementations.
    *
    * @return the allowable properties.
    */
   Set<String> getAllowableProperties();

   /**
    * Indicates if connectors from this factory are reliable or not. If a connector is reliable then connection
    * monitoring (i.e. pings/pongs) will be disabled.
    *
    * @return whether or not connectors from this factory are reliable
    */
   boolean isReliable();
}

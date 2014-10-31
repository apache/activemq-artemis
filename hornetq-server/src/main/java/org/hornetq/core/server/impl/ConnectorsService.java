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
package org.hornetq.core.server.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.ConnectorServiceConfiguration;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.ConnectorService;
import org.hornetq.core.server.ConnectorServiceFactory;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.utils.ClassloadingUtil;
import org.hornetq.utils.ConfigurationHelper;

/**
 * ConnectorsService will pool some resource for updates, e.g. Twitter, then the changes are picked
 * and converted into a ServerMessage for a given destination (queue).
 * <p/>
 * It may also listen to a queue, and forward them (e.g. messages arriving at the queue are picked
 * and tweeted to some Twitter account).
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a> Created Jun 29, 2010
 */
public final class ConnectorsService implements HornetQComponent
{
   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final ScheduledExecutorService scheduledPool;

   private boolean isStarted = false;

   private final Configuration configuration;

   private final Set<ConnectorService> connectors = new HashSet<ConnectorService>();

   public ConnectorsService(final Configuration configuration,
                            final StorageManager storageManager,
                            final ScheduledExecutorService scheduledPool,
                            final PostOffice postOffice)
   {
      this.configuration = configuration;
      this.storageManager = storageManager;
      this.scheduledPool = scheduledPool;
      this.postOffice = postOffice;
   }

   public void start() throws Exception
   {
      List<ConnectorServiceConfiguration> configurationList = configuration.getConnectorServiceConfigurations();

      for (ConnectorServiceConfiguration info : configurationList)
      {
         ConnectorServiceFactory factory = (ConnectorServiceFactory)ClassloadingUtil.newInstanceFromClassLoader(info.getFactoryClassName());

         if (info.getParams() != null)
         {
            Set<String> invalid = ConfigurationHelper.checkKeys(factory.getAllowableProperties(), info.getParams()
               .keySet());

            if (!invalid.isEmpty())
            {
               HornetQServerLogger.LOGGER.connectorKeysInvalid(ConfigurationHelper.stringSetToCommaListString(invalid));

               continue;
            }
         }
         Set<String> invalid = ConfigurationHelper.checkKeysExist(factory.getRequiredProperties(), info.getParams()
            .keySet());

         if (!invalid.isEmpty())
         {
            HornetQServerLogger.LOGGER.connectorKeysMissing(ConfigurationHelper.stringSetToCommaListString(invalid));

            continue;
         }
         ConnectorService connectorService = factory.createConnectorService(info.getConnectorName(), info.getParams(), storageManager, postOffice, scheduledPool);
         connectors.add(connectorService);
      }

      for (ConnectorService connector : connectors)
      {
         try
         {
            connector.start();
         }
         catch (Throwable e)
         {
            HornetQServerLogger.LOGGER.errorStartingConnectorService(e, connector.getName());
         }
      }
      isStarted = true;
   }

   public void stop() throws Exception
   {
      if (!isStarted)
      {
         return;
      }
      for (ConnectorService connector : connectors)
      {
         try
         {
            connector.stop();
         }
         catch (Throwable e)
         {
            HornetQServerLogger.LOGGER.errorStoppingConnectorService(e, connector.getName());
         }
      }
      connectors.clear();
      isStarted = false;
   }

   public boolean isStarted()
   {
      return isStarted;
   }

   public Set<ConnectorService> getConnectors()
   {
      return connectors;
   }
}

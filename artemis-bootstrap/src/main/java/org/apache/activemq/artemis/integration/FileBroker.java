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
package org.apache.activemq.integration;

import org.apache.activemq.core.config.FileDeploymentManager;
import org.apache.activemq.core.config.impl.FileConfiguration;
import org.apache.activemq.core.server.ActiveMQComponent;
import org.apache.activemq.dto.ServerDTO;
import org.apache.activemq.integration.bootstrap.ActiveMQBootstrapLogger;
import org.apache.activemq.jms.server.config.impl.FileJMSConfiguration;
import org.apache.activemq.spi.core.security.ActiveMQSecurityManager;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Map;

public class FileBroker implements Broker
{
   private final String configurationUrl;

   private boolean started;

   private final ActiveMQSecurityManager securityManager;

   private Map<String, ActiveMQComponent> components;

   public FileBroker(ServerDTO broker, ActiveMQSecurityManager security)
   {
      this.securityManager = security;
      this.configurationUrl = broker.configuration;
   }


   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      //todo if we start to pullout more configs from the main config then we should pull out the configuration objects from factories if available
      FileConfiguration configuration = new FileConfiguration();
      FileJMSConfiguration jmsConfiguration = new FileJMSConfiguration();

      FileDeploymentManager fileDeploymentManager = new FileDeploymentManager(configurationUrl);
      fileDeploymentManager.addDeployable(configuration).addDeployable(jmsConfiguration);
      fileDeploymentManager.readConfiguration();

      components = fileDeploymentManager.buildService(securityManager, ManagementFactory.getPlatformMBeanServer());

      ArrayList<ActiveMQComponent> componentsByStartOrder = getComponentsByStartOrder(components);
      ActiveMQBootstrapLogger.LOGGER.serverStarting();
      for (ActiveMQComponent component : componentsByStartOrder)
      {
         component.start();
      }
      started = true;


   }

   @Override
   public void stop() throws Exception
   {
      if (!started)
      {
         return;
      }
      ActiveMQComponent[] mqComponents = new ActiveMQComponent[components.size()];
      components.values().toArray(mqComponents);
      for (int i = mqComponents.length - 1; i >= 0; i--)
      {
         mqComponents[i].stop();
      }
      started = false;
   }

   @Override
   public boolean isStarted()
   {
      return started;
   }

   public Map<String, ActiveMQComponent> getComponents()
   {
      return components;
   }

   /*
   * this makes sure the components are started in the correct order. Its simple at the mo as e only have core and jms but
   * will need impproving if we get more.
   * */
   public ArrayList<ActiveMQComponent> getComponentsByStartOrder(Map<String, ActiveMQComponent> components)
   {
      ArrayList<ActiveMQComponent> activeMQComponents = new ArrayList<ActiveMQComponent>();
      ActiveMQComponent jmsComponent = components.get("jms");
      if (jmsComponent != null)
      {
         activeMQComponents.add(jmsComponent);
      }
      activeMQComponents.add(components.get("core"));
      return activeMQComponents;
   }
}

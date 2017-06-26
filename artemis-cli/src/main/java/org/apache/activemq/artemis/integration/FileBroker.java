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
package org.apache.activemq.artemis.integration;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServiceComponent;
import org.apache.activemq.artemis.dto.ServerDTO;
import org.apache.activemq.artemis.integration.bootstrap.ActiveMQBootstrapLogger;
import org.apache.activemq.artemis.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq.artemis.jms.server.config.TopicConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.FileJMSConfiguration;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

public class FileBroker implements Broker {

   private final String configurationUrl;

   private boolean started;

   private final ActiveMQSecurityManager securityManager;

   private Map<String, ActiveMQComponent> components;

   public FileBroker(ServerDTO broker, ActiveMQSecurityManager security) {
      this.securityManager = security;
      this.configurationUrl = broker.configuration;
   }

   @Override
   public synchronized void start() throws Exception {
      if (started) {
         return;
      }

      //todo if we start to pullout more configs from the main config then we should pull out the configuration objects from factories if available
      FileConfiguration configuration = new FileConfiguration();

      // Keep this as we still want to parse destinations in the <jms> element
      FileJMSConfiguration jmsConfiguration = new FileJMSConfiguration();

      FileDeploymentManager fileDeploymentManager = new FileDeploymentManager(configurationUrl);
      fileDeploymentManager.addDeployable(configuration).addDeployable(jmsConfiguration);
      fileDeploymentManager.readConfiguration();

      createDirectories(configuration);

      /**
       * This is a bit of a hack for backwards config compatibility since we no longer want to start the broker
       * using the JMSServerManager which would normally deploy JMS destinations. Here we take the JMS destination
       * configurations from the parsed JMS configuration and add them to the core configuration.
       *
       * It's also important here that we are adding them to the core ADDRESS configurations as those will be
       * deployed first and therefore their configuration will take precedence over other legacy queue configurations
       * which are deployed later. This is so we can maintain support for configurations like those found in the
       * bridge and divert examples where there are JMS and core queues with the same name (which was itself a bit
       * of a hack).
       *
       * This should be removed when support for the old "jms" configuation element is also removed.
       */
      {
         for (JMSQueueConfiguration jmsQueueConfig : jmsConfiguration.getQueueConfigurations()) {
            List<CoreAddressConfiguration> coreAddressConfigurations = configuration.getAddressConfigurations();
            coreAddressConfigurations.add(new CoreAddressConfiguration()
                                             .setName(jmsQueueConfig.getName())
                                             .addRoutingType(RoutingType.ANYCAST)
                                             .addQueueConfiguration(new CoreQueueConfiguration()
                                                                       .setAddress(jmsQueueConfig.getName())
                                                                       .setName(jmsQueueConfig.getName())
                                                                       .setFilterString(jmsQueueConfig.getSelector())
                                                                       .setRoutingType(RoutingType.ANYCAST)));
         }

         for (TopicConfiguration topicConfig : jmsConfiguration.getTopicConfigurations()) {
            List<CoreAddressConfiguration> coreAddressConfigurations = configuration.getAddressConfigurations();
            coreAddressConfigurations.add(new CoreAddressConfiguration()
                                             .setName(topicConfig.getName())
                                             .addRoutingType(RoutingType.MULTICAST));
         }
      }

      components = fileDeploymentManager.buildService(securityManager, ManagementFactory.getPlatformMBeanServer());

      ArrayList<ActiveMQComponent> componentsByStartOrder = getComponentsByStartOrder(components);
      ActiveMQBootstrapLogger.LOGGER.serverStarting();
      for (ActiveMQComponent component : componentsByStartOrder) {
         component.start();
      }
      started = true;

   }


   private void createDirectories(FileConfiguration fileConfiguration) {
      fileConfiguration.getPagingLocation().mkdirs();
      fileConfiguration.getJournalLocation().mkdirs();
      fileConfiguration.getBindingsLocation().mkdirs();
      fileConfiguration.getLargeMessagesLocation().mkdirs();
   }

   @Override
   public void stop() throws Exception {
      stop(true);
   }

   @Override
   public void stop(boolean isShutdown) throws Exception {
      if (!started) {
         return;
      }
      ActiveMQComponent[] mqComponents = new ActiveMQComponent[components.size()];
      components.values().toArray(mqComponents);
      for (int i = mqComponents.length - 1; i >= 0; i--) {
         if (mqComponents[i] instanceof ServiceComponent) {
            ((ServiceComponent) mqComponents[i]).stop(isShutdown);
         } else {
            mqComponents[i].stop();
         }
      }
      started = false;
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   public Map<String, ActiveMQComponent> getComponents() {
      return components;
   }

   /*
   * this makes sure the components are started in the correct order. Its simple at the mo as e only have core and jms but
   * will need impproving if we get more.
   * */
   private ArrayList<ActiveMQComponent> getComponentsByStartOrder(Map<String, ActiveMQComponent> components) {
      ArrayList<ActiveMQComponent> activeMQComponents = new ArrayList<>();
      ActiveMQComponent jmsComponent = components.get("jms");
      if (jmsComponent != null) {
         activeMQComponents.add(jmsComponent);
      }
      activeMQComponents.add(components.get("core"));
      return activeMQComponents;
   }

   @Override
   public ActiveMQServer getServer() {
      return (ActiveMQServer) components.get("core");
   }

}

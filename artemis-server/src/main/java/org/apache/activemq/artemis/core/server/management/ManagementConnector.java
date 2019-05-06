/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.management;

import org.apache.activemq.artemis.core.config.JMXConnectorConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.HashMap;
import java.util.Map;


public class ManagementConnector implements ActiveMQComponent {

   private final JMXConnectorConfiguration configuration;
   private ConnectorServerFactory connectorServerFactory;
   private RmiRegistryFactory rmiRegistryFactory;
   private MBeanServerFactory mbeanServerFactory;

   public ManagementConnector(JMXConnectorConfiguration configuration) {
      this.configuration = configuration;
   }

   @Override
   public boolean isStarted() {
      return rmiRegistryFactory != null;
   }

   @Override
   public void start() throws Exception {
      rmiRegistryFactory = new RmiRegistryFactory();
      rmiRegistryFactory.setPort(configuration.getConnectorPort());
      rmiRegistryFactory.init();

      mbeanServerFactory = new MBeanServerFactory();
      mbeanServerFactory.setLocateExistingServerIfPossible(true);
      mbeanServerFactory.init();

      MBeanServer mbeanServer = mbeanServerFactory.getServer();

      JaasAuthenticator jaasAuthenticator = new JaasAuthenticator();
      jaasAuthenticator.setRealm(configuration.getJmxRealm());

      connectorServerFactory = new ConnectorServerFactory();
      connectorServerFactory.setServer(mbeanServer);
      connectorServerFactory.setServiceUrl(configuration.getServiceUrl());
      connectorServerFactory.setRmiServerHost(configuration.getConnectorHost());
      connectorServerFactory.setObjectName(new ObjectName(configuration.getObjectName()));
      Map<String, Object> environment = new HashMap<>();
      environment.put("jmx.remote.authenticator", jaasAuthenticator);
      try {
         connectorServerFactory.setEnvironment(environment);
         connectorServerFactory.setAuthenticatorType(configuration.getAuthenticatorType());
         connectorServerFactory.setSecured(configuration.isSecured());
         connectorServerFactory.setKeyStorePath(configuration.getKeyStorePath());
         connectorServerFactory.setkeyStoreProvider(configuration.getKeyStoreProvider());
         connectorServerFactory.setKeyStorePassword(configuration.getKeyStorePassword());
         connectorServerFactory.setTrustStorePath(configuration.getTrustStorePath());
         connectorServerFactory.setTrustStoreProvider(configuration.getTrustStoreProvider());
         connectorServerFactory.setTrustStorePassword(configuration.getTrustStorePassword());
         connectorServerFactory.init();
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.error("Can't init JMXConnectorServer: " + e.getMessage());
      }
   }

   @Override
   public void stop() {
      if (connectorServerFactory != null) {
         try {
            connectorServerFactory.destroy();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.warn("Error destroying ConnectorServerFactory", e);
         }
         connectorServerFactory = null;
      }
      if (mbeanServerFactory != null) {
         try {
            mbeanServerFactory.destroy();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.warn("Error destroying MBeanServerFactory", e);
         }
         mbeanServerFactory = null;
      }
      if (rmiRegistryFactory != null) {
         try {
            rmiRegistryFactory.destroy();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.warn("Error destroying RMIRegistryFactory", e);
         }
         rmiRegistryFactory = null;
      }
   }

}

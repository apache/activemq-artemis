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
package org.apache.activemq.artemis.tests.unit.core.config.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.activemq.artemis.core.server.ServiceRegistry;
import org.apache.activemq.artemis.tests.unit.core.config.impl.fakes.FakeConnectorServiceFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ConnectorService;
import org.apache.activemq.artemis.core.server.impl.ConnectorsService;
import org.apache.activemq.artemis.core.server.impl.ServiceRegistryImpl;
import org.apache.activemq.artemis.tests.unit.core.config.impl.fakes.FakeConnectorService;
import org.junit.Before;
import org.junit.Test;

public class ConnectorsServiceTest extends ActiveMQTestBase {

   private Configuration configuration;

   private ServiceRegistry serviceRegistry;

   @Override
   @Before
   public void setUp() throws Exception {
      // Setup Configuration
      configuration = new ConfigurationImpl();
      serviceRegistry = new ServiceRegistryImpl();
   }

   /**
    * Test that the connectors added via the service registry are added to the connectorsService,
    *
    * @throws Exception
    */
   @Test
   public void testConnectorsServiceUsesInjectedConnectorServiceFactory() throws Exception {
      ConnectorServiceConfiguration connectorServiceConfiguration = new ConnectorServiceConfiguration().setFactoryClassName(null).setParams(new HashMap<String, Object>()).setName("myfact");

      // Creates a fake connector service factory that returns the fake connector service object
      ConnectorService connectorService = new FakeConnectorService();
      FakeConnectorServiceFactory connectorServiceFactory = new FakeConnectorServiceFactory();

      serviceRegistry.addConnectorService(connectorServiceFactory, connectorServiceConfiguration);
      ConnectorsService connectorsService = new ConnectorsService(configuration, null, null, null, serviceRegistry);
      connectorsService.start();

      assertTrue(connectorsService.getConnectors().size() == 1);
      assertTrue(connectorsService.getConnectors().contains(connectorServiceFactory.getConnectorService()));
   }

   /**
    * Test that the connectors added via the config are added to the connectors service.
    *
    * @throws Exception
    */
   @Test
   public void testConnectorsServiceUsesConfiguredConnectorServices() throws Exception {
      ConnectorServiceConfiguration connectorServiceConfiguration = new ConnectorServiceConfiguration().setFactoryClassName(FakeConnectorServiceFactory.class.getCanonicalName()).setParams(new HashMap<String, Object>()).setName("myfact");

      List<ConnectorServiceConfiguration> connectorServiceConfigurations = new ArrayList<>();
      connectorServiceConfigurations.add(connectorServiceConfiguration);

      configuration.setConnectorServiceConfigurations(connectorServiceConfigurations);
      ConnectorsService connectorsService = new ConnectorsService(configuration, null, null, null, serviceRegistry);
      connectorsService.start();

      assertTrue(connectorsService.getConnectors().size() == 1);
   }
}

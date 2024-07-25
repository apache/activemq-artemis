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
package org.apache.activemq.artemis.core.server.routing;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Collections;

import org.apache.activemq.artemis.core.config.routing.ConnectionRouterConfiguration;
import org.apache.activemq.artemis.core.config.routing.NamedPropertyConfiguration;
import org.apache.activemq.artemis.core.config.routing.PoolConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.routing.policies.ConsistentHashModuloPolicy;
import org.apache.activemq.artemis.core.server.routing.policies.ConsistentHashPolicy;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ConnectionRouterManagerTest {

   ActiveMQServer mockServer;
   ConnectionRouterManager underTest;

   @BeforeEach
   public void setUp() throws Exception {

      mockServer = mock(ActiveMQServer.class);

      underTest = new ConnectionRouterManager(null, mockServer, null);
      underTest.start();
   }

   @AfterEach
   public void tearDown() throws Exception {
      if (underTest != null) {
         underTest.stop();
      }
   }

   @Test
   public void deployLocalOnlyPoolInvalid() throws Exception {
      assertThrows(IllegalStateException.class, () -> {

         ConnectionRouterConfiguration connectionRouterConfiguration = new ConnectionRouterConfiguration();
         connectionRouterConfiguration.setName("partition-local-pool");
         NamedPropertyConfiguration policyConfig = new NamedPropertyConfiguration();
         policyConfig.setName(ConsistentHashPolicy.NAME);
         connectionRouterConfiguration.setPolicyConfiguration(policyConfig);

         PoolConfiguration poolConfiguration = new PoolConfiguration();
         poolConfiguration.setLocalTargetEnabled(true);
         connectionRouterConfiguration.setPoolConfiguration(poolConfiguration);

         underTest.deployConnectionRouter(connectionRouterConfiguration);
      });
   }

   @Test
   public void deployLocalOnly() throws Exception {

      ManagementService mockManagementService = Mockito.mock(ManagementService.class);
      Mockito.when(mockServer.getManagementService()).thenReturn(mockManagementService);

      ConnectionRouterConfiguration connectionRouterConfiguration = new ConnectionRouterConfiguration();
      connectionRouterConfiguration.setName("partition-local-pool");

      underTest.deployConnectionRouter(connectionRouterConfiguration);
   }

   @Test
   public void deployLocalOnlyWithPolicy() throws Exception {

      ManagementService mockManagementService = Mockito.mock(ManagementService.class);
      Mockito.when(mockServer.getManagementService()).thenReturn(mockManagementService);

      ConnectionRouterConfiguration connectionRouterConfiguration = new ConnectionRouterConfiguration();
      connectionRouterConfiguration.setName("partition-local-consistent-hash").setKeyType(KeyType.CLIENT_ID).setLocalTargetFilter(String.valueOf(2));
      NamedPropertyConfiguration policyConfig = new NamedPropertyConfiguration()
         .setName(ConsistentHashModuloPolicy.NAME)
         .setProperties(Collections.singletonMap(ConsistentHashModuloPolicy.MODULO, String.valueOf(2)));
      connectionRouterConfiguration.setPolicyConfiguration(policyConfig);


      underTest.deployConnectionRouter(connectionRouterConfiguration);
   }

   @Test
   public void deploy2LocalOnlyWithSamePolicy() throws Exception {

      ManagementService mockManagementService = Mockito.mock(ManagementService.class);
      Mockito.when(mockServer.getManagementService()).thenReturn(mockManagementService);

      ConnectionRouterConfiguration connectionRouterConfiguration = new ConnectionRouterConfiguration();
      connectionRouterConfiguration.setName("partition-local-consistent-hash").setKeyType(KeyType.CLIENT_ID).setLocalTargetFilter(String.valueOf(2));
      NamedPropertyConfiguration policyConfig = new NamedPropertyConfiguration()
         .setName(ConsistentHashModuloPolicy.NAME)
         .setProperties(Collections.singletonMap(ConsistentHashModuloPolicy.MODULO, String.valueOf(2)));
      connectionRouterConfiguration.setPolicyConfiguration(policyConfig);
      underTest.deployConnectionRouter(connectionRouterConfiguration);

      connectionRouterConfiguration.setName("partition-local-consistent-hash-bis");
      underTest.deployConnectionRouter(connectionRouterConfiguration);
   }
}

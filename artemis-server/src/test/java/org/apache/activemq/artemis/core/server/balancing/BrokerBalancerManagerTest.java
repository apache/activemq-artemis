/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.balancing;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.balancing.BrokerBalancerConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PolicyConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PoolConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancing.policies.ConsistentHashPolicy;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BrokerBalancerManagerTest {

   ActiveMQServer mockServer;
   BrokerBalancerManager underTest;

   @Before
   public void setUp() throws Exception {

      mockServer = mock(ActiveMQServer.class);
      Mockito.when(mockServer.getNodeID()).thenReturn(SimpleString.toSimpleString("UUID"));

      underTest = new BrokerBalancerManager(null, mockServer, null);
      underTest.start();
   }

   @After
   public void tearDown() throws Exception {
      if (underTest != null) {
         underTest.stop();
      }
   }

   @Test(expected = IllegalStateException.class)
   public void deployLocalOnlyPoolInvalid() throws Exception {

      BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration();
      brokerBalancerConfiguration.setName("partition-local-pool");
      PolicyConfiguration policyConfig = new PolicyConfiguration();
      policyConfig.setName(ConsistentHashPolicy.NAME);
      brokerBalancerConfiguration.setPolicyConfiguration(policyConfig);

      PoolConfiguration poolConfiguration = new PoolConfiguration();
      poolConfiguration.setLocalTargetEnabled(true);
      brokerBalancerConfiguration.setPoolConfiguration(poolConfiguration);

      underTest.deployBrokerBalancer(brokerBalancerConfiguration);
   }

   @Test
   public void deployLocalOnly() throws Exception {

      ManagementService mockManagementService = Mockito.mock(ManagementService.class);
      Mockito.when(mockServer.getManagementService()).thenReturn(mockManagementService);

      BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration();
      brokerBalancerConfiguration.setName("partition-local-pool");

      underTest.deployBrokerBalancer(brokerBalancerConfiguration);
   }
}
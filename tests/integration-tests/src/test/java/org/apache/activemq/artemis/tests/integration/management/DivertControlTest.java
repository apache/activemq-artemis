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
package org.apache.activemq.artemis.tests.integration.management;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.DivertControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.transformer.AddHeadersTransformer;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DivertControlTest extends ManagementTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ActiveMQServer server;

   private DivertConfiguration divertConfig;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testAttributes() throws Exception {
      checkResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(divertConfig.getName(), divertConfig.getAddress()));

      DivertControl divertControl = createDivertManagementControl(divertConfig.getName(), divertConfig.getAddress());

      Assert.assertEquals(divertConfig.getFilterString(), divertControl.getFilter());

      Assert.assertEquals(divertConfig.isExclusive(), divertControl.isExclusive());

      Assert.assertEquals(divertConfig.getName(), divertControl.getUniqueName());

      Assert.assertEquals(divertConfig.getRoutingName(), divertControl.getRoutingName());

      Assert.assertEquals(divertConfig.getAddress(), divertControl.getAddress());

      Assert.assertEquals(divertConfig.getForwardingAddress(), divertControl.getForwardingAddress());

      Assert.assertEquals(divertConfig.getTransformerConfiguration().getClassName(), divertControl.getTransformerClassName());

      Assert.assertEquals(divertConfig.getTransformerConfiguration().getProperties(), divertControl.getTransformerProperties());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      CoreQueueConfiguration queueConfig = new CoreQueueConfiguration().setAddress(RandomUtil.randomString()).setName(RandomUtil.randomString()).setDurable(false);
      CoreQueueConfiguration forwardQueueConfig = new CoreQueueConfiguration().setAddress(RandomUtil.randomString()).setName(RandomUtil.randomString()).setDurable(false);

      divertConfig = new DivertConfiguration().setName(RandomUtil.randomString()).setRoutingName(RandomUtil.randomString()).setAddress(queueConfig.getAddress()).setForwardingAddress(forwardQueueConfig.getAddress()).setExclusive(RandomUtil.randomBoolean()).
         setTransformerConfiguration(new TransformerConfiguration(AddHeadersTransformer.class.getName()));

      TransportConfiguration connectorConfig = new TransportConfiguration(INVM_CONNECTOR_FACTORY);

      Configuration config = createDefaultInVMConfig().setJMXManagementEnabled(true).addQueueConfiguration(queueConfig).addQueueConfiguration(forwardQueueConfig).addDivertConfiguration(divertConfig).addConnectorConfiguration(connectorConfig.getName(), connectorConfig);

      server = addServer(ActiveMQServers.newActiveMQServer(config, mbeanServer, false));
      server.start();
   }

   protected DivertControl createDivertManagementControl(final String name, final String address) throws Exception {
      return ManagementControlHelper.createDivertControl(name, address, mbeanServer);
   }
}

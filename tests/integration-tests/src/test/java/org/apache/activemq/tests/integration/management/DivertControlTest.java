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
package org.apache.activemq.tests.integration.management;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import org.junit.Assert;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.management.DivertControl;
import org.apache.activemq.api.core.management.ObjectNameBuilder;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.CoreQueueConfiguration;
import org.apache.activemq.core.config.DivertConfiguration;
import org.apache.activemq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.tests.util.RandomUtil;

public class DivertControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ActiveMQServer service;

   private DivertConfiguration divertConfig;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testAttributes() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(divertConfig.getName()));

      DivertControl divertControl = createManagementControl(divertConfig.getName());

      Assert.assertEquals(divertConfig.getFilterString(), divertControl.getFilter());

      Assert.assertEquals(divertConfig.isExclusive(), divertControl.isExclusive());

      Assert.assertEquals(divertConfig.getName(), divertControl.getUniqueName());

      Assert.assertEquals(divertConfig.getRoutingName(), divertControl.getRoutingName());

      Assert.assertEquals(divertConfig.getAddress(), divertControl.getAddress());

      Assert.assertEquals(divertConfig.getForwardingAddress(), divertControl.getForwardingAddress());

      Assert.assertEquals(divertConfig.getTransformerClassName(), divertControl.getTransformerClassName());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName());

      CoreQueueConfiguration queueConfig = new CoreQueueConfiguration()
         .setAddress(RandomUtil.randomString())
         .setName(RandomUtil.randomString())
         .setDurable(false);
      CoreQueueConfiguration forwardQueueConfig = new CoreQueueConfiguration()
         .setAddress(RandomUtil.randomString())
         .setName(RandomUtil.randomString())
         .setDurable(false);

      divertConfig = new DivertConfiguration()
         .setName(RandomUtil.randomString())
         .setRoutingName(RandomUtil.randomString())
         .setAddress(queueConfig.getAddress())
         .setForwardingAddress(forwardQueueConfig.getAddress())
         .setExclusive(RandomUtil.randomBoolean());

      Configuration conf = createBasicConfig()
         .addQueueConfiguration(queueConfig)
         .addQueueConfiguration(forwardQueueConfig)
         .addDivertConfiguration(divertConfig)
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()))
         .addConnectorConfiguration(connectorConfig.getName(), connectorConfig);

      service = ActiveMQServers.newActiveMQServer(conf, mbeanServer, false);
      service.start();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      service.stop();

      checkNoResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(divertConfig.getName()));

      service = null;

      divertConfig = null;

      super.tearDown();
   }

   protected DivertControl createManagementControl(final String name) throws Exception
   {
      return ManagementControlHelper.createDivertControl(name, mbeanServer);
   }
}

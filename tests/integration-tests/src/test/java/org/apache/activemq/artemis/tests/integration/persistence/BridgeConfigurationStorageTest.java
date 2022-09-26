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
package org.apache.activemq.artemis.tests.integration.persistence;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.persistence.config.PersistedBridgeConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BridgeConfigurationStorageTest extends StorageManagerTestBase {

   public BridgeConfigurationStorageTest(StoreConfiguration.StoreType storeType) {
      super(storeType);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }

   @Test
   public void testStoreBridgeConfiguration() throws Exception {
      createStorage();

      BridgeConfiguration configuration = new BridgeConfiguration();
      configuration.setName("name");
      configuration.setParentName("name");
      configuration.setQueueName("QueueName");
      configuration.setConcurrency(2);
      configuration.setForwardingAddress("forward");
      configuration.setProducerWindowSize(123123);
      configuration.setConfirmationWindowSize(123123);
      configuration.setStaticConnectors(Arrays.asList("connector1", "connector2"));
      TransformerConfiguration mytransformer = new TransformerConfiguration("mytransformer");
      mytransformer.getProperties().put("key1", "prop1");
      mytransformer.getProperties().put("key2", "prop2");
      mytransformer.getProperties().put("key3", "prop3");
      configuration.setTransformerConfiguration(mytransformer);

      journal.storeBridgeConfiguration(new PersistedBridgeConfiguration(configuration));

      journal.stop();
      journal.start();

      List<PersistedBridgeConfiguration> bridgeConfigurations = journal.recoverBridgeConfigurations();

      Assert.assertEquals(1, bridgeConfigurations.size());

      PersistedBridgeConfiguration persistedBridgeConfiguration = bridgeConfigurations.get(0);
      Assert.assertEquals(configuration.getName(), persistedBridgeConfiguration.getBridgeConfiguration().getName());
      Assert.assertEquals(configuration.getQueueName(), persistedBridgeConfiguration.getBridgeConfiguration().getQueueName());
      Assert.assertEquals(configuration.getConcurrency(), persistedBridgeConfiguration.getBridgeConfiguration().getConcurrency());
      Assert.assertEquals(configuration.getForwardingAddress(), persistedBridgeConfiguration.getBridgeConfiguration().getForwardingAddress());
      Assert.assertEquals(configuration.getStaticConnectors(), persistedBridgeConfiguration.getBridgeConfiguration().getStaticConnectors());
      Assert.assertNotNull(persistedBridgeConfiguration.getBridgeConfiguration().getTransformerConfiguration());
      Assert.assertEquals("mytransformer", persistedBridgeConfiguration.getBridgeConfiguration().getTransformerConfiguration().getClassName());
      Map<String, String> properties = persistedBridgeConfiguration.getBridgeConfiguration().getTransformerConfiguration().getProperties();
      Assert.assertEquals(3, properties.size());
      Assert.assertEquals("prop1", properties.get("key1"));
      Assert.assertEquals("prop2", properties.get("key2"));
      Assert.assertEquals("prop3", properties.get("key3"));
      journal.stop();

      journal = null;

   }

   @Test
   public void testStoreBridgeConfigurationNoTransformer() throws Exception {
      createStorage();

      BridgeConfiguration configuration = new BridgeConfiguration();
      configuration.setName("name");
      configuration.setQueueName("QueueName");
      configuration.setConcurrency(2);
      configuration.setForwardingAddress("forward");
      configuration.setStaticConnectors(Arrays.asList("connector1", "connector2"));

      journal.storeBridgeConfiguration(new PersistedBridgeConfiguration(configuration));

      journal.stop();

      journal.start();

      List<PersistedBridgeConfiguration> bridgeConfigurations = journal.recoverBridgeConfigurations();

      Assert.assertEquals(1, bridgeConfigurations.size());

      PersistedBridgeConfiguration persistedBridgeConfiguration = bridgeConfigurations.get(0);
      Assert.assertEquals(configuration.getName(), persistedBridgeConfiguration.getBridgeConfiguration().getName());
      Assert.assertEquals(configuration.getQueueName(), persistedBridgeConfiguration.getBridgeConfiguration().getQueueName());
      Assert.assertEquals(configuration.getConcurrency(), persistedBridgeConfiguration.getBridgeConfiguration().getConcurrency());
      Assert.assertEquals(configuration.getForwardingAddress(), persistedBridgeConfiguration.getBridgeConfiguration().getForwardingAddress());
      Assert.assertEquals(configuration.getStaticConnectors(), persistedBridgeConfiguration.getBridgeConfiguration().getStaticConnectors());
      Assert.assertNull(persistedBridgeConfiguration.getBridgeConfiguration().getTransformerConfiguration());
      journal.stop();

      journal = null;

   }

}

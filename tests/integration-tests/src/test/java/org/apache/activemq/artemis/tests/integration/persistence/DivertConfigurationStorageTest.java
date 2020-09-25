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

import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.persistence.config.PersistedDivertConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DivertConfigurationStorageTest  extends StorageManagerTestBase {

   public DivertConfigurationStorageTest(StoreConfiguration.StoreType storeType) {
      super(storeType);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }

   @Test
   public void testStoreDivertConfiguration() throws Exception {
      createStorage();

      DivertConfiguration configuration = new DivertConfiguration();
      configuration.setName("name");
      configuration.setAddress("address");
      configuration.setExclusive(true);
      configuration.setForwardingAddress("forward");
      configuration.setRoutingName("routiingName");
      TransformerConfiguration mytransformer = new TransformerConfiguration("mytransformer");
      mytransformer.getProperties().put("key1", "prop1");
      mytransformer.getProperties().put("key2", "prop2");
      mytransformer.getProperties().put("key3", "prop3");
      configuration.setTransformerConfiguration(mytransformer);

      journal.storeDivertConfiguration(new PersistedDivertConfiguration(configuration));

      journal.stop();

      journal.start();

      List<PersistedDivertConfiguration> divertConfigurations = journal.recoverDivertConfigurations();

      Assert.assertEquals(1, divertConfigurations.size());

      PersistedDivertConfiguration persistedDivertConfiguration = divertConfigurations.get(0);
      Assert.assertEquals(configuration.getName(), persistedDivertConfiguration.getDivertConfiguration().getName());
      Assert.assertEquals(configuration.getAddress(), persistedDivertConfiguration.getDivertConfiguration().getAddress());
      Assert.assertEquals(configuration.isExclusive(), persistedDivertConfiguration.getDivertConfiguration().isExclusive());
      Assert.assertEquals(configuration.getForwardingAddress(), persistedDivertConfiguration.getDivertConfiguration().getForwardingAddress());
      Assert.assertEquals(configuration.getRoutingName(), persistedDivertConfiguration.getDivertConfiguration().getRoutingName());
      Assert.assertNotNull(persistedDivertConfiguration.getDivertConfiguration().getTransformerConfiguration());
      Assert.assertEquals("mytransformer", persistedDivertConfiguration.getDivertConfiguration().getTransformerConfiguration().getClassName());
      Map<String, String> properties = persistedDivertConfiguration.getDivertConfiguration().getTransformerConfiguration().getProperties();
      Assert.assertEquals(3, properties.size());
      Assert.assertEquals("prop1", properties.get("key1"));
      Assert.assertEquals("prop2", properties.get("key2"));
      Assert.assertEquals("prop3", properties.get("key3"));
      journal.stop();

      journal = null;

   }

   @Test
   public void testStoreDivertConfigurationNoTransformer() throws Exception {
      createStorage();

      DivertConfiguration configuration = new DivertConfiguration();
      configuration.setName("name");
      configuration.setAddress("address");
      configuration.setExclusive(true);
      configuration.setForwardingAddress("forward");
      configuration.setRoutingName("routiingName");

      journal.storeDivertConfiguration(new PersistedDivertConfiguration(configuration));

      journal.stop();

      journal.start();

      List<PersistedDivertConfiguration> divertConfigurations = journal.recoverDivertConfigurations();

      Assert.assertEquals(1, divertConfigurations.size());

      PersistedDivertConfiguration persistedDivertConfiguration = divertConfigurations.get(0);
      Assert.assertEquals(configuration.getName(), persistedDivertConfiguration.getDivertConfiguration().getName());
      Assert.assertEquals(configuration.getAddress(), persistedDivertConfiguration.getDivertConfiguration().getAddress());
      Assert.assertEquals(configuration.isExclusive(), persistedDivertConfiguration.getDivertConfiguration().isExclusive());
      Assert.assertEquals(configuration.getForwardingAddress(), persistedDivertConfiguration.getDivertConfiguration().getForwardingAddress());
      Assert.assertEquals(configuration.getRoutingName(), persistedDivertConfiguration.getDivertConfiguration().getRoutingName());
      Assert.assertNull(persistedDivertConfiguration.getDivertConfiguration().getTransformerConfiguration());
      journal.stop();

      journal = null;

   }
}

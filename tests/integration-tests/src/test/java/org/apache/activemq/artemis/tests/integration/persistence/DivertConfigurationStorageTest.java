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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.persistence.config.PersistedDivertConfiguration;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;

//Parameters set in super class
@ExtendWith(ParameterizedTestExtension.class)
public class DivertConfigurationStorageTest extends StorageManagerTestBase {

   public DivertConfigurationStorageTest(StoreConfiguration.StoreType storeType) {
      super(storeType);
   }

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();
   }

   @TestTemplate
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

      assertEquals(1, divertConfigurations.size());

      PersistedDivertConfiguration persistedDivertConfiguration = divertConfigurations.get(0);
      assertEquals(configuration.getName(), persistedDivertConfiguration.getDivertConfiguration().getName());
      assertEquals(configuration.getAddress(), persistedDivertConfiguration.getDivertConfiguration().getAddress());
      assertEquals(configuration.isExclusive(), persistedDivertConfiguration.getDivertConfiguration().isExclusive());
      assertEquals(configuration.getForwardingAddress(), persistedDivertConfiguration.getDivertConfiguration().getForwardingAddress());
      assertEquals(configuration.getRoutingName(), persistedDivertConfiguration.getDivertConfiguration().getRoutingName());
      assertNotNull(persistedDivertConfiguration.getDivertConfiguration().getTransformerConfiguration());
      assertEquals("mytransformer", persistedDivertConfiguration.getDivertConfiguration().getTransformerConfiguration().getClassName());
      Map<String, String> properties = persistedDivertConfiguration.getDivertConfiguration().getTransformerConfiguration().getProperties();
      assertEquals(3, properties.size());
      assertEquals("prop1", properties.get("key1"));
      assertEquals("prop2", properties.get("key2"));
      assertEquals("prop3", properties.get("key3"));
      journal.stop();

      journal = null;

   }

   @TestTemplate
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

      assertEquals(1, divertConfigurations.size());

      PersistedDivertConfiguration persistedDivertConfiguration = divertConfigurations.get(0);
      assertEquals(configuration.getName(), persistedDivertConfiguration.getDivertConfiguration().getName());
      assertEquals(configuration.getAddress(), persistedDivertConfiguration.getDivertConfiguration().getAddress());
      assertEquals(configuration.isExclusive(), persistedDivertConfiguration.getDivertConfiguration().isExclusive());
      assertEquals(configuration.getForwardingAddress(), persistedDivertConfiguration.getDivertConfiguration().getForwardingAddress());
      assertEquals(configuration.getRoutingName(), persistedDivertConfiguration.getDivertConfiguration().getRoutingName());
      assertNull(persistedDivertConfiguration.getDivertConfiguration().getTransformerConfiguration());
      journal.stop();

      journal = null;

   }
}

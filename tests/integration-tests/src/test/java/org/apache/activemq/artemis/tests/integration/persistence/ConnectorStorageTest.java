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

import java.util.List;

import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.persistence.config.PersistedConnector;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConnectorStorageTest extends StorageManagerTestBase {

   public ConnectorStorageTest(StoreConfiguration.StoreType storeType) {
      super(storeType);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }

   @Test
   public void testStoreConnector() throws Exception {
      final String NAME = RandomUtil.randomString();
      final String URL = RandomUtil.randomString();
      createStorage();

      PersistedConnector connector = new PersistedConnector(NAME, URL);

      journal.storeConnector(connector);

      journal.stop();
      journal.start();

      List<PersistedConnector> connectors = journal.recoverConnectors();

      Assert.assertEquals(1, connectors.size());

      PersistedConnector persistedConnector = connectors.get(0);
      Assert.assertEquals(NAME, persistedConnector.getName());
      Assert.assertEquals(URL, persistedConnector.getUrl());
      journal.stop();

      journal = null;

   }
}

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
import org.apache.activemq.artemis.jms.persistence.config.PersistedBindings;
import org.apache.activemq.artemis.jms.persistence.config.PersistedDestination;
import org.apache.activemq.artemis.jms.persistence.config.PersistedType;
import org.junit.Assert;
import org.junit.Test;

public class JMSStorageManagerTest extends StorageManagerTestBase {

   public JMSStorageManagerTest(StoreConfiguration.StoreType storeType) {
      super(storeType);
   }

   //https://issues.jboss.org/browse/HORNETQ-812
   @Test
   public void testJNDIPersistence() throws Exception {
      createJMSStorage();

      jmsJournal.storeDestination(new PersistedDestination(PersistedType.Queue, "jndiPersistQueue", null, true));

      jmsJournal.addBindings(PersistedType.Queue, "jndiPersistQueue", "jndi-1");

      List<PersistedDestination> destinations = jmsJournal.recoverDestinations();

      List<PersistedBindings> jndiList = jmsJournal.recoverPersistedBindings();

      Assert.assertEquals(1, destinations.size());

      Assert.assertEquals(1, jndiList.size());

      jmsJournal.deleteDestination(PersistedType.Queue, "jndiPersistQueue");

      destinations = jmsJournal.recoverDestinations();

      Assert.assertEquals(0, destinations.size());

      jmsJournal.stop();

      createJMSStorage();

      destinations = jmsJournal.recoverDestinations();

      Assert.assertEquals(0, destinations.size());

      jndiList = jmsJournal.recoverPersistedBindings();

      Assert.assertEquals(1, jndiList.size());

      PersistedBindings jndi = jndiList.get(0);

      List<String> jndis = jndi.getBindings();

      Assert.assertEquals(1, jndis.size());

      Assert.assertEquals("jndi-1", jndis.get(0));

   }

}

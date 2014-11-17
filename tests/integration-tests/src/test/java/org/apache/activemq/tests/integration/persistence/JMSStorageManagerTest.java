/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.tests.integration.persistence;

import org.junit.Test;

import java.util.List;

import org.apache.activemq6.jms.persistence.config.PersistedDestination;
import org.apache.activemq6.jms.persistence.config.PersistedJNDI;
import org.apache.activemq6.jms.persistence.config.PersistedType;

/**
 * A JMSStorageManagerTest
 *
 * @author <mailto:hgao@redhat.com">Howard Gao</a>
 *
 *
 */
public class JMSStorageManagerTest extends StorageManagerTestBase
{
   //https://issues.jboss.org/browse/HORNETQ-812
   @Test
   public void testJNDIPersistence() throws Exception
   {
      createJMSStorage();

      jmsJournal.storeDestination(new PersistedDestination(PersistedType.Queue,
            "jndiPersistQueue", null, true));

      jmsJournal.addJNDI(PersistedType.Queue, "jndiPersistQueue", "jndi-1");

      List<PersistedDestination> destinations = jmsJournal.recoverDestinations();

      List<PersistedJNDI> jndiList = jmsJournal.recoverPersistedJNDI();

      assertEquals(1, destinations.size());

      assertEquals(1, jndiList.size());

      jmsJournal.deleteDestination(PersistedType.Queue, "jndiPersistQueue");

      destinations = jmsJournal.recoverDestinations();

      assertEquals(0, destinations.size());

      jmsJournal.stop();

      createJMSStorage();

      destinations = jmsJournal.recoverDestinations();

      assertEquals(0, destinations.size());

      jndiList = jmsJournal.recoverPersistedJNDI();

      assertEquals(1, jndiList.size());

      PersistedJNDI jndi = jndiList.get(0);

      List<String> jndis = jndi.getJndi();

      assertEquals(1, jndis.size());

      assertEquals("jndi-1", jndis.get(0));

   }

}

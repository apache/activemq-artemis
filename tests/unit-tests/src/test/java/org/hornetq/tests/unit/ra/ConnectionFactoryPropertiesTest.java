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


package org.hornetq.tests.unit.ra;

import java.beans.PropertyDescriptor;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.Test;

import static java.beans.Introspector.getBeanInfo;

public class ConnectionFactoryPropertiesTest extends UnitTestCase
{

   private static final SortedSet<String> UNSUPPORTED_CF_PROPERTIES;
   private static final SortedSet<String> UNSUPPORTED_RA_PROPERTIES;

   static
   {
      UNSUPPORTED_CF_PROPERTIES = new TreeSet<String>();
      UNSUPPORTED_CF_PROPERTIES.add("discoveryGroupName");

      UNSUPPORTED_RA_PROPERTIES = new TreeSet<String>();
      UNSUPPORTED_RA_PROPERTIES.add("HA");
      UNSUPPORTED_RA_PROPERTIES.add("jgroupsChannelName");
      UNSUPPORTED_RA_PROPERTIES.add("jgroupsFile");
      UNSUPPORTED_RA_PROPERTIES.add("discoveryAddress");
      UNSUPPORTED_RA_PROPERTIES.add("discoveryPort");
      UNSUPPORTED_RA_PROPERTIES.add("discoveryLocalBindAddress");
      UNSUPPORTED_RA_PROPERTIES.add("discoveryRefreshTimeout");
      UNSUPPORTED_RA_PROPERTIES.add("discoveryInitialWaitTimeout");
      UNSUPPORTED_RA_PROPERTIES.add("connectionParameters");
      UNSUPPORTED_RA_PROPERTIES.add("connectorClassName");
      UNSUPPORTED_RA_PROPERTIES.add("transactionManagerLocatorClass");
      UNSUPPORTED_RA_PROPERTIES.add("transactionManagerLocatorMethod");
      UNSUPPORTED_RA_PROPERTIES.add("managedConnectionFactory");
      UNSUPPORTED_RA_PROPERTIES.add("jndiParams");
      UNSUPPORTED_RA_PROPERTIES.add("password");
      UNSUPPORTED_RA_PROPERTIES.add("passwordCodec");
      UNSUPPORTED_RA_PROPERTIES.add("useMaskedPassword");
      UNSUPPORTED_RA_PROPERTIES.add("useAutoRecovery");
      UNSUPPORTED_RA_PROPERTIES.add("useLocalTx");
      UNSUPPORTED_RA_PROPERTIES.add("userName");
      UNSUPPORTED_RA_PROPERTIES.add("jgroupsChannelLocatorClass");
      UNSUPPORTED_RA_PROPERTIES.add("jgroupsChannelRefName");
      UNSUPPORTED_RA_PROPERTIES.add("entries");

      // TODO: shouldn't this be also set on the HornetQConnectionFactory:
      // https://community.jboss.org/thread/211815?tstart=0
      UNSUPPORTED_RA_PROPERTIES.add("connectionPoolName");
   }

   @Test
   public void testCompareConnectionFactoryAndResourceAdapterProperties() throws Exception
   {
      SortedSet<String> connectionFactoryProperties = findAllPropertyNames(HornetQConnectionFactory.class);
      connectionFactoryProperties.removeAll(UNSUPPORTED_CF_PROPERTIES);
      SortedSet<String> raProperties = findAllPropertyNames(HornetQResourceAdapter.class);
      raProperties.removeAll(UNSUPPORTED_RA_PROPERTIES);

      compare("HornetQ Connection Factory", connectionFactoryProperties,
              "HornetQ Resource Adapter", raProperties);
   }

   private static void compare(String name1, SortedSet<String> set1,
                               String name2, SortedSet<String> set2)
   {
      Set<String> onlyInSet1 = new TreeSet<String>(set1);
      onlyInSet1.removeAll(set2);

      Set<String> onlyInSet2 = new TreeSet<String>(set2);
      onlyInSet2.removeAll(set1);

      if (!onlyInSet1.isEmpty() || !onlyInSet2.isEmpty())
      {
         fail(String.format("in %s only: %s\nin %s only: %s", name1, onlyInSet1, name2, onlyInSet2));
      }

      assertEquals(set2, set1);
   }

   private SortedSet<String> findAllPropertyNames(Class<?> clazz) throws Exception
   {
      SortedSet<String> names = new TreeSet<String>();
      for (PropertyDescriptor propDesc : getBeanInfo(clazz).getPropertyDescriptors())
      {
         if (propDesc == null
            || propDesc.getWriteMethod() == null)
         {
            continue;
         }
         names.add(propDesc.getDisplayName());
      }
      return names;
   }
}

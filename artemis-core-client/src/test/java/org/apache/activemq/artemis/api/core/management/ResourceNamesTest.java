/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.api.core.management;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class ResourceNamesTest extends Assert {

   char delimiterChar;
   final String delimiter;
   final SimpleString testAddress;
   final String prefix;
   final String baseName;
   final String testResourceAddressName;
   final String testResourceMulticastQueueName;
   final String testResourceAnycastQueueName;
   final String testResourceDivertName;


   @Parameterized.Parameters(name = "delimiterChar={0}")
   public static Collection<Object[]> getParams() {
      return Arrays.asList(new Object[][] {{'/'}, {'.'}});
   }

   public ResourceNamesTest(char delimiterChar) {
      super();
      this.delimiterChar = delimiterChar;
      delimiter = "" + delimiterChar;
      testAddress = SimpleString.toSimpleString(UUID.randomUUID().toString());
      prefix = ActiveMQDefaultConfiguration.getInternalNamingPrefix().replace('.', delimiterChar);
      baseName = prefix + testAddress + delimiter;
      testResourceAddressName = baseName + ResourceNames.ADDRESS.replace('.', delimiterChar) + ResourceNames.RETROACTIVE_SUFFIX;
      testResourceMulticastQueueName = baseName + ResourceNames.QUEUE.replace('.', delimiterChar) + RoutingType.MULTICAST.toString().toLowerCase() + delimiter + ResourceNames.RETROACTIVE_SUFFIX;
      testResourceAnycastQueueName = baseName + ResourceNames.QUEUE.replace('.', delimiterChar) + RoutingType.ANYCAST.toString().toLowerCase() + delimiter + ResourceNames.RETROACTIVE_SUFFIX;
      testResourceDivertName = baseName + ResourceNames.DIVERT.replace('.', delimiterChar) + ResourceNames.RETROACTIVE_SUFFIX;
   }

   @Test
   public void testGetRetroactiveResourceAddressName() {
      assertEquals(testResourceAddressName, ResourceNames.getRetroactiveResourceAddressName(prefix, delimiter, testAddress).toString());
   }

   @Test
   public void testGetRetroactiveResourceQueueName() {
      assertEquals(testResourceMulticastQueueName, ResourceNames.getRetroactiveResourceQueueName(prefix, delimiter, testAddress, RoutingType.MULTICAST).toString());
      assertEquals(testResourceAnycastQueueName, ResourceNames.getRetroactiveResourceQueueName(prefix, delimiter, testAddress, RoutingType.ANYCAST).toString());
   }

   @Test
   public void testGetRetroactiveResourceDivertName() {
      assertEquals(testResourceDivertName, ResourceNames.getRetroactiveResourceDivertName(prefix, delimiter, testAddress).toString());
   }

   @Test
   public void testDecomposeRetroactiveResourceAddressName() {
      assertEquals(testAddress.toString(), ResourceNames.decomposeRetroactiveResourceAddressName(prefix, delimiter, testResourceAddressName));
   }

   @Test
   public void testIsRetroactiveResource() {
      assertTrue(ResourceNames.isRetroactiveResource(prefix, SimpleString.toSimpleString(testResourceAddressName)));
      assertTrue(ResourceNames.isRetroactiveResource(prefix, SimpleString.toSimpleString(testResourceMulticastQueueName)));
      assertTrue(ResourceNames.isRetroactiveResource(prefix, SimpleString.toSimpleString(testResourceDivertName)));
   }
}

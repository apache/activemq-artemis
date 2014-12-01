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
package org.apache.activemq.tests.unit.core.config.impl;

import org.junit.Test;

import org.junit.Assert;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.tests.util.UnitTestCase;

/**
 * A TransportConfigurationTest
 *
 * @author jmesnil
 *
 * Created 20 janv. 2009 14:46:35
 *
 *
 */
public class TransportConfigurationTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testSplitNullAddress() throws Exception
   {
      String[] addresses = TransportConfiguration.splitHosts(null);

      Assert.assertNotNull(addresses);
      Assert.assertEquals(0, addresses.length);
   }

   @Test
   public void testSplitSingleAddress() throws Exception
   {
      String[] addresses = TransportConfiguration.splitHosts("localhost");

      Assert.assertNotNull(addresses);
      Assert.assertEquals(1, addresses.length);
      Assert.assertEquals("localhost", addresses[0]);
   }

   @Test
   public void testSplitManyAddresses() throws Exception
   {
      String[] addresses = TransportConfiguration.splitHosts("localhost, 127.0.0.1, 192.168.0.10");

      Assert.assertNotNull(addresses);
      Assert.assertEquals(3, addresses.length);
      Assert.assertEquals("localhost", addresses[0]);
      Assert.assertEquals("127.0.0.1", addresses[1]);
      Assert.assertEquals("192.168.0.10", addresses[2]);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

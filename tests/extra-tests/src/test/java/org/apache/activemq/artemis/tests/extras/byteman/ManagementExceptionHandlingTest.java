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
package org.apache.activemq.artemis.tests.extras.byteman;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.RuntimeMBeanException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class ManagementExceptionHandlingTest extends ActiveMQTestBase {
   private static final Logger log = Logger.getLogger(ManagementExceptionHandlingTest.class);

   protected ActiveMQServer server = null;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(createDefaultNettyConfig());
      server.getConfiguration().setJMXManagementEnabled(true);

      server.start();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      if (server != null) {
         server.stop();
      }
      super.tearDown();
   }

   @Test
   @BMRules(
           rules = {@BMRule(
                   name = "checking ActiveMQServerControl methods",
                   targetClass = "org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl",
                   targetMethod = "createQueue(org.apache.activemq.artemis.api.core.SimpleString, org.apache.activemq.artemis.api.core.RoutingType, org.apache.activemq.artemis.api.core.SimpleString, org.apache.activemq.artemis.api.core.SimpleString, boolean, boolean, int, boolean, boolean)",
                   targetLocation = "EXIT",
                   action = "throw new org.apache.activemq.artemis.api.core.ActiveMQException(\"gotcha\")")})
   public void testActiveMQServerControl() throws Exception {
      try {
         server.getActiveMQServerControl().createQueue(new QueueConfiguration("some.queue").setAddress("some.address").setRoutingType(RoutingType.ANYCAST).toJSON());
         fail("test should have gotten an exception!");
      } catch (ActiveMQException e) {
         fail("Wrong exception got!");
      } catch (IllegalStateException e) {
         assertEquals("gotcha", e.getMessage());
      }
   }

   @Test
   @BMRules(
           rules = {@BMRule(
                   name = "checking ActiveMQServerControl methods",
                   targetClass = "org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl",
                   targetMethod = "route(org.apache.activemq.artemis.api.core.Message, boolean)",
                   targetLocation = "ENTRY",
                   action = "throw new org.apache.activemq.artemis.api.core.ActiveMQException(\"gotcha\")")})
   public void testAddressControl() throws Exception {
      server.getActiveMQServerControl().createAddress("test.address", "ANYCAST");
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      log.debug("server is " + mbs);
      ObjectName objectName = new ObjectName("org.apache.activemq.artemis:broker=\"localhost\",component=addresses,address=\"test.address\"");
      Object[] params = new Object[] {new HashMap(), 3, "aGVsbG8=", true, null, null};
      String[] signature = new String[] {"java.util.Map", "int", "java.lang.String", "boolean", "java.lang.String", "java.lang.String"};
      try {
         mbs.invoke(objectName, "sendMessage", params, signature);
         fail("test should have gotten an exception!");
      } catch (RuntimeMBeanException ex) {
         assertTrue(ex.getCause() instanceof IllegalStateException);
         IllegalStateException e = (IllegalStateException) ex.getCause();
         assertEquals("gotcha", e.getMessage());
      }
   }
}

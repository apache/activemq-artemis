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

package org.apache.activemq.artemis.tests.smoke.jmxrbac;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.util.Collections;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.util.ServerUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JmxRBACTest extends SmokeTestBase {
   // This test will use a smoke created by the pom on this project (smoke-tsts)

   private static final String JMX_SERVER_HOSTNAME = "localhost";
   private static final int JMX_SERVER_PORT = 10099;

   public static final String SERVER_NAME_0 = "jmx-rbac";

   public static final String SERVER_ADMIN = "admin";
   public static final String SERVER_USER = "user";


   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      disableCheckThread();
      startServer(SERVER_NAME_0, 0, 0);
      ServerUtil.waitForServerToStart(0, SERVER_ADMIN, SERVER_ADMIN, 30000);
   }

   @Test
   public void testManagementRoleAccess() throws Exception {
      // Without this, the RMI server would bind to the default interface IP (the user's local IP mostly)
      System.setProperty("java.rmi.server.hostname", JMX_SERVER_HOSTNAME);

      // I don't specify both ports here manually on purpose. See actual RMI registry connection port extraction below.
      String urlString = "service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + JMX_SERVER_PORT + "/jmxrmi";

      JMXServiceURL url = new JMXServiceURL(urlString);
      JMXConnector jmxConnector;

      try {
         //Connect using the admin.
         jmxConnector = JMXConnectorFactory.connect(url, Collections.singletonMap(
            "jmx.remote.credentials", new String[] {SERVER_ADMIN, SERVER_ADMIN}));
         System.out.println("Successfully connected to: " + urlString);
      } catch (Exception e) {
         jmxConnector = null;
         e.printStackTrace();
         Assert.fail(e.getMessage());
      }

      try {
         //Create an user.
         MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
         String brokerName = "0.0.0.0";  // configured e.g. in broker.xml <broker-name> element
         ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, true);
         ActiveMQServerControl activeMQServerControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getActiveMQServerObjectName(), ActiveMQServerControl.class, false);
         ObjectName memoryObjectName = new ObjectName("java.lang:type=Memory");

         try {
            activeMQServerControl.removeUser(SERVER_USER);
         } catch (Exception ignore) {
         }
         activeMQServerControl.addUser(SERVER_USER, SERVER_USER, "amq-user", true);

         activeMQServerControl.getVersion();

         try {
            mBeanServerConnection.invoke(memoryObjectName, "gc", null, null);
            Assert.fail(SERVER_ADMIN + " should not access to " + memoryObjectName);
         } catch (Exception e) {
            Assert.assertEquals(SecurityException.class, e.getClass());
         }
      } finally {
         jmxConnector.close();
      }

      try {
         //Connect using an user.
         jmxConnector = JMXConnectorFactory.connect(url, Collections.singletonMap(
            "jmx.remote.credentials", new String[] {SERVER_USER, SERVER_USER}));
         System.out.println("Successfully connected to: " + urlString);
      } catch (Exception e) {
         jmxConnector = null;
         e.printStackTrace();
         Assert.fail(e.getMessage());
      }


      try {
         MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
         String brokerName = "0.0.0.0";  // configured e.g. in broker.xml <broker-name> element
         ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, true);
         ActiveMQServerControl activeMQServerControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getActiveMQServerObjectName(), ActiveMQServerControl.class, false);
         ObjectName memoryObjectName = new ObjectName("java.lang:type=Memory");

         mBeanServerConnection.invoke(memoryObjectName, "gc", null, null);

         try {
            activeMQServerControl.getVersion();
            Assert.fail(SERVER_USER + " should not access to " + objectNameBuilder.getActiveMQServerObjectName());
         } catch (Exception e) {
            Assert.assertEquals(SecurityException.class, e.getClass());
         }
      } finally {
         jmxConnector.close();
      }
   }
}

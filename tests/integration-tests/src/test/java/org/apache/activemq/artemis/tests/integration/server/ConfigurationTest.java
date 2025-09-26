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
package org.apache.activemq.artemis.tests.integration.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.jms.server.config.impl.FileJMSConfiguration;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.Test;

public class ConfigurationTest extends ActiveMQTestBase {

   @Test
   public void testStartWithDuplicateQueues() throws Exception {
      ActiveMQServer server = getActiveMQServer("duplicate-queues.xml");
      try {
         server.start();
         Bindings mytopic_1 = server.getPostOffice().getBindingsForAddress(SimpleString.of("mytopic_1"));
         assertEquals(0, mytopic_1.getBindings().size());
         Bindings mytopic_2 = server.getPostOffice().getBindingsForAddress(SimpleString.of("mytopic_2"));
         assertEquals(3, mytopic_2.getBindings().size());
      } finally {
         try {
            server.stop();
         } catch (Exception e) {
         }
      }
   }

   @Test
   public void testQueueWithoutAddressName() throws Exception {
      final SimpleString QUEUE_NAME = RandomUtil.randomUUIDSimpleString();
      ActiveMQServer server = createServer(false, createDefaultInVMConfig());
      try {
         server.getConfiguration().addQueueConfiguration(new CoreQueueConfiguration().setName(QUEUE_NAME.toString()));
         server.start();
         assertNotNull(server.getAddressInfo(QUEUE_NAME));
      } finally {
         try {
            server.stop();
         } catch (Exception e) {
         }
      }
   }

   @Test
   public void testPropertiesConfigReload() throws Exception {

      File propsFile = new File(getTestDirfile(), "some.props");
      propsFile.createNewFile();

      ConfigurationImpl.InsertionOrderedProperties config = new ConfigurationImpl.InsertionOrderedProperties();
      config.put("configurationFileRefreshPeriod", "100");

      config.put("addressConfigurations.mytopic_3.routingTypes", "MULTICAST");

      config.put("addressConfigurations.mytopic_3.queueConfigs.\"queue.A3\".address", "mytopic_3");
      config.put("addressConfigurations.mytopic_3.queueConfigs.\"queue.A3\".routingType", "MULTICAST");

      config.put("addressConfigurations.mytopic_3.queueConfigs.\"queue.B3\".address", "mytopic_3");
      config.put("addressConfigurations.mytopic_3.queueConfigs.\"queue.B3\".routingType", "MULTICAST");

      config.put("addressConfigurations.myqueue_1.routingTypes", "ANYCAST");
      config.put("addressConfigurations.myqueue_1.queueConfigs.\"myqueue_1\".routingType", "ANYCAST");

      config.put("addressConfigurations.myqueue_2.routingTypes", "ANYCAST");
      config.put("addressConfigurations.myqueue_2.queueConfigs.\"queue.Q1\".routingType", "ANYCAST");
      config.put("addressConfigurations.myqueue_2.queueConfigs.\"queue.Q2\".routingType", "ANYCAST");

      config.put("addressConfigurations.mytopic_4.routingTypes", "MULTICAST");
      config.put("addressConfigurations.mytopic_4.queueConfigs.\"queue.A4\".routingType", "MULTICAST");
      config.put("addressConfigurations.mytopic_4.queueConfigs.\"queue.B4\".routingType", "MULTICAST");

      config.put("status", "{\"generation\": \"1\"}");

      try (FileOutputStream outStream = new FileOutputStream(propsFile)) {
         config.store(outStream, null);
      }

      assertTrue(propsFile.exists());

      ActiveMQServer server = getActiveMQServer("duplicate-queues.xml");
      server.setProperties(propsFile.getAbsolutePath());
      try {

         server.start();
         Bindings mytopic_1 = server.getPostOffice().getBindingsForAddress(SimpleString.of("mytopic_1"));
         assertEquals(0, mytopic_1.getBindings().size());
         Bindings mytopic_2 = server.getPostOffice().getBindingsForAddress(SimpleString.of("mytopic_2"));
         assertEquals(3, mytopic_2.getBindings().size());

         Bindings mytopic_3 = server.getPostOffice().getBindingsForAddress(SimpleString.of("mytopic_3"));
         assertEquals(2, mytopic_3.getBindings().size());

         Bindings myqueue_1 = server.getPostOffice().getBindingsForAddress(SimpleString.of("myqueue_1"));
         assertEquals(1, myqueue_1.getBindings().size());

         Bindings myqueue_2 = server.getPostOffice().getBindingsForAddress(SimpleString.of("myqueue_2"));
         assertEquals(2, myqueue_2.getBindings().size());

         Bindings mytopic_4 = server.getPostOffice().getBindingsForAddress(SimpleString.of("mytopic_4"));
         assertEquals(2, mytopic_4.getBindings().size());

         // add new binding from props update
         config.put("addressConfigurations.mytopic_3.queueConfigs.\"queue.C3\".address", "mytopic_3");
         config.put("status", "{\"generation\": \"2\"}");

         try (FileOutputStream outStream = new FileOutputStream(propsFile)) {
            config.store(outStream, null);
         }

         Wait.assertTrue(() -> {
            Bindings mytopic_31 = server.getPostOffice().getBindingsForAddress(SimpleString.of("mytopic_3"));
            return mytopic_31.getBindings().size() == 3;
         });

         // verify round trip apply
         assertTrue(server.getActiveMQServerControl().getStatus().contains("2"));

         // verify some server attributes
         assertTrue(server.getActiveMQServerControl().getStatus().contains("version"));
         assertTrue(server.getActiveMQServerControl().getStatus().contains("uptime"));

      } finally {
         try {
            server.stop();
         } catch (Exception e) {
         }
      }
   }

   @Test
   public void testPropertiesOnlyConfigReload() throws Exception {

      File propsFile = new File(getTestDirfile(), "somemore.props");
      propsFile.createNewFile();


      Properties properties = new ConfigurationImpl.InsertionOrderedProperties();
      properties.put("configurationFileRefreshPeriod", "100");
      properties.put("persistenceEnabled", "false");
      properties.put("connectionRouters.joe.localTargetFilter", "LF");

      try (FileOutputStream outStream = new FileOutputStream(propsFile)) {
         properties.store(outStream, null);
      }
      assertTrue(propsFile.exists());

      FileConfiguration fc = new FileConfiguration();
      ActiveMQJAASSecurityManager sm = new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), new SecurityConfiguration());
      ActiveMQServer server = addServer(new ActiveMQServerImpl(fc, sm));
      server.setProperties(propsFile.getAbsolutePath());    // no xml config
      try {

         server.start();

         assertEquals(1, server.getConfiguration().getConnectionRouters().size());
         assertEquals("LF", server.getConfiguration().getConnectionRouters().get(0).getLocalTargetFilter());

         properties.put("persistenceEnabled", "false");
         properties.put("configurationFileRefreshPeriod", "100");

         // verify update
         properties.put("connectionRouters.joe.localTargetFilter", "UPDATED");
         try (FileOutputStream outStream = new FileOutputStream(propsFile)) {
            properties.store(outStream, null);
         }

         Wait.assertTrue(() -> {
            return "UPDATED".equals(server.getConfiguration().getConnectionRouters().get(0).getLocalTargetFilter());
         });

      } finally {
         try {
            server.stop();
         } catch (Exception e) {
         }
      }
   }

   protected ActiveMQServer getActiveMQServer(String brokerConfig) throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileJMSConfiguration fileConfiguration = new FileJMSConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(brokerConfig);
      deploymentManager.addDeployable(fc);
      deploymentManager.addDeployable(fileConfiguration);
      deploymentManager.readConfiguration();

      ActiveMQJAASSecurityManager sm = new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), new SecurityConfiguration());

      return addServer(new ActiveMQServerImpl(fc, sm));
   }

}

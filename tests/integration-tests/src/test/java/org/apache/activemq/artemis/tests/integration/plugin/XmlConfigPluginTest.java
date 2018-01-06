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
package org.apache.activemq.artemis.tests.integration.plugin;

import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.plugin.impl.LoggingActiveMQServerPlugin;
import org.apache.activemq.artemis.jms.server.config.impl.FileJMSConfiguration;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Test;

public class XmlConfigPluginTest extends ActiveMQTestBase {

   @Test
   public void testStopStart1() throws Exception {
      ActiveMQServer server = createServerFromConfig("broker-plugins-config.xml");
      try {
         server.start();
         assertEquals(2, server.getBrokerPlugins().size());
         assertTrue(server.getBrokerPlugins().get(0) instanceof MethodCalledVerifier);
         assertTrue(server.getBrokerPlugins().get(1) instanceof ConfigurationVerifier);
         ConfigurationVerifier configurationVerifier = (ConfigurationVerifier) server.getBrokerPlugins().get(1);
         assertEquals("value1", "val_1", configurationVerifier.value1);
         assertEquals("value2", "val_2", configurationVerifier.value2);
         assertNull("value3 should not have been set", configurationVerifier.value3);
      } finally {
         if (server != null) {
            server.stop();
         }
      }
   }

   /**
    * Ensure the configuration is bring picked up correctly by LoggingActiveMQServerPlugin
    * @throws Exception
    */
   @Test
   public void testLoggingActiveMQServerPlugin() throws Exception {
      ActiveMQServer server = createServerFromConfig("broker-logging-plugin.xml");
      try {
         server.start();
         assertEquals("only one plugin should be registered",1, server.getBrokerPlugins().size());
         assertTrue("ensure LoggingActiveMQServerPlugin is registered",server.getBrokerPlugins().get(0) instanceof LoggingActiveMQServerPlugin);
         LoggingActiveMQServerPlugin loggingActiveMQServerPlugin = (LoggingActiveMQServerPlugin) server.getBrokerPlugins().get(0);
         assertEquals("check logAll", true, loggingActiveMQServerPlugin.isLogAll());
         assertEquals("check logConnectionEvents", true, loggingActiveMQServerPlugin.isLogConnectionEvents());
         assertEquals("check logSessionEvents", true, loggingActiveMQServerPlugin.isLogSessionEvents());
         assertEquals("check logConsumerEvents", true, loggingActiveMQServerPlugin.isLogConsumerEvents());
         assertEquals("check logDeliveringEvents", true, loggingActiveMQServerPlugin.isLogDeliveringEvents());
         assertEquals("check logSendingEvents", true, loggingActiveMQServerPlugin.isLogSendingEvents());
         assertEquals("check logInternalEvents", true, loggingActiveMQServerPlugin.isLogInternalEvents());

      } finally {
         if (server != null) {
            server.stop();
         }
      }
   }

   /**
    *  ensure the LoggingActiveMQServerPlugin uses default values when configured with incorrect values
    * @throws Exception
    */
   @Test
   public void testLoggingActiveMQServerPluginWrongValue() throws Exception {
      ActiveMQServer server = createServerFromConfig("broker-logging-plugin-wrong.xml");
      try {
         server.start();
         assertEquals("only one plugin should be registered",1, server.getBrokerPlugins().size());
         assertTrue("ensure LoggingActiveMQServerPlugin is registered",server.getBrokerPlugins().get(0) instanceof LoggingActiveMQServerPlugin);
         LoggingActiveMQServerPlugin loggingActiveMQServerPlugin = (LoggingActiveMQServerPlugin) server.getBrokerPlugins().get(0);
         assertEquals("check logAll", false, loggingActiveMQServerPlugin.isLogAll());
         assertEquals("check logConnectionEvents", false, loggingActiveMQServerPlugin.isLogConnectionEvents());
         assertEquals("check logSessionEvents", false, loggingActiveMQServerPlugin.isLogSessionEvents());
         assertEquals("check logConsumerEvents", false, loggingActiveMQServerPlugin.isLogConsumerEvents());
         assertEquals("check logDeliveringEvents", false, loggingActiveMQServerPlugin.isLogDeliveringEvents());
         assertEquals("check logSendingEvents", false, loggingActiveMQServerPlugin.isLogSendingEvents());
         assertEquals("check logInternalEvents", false, loggingActiveMQServerPlugin.isLogInternalEvents());

      } finally {
         if (server != null) {
            server.stop();
         }
      }
   }

   private ActiveMQServer createServerFromConfig(String configFileName) throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileJMSConfiguration fileConfiguration = new FileJMSConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(configFileName);
      deploymentManager.addDeployable(fc);
      deploymentManager.addDeployable(fileConfiguration);
      deploymentManager.readConfiguration();

      return addServer(new ActiveMQServerImpl(fc));
   }

}

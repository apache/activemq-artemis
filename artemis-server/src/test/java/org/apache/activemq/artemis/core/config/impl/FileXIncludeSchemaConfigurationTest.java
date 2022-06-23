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
package org.apache.activemq.artemis.core.config.impl;

import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBasePlugin;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;

public class FileXIncludeSchemaConfigurationTest extends FileConfigurationTest {

   public FileXIncludeSchemaConfigurationTest(boolean xxeEnabled) {
      super(xxeEnabled);
      Assume.assumeTrue(xxeEnabled);
   }

   @BeforeClass
   public static void setupProperties() {
      System.setProperty("xincludePath", "./src/test/resources");
      System.setProperty("a2Prop", "a2");
      System.setProperty("falseProp", "false");
      System.setProperty("trueProp", "true");
      System.setProperty("ninetyTwoProp", "92");
   }

   @AfterClass
   public static void clearProperties() {
      System.clearProperty("xincludePath");
      System.clearProperty("a2Prop");
      System.clearProperty("falseProp");
      System.clearProperty("trueProp");
      System.clearProperty("ninetyTwoProp");
   }

   @Override
   public void testSerialize() throws Exception {
      // ConfigurationImplTest#testSerialize() assumes the one plugin it registers is the only one in the configuration.

      // Check the expected 2 from the include file are present
      assertEquals("included broker plugins are not present", 2, conf.getBrokerPlugins().size());

      // Clear the list
      for (ActiveMQServerBasePlugin plugin : conf.getBrokerPlugins()) {
         conf.unRegisterBrokerPlugin(plugin);
      }

      // Allow the test to proceed
      super.testSerialize();
   }

   @Override
   protected String getConfigurationName() {
      return "ConfigurationTest-xinclude-schema-config.xml";
   }
}

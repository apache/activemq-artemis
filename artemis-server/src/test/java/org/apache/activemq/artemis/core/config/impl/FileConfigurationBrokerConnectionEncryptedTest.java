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

import java.util.List;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class FileConfigurationBrokerConnectionEncryptedTest extends ConfigurationImplTest {

   protected String getConfigurationName() {
      return "ConfigurationTest-broker-connection-encrypted-config.xml";
   }

   @Override
   @Test
   public void testDefaults() {
      // empty
   }

   @Test
   public void testAMQPBrokerConfigEncryptedUserAndPassword() {

      List<AMQPBrokerConnectConfiguration> brokerConnections = conf.getAMQPConnection();
      Assert.assertNotNull("brokerConnections is null", brokerConnections);
      Assert.assertFalse("brokerConnections is empty", brokerConnections.isEmpty());

      boolean encTest = false;
      boolean plainTest = false;
      boolean emptyTest = false;

      for (AMQPBrokerConnectConfiguration brokerConnection : brokerConnections) {
         // Check each expected configuration is present
         encTest = encTest || "enc-test".equals(brokerConnection.getName());
         plainTest = plainTest || "plain-test".equals(brokerConnection.getName());
         emptyTest = emptyTest || "empty-test".equals(brokerConnection.getName());

         if ("empty-test".equals(brokerConnection.getName())) {

            // Empty configuration should have null user and password
            Assert.assertNull(brokerConnection.getUser());
            Assert.assertNull(brokerConnection.getPassword());

         } else {

            // Both the encrypted and plain user and password use the same expected value
            Assert.assertEquals("testuser", brokerConnection.getUser());
            Assert.assertEquals("testpassword", brokerConnection.getPassword());

         }
      }

      Assert.assertTrue("enc-test configuration is not present", encTest);
      Assert.assertTrue("plain-test configuration is not present", plainTest);
      Assert.assertTrue("empty-test configuration is not present", emptyTest);

   }

   @Override
   protected Configuration createConfiguration() throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(getConfigurationName());
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();
      return fc;
   }

}

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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.utils.SensitiveDataCodec;
import org.junit.jupiter.api.Test;

public class FileConfigurationDbEncryptedPassTest extends AbstractConfigurationTestBase {

   protected String getConfigurationName() {
      return "ConfigurationTest-db-encrypted-pass-config.xml";
   }

   @Override
   protected Configuration createConfiguration() throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(getConfigurationName());
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();
      return fc;
   }

   @Test
   public void testJdbcPasswordWithCustomCodec() {
      assertTrue(MySensitiveStringCodec.decoded);
   }

   @Test
   public void testSetGetAttributes() throws Exception {
      doSetGetAttributesTestImpl(conf);
   }

   @Test
   public void testGetSetInterceptors() {
      doGetSetInterceptorsTestImpl(conf);
   }

   @Test
   public void testSerialize() throws Exception {
      doSerializeTestImpl(conf);
   }

   @Test
   public void testSetConnectionRoutersPolicyConfiguration() throws Throwable {
      doSetConnectionRoutersPolicyConfigurationTestImpl((ConfigurationImpl) conf);
   }

   public static class MySensitiveStringCodec implements SensitiveDataCodec<String> {
      public static boolean decoded = false;

      @Override
      public String decode(Object mask) throws Exception {
         decoded = true;
         return null;
      }

      @Override
      public String encode(Object secret) throws Exception {
         return null;
      }
   }
}

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
package org.apache.activemq.artemis.tests.integration.management;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.junit.Test;

public class SecurityManagementWithModifiedConfigurationTest extends SecurityManagementTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String configuredClusterPassword = "this is not the default password";

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testSendManagementMessageWithModifiedClusterAdminUser() throws Exception {
      doSendManagementMessage(ActiveMQDefaultConfiguration.getDefaultClusterUser(), configuredClusterPassword, true);
   }

   @Test
   public void testSendManagementMessageWithDefaultClusterAdminUser() throws Exception {
      doSendManagementMessage(ActiveMQDefaultConfiguration.getDefaultClusterUser(), ActiveMQDefaultConfiguration.getDefaultClusterPassword(), false);
   }

   @Test
   public void testSendManagementMessageWithGuest() throws Exception {
      doSendManagementMessage("guest", "guest", false);
   }

   @Test
   public void testSendManagementMessageWithoutUserCredentials() throws Exception {
      doSendManagementMessage(null, null, false);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected ActiveMQServer setupAndStartActiveMQServer() throws Exception {
      Configuration conf = createDefaultInVMConfig().setSecurityEnabled(true).setClusterPassword(configuredClusterPassword);
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(conf, false));
      server.start();

      return server;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

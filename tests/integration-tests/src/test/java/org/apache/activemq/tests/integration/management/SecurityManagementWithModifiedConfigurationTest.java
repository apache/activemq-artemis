/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.tests.integration.management;

import org.junit.Test;

import org.apache.activemq.api.config.HornetQDefaultConfiguration;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.config.impl.ConfigurationImpl;
import org.apache.activemq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.HornetQServers;

/**
 * A SecurityManagementTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class SecurityManagementWithModifiedConfigurationTest extends SecurityManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String configuredClusterPassword = "this is not the default password";

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testSendManagementMessageWithModifiedClusterAdminUser() throws Exception
   {
      doSendManagementMessage(HornetQDefaultConfiguration.getDefaultClusterUser(), configuredClusterPassword, true);
   }

   @Test
   public void testSendManagementMessageWithDefaultClusterAdminUser() throws Exception
   {
      doSendManagementMessage(HornetQDefaultConfiguration.getDefaultClusterUser(),
                              HornetQDefaultConfiguration.getDefaultClusterPassword(),
                              false);
   }

   @Test
   public void testSendManagementMessageWithGuest() throws Exception
   {
      doSendManagementMessage("guest", "guest", false);
   }

   @Test
   public void testSendManagementMessageWithoutUserCredentials() throws Exception
   {
      doSendManagementMessage(null, null, false);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected HornetQServer setupAndStartHornetQServer() throws Exception
   {
      ConfigurationImpl conf = createBasicConfig()
         .setSecurityEnabled(true)
         .setClusterPassword(configuredClusterPassword)
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      HornetQServer server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      return server;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

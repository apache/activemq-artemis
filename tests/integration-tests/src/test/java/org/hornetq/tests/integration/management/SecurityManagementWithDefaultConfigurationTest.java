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
package org.hornetq.tests.integration.management;

import org.junit.Test;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;

/**
 * A SecurityManagementTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class SecurityManagementWithDefaultConfigurationTest extends SecurityManagementTestBase
{

   @Test
   public void testSendManagementMessageWithDefaultClusterAdminUser() throws Exception
   {
      doSendManagementMessage(HornetQDefaultConfiguration.getDefaultClusterUser(),
                              HornetQDefaultConfiguration.getDefaultClusterPassword(),
                              true);
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
      Configuration conf = createBasicConfig()
         .setClusterPassword(HornetQDefaultConfiguration.getDefaultClusterPassword())
         .setSecurityEnabled(true)
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      HornetQServer server = addServer(HornetQServers.newHornetQServer(conf, false));
      server.start();

      return server;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

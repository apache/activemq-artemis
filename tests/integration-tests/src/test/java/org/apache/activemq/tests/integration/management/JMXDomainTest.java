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

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.management.ObjectNameBuilder;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.HornetQServers;
import org.junit.After;
import org.junit.Test;

/**
 * A JMXDomainTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMXDomainTest extends ManagementTestBase
{
   HornetQServer server_0 = null;
   HornetQServer server_1 = null;

   @Test
   public void test2HornetQServersManagedFrom1MBeanServer() throws Exception
   {
      Configuration config_0 = createDefaultConfig()
         .setJMXManagementEnabled(true);

      String jmxDomain_1 = ActiveMQDefaultConfiguration.getDefaultJmxDomain() + ".1";

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      Configuration config_1 = createBasicConfig()
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName(), params))
         .setJMXDomain(jmxDomain_1);

      server_0 = HornetQServers.newHornetQServer(config_0, mbeanServer, false);
      server_1 = HornetQServers.newHornetQServer(config_1, mbeanServer, false);

      ObjectNameBuilder builder_0 = ObjectNameBuilder.DEFAULT;
      ObjectNameBuilder builder_1 = ObjectNameBuilder.create(jmxDomain_1);

      checkNoResource(builder_0.getHornetQServerObjectName());
      checkNoResource(builder_1.getHornetQServerObjectName());

      server_0.start();

      checkResource(builder_0.getHornetQServerObjectName());
      checkNoResource(builder_1.getHornetQServerObjectName());

      server_1.start();

      checkResource(builder_0.getHornetQServerObjectName());
      checkResource(builder_1.getHornetQServerObjectName());

      server_0.stop();

      checkNoResource(builder_0.getHornetQServerObjectName());
      checkResource(builder_1.getHornetQServerObjectName());

      server_1.stop();

      checkNoResource(builder_0.getHornetQServerObjectName());
      checkNoResource(builder_1.getHornetQServerObjectName());
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (server_0 != null)
      {
         server_0.stop();
      }

      if (server_1 != null)
      {
         server_1.stop();
      }

      super.tearDown();
   }
}

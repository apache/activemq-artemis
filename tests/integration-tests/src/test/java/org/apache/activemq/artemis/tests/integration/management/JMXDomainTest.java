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

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.junit.Test;

public class JMXDomainTest extends ManagementTestBase {

   ActiveMQServer server_0 = null;
   ActiveMQServer server_1 = null;

   @Test
   public void test2ActiveMQServersManagedFrom1MBeanServer() throws Exception {
      Configuration config_0 = createDefaultInVMConfig().setJMXManagementEnabled(true);

      String jmxDomain_1 = ActiveMQDefaultConfiguration.getDefaultJmxDomain() + ".1";

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      Configuration config_1 = createBasicConfig().addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName(), params)).setJMXDomain(jmxDomain_1);

      server_0 = addServer(ActiveMQServers.newActiveMQServer(config_0, mbeanServer, false));
      server_1 = addServer(ActiveMQServers.newActiveMQServer(config_1, mbeanServer, false));

      ObjectNameBuilder builder_0 = ObjectNameBuilder.DEFAULT;
      ObjectNameBuilder builder_1 = ObjectNameBuilder.create(jmxDomain_1, "localhost");

      checkNoResource(builder_0.getActiveMQServerObjectName());
      checkNoResource(builder_1.getActiveMQServerObjectName());

      server_0.start();

      checkResource(builder_0.getActiveMQServerObjectName());
      checkNoResource(builder_1.getActiveMQServerObjectName());

      server_1.start();

      checkResource(builder_0.getActiveMQServerObjectName());
      checkResource(builder_1.getActiveMQServerObjectName());

      server_0.stop();

      checkNoResource(builder_0.getActiveMQServerObjectName());
      checkResource(builder_1.getActiveMQServerObjectName());

      server_1.stop();

      checkNoResource(builder_0.getActiveMQServerObjectName());
      checkNoResource(builder_1.getActiveMQServerObjectName());
   }
}

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
package org.apache.activemq.artemis.tests.integration.federation;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;

/**
 * Federation Test Base
 */
public class FederatedTestBase extends ActiveMQTestBase {

   protected List<MBeanServer> mBeanServers = new ArrayList<>();
   protected List<ActiveMQServer> servers = new ArrayList<>();


   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      for (int i = 0; i < numberOfServers(); i++) {
         MBeanServer mBeanServer = MBeanServerFactory.createMBeanServer();
         mBeanServers.add(mBeanServer);
         Configuration config = createDefaultConfig(i, false).setSecurityEnabled(false);
         for (int j = 0; j < numberOfServers(); j++) {
            config.addConnectorConfiguration("server" + j, "vm://" + j);
         }
         ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, mBeanServer, false));

         servers.add(server);
         server.start();

         configureQueues(server);
      }
   }

   protected void configureQueues(ActiveMQServer server) throws Exception {
   }

   protected void createSimpleQueue(ActiveMQServer server, String queueName) throws Exception {
      SimpleString simpleStringQueueName = SimpleString.toSimpleString(queueName);
      try {
         server.createQueue(new QueueConfiguration(simpleStringQueueName).setRoutingType(RoutingType.ANYCAST).setAutoCreateAddress(true));
      } catch (Exception ignored) {
      }

   }

   protected int numberOfServers() {
      return 3;
   }

   public ActiveMQServer getServer(int i) {
      return servers.get(i);
   }


}

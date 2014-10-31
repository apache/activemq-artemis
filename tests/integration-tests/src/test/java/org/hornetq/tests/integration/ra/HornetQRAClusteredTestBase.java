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
package org.hornetq.tests.integration.ra;

import java.util.HashMap;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.Before;

public class HornetQRAClusteredTestBase extends HornetQRATestBase
{
   protected HornetQServer secondaryServer;
   protected JMSServerManagerImpl secondaryJmsServer;
   protected TransportConfiguration secondaryConnector;
   protected TransportConfiguration primaryConnector;

   @Before
   @Override
   public void setUp() throws Exception
   {
      super.setUp();

      primaryConnector = new TransportConfiguration(INVM_CONNECTOR_FACTORY);
      HashMap<String, Object> params = new HashMap();
      params.put("server-id", "1");
      secondaryConnector = new TransportConfiguration(INVM_CONNECTOR_FACTORY, params);
      Configuration conf = createSecondaryDefaultConfig(true, true);

      secondaryServer = HornetQServers.newHornetQServer(conf, mbeanServer, usePersistence());
      addServer(secondaryServer);
      secondaryJmsServer = new JMSServerManagerImpl(secondaryServer);
      secondaryJmsServer.start();
      waitForTopology(secondaryServer, 2);
   }

   @Override
   public void tearDown() throws Exception
   {
      if (secondaryJmsServer != null)
         secondaryJmsServer.stop();
      super.tearDown();
   }

   protected Configuration createDefaultConfig(boolean netty) throws Exception
   {
      Configuration conf = createSecondaryDefaultConfig(netty, false);
      return conf;
   }

   protected Configuration createSecondaryDefaultConfig(boolean netty, boolean secondary) throws Exception
   {
      ConfigurationImpl configuration = createBasicConfig(-1);

      configuration.setFileDeploymentEnabled(false);
      configuration.setJMXManagementEnabled(false);

      configuration.getAcceptorConfigurations().clear();

      HashMap invmMap = new HashMap();
      if (secondary)
      {
         invmMap.put("server-id", "1");
      }
      TransportConfiguration invmTransportConfig = new TransportConfiguration(INVM_ACCEPTOR_FACTORY, invmMap);
      configuration.getAcceptorConfigurations().add(invmTransportConfig);

      HashMap nettyMap = new HashMap();
      if (secondary)
      {
         nettyMap.put("port", "5545");
      }
      TransportConfiguration nettyTransportConfig = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, nettyMap);
      configuration.getAcceptorConfigurations().add(nettyTransportConfig);

      if (secondary)
      {
         configuration.getConnectorConfigurations().put("invm2", secondaryConnector);
         configuration.getConnectorConfigurations().put("invm", primaryConnector);
         UnitTestCase.basicClusterConnectionConfig(configuration, "invm2", "invm");
         configuration.setJournalDirectory(getTestDir() + "/secondJournal/");
         configuration.setBindingsDirectory(getTestDir() + "/secondBind/");
         configuration.setLargeMessagesDirectory(getTestDir() + "/secondLarge/");
         configuration.setPagingDirectory(getTestDir() + "/secondPage/");
      }
      else
      {
         configuration.getConnectorConfigurations().put("invm", secondaryConnector);
         configuration.getConnectorConfigurations().put("invm2", primaryConnector);
         UnitTestCase.basicClusterConnectionConfig(configuration, "invm", "invm2");
      }

      configuration.setSecurityEnabled(false);
      configuration.setJMXManagementEnabled(true);
      return configuration;
   }
}

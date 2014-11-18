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
package org.apache.activemq.tests.integration.ra;

import java.util.HashMap;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.impl.ConfigurationImpl;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.Before;

public class ActiveMQRAClusteredTestBase extends ActiveMQRATestBase
{
   protected ActiveMQServer secondaryServer;
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

      secondaryServer = ActiveMQServers.newActiveMQServer(conf, mbeanServer, usePersistence());
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
      HashMap invmMap = new HashMap();
      HashMap nettyMap = new HashMap();
      String primaryConnectorName = "invm2";
      String secondaryConnectorName = "invm";
      String directoryPrefix = "first";

      if (secondary)
      {
         invmMap.put("server-id", "1");
         nettyMap.put("port", "5545");
         primaryConnectorName = "invm";
         secondaryConnectorName = "invm2";
         directoryPrefix = "second";
      }

      ConfigurationImpl configuration = createBasicConfig(-1)
         .setFileDeploymentEnabled(false)
         .setJMXManagementEnabled(false)
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, invmMap))
         .addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, nettyMap))
         .setJournalDirectory(getTestDir() + "/" + directoryPrefix + "Journal/")
         .setBindingsDirectory(getTestDir() + "/" + directoryPrefix + "Bind/")
         .setLargeMessagesDirectory(getTestDir() + "/" + directoryPrefix + "Large/")
         .setPagingDirectory(getTestDir() + "/" + directoryPrefix + "Page / ")
         .addConnectorConfiguration(secondaryConnectorName, secondaryConnector)
         .addConnectorConfiguration(primaryConnectorName, primaryConnector)
         .addClusterConfiguration(UnitTestCase.basicClusterConnectionConfig(secondaryConnectorName, primaryConnectorName));

      return configuration;
   }
}

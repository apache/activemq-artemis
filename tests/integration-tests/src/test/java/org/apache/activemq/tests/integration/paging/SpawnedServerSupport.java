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

package org.apache.activemq6.tests.integration.paging;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.core.client.HornetQClient;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.core.config.ClusterConnectionConfiguration;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.config.HAPolicyConfiguration;
import org.apache.activemq6.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq6.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq6.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq6.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.core.server.HornetQServers;
import org.apache.activemq6.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq6.core.settings.impl.AddressSettings;
import org.apache.activemq6.tests.util.ServiceTestBase;

/**
 * Support class for server that are using an external process on the testsuite
 *
 * @author Clebert Suconic
 */

public class SpawnedServerSupport
{

   static HornetQServer createServer(String folder)
   {
      Configuration conf = createConfig(folder);
      return HornetQServers.newHornetQServer(conf, true);
   }

   static Configuration createConfig(String folder)
   {
      AddressSettings settings = new AddressSettings();
      settings.setMaxDeliveryAttempts(-1);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      settings.setPageSizeBytes(10 * 1024);
      settings.setMaxSizeBytes(100 * 1024);

      Configuration conf = ServiceTestBase.createBasicConfig(folder, 0)
         .setPersistenceEnabled(true)
         .addAddressesSetting("#", settings)
         .addAcceptorConfiguration(new TransportConfiguration("org.apache.activemq6.core.remoting.impl.netty.NettyAcceptorFactory"));

      return conf;
   }

   static Configuration createSharedFolderConfig(String folder, int thisport, int otherport, boolean isBackup)
   {
      HAPolicyConfiguration haPolicyConfiguration = null;

      if (isBackup)
      {
         haPolicyConfiguration = new SharedStoreSlavePolicyConfiguration();
         ((SharedStoreSlavePolicyConfiguration)haPolicyConfiguration).setAllowFailBack(false);
      }
      else
      {
         haPolicyConfiguration = new SharedStoreMasterPolicyConfiguration();
      }

      Configuration conf = createConfig(folder)
         .clearAcceptorConfigurations()
         .setJournalFileSize(15 * 1024 * 1024)
         .addAcceptorConfiguration(createTransportConfigiguration(true, thisport))
         .addConnectorConfiguration("thisServer", createTransportConfigiguration(false, thisport))
         .addConnectorConfiguration("otherServer", createTransportConfigiguration(false, otherport))
         .setMessageExpiryScanPeriod(500)
         .addClusterConfiguration(isBackup ? setupClusterConn("thisServer", "otherServer") : setupClusterConn("thisServer"))
         .setHAPolicyConfiguration(haPolicyConfiguration);

      return conf;
   }

   protected static final ClusterConnectionConfiguration setupClusterConn(String connectorName, String... connectors)
   {
      List<String> connectorList = new LinkedList<String>();
      for (String conn : connectors)
      {
         connectorList.add(conn);
      }

      ClusterConnectionConfiguration ccc = new ClusterConnectionConfiguration()
         .setName("cluster1")
         .setAddress("jms")
         .setConnectorName(connectorName)
         .setRetryInterval(10)
         .setDuplicateDetection(false)
         .setForwardWhenNoConsumers(true)
         .setConfirmationWindowSize(1)
         .setStaticConnectors(connectorList);

      return ccc;
   }


   public static ServerLocator createLocator(int port)
   {
      TransportConfiguration config = createTransportConfigiguration(false, port);
      return HornetQClient.createServerLocator(true, config);
   }


   static TransportConfiguration createTransportConfigiguration(boolean acceptor, int port)
   {
      String className;

      if (acceptor)
      {
         className = NettyAcceptorFactory.class.getName();
      }
      else
      {
         className = NettyConnectorFactory.class.getName();
      }
      Map<String, Object> serverParams = new HashMap<String, Object>();
      serverParams.put(org.apache.activemq6.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, port);
      return new TransportConfiguration(className, serverParams);
   }


   static HornetQServer createSharedFolderServer(String folder, int thisPort, int otherPort, boolean isBackup)
   {
      Configuration conf = createSharedFolderConfig(folder, thisPort, otherPort, isBackup);
      return HornetQServers.newHornetQServer(conf, true);
   }
}

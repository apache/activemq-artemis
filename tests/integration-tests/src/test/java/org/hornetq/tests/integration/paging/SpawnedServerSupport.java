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

package org.hornetq.tests.integration.paging;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.cluster.ha.HAPolicy;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;

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
      Configuration conf = ServiceTestBase.createBasicConfig(folder, 0);
      conf.setSecurityEnabled(false);
      conf.setPersistenceEnabled(true);

      AddressSettings settings = new AddressSettings();
      settings.setMaxDeliveryAttempts(-1);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      settings.setPageSizeBytes(10 * 1024);
      settings.setMaxSizeBytes(100 * 1024);
      conf.getAddressesSettings().put("#", settings);
      conf.getAcceptorConfigurations().add(new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory"));
      return conf;
   }

   static Configuration createSharedFolderConfig(String folder, int thisport, int otherport, boolean isBackup)
   {
      Configuration conf = createConfig(folder);
      conf.getAcceptorConfigurations().clear();
      conf.setJournalFileSize(15 * 1024 * 1024);
      conf.getAcceptorConfigurations().add(createTransportConfigiguration(true, thisport));
      conf.getConnectorConfigurations().put("thisServer", createTransportConfigiguration(false, thisport));
      conf.getConnectorConfigurations().put("otherServer", createTransportConfigiguration(false, otherport));
      conf.setMessageExpiryScanPeriod(500);

      if (isBackup)
      {
         setupClusterConn(conf, "thisServer", "otherServer");
      }
      else
      {
         setupClusterConn(conf, "thisServer");
      }

      if (isBackup)
         conf.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_SHARED_STORE);
      else
         conf.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.SHARED_STORE);

      conf.setAllowAutoFailBack(false);

      return conf;
   }

   protected static final void setupClusterConn(Configuration mainConfig,
                                                String connectorName,
                                                String... connectors)
   {
      List<String> connectorList = new LinkedList<String>();
      for (String conn : connectors)
      {
         connectorList.add(conn);
      }

      ClusterConnectionConfiguration ccc =
         new ClusterConnectionConfiguration("cluster1", "jms", connectorName, 10, false, true, 1, 1, connectorList,
                                            false);
      mainConfig.getClusterConfigurations().add(ccc);
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
      serverParams.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, port);
      return new TransportConfiguration(className, serverParams);
   }


   static HornetQServer createSharedFolderServer(String folder, int thisPort, int otherPort, boolean isBackup)
   {
      Configuration conf = createSharedFolderConfig(folder, thisPort, otherPort, isBackup);
      return HornetQServers.newHornetQServer(conf, true);
   }


}

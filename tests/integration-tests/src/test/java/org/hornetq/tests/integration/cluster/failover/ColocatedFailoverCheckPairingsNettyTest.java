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
package org.hornetq.tests.integration.cluster.failover;


import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.client.impl.TopologyMemberImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.server.cluster.ha.HAPolicy;
import org.hornetq.core.server.impl.InVMNodeManager;

import java.util.HashMap;

public class ColocatedFailoverCheckPairingsNettyTest extends ColocatedFailoverCheckPairingsTest
{

   protected void assertPairedCorrectly(TopologyMemberImpl m1, TopologyMemberImpl m2)
   {
      String backup1Port = (String) m1.getBackup().getParams().get("port");
      String backup2Port = (String) m2.getBackup().getParams().get("port");
      assertEquals(backup1Port, "5449");
      assertEquals(backup2Port, "5448");
   }
   @Override
   protected void createConfigs() throws Exception
   {

      TransportConfiguration liveConnector1 = getConnectorTransportConfiguration(1);
      liveConfiguration1 = super.createDefaultConfig();
      liveConfiguration1.getAcceptorConfigurations().clear();
      liveConfiguration1.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(1));
      liveConfiguration1.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.REPLICATED);
      liveConfiguration1.getHAPolicy().setFailbackDelay(1000);
      liveConfiguration1.setJournalDirectory(getTestDir() + "/live1/journal");
      liveConfiguration1.setBindingsDirectory(getTestDir() + "/live1/bindings");
      liveConfiguration1.getQueueConfigurations().add(new CoreQueueConfiguration("jms.queue.testQueue", "jms.queue.testQueue", null, true));

      TransportConfiguration liveConnector2 = getConnectorTransportConfiguration(2);
      basicClusterConnectionConfig(liveConfiguration1, liveConnector1.getName(), liveConnector2.getName());
      liveConfiguration1.getConnectorConfigurations().put(liveConnector1.getName(), liveConnector1);
      liveConfiguration1.getConnectorConfigurations().put(liveConnector2.getName(), liveConnector2);

      Configuration backupConfiguration1 = liveConfiguration1.copy();
      TransportConfiguration backupConnector1 = getConnectorTransportConfiguration(3);
      backupConfiguration1.getConnectorConfigurations().put(backupConnector1.getName(), backupConnector1);
      backupConfiguration1.setJournalDirectory(getTestDir() + "/backup1/journal");

      backupConfiguration1.setBindingsDirectory(getTestDir() + "/backup1/bindings");
      backupConfiguration1.getAcceptorConfigurations().clear();
      backupConfiguration1.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_REPLICATED);
      backupConfiguration1.getClusterConfigurations().clear();
      basicClusterConnectionConfig(backupConfiguration1, backupConnector1.getName(), liveConnector1.getName());
      liveConfiguration1.getBackupServerConfigurations().add(backupConfiguration1);


      liveConfiguration2 = super.createDefaultConfig();
      liveConfiguration2.getAcceptorConfigurations().clear();
      liveConfiguration2.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(2));
      liveConfiguration2.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.REPLICATED);
      liveConfiguration2.getHAPolicy().setFailbackDelay(1000);
      liveConfiguration2.setJournalDirectory(getTestDir() + "/live2/journal");
      liveConfiguration2.setBindingsDirectory(getTestDir() + "/live2/bindings");
      liveConfiguration2.getQueueConfigurations().add(new CoreQueueConfiguration("jms.queue.testQueue", "jms.queue.testQueue", null, true));

      basicClusterConnectionConfig(liveConfiguration2, liveConnector2.getName(), liveConnector1.getName());
      liveConfiguration2.getConnectorConfigurations().put(liveConnector1.getName(), liveConnector1);
      liveConfiguration2.getConnectorConfigurations().put(liveConnector2.getName(), liveConnector2);

      Configuration backupConfiguration2 = liveConfiguration2.copy();
      TransportConfiguration backupConnector2 = getConnectorTransportConfiguration(4);
      backupConfiguration2.getAcceptorConfigurations().clear();
      backupConfiguration2.getConnectorConfigurations().put(backupConnector2.getName(), backupConnector2);
      backupConfiguration2.setJournalDirectory(getTestDir() + "/backup2/journal");
      backupConfiguration2.setBindingsDirectory(getTestDir() + "/backup2/bindings");
      backupConfiguration2.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_REPLICATED);
      backupConfiguration2.getClusterConfigurations().clear();
      basicClusterConnectionConfig(backupConfiguration2, backupConnector2.getName(), liveConnector2.getName());
      liveConfiguration2.getBackupServerConfigurations().add(backupConfiguration2);

      nodeManagerLive1 = new InVMNodeManager(true, backupConfiguration2.getJournalDirectory());
      nodeManagerLive2 = new InVMNodeManager(true, backupConfiguration1.getJournalDirectory());

      liveServer1 = createTestableServer(liveConfiguration1, nodeManagerLive1, nodeManagerLive2, 1);
      liveServer2 = createTestableServer(liveConfiguration2, nodeManagerLive2, nodeManagerLive1, 2);
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(int node)
   {
      HashMap<String, Object> params = new HashMap<>();
      params.put("port", "" + (5445 + node));
      return new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(int node)
   {
      HashMap<String, Object> params = new HashMap<>();
      params.put("port", "" + (5445 + node));
      return new TransportConfiguration(NETTY_CONNECTOR_FACTORY, params);
   }
}

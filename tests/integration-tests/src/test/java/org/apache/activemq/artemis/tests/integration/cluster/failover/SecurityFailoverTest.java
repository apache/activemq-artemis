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
package org.apache.activemq.artemis.tests.integration.cluster.failover;

import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;

public class SecurityFailoverTest extends FailoverTest {

   protected ClientSession createSession(ClientSessionFactory sf,
                                         boolean isXA,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         int ackBatchSize) throws Exception {
      ClientSession session = sf.createSession("a", "b", isXA, autoCommitSends, autoCommitAcks, sf.getServerLocator().isPreAcknowledge(), ackBatchSize);
      addClientSession(session);
      return session;
   }

   @Override
   protected ClientSession createSession(ClientSessionFactory sf,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         int ackBatchSize) throws Exception {
      ClientSession session = sf.createSession("a", "b", false, autoCommitSends, autoCommitAcks, sf.getServerLocator().isPreAcknowledge(), ackBatchSize);
      addClientSession(session);
      return session;
   }

   @Override
   protected ClientSession createSession(ClientSessionFactory sf,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return createSession(sf, autoCommitSends, autoCommitAcks, sf.getServerLocator().getAckBatchSize());
   }

   @Override
   protected ClientSession createSession(ClientSessionFactory sf) throws Exception {
      return createSession(sf, true, true, sf.getServerLocator().getAckBatchSize());
   }

   @Override
   protected ClientSession createSession(ClientSessionFactory sf,
                                         boolean xa,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return createSession(sf, xa, autoCommitSends, autoCommitAcks, sf.getServerLocator().getAckBatchSize());
   }

   /**
    * @throws Exception
    */
   @Override
   protected void createConfigs() throws Exception {
      nodeManager = new InVMNodeManager(false);
      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);

      backupConfig = super.createDefaultInVMConfig().clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(false)).setSecurityEnabled(true).setHAPolicyConfiguration(new SharedStoreSlavePolicyConfiguration()).addConnectorConfiguration(liveConnector.getName(), liveConnector).addConnectorConfiguration(backupConnector.getName(), backupConnector).addClusterConfiguration(basicClusterConnectionConfig(backupConnector.getName(), liveConnector.getName()));

      backupServer = createTestableServer(backupConfig);
      ActiveMQJAASSecurityManager securityManager = installSecurity(backupServer);
      securityManager.getConfiguration().setDefaultUser(null);

      liveConfig = super.createDefaultInVMConfig().clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(true)).setSecurityEnabled(true).setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration()).addClusterConfiguration(basicClusterConnectionConfig(liveConnector.getName())).addConnectorConfiguration(liveConnector.getName(), liveConnector);

      liveServer = createTestableServer(liveConfig);
      installSecurity(liveServer);
   }

   @Override
   protected void beforeRestart(TestableServer server) {
      installSecurity(server);
   }

   /**
    * @return
    */
   protected ActiveMQJAASSecurityManager installSecurity(TestableServer server) {
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getServer().getSecurityManager();
      securityManager.getConfiguration().addUser("a", "b");
      Role role = new Role("arole", true, true, true, true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      server.getServer().getSecurityRepository().addMatch("#", roles);
      securityManager.getConfiguration().addRole("a", "arole");
      return securityManager;
   }
}

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
package org.apache.activemq.artemis.tests.integration.jms.cluster;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;

public class ReplicatedJMSFailoverTest extends JMSFailoverTest
{

   /**
    * @throws Exception
    */
   @Override
   protected void startServers() throws Exception
   {
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);

      backupConf = createBasicConfig()
         .setJournalType(getDefaultJournalType())
         .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, backupParams))
         .setBindingsDirectory(getBindingsDir(0, true))
         .setJournalMinFiles(2)
         .setJournalDirectory(getJournalDir(0, true))
         .setPagingDirectory(getPageDir(0, true))
         .setLargeMessagesDirectory(getLargeMessagesDir(0, true))
         .setHAPolicyConfiguration(new ReplicaPolicyConfiguration());

      backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConf, true));

      backupJMSServer = new JMSServerManagerImpl(backupServer);

      backupJMSServer.setRegistry(new JndiBindingRegistry(ctx2));

      backupJMSServer.start();

      liveConf = createBasicConfig()
         .setJournalType(getDefaultJournalType())
         .addConnectorConfiguration("toBackup", new TransportConfiguration(INVM_CONNECTOR_FACTORY, backupParams))
         .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY))
         .setBindingsDirectory(getBindingsDir(0, false))
         .setJournalMinFiles(2)
         .setJournalDirectory(getJournalDir(0, false))
         .setPagingDirectory(getPageDir(0, false))
         .setLargeMessagesDirectory(getLargeMessagesDir(0, false))
         .setHAPolicyConfiguration(new ReplicatedPolicyConfiguration());

      liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConf, true));

      liveJMSServer = new JMSServerManagerImpl(liveServer);

      liveJMSServer.setRegistry(new JndiBindingRegistry(ctx1));

      liveJMSServer.start();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

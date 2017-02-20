/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.karaf.version;

import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.JournalType;
import org.junit.Ignore;
import org.junit.Test;

// Ignored as this is executed by PaxExam on RemoteTest
@Ignore
public class ProbeRemoteServer {

   ActiveMQServer server;

   @Test
   public void probe1() throws Exception {
      System.out.println("probe1 with ");
      ClientMessageImpl message = new ClientMessageImpl();

      ConfigurationImpl config = new ConfigurationImpl().setSecurityEnabled(false).setJournalMinFiles(2).
         setJournalFileSize(100 * 1024).setJournalType(JournalType.NIO).
         setJournalDirectory("./data/journal").
         setBindingsDirectory("./data/binding").
         setPagingDirectory("./data/paging").
         setLargeMessagesDirectory("./data/lm").setJournalCompactMinFiles(0).setJournalCompactPercentage(0).setClusterPassword("mycluster").setJournalDatasync(false).setSecurityEnabled(false);
      config.addAcceptorConfiguration("netty", "tcp://localhost:61616");

      server = ActiveMQServers.newActiveMQServer(config, false);

      server.start();
   }
}

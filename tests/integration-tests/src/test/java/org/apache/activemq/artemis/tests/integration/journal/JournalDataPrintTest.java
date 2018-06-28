/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.journal;

import org.apache.activemq.artemis.cli.commands.tools.PrintData;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.jms.server.config.impl.FileJMSConfiguration;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class JournalDataPrintTest extends ActiveMQTestBase {

   protected ActiveMQServer server;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }

   @Test
   public void testJournalDataPrint() throws Exception {
      ActiveMQServer server = getActiveMQServer("dataprint/etc/broker.xml");
      try {
         server.start();
         server.stop();
         System.setProperty("artemis.instance",
               this.getClass().getClassLoader().getResource("dataprint").getFile());
         PrintData.printData(server.getConfiguration().getBindingsLocation().getAbsoluteFile(), server.getConfiguration().getJournalLocation().getAbsoluteFile(), server.getConfiguration().getPagingLocation().getAbsoluteFile());

         // list journal file
         File dirFile = server.getConfiguration().getJournalLocation().getAbsoluteFile();
         File[] files = dirFile.listFiles();
         for (int i = 0; i < files.length; i++) {
            File journalFile = files[i];
            Assert.assertEquals(30 * 1024 * 1024L, journalFile.length());
         }

         server.start();
      } finally {
         try {
            server.stop();
         } catch (Exception e) {
         }
      }
   }

   protected ActiveMQServer getActiveMQServer(String brokerConfig) throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileJMSConfiguration fileConfiguration = new FileJMSConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(brokerConfig);
      deploymentManager.addDeployable(fc);
      deploymentManager.addDeployable(fileConfiguration);
      deploymentManager.readConfiguration();

      ActiveMQJAASSecurityManager sm = new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), new SecurityConfiguration());

      recreateDirectory(fc.getBindingsDirectory());
      recreateDirectory(fc.getJournalDirectory());
      recreateDirectory(fc.getPagingDirectory());
      recreateDirectory(fc.getLargeMessagesDirectory());

      return addServer(new ActiveMQServerImpl(fc, sm));
   }
}

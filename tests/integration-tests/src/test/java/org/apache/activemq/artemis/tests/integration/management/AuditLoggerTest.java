/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.management;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.HashSet;
import java.util.UUID;
import java.util.logging.LogManager;

// https://issues.apache.org/jira/browse/ARTEMIS-2730 this test needs to be moved as a SmokeTest
// as it's messing up with logging configurations
@Ignore
public class AuditLoggerTest extends ManagementTestBase {

   private static final File auditLog = new File("target/audit.log");

   private ActiveMQServer server;
   private Configuration conf;
   protected ClientSession session;
   private ServerLocator locator;
   private ClientSessionFactory sf;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      emptyLogFile();

      TransportConfiguration connectorConfig = new TransportConfiguration(INVM_CONNECTOR_FACTORY);

      conf = createDefaultNettyConfig().setJMXManagementEnabled(true).addConnectorConfiguration(connectorConfig.getName(), connectorConfig);
      conf.setSecurityEnabled(true);
      SecurityConfiguration securityConfiguration = new SecurityConfiguration();
      securityConfiguration.addUser("guest", "guest");
      securityConfiguration.addUser("myUser", "myPass");
      securityConfiguration.addRole("guest", "guest");
      securityConfiguration.addRole("myUser", "guest");
      securityConfiguration.setDefaultUser("guest");
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), securityConfiguration);
      server = addServer(ActiveMQServers.newActiveMQServer(conf, mbeanServer, securityManager, true));
      server.start();

      HashSet<Role> role = new HashSet<>();
      //role guest cannot delete queues
      role.add(new Role("guest", true, true, true, false, true, false, true, true, true, true));
      server.getSecurityRepository().addMatch("#", role);

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true);
      sf = createSessionFactory(locator);
      session = sf.createSession("guest", "guest", false, true, false, false, 100);
      session.start();
      addClientSession(session);
   }

   @After
   @Override
   public void tearDown() throws Exception {
      super.tearDown();

      String originalLoggingConfig = System.getProperty("logging.configuration");

      if (originalLoggingConfig != null) {
         File file = new File(new URI(originalLoggingConfig));
         InputStream inputStream = new FileInputStream(file);

         LogManager logManager = LogManager.getLogManager();
         try {
            logManager.readConfiguration(inputStream);
         } catch (IOException e) {
            System.out.println("error loading logging conifg");
            e.printStackTrace();
         }

         inputStream.close();
      }
   }

   private void emptyLogFile() throws Exception {
      if (auditLog.exists()) {
         try (PrintWriter writer = new PrintWriter(new FileWriter(auditLog))) {
            writer.print("");
         }
      }
   }

   @Test
   public void testAuditLog() throws Exception {
      reloadLoggingConfig("audit.logging.properties");
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      final AddressControl addressControl = ManagementControlHelper.createAddressControl(address, mbeanServer);

      Assert.assertEquals(0, addressControl.getQueueNames().length);
      session.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));
      Assert.assertEquals(1, addressControl.getQueueNames().length);
      String uniqueStr = Base64.encodeBytes(UUID.randomUUID().toString().getBytes());
      addressControl.sendMessage(null, Message.BYTES_TYPE, uniqueStr, false, null, null);

      Wait.waitFor(() -> addressControl.getMessageCount() == 1);
      Assert.assertEquals(1, addressControl.getMessageCount());

      checkAuditLogRecord(true, "sending a message", uniqueStr);

      //failure log
      address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      final AddressControl addressControl2 = ManagementControlHelper.createAddressControl(address, mbeanServer);

      Assert.assertEquals(1, addressControl.getQueueNames().length);

      session.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST).setDurable(false));
      Wait.waitFor(() -> addressControl2.getQueueNames().length == 1);

      try {
         session.deleteQueue(address);
         fail("Deleting queue should get exception");
      } catch (Exception e) {
         //ignore
      }

      checkAuditLogRecord(true, "gets security check failure:", "guest does not have permission='DELETE_NON_DURABLE_QUEUE'");
      //hot patch not in log
      checkAuditLogRecord(false, "is sending a core message");
   }

   private void reloadLoggingConfig(String logFile) {
      ClassLoader cl = AuditLoggerTest.class.getClassLoader();
      InputStream inputStream = cl.getResourceAsStream(logFile);
      LogManager logManager = LogManager.getLogManager();
      try {
         logManager.readConfiguration(inputStream);
         inputStream.close();
      } catch (IOException e) {
         System.out.println("error loading logging conifg");
         e.printStackTrace();
      }

   }

   @Test
   public void testAuditHotLog() throws Exception {
      reloadLoggingConfig("audit.logging.hot.properties");
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      final AddressControl addressControl = ManagementControlHelper.createAddressControl(address, mbeanServer);

      Assert.assertEquals(0, addressControl.getQueueNames().length);
      session.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));
      Assert.assertEquals(1, addressControl.getQueueNames().length);
      String uniqueStr = Base64.encodeBytes(UUID.randomUUID().toString().getBytes());
      addressControl.sendMessage(null, Message.BYTES_TYPE, uniqueStr, false, null, null);

      Wait.waitFor(() -> addressControl.getMessageCount() == 1);
      Assert.assertEquals(1, addressControl.getMessageCount());

      checkAuditLogRecord(true, "sending a core message");
   }

   //check the audit log has a line that contains all the values
   private void checkAuditLogRecord(boolean exist, String... values) throws Exception {
      assertTrue(auditLog.exists());
      boolean hasRecord = false;
      try (BufferedReader reader = new BufferedReader(new FileReader(auditLog))) {
         String line = reader.readLine();
         while (line != null) {
            if (line.contains(values[0])) {
               boolean hasAll = true;
               for (int i = 1; i < values.length; i++) {
                  if (!line.contains(values[i])) {
                     hasAll = false;
                     break;
                  }
               }
               if (hasAll) {
                  hasRecord = true;
                  System.out.println("audit has it: " + line);
                  break;
               }
            }
            line = reader.readLine();
         }
         if (exist) {
            assertTrue(hasRecord);
         } else {
            assertFalse(hasRecord);
         }
      }
   }
}

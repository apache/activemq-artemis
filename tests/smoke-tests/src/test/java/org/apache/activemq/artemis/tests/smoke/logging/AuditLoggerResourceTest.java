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
package org.apache.activemq.artemis.tests.smoke.logging;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;

public class AuditLoggerResourceTest extends SmokeTestBase {

   private static final File auditLog = new File("target/audit-logging/log/audit.log");

   private static final String JMX_SERVER_HOSTNAME = "localhost";
   private static final int JMX_SERVER_PORT = 10099;

   public static final String SERVER_NAME = "audit-logging";

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME);
      disableCheckThread();
      startServer(SERVER_NAME, 0, 30000);
      emptyLogFile();
   }

   private void emptyLogFile() throws Exception {
      if (auditLog.exists()) {
         try (PrintWriter writer = new PrintWriter(new FileWriter(auditLog))) {
            writer.print("");
         }
      }
   }

   @Test
   public void testAuditResourceLog() throws Exception {
      HashMap   environment = new HashMap();
      String[]  credentials = new String[] {"admin", "admin"};
      environment.put(JMXConnector.CREDENTIALS, credentials);
      // Without this, the RMI server would bind to the default interface IP (the user's local IP mostly)
      System.setProperty("java.rmi.server.hostname", JMX_SERVER_HOSTNAME);

      // I don't specify both ports here manually on purpose. See actual RMI registry connection port extraction below.
      String urlString = "service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + JMX_SERVER_PORT + "/jmxrmi";

      JMXServiceURL url = new JMXServiceURL(urlString);
      JMXConnector jmxConnector = null;

      try {
         jmxConnector = JMXConnectorFactory.connect(url, environment);
         System.out.println("Successfully connected to: " + urlString);
      } catch (Exception e) {
         jmxConnector = null;
         e.printStackTrace();
         Assert.fail(e.getMessage());
      }

      try {
         MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
         String brokerName = "0.0.0.0";  // configured e.g. in broker.xml <broker-name> element
         ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, true);
         ActiveMQServerControl serverControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getActiveMQServerObjectName(), ActiveMQServerControl.class, false);

         serverControl.createAddress("auditAddress", "ANYCAST,MULTICAST");
         checkAuditLogRecord(true, "successfully created Address:");
         serverControl.updateAddress("auditAddress", "ANYCAST");
         checkAuditLogRecord(true, "successfully updated Address:");
         serverControl.deleteAddress("auditAddress");
         checkAuditLogRecord(true, "successfully deleted Address:");
         serverControl.createQueue("auditAddress", "auditQueue", "ANYCAST");
         checkAuditLogRecord(true, "successfully created Queue:");
         serverControl.updateQueue("auditQueue", "ANYCAST", -1, false);
         final QueueControl queueControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection,
               objectNameBuilder.getQueueObjectName(new SimpleString( "auditAddress"), new SimpleString("auditQueue"), RoutingType.ANYCAST),
               QueueControl.class,
               false);
         checkAuditLogRecord(true, "successfully updated Queue:");
         queueControl.removeAllMessages();
         checkAuditLogRecord(true, "has removed 0 messages");
         queueControl.sendMessage(new HashMap<>(), 0, "foo", true, "admin", "admin");
         checkAuditLogRecord(true, "sent message to");
         CompositeData[] browse = queueControl.browse();
         checkAuditLogRecord(true, "browsed " + browse.length + " messages");
         serverControl.destroyQueue("auditQueue");
         checkAuditLogRecord(true, "successfully deleted Queue:");

         ServerLocator locator = createNettyNonHALocator();
         ClientSessionFactory sessionFactory = locator.createSessionFactory();
         ClientSession session = sessionFactory.createSession("admin", "admin", false, false, true, false, 0);
         ClientProducer producer = session.createProducer("myQ");
         producer.send(session.createMessage(true));
         locator.close();

      } finally {
         jmxConnector.close();
      }
   }

   //check the audit log has a line that contains all the values
   private void checkAuditLogRecord(boolean exist, String... values) throws Exception {
      Assert.assertTrue(auditLog.exists());
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
            Assert.assertTrue(hasRecord);
         } else {
            Assert.assertFalse(hasRecord);
         }
      }
   }
}

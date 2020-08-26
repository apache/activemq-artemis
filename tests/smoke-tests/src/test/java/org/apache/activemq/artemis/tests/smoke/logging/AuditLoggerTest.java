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
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.UUID;

public class AuditLoggerTest extends SmokeTestBase {

   private static final File auditLog = new File("target/audit-logging2/log/audit.log");

   private static final String JMX_SERVER_HOSTNAME = "localhost";
   private static final int JMX_SERVER_PORT = 10099;

   public static final String SERVER_NAME = "audit-logging2";

   protected ClientSession session;
   private ServerLocator locator;
   private ClientSessionFactory sf;

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME);
      disableCheckThread();
      startServer(SERVER_NAME, 0, 30000);
      emptyLogFile();
      locator = createNonHALocator(true).setBlockOnNonDurableSend(true);
      sf = createSessionFactory(locator);
      session = sf.createSession("guest", "guest", false, true, false, false, 100);
      session.start();
      addClientSession(session);
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
      JMXConnector jmxConnector = getJmxConnector();

      MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
      String brokerName = "0.0.0.0";  // configured e.g. in broker.xml <broker-name> element
      ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, true);
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      final AddressControl addressControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getAddressObjectName(address), AddressControl.class, false);

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

      final AddressControl addressControl2 = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getAddressObjectName(address), AddressControl.class, false);

      Assert.assertEquals(1, addressControl.getQueueNames().length);

      session.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST).setDurable(false));
      Wait.waitFor(() -> addressControl2.getQueueNames().length == 1);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(true));
      Wait.waitFor(() -> addressControl.getMessageCount() == 1);
      try {
         session.deleteQueue(address);
         Assert.fail("Deleting queue should get exception");
      } catch (Exception e) {
         //ignore
      }

      checkAuditLogRecord(true, "gets security check failure:", "guest does not have permission='DELETE_NON_DURABLE_QUEUE'");
      //hot patch not in log
      checkAuditLogRecord(true, "is sending a message");
   }

   protected JMXConnector getJmxConnector() throws MalformedURLException {
      HashMap environment = new HashMap();
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
      return jmxConnector;
   }

   @Test
   public void testAuditHotLogCore() throws Exception {
      internalSend("CORE");
   }

   @Test
   public void testAuditHotLogAMQP() throws Exception {
      internalSend("AMQP");
   }

   public void internalSend(String protocol) throws Exception {
      JMXConnector jmxConnector = getJmxConnector();
      MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
      String brokerName = "0.0.0.0";  // configured e.g. in broker.xml <broker-name> element
      ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, true);
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      final AddressControl addressControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getAddressObjectName(address), AddressControl.class, false);

      Assert.assertEquals(0, addressControl.getQueueNames().length);
      session.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));
      Assert.assertEquals(1, addressControl.getQueueNames().length);
      String uniqueStr = RandomUtil.randomString();

      session.close();

      ConnectionFactory factory = createConnectionFactory(protocol, "tcp://localhost:61616");
      Connection connection = factory.createConnection();
      try {
         Session session = connection.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(address.toString()));
         TextMessage message = session.createTextMessage("msg1");
         message.setStringProperty("str", uniqueStr);
         producer.send(message);

         message = session.createTextMessage("msg2");
         message.setStringProperty("str", "Hello2");
         producer.send(message);
        // addressControl.sendMessage(null, Message.BYTES_TYPE, uniqueStr, false, null, null);

         Wait.waitFor(() -> addressControl.getMessageCount() == 2);
         Assert.assertEquals(2, addressControl.getMessageCount());

         checkAuditLogRecord(true, "sending a message");
         checkAuditLogRecord(true, uniqueStr);
         checkAuditLogRecord(true, "Hello2");

         connection.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(address.toString()));
         javax.jms.Message clientMessage = consumer.receive(5000);
         Assert.assertNotNull(clientMessage);
         clientMessage = consumer.receive(5000);
         Assert.assertNotNull(clientMessage);
         checkAuditLogRecord(true, "is consuming a message from");
      } finally {
         connection.close();
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

   public static ConnectionFactory createConnectionFactory(String protocol, String uri) {
      if (protocol.toUpperCase().equals("OPENWIRE")) {
         return new org.apache.activemq.ActiveMQConnectionFactory(uri);
      } else if (protocol.toUpperCase().equals("AMQP")) {

         if (uri.startsWith("tcp://")) {
            // replacing tcp:// by amqp://
            uri = "amqp" + uri.substring(3);
         }
         return new JmsConnectionFactory(uri);
      } else if (protocol.toUpperCase().equals("CORE") || protocol.toUpperCase().equals("ARTEMIS")) {
         return new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(uri);
      } else {
         throw new IllegalStateException("Unkown:" + protocol);
      }
   }
}

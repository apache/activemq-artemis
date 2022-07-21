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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.remote.JMXConnector;
import java.util.UUID;

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
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AuditLoggerTest extends AuditLoggerTestBase {

   protected ClientSession session;
   private ServerLocator locator;
   private ClientSessionFactory sf;

   @Before
   @Override
   public void before() throws Exception {
      super.before();
      locator = createNonHALocator(true).setBlockOnNonDurableSend(true);
      sf = createSessionFactory(locator);
      session = sf.createSession("guest", "guest", false, true, false, false, 100);
      session.start();
      addClientSession(session);
   }

   @Override
   protected String getServerName() {
      return "audit-logging2";
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

      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
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

         checkAuditLogRecord(false, "messageID=0");
         checkAuditLogRecord(true, "sent a message");
         checkAuditLogRecord(true, uniqueStr);
         checkAuditLogRecord(true, "Hello2");

         connection.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(address.toString()));
         javax.jms.Message clientMessage = consumer.receive(5000);
         Assert.assertNotNull(clientMessage);
         clientMessage = consumer.receive(5000);
         Assert.assertNotNull(clientMessage);
      } finally {
         connection.close();
      }
      checkAuditLogRecord(true, "is consuming a message from");
      checkAuditLogRecord(true, "acknowledged message from");
   }
}

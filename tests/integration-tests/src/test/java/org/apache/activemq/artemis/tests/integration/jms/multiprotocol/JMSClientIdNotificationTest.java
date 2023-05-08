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
package org.apache.activemq.artemis.tests.integration.jms.multiprotocol;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.activemq.artemis.api.core.management.ManagementHelper.HDR_CLIENT_ID;

public class JMSClientIdNotificationTest extends MultiprotocolJMSClientTestSupport {

   private ClientConsumer notificationConsumer;
   private String clientID;

   @Before
   public void setClientID() {
      clientID = RandomUtil.randomString();
   }

   @Before
   public void createNotificationConsumer() throws Exception {
      ServerLocator locator = addServerLocator(createInVMNonHALocator());
      ClientSessionFactory sf = addSessionFactory(locator.createSessionFactory());
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      session.start();
      SimpleString notificationQueue = RandomUtil.randomSimpleString();
      session.createQueue(new QueueConfiguration(notificationQueue).setAddress(ActiveMQDefaultConfiguration.getDefaultManagementNotificationAddress()).setDurable(false));
      notificationConsumer = addClientConsumer(session.createConsumer(notificationQueue));
   }

   private void flush() throws ActiveMQException {
      ClientMessage message;
      do {
         message = notificationConsumer.receiveImmediate();
      }
      while (message != null);
   }

   @Override
   protected void addConfiguration(ActiveMQServer server) throws Exception {
      server.getConfiguration().addAcceptorConfiguration("invm", "vm://0");
   }

   @Test(timeout = 30000)
   public void testConsumerNotificationAMQP() throws Exception {
      testConsumerNotification(createConnection(getBrokerQpidJMSConnectionURI(), null, null, clientID, true));
   }

   @Test(timeout = 30000)
   public void testConsumerNotificationCore() throws Exception {
      testConsumerNotification(createCoreConnection(getBrokerCoreJMSConnectionString(), null, null, clientID, true));
   }

   @Test(timeout = 30000)
   public void testConsumerNotificationOpenWire() throws Exception {
      testConsumerNotification(createOpenWireConnection(getBrokerOpenWireJMSConnectionString(), null, null, clientID, true));
   }

   private void testConsumerNotification(Connection connection) throws Exception {
      final String subscriptionName = "mySub";
      try {
         flush();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(getTopicName());
         flush();
         MessageConsumer consumer = session.createDurableSubscriber(topic, subscriptionName);
         notificationConsumer.receiveImmediate(); // clear the BINDING_ADDED notification for the subscription queue
         validateClientIdOnNotification(CoreNotificationType.CONSUMER_CREATED);
         consumer.close();
         validateClientIdOnNotification(CoreNotificationType.CONSUMER_CLOSED);
         session.unsubscribe(subscriptionName);
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testSessionNotificationAMQP() throws Exception {
      testSessionNotification(createConnection(getBrokerQpidJMSConnectionURI(), null, null, clientID, true));
   }

   @Test(timeout = 30000)
   public void testSessionNotificationCore() throws Exception {
      testSessionNotification(createCoreConnection(getBrokerCoreJMSConnectionString(), null, null, clientID, true));
   }

   @Test(timeout = 30000)
   public void testSessionNotificationOpenWire() throws Exception {
      testSessionNotification(createOpenWireConnection(getBrokerOpenWireJMSConnectionString(), null, null, clientID, true));
   }

   private void testSessionNotification(Connection connection) throws Exception {
      try {
         flush();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         validateClientIdOnNotification(CoreNotificationType.SESSION_CREATED);
         session.close();
         validateClientIdOnNotification(CoreNotificationType.SESSION_CLOSED);
      } finally {
         connection.close();
      }
   }

   private void validateClientIdOnNotification(CoreNotificationType notificationType) throws ActiveMQException {
      Message m = notificationConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(notificationType.toString(), m.getStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE));
      assertTrue(m.getPropertyNames().contains(HDR_CLIENT_ID));
      assertEquals(clientID, m.getStringProperty(HDR_CLIENT_ID));
   }
}

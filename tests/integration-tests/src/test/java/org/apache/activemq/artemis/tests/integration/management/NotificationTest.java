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
package org.apache.activemq.artemis.tests.integration.management;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.plugin.impl.NotificationActiveMQServerPlugin;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.activemq.artemis.api.core.management.CoreNotificationType.CONNECTION_CREATED;
import static org.apache.activemq.artemis.api.core.management.CoreNotificationType.CONNECTION_DESTROYED;
import static org.apache.activemq.artemis.api.core.management.CoreNotificationType.SESSION_CREATED;
import static org.apache.activemq.artemis.api.core.management.CoreNotificationType.SESSION_CLOSED;
import static org.apache.activemq.artemis.api.core.management.CoreNotificationType.ADDRESS_ADDED;
import static org.apache.activemq.artemis.api.core.management.CoreNotificationType.ADDRESS_REMOVED;
import static org.apache.activemq.artemis.api.core.management.CoreNotificationType.BINDING_ADDED;
import static org.apache.activemq.artemis.api.core.management.CoreNotificationType.BINDING_REMOVED;
import static org.apache.activemq.artemis.api.core.management.CoreNotificationType.CONSUMER_CLOSED;
import static org.apache.activemq.artemis.api.core.management.CoreNotificationType.CONSUMER_CREATED;
import static org.apache.activemq.artemis.api.core.management.CoreNotificationType.MESSAGE_DELIVERED;
import static org.apache.activemq.artemis.api.core.management.CoreNotificationType.MESSAGE_EXPIRED;

public class NotificationTest extends ActiveMQTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ActiveMQServer server;

   private ClientSession session;

   private ClientConsumer notifConsumer;

   private SimpleString notifQueue;
   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testBINDING_ADDED() throws Exception {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      NotificationTest.flush(notifConsumer);

      session.createQueue(address, queue, durable);

      //the first message received will be for the address creation
      ClientMessage[] notifications = NotificationTest.consumeMessages(2, notifConsumer);
      Assert.assertEquals(BINDING_ADDED.toString(), notifications[1].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(queue.toString(), notifications[1].getObjectProperty(ManagementHelper.HDR_ROUTING_NAME).toString());
      Assert.assertEquals(address.toString(), notifications[1].getObjectProperty(ManagementHelper.HDR_ADDRESS).toString());

      session.deleteQueue(queue);
   }

   @Test
   public void testBINDING_ADDEDWithMatchingFilter() throws Exception {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      System.out.println(queue);
      notifConsumer.close();
      notifConsumer = session.createConsumer(notifQueue.toString(), ManagementHelper.HDR_ROUTING_NAME + "= '" +
         queue +
         "'");
      NotificationTest.flush(notifConsumer);

      session.createQueue(address, queue, durable);

      ClientMessage[] notifications = NotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(BINDING_ADDED.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(queue.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_NAME).toString());
      Assert.assertEquals(address.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS).toString());

      session.deleteQueue(queue);
   }

   @Test
   public void testBINDING_ADDEDWithNonMatchingFilter() throws Exception {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      System.out.println(queue);
      notifConsumer.close();
      notifConsumer = session.createConsumer(notifQueue.toString(), ManagementHelper.HDR_ROUTING_NAME + " <> '" +
         queue + "' AND " + ManagementHelper.HDR_ADDRESS + " <> '" + address + "'");
      NotificationTest.flush(notifConsumer);

      session.createQueue(address, queue, durable);

      NotificationTest.consumeMessages(0, notifConsumer);

      session.deleteQueue(queue);
   }

   @Test
   public void testBINDING_REMOVED() throws Exception {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      session.createQueue(address, queue, durable);

      NotificationTest.flush(notifConsumer);

      session.deleteQueue(queue);

      //There will be 2 notifications, first is for binding removal, second is for address removal
      ClientMessage[] notifications = NotificationTest.consumeMessages(2, notifConsumer);
      Assert.assertEquals(BINDING_REMOVED.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(queue.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_NAME).toString());
      Assert.assertEquals(address.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS).toString());
   }

   @Test
   public void testCONSUMER_CREATED() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession mySession = sf.createSession("myUser", "myPassword", false, true, true, locator.isPreAcknowledge(), locator.getAckBatchSize());

      mySession.start();

      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      session.createQueue(address, queue, durable);

      NotificationTest.flush(notifConsumer);

      ClientConsumer consumer = mySession.createConsumer(queue);
      SimpleString consumerName = SimpleString.toSimpleString(((ClientSessionInternal) mySession).getName());

      ClientMessage[] notifications = NotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(CONSUMER_CREATED.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(queue.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_NAME).toString());
      Assert.assertEquals(address.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS).toString());
      Assert.assertEquals(1, notifications[0].getObjectProperty(ManagementHelper.HDR_CONSUMER_COUNT));
      Assert.assertEquals(SimpleString.toSimpleString("myUser"), notifications[0].getSimpleStringProperty(ManagementHelper.HDR_USER));
      Assert.assertEquals(SimpleString.toSimpleString("invm:0"), notifications[0].getSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS));
      Assert.assertEquals(consumerName, notifications[0].getSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME));

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testCONSUMER_CLOSED() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession mySession = sf.createSession("myUser", "myPassword", false, true, true, locator.isPreAcknowledge(), locator.getAckBatchSize());

      mySession.start();

      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      mySession.createQueue(address, queue, durable);
      ClientConsumer consumer = mySession.createConsumer(queue);
      SimpleString sessionName = SimpleString.toSimpleString(((ClientSessionInternal) mySession).getName());

      NotificationTest.flush(notifConsumer);

      consumer.close();

      ClientMessage[] notifications = NotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(CONSUMER_CLOSED.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(queue.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_NAME).toString());
      Assert.assertEquals(address.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS).toString());
      Assert.assertEquals(0, notifications[0].getObjectProperty(ManagementHelper.HDR_CONSUMER_COUNT));
      Assert.assertEquals(SimpleString.toSimpleString("myUser"), notifications[0].getSimpleStringProperty(ManagementHelper.HDR_USER));
      Assert.assertEquals(SimpleString.toSimpleString("invm:0"), notifications[0].getSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS));
      Assert.assertEquals(sessionName, notifications[0].getSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME));

      session.deleteQueue(queue);
   }

   @Test
   public void testAddressAdded() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();

      NotificationTest.flush(notifConsumer);

      session.createAddress(address, RoutingType.ANYCAST, true);

      ClientMessage[] notifications = NotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(ADDRESS_ADDED.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(RoutingType.ANYCAST.getType(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_TYPE));
      Assert.assertEquals(address.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS).toString());

   }

   @Test
   public void testAddressRemoved() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, true);
      NotificationTest.flush(notifConsumer);

      server.getPostOffice().removeAddressInfo(address);

      ClientMessage[] notifications = NotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(ADDRESS_REMOVED.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertEquals(RoutingType.ANYCAST.getType(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_TYPE));
      Assert.assertEquals(address.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS).toString());
   }

   @Test
   public void testConnectionCreatedAndDestroyed() throws Exception {
      NotificationTest.flush(notifConsumer);

      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession mySession = sf.createSession("myUser", "myPassword", false, true, true, locator.isPreAcknowledge(), locator.getAckBatchSize());
      mySession.start();

      ClientMessage[] notifications = NotificationTest.consumeMessages(2, notifConsumer);
      Assert.assertEquals(CONNECTION_CREATED.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertNotNull(notifications[0].getObjectProperty(ManagementHelper.HDR_CONNECTION_NAME));
      final String connectionId = notifications[0].getObjectProperty(ManagementHelper.HDR_CONNECTION_NAME).toString();

      Assert.assertEquals(SESSION_CREATED.toString(), notifications[1].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertNotNull(notifications[1].getObjectProperty(ManagementHelper.HDR_CONNECTION_NAME));
      Assert.assertNotNull(notifications[1].getObjectProperty(ManagementHelper.HDR_SESSION_NAME));
      Assert.assertEquals(SimpleString.toSimpleString("myUser"), notifications[1].getObjectProperty(ManagementHelper.HDR_USER));

      NotificationTest.flush(notifConsumer);
      mySession.close();
      sf.close();

      notifications = NotificationTest.consumeMessages(2, notifConsumer);

      Assert.assertEquals(SESSION_CLOSED.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertNotNull(notifications[0].getObjectProperty(ManagementHelper.HDR_CONNECTION_NAME));
      Assert.assertNotNull(notifications[0].getObjectProperty(ManagementHelper.HDR_SESSION_NAME));
      Assert.assertEquals(SimpleString.toSimpleString("myUser"), notifications[0].getObjectProperty(ManagementHelper.HDR_USER));

      Assert.assertEquals(CONNECTION_DESTROYED.toString(), notifications[1].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertNotNull(notifications[1].getObjectProperty(ManagementHelper.HDR_CONNECTION_NAME));
      Assert.assertEquals(connectionId, notifications[1].getObjectProperty(ManagementHelper.HDR_CONNECTION_NAME).toString());
   }

   @Test
   public void testMessageDelivered() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession mySession = sf.createSession("myUser", "myPassword", false, true, true, locator.isPreAcknowledge(), locator.getAckBatchSize());

      mySession.start();

      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      session.createQueue(address, queue, durable);

      ClientConsumer consumer = mySession.createConsumer(queue);
      ClientProducer producer = mySession.createProducer(address);

      NotificationTest.flush(notifConsumer);

      ClientMessage msg = session.createMessage(false);
      msg.putStringProperty("someKey", "someValue");
      producer.send(msg);
      consumer.receive(1000);

      ClientMessage[] notifications = NotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(MESSAGE_DELIVERED.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertNotNull(notifications[0].getObjectProperty(ManagementHelper.HDR_MESSAGE_ID));
      Assert.assertNotNull(notifications[0].getObjectProperty(ManagementHelper.HDR_CONSUMER_NAME));
      Assert.assertEquals(address, notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS));
      Assert.assertEquals(queue, notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_NAME));
      Assert.assertEquals(RoutingType.MULTICAST.getType(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_TYPE));

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testMessageExpired() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession mySession = sf.createSession("myUser", "myPassword", false, true, true, locator.isPreAcknowledge(), locator.getAckBatchSize());

      mySession.start();

      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();

      session.createQueue(address, queue, durable);

      ClientConsumer consumer = mySession.createConsumer(queue);
      ClientProducer producer = mySession.createProducer(address);

      NotificationTest.flush(notifConsumer);

      ClientMessage msg = session.createMessage(false);
      msg.putStringProperty("someKey", "someValue");
      msg.setExpiration(1);
      producer.send(msg);
      Thread.sleep(500);
      consumer.receive(500);

      ClientMessage[] notifications = NotificationTest.consumeMessages(1, notifConsumer);
      Assert.assertEquals(MESSAGE_EXPIRED.toString(), notifications[0].getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
      Assert.assertNotNull(notifications[0].getObjectProperty(ManagementHelper.HDR_MESSAGE_ID));
      Assert.assertEquals(address, notifications[0].getObjectProperty(ManagementHelper.HDR_ADDRESS));
      Assert.assertEquals(queue, notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_NAME));
      Assert.assertEquals(RoutingType.MULTICAST.getType(), notifications[0].getObjectProperty(ManagementHelper.HDR_ROUTING_TYPE));

      consumer.close();
      session.deleteQueue(queue);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig(), false));
      NotificationActiveMQServerPlugin notificationPlugin = new NotificationActiveMQServerPlugin();
      notificationPlugin.setSendAddressNotifications(true);
      notificationPlugin.setSendConnectionNotifications(true);
      notificationPlugin.setSendSessionNotifications(true);
      notificationPlugin.setSendDeliveredNotifications(true);
      notificationPlugin.setSendExpiredNotifications(true);

      server.registerBrokerPlugin(notificationPlugin);
      server.start();

      locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true);
      session.start();

      notifQueue = RandomUtil.randomSimpleString();

      session.createQueue(ActiveMQDefaultConfiguration.getDefaultManagementNotificationAddress(), notifQueue, null, false);

      notifConsumer = session.createConsumer(notifQueue);
   }

   // Private -------------------------------------------------------

   private static void flush(final ClientConsumer notifConsumer) throws ActiveMQException {
      ClientMessage message = null;
      do {
         message = notifConsumer.receive(500);
      }
      while (message != null);
   }

   protected static ClientMessage[] consumeMessages(final int expected,
                                                    final ClientConsumer consumer) throws Exception {
      ClientMessage[] messages = new ClientMessage[expected];

      ClientMessage m = null;
      for (int i = 0; i < expected; i++) {
         m = consumer.receive(500);
         if (m != null) {
            for (SimpleString key : m.getPropertyNames()) {
               System.out.println(key + "=" + m.getObjectProperty(key));
            }
         }
         Assert.assertNotNull("expected to received " + expected + " messages, got only " + i, m);
         messages[i] = m;
         m.acknowledge();
      }
      m = consumer.receiveImmediate();
      if (m != null) {
         for (SimpleString key : m.getPropertyNames()) {
            System.out.println(key + "=" + m.getObjectProperty(key));
         }
      }
      Assert.assertNull("received one more message than expected (" + expected + ")", m);

      return messages;
   }

   // Inner classes -------------------------------------------------

}

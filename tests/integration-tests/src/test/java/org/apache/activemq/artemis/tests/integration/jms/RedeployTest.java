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

package org.apache.activemq.artemis.tests.integration.jms;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.junit.Wait;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.junit.Assert;
import org.junit.Test;

public class RedeployTest extends ActiveMQTestBase {

   @Test
   public void testRedeploy() throws Exception {
      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      URL url1 = RedeployTest.class.getClassLoader().getResource("reload-test-jms.xml");
      URL url2 = RedeployTest.class.getClassLoader().getResource("reload-test-updated-jms.xml");
      Files.copy(url1.openStream(), brokerXML);

      EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
      embeddedActiveMQ.setConfigResourcePath(brokerXML.toUri().toString());
      embeddedActiveMQ.start();

      final ReusableLatch latch = new ReusableLatch(1);

      Runnable tick = new Runnable() {
         @Override
         public void run() {
            latch.countDown();
         }
      };

      embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(tick);

      try {
         latch.await(10, TimeUnit.SECONDS);
         Assert.assertEquals("DLQ", embeddedActiveMQ.getActiveMQServer().getAddressSettingsRepository().getMatch("jms").getDeadLetterAddress().toString());
         Assert.assertEquals("ExpiryQueue", embeddedActiveMQ.getActiveMQServer().getAddressSettingsRepository().getMatch("jms").getExpiryAddress().toString());
         Assert.assertFalse(tryConsume());
         Files.copy(url2.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
         brokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
         latch.setCount(1);
         embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(tick);
         latch.await(10, TimeUnit.SECONDS);

         Assert.assertTrue(tryConsume());

         Assert.assertEquals("NewQueue", embeddedActiveMQ.getActiveMQServer().getAddressSettingsRepository().getMatch("jms").getDeadLetterAddress().toString());
         Assert.assertEquals("NewQueue", embeddedActiveMQ.getActiveMQServer().getAddressSettingsRepository().getMatch("jms").getExpiryAddress().toString());

         ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
         try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession();
            Queue queue = session.createQueue("DivertQueue");
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("text"));
            connection.start();
            MessageConsumer consumer = session.createConsumer(session.createQueue("NewQueue"));
            Assert.assertNotNull("Divert wasn't redeployed accordingly", consumer.receive(5000));
         }

      } finally {
         embeddedActiveMQ.stop();
      }
   }

   @Test
   public void testRedeployFilter() throws Exception {
      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      URL url1 = RedeployTest.class.getClassLoader().getResource("reload-queue-filter.xml");
      URL url2 = RedeployTest.class.getClassLoader().getResource("reload-queue-filter-updated.xml");
      Files.copy(url1.openStream(), brokerXML);

      EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
      embeddedActiveMQ.setConfigResourcePath(brokerXML.toUri().toString());
      embeddedActiveMQ.start();

      final ReusableLatch latch = new ReusableLatch(1);

      Runnable tick = new Runnable() {
         @Override
         public void run() {
            latch.countDown();
         }
      };

      embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(tick);

      try {
         latch.await(10, TimeUnit.SECONDS);

         try (ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
              Connection connection = factory.createConnection();
              Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE)) {
            connection.start();
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createMessage();
            message.setStringProperty("x", "x");
            producer.send(message);
            MessageConsumer consumer = session.createConsumer(queue);
            assertNotNull(consumer.receive(2000));
         }

         Files.copy(url2.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
         brokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
         latch.setCount(1);
         embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(tick);
         latch.await(10, TimeUnit.SECONDS);

         try (ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
              Connection connection = factory.createConnection();
              Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE)) {
            connection.start();
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createMessage();
            message.setStringProperty("x", "y");
            producer.send(message);
            MessageConsumer consumer = session.createConsumer(queue);
            assertNotNull(consumer.receive(2000));
         }

      } finally {
         embeddedActiveMQ.stop();
      }
   }

   @Test
   public void testRedeployWithFailover() throws Exception {
      EmbeddedActiveMQ live = new EmbeddedActiveMQ();
      EmbeddedActiveMQ backup = new EmbeddedActiveMQ();

      try {
         // set these system properties to use in the relevant broker.xml files
         System.setProperty("live-data-dir", getTestDirfile().toPath() + "/redeploy-live-data");
         System.setProperty("backup-data-dir", getTestDirfile().toPath() + "/redeploy-backup-data");

         Path liveBrokerXML = getTestDirfile().toPath().resolve("live.xml");
         Path backupBrokerXML = getTestDirfile().toPath().resolve("backup.xml");
         URL url1 = RedeployTest.class.getClassLoader().getResource("reload-live-original.xml");
         URL url2 = RedeployTest.class.getClassLoader().getResource("reload-live-changed.xml");
         URL url3 = RedeployTest.class.getClassLoader().getResource("reload-backup-original.xml");
         URL url4 = RedeployTest.class.getClassLoader().getResource("reload-backup-changed.xml");
         Files.copy(url1.openStream(), liveBrokerXML);
         Files.copy(url3.openStream(), backupBrokerXML);

         live.setConfigResourcePath(liveBrokerXML.toUri().toString());
         live.start();

         waitForServerToStart(live.getActiveMQServer());

         backup.setConfigResourcePath(backupBrokerXML.toUri().toString());
         backup.start();

         assertTrue(Wait.waitFor(() -> backup.getActiveMQServer().isReplicaSync(), 15000, 200));

         final ReusableLatch liveReloadLatch = new ReusableLatch(1);
         Runnable liveTick = () -> liveReloadLatch.countDown();
         live.getActiveMQServer().getReloadManager().setTick(liveTick);

         final ReusableLatch backupReloadTickLatch = new ReusableLatch(1);
         Runnable backupTick = () -> backupReloadTickLatch.countDown();
         backup.getActiveMQServer().getReloadManager().setTick(backupTick);

         liveReloadLatch.await(10, TimeUnit.SECONDS);
         Files.copy(url2.openStream(), liveBrokerXML, StandardCopyOption.REPLACE_EXISTING);
         liveBrokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
         liveReloadLatch.countUp();
         live.getActiveMQServer().getReloadManager().setTick(liveTick);
         liveReloadLatch.await(10, TimeUnit.SECONDS);

         backupReloadTickLatch.await(10, TimeUnit.SECONDS);
         Files.copy(url4.openStream(), backupBrokerXML, StandardCopyOption.REPLACE_EXISTING);
         backupBrokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
         backupReloadTickLatch.countUp();
         backup.getActiveMQServer().getReloadManager().setTick(backupTick);
         backupReloadTickLatch.await(10, TimeUnit.SECONDS);

         ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
         try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession();
            Queue queue = session.createQueue("myQueue2");
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("text"));
         }

         live.stop();

         assertTrue(Wait.waitFor(() -> (backup.getActiveMQServer().isActive()), 5000, 100));

         factory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61617");
         try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession();
            Queue queue = session.createQueue("myQueue2");
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("text"));
            connection.start();
            MessageConsumer consumer = session.createConsumer(session.createQueue("myQueue2"));
            Assert.assertNotNull("Queue wasn't deployed accordingly", consumer.receive(5000));
            Assert.assertNotNull(consumer.receive(5000));
         }
      } finally {
         live.stop();
         backup.stop();
         System.clearProperty("live-data-dir");
         System.clearProperty("backup-data-dir");
      }
   }

   private boolean tryConsume() throws JMSException {
      try (ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
           Connection connection = factory.createConnection();
           Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE)) {
         Queue queue = session.createQueue("NewQueue");
         MessageConsumer consumer = session.createConsumer(queue);
         return true;
      } catch (JMSException e) {
         return false;
      }

   }

   @Test
   public void testRedeployAddressQueue() throws Exception {
      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      URL url1 = RedeployTest.class.getClassLoader().getResource("reload-address-queues.xml");
      URL url2 = RedeployTest.class.getClassLoader().getResource("reload-address-queues-updated.xml");
      Files.copy(url1.openStream(), brokerXML);

      EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
      embeddedActiveMQ.setConfigResourcePath(brokerXML.toUri().toString());
      embeddedActiveMQ.start();

      final ReusableLatch latch = new ReusableLatch(1);

      Runnable tick = new Runnable() {
         @Override
         public void run() {
            latch.countDown();
         }
      };

      embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(tick);

      try {
         latch.await(10, TimeUnit.SECONDS);
         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_address_removal_no_queue"));
         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_removal").contains("config_test_queue_removal_queue_1"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_removal").contains("config_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "permanent_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "permanent_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "permanent_test_queue_removal").contains("permanent_test_queue_removal_queue_1"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "permanent_test_queue_removal").contains("permanent_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_queue_change"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_change").contains("config_test_queue_change_queue"));
         Assert.assertEquals(10, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").getMaxConsumers());
         Assert.assertEquals(false, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").isPurgeOnNoConsumers());

         Files.copy(url2.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
         brokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
         latch.setCount(1);
         embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(tick);
         latch.await(10, TimeUnit.SECONDS);

         Assert.assertNull(getAddressInfo(embeddedActiveMQ, "config_test_address_removal_no_queue"));
         Assert.assertNull(getAddressInfo(embeddedActiveMQ, "config_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_removal").contains("config_test_queue_removal_queue_1"));
         Assert.assertFalse(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_removal").contains("config_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "permanent_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "permanent_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "permanent_test_queue_removal").contains("permanent_test_queue_removal_queue_1"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "permanent_test_queue_removal").contains("permanent_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_queue_change"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_change").contains("config_test_queue_change_queue"));
         Assert.assertEquals(1, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").getMaxConsumers());
         Assert.assertEquals(true, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").isPurgeOnNoConsumers());

         Assert.assertNull(getAddressInfo(embeddedActiveMQ, "config_test_queue_change_queue"));
         Assert.assertNull(getAddressInfo(embeddedActiveMQ, "config_test_queue_removal_queue_1"));
      } finally {
         embeddedActiveMQ.stop();
      }
   }

   @Test
   public void testRedeployChangeQueueRoutingType() throws Exception {
      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      URL url1 = RedeployTest.class.getClassLoader().getResource("reload-queue-routingtype.xml");
      URL url2 = RedeployTest.class.getClassLoader().getResource("reload-queue-routingtype-updated.xml");
      Files.copy(url1.openStream(), brokerXML);

      EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
      embeddedActiveMQ.setConfigResourcePath(brokerXML.toUri().toString());
      embeddedActiveMQ.start();

      final ReusableLatch latch = new ReusableLatch(1);

      Runnable tick = new Runnable() {
         @Override
         public void run() {
            latch.countDown();
         }
      };

      embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(tick);

      try {
         latch.await(10, TimeUnit.SECONDS);
         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "myAddress"));
         Assert.assertEquals(RoutingType.MULTICAST, getQueue(embeddedActiveMQ, "myQueue").getRoutingType());

         Files.copy(url2.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
         brokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
         latch.setCount(1);
         embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(tick);
         latch.await(10, TimeUnit.SECONDS);

         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "myAddress"));
         Assert.assertEquals(RoutingType.ANYCAST, getQueue(embeddedActiveMQ, "myQueue").getRoutingType());
      } finally {
         embeddedActiveMQ.stop();
      }
   }



   /**
    * Simulates Stop and Start that occurs when network health checker stops the server when network is detected unhealthy
    * and re-starts the broker once detected that it is healthy again.
    *
    * @throws Exception for anything un-expected, test will fail.
    */
   @Test
   public void testRedeployStopAndRestart() throws Exception {
      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      URL url1 = RedeployTest.class.getClassLoader().getResource("reload-original.xml");
      URL url2 = RedeployTest.class.getClassLoader().getResource("reload-changed.xml");
      Files.copy(url1.openStream(), brokerXML);

      EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
      embeddedActiveMQ.setConfigResourcePath(brokerXML.toUri().toString());
      embeddedActiveMQ.start();

      final ReusableLatch latch = new ReusableLatch(1);

      Runnable tick = latch::countDown;

      embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(tick);

      try {
         latch.await(10, TimeUnit.SECONDS);

         Assert.assertEquals(getSecurityRoles(embeddedActiveMQ, "security_address").size(), 1);
         Assert.assertEquals(getSecurityRoles(embeddedActiveMQ, "security_address").iterator().next().getName(), "b");

         Assert.assertEquals(getAddressSettings(embeddedActiveMQ, "address_settings_address").getDeadLetterAddress(), SimpleString.toSimpleString("OriginalDLQ"));
         Assert.assertEquals(getAddressSettings(embeddedActiveMQ, "address_settings_address").getExpiryAddress(), SimpleString.toSimpleString("OriginalExpiryQueue"));

         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_address_removal_no_queue"));
         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_removal").contains("config_test_queue_removal_queue_1"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_removal").contains("config_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_queue_change"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_change").contains("config_test_queue_change_queue"));
         Assert.assertEquals(10, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").getMaxConsumers());
         Assert.assertEquals(false, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").isPurgeOnNoConsumers());

         Files.copy(url2.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
         brokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
         latch.setCount(1);
         embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(tick);
         latch.await(10, TimeUnit.SECONDS);

         //Assert that the security settings change applied
         Assert.assertEquals(getSecurityRoles(embeddedActiveMQ, "security_address").size(), 1);
         Assert.assertEquals(getSecurityRoles(embeddedActiveMQ, "security_address").iterator().next().getName(), "c");

         //Assert that the address settings change applied
         Assert.assertEquals(getAddressSettings(embeddedActiveMQ, "address_settings_address").getDeadLetterAddress(), SimpleString.toSimpleString("NewDLQ"));
         Assert.assertEquals(getAddressSettings(embeddedActiveMQ, "address_settings_address").getExpiryAddress(), SimpleString.toSimpleString("NewExpiryQueue"));

         //Assert the address and queue changes applied
         Assert.assertNull(getAddressInfo(embeddedActiveMQ, "config_test_address_removal_no_queue"));
         Assert.assertNull(getAddressInfo(embeddedActiveMQ, "config_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_removal").contains("config_test_queue_removal_queue_1"));
         Assert.assertFalse(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_removal").contains("config_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_queue_change"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_change").contains("config_test_queue_change_queue"));
         Assert.assertEquals(1, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").getMaxConsumers());
         Assert.assertEquals(true, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").isPurgeOnNoConsumers());
      } finally {
         embeddedActiveMQ.stop();
      }


      try {
         embeddedActiveMQ.start();

         //Assert that the security settings changes persist a stop and start server (e.g. like what occurs if network health check stops the node), but JVM remains up.
         Assert.assertEquals(getSecurityRoles(embeddedActiveMQ, "security_address").size(), 1);
         Assert.assertEquals(getSecurityRoles(embeddedActiveMQ, "security_address").iterator().next().getName(), "c");

         //Assert that the address settings changes persist a stop and start server (e.g. like what occurs if network health check stops the node), but JVM remains up.
         Assert.assertEquals(getAddressSettings(embeddedActiveMQ, "address_settings_address").getDeadLetterAddress(), SimpleString.toSimpleString("NewDLQ"));
         Assert.assertEquals(getAddressSettings(embeddedActiveMQ, "address_settings_address").getExpiryAddress(), SimpleString.toSimpleString("NewExpiryQueue"));

         //Assert that the address and queue changes persist a stop and start server (e.g. like what occurs if network health check stops the node), but JVM remains up.
         Assert.assertNull(getAddressInfo(embeddedActiveMQ, "config_test_address_removal_no_queue"));
         Assert.assertNull(getAddressInfo(embeddedActiveMQ, "config_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_removal").contains("config_test_queue_removal_queue_1"));
         Assert.assertFalse(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_removal").contains("config_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "config_test_queue_change"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_queue_change").contains("config_test_queue_change_queue"));
         Assert.assertEquals(1, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").getMaxConsumers());
         Assert.assertEquals(true, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").isPurgeOnNoConsumers());

      } finally {
         embeddedActiveMQ.stop();
      }
   }

   private AddressSettings getAddressSettings(EmbeddedActiveMQ embeddedActiveMQ, String address) {
      return embeddedActiveMQ.getActiveMQServer().getAddressSettingsRepository().getMatch(address);
   }

   private Set<Role> getSecurityRoles(EmbeddedActiveMQ embeddedActiveMQ, String address) {
      return embeddedActiveMQ.getActiveMQServer().getSecurityRepository().getMatch(address);
   }

   private AddressInfo getAddressInfo(EmbeddedActiveMQ embeddedActiveMQ, String address) {
      return embeddedActiveMQ.getActiveMQServer().getPostOffice().getAddressInfo(SimpleString.toSimpleString(address));
   }

   private org.apache.activemq.artemis.core.server.Queue getQueue(EmbeddedActiveMQ embeddedActiveMQ, String queueName) throws Exception {
      QueueBinding queueBinding = (QueueBinding) embeddedActiveMQ.getActiveMQServer().getPostOffice().getBinding(SimpleString.toSimpleString(queueName));
      return queueBinding == null ? null : queueBinding.getQueue();
   }

   private List<String> listQueuesNamesForAddress(EmbeddedActiveMQ embeddedActiveMQ, String address) throws Exception {
      return embeddedActiveMQ.getActiveMQServer().getPostOffice().listQueuesForAddress(SimpleString.toSimpleString(address)).stream().map(
          org.apache.activemq.artemis.core.server.Queue::getName).map(SimpleString::toString).collect(Collectors.toList());
   }

}

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

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.postoffice.impl.DivertBinding;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.cluster.impl.RemoteQueueBindingImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.tests.unit.core.postoffice.impl.FakeQueue;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.junit.Assert;
import org.junit.Test;

public class RedeployTest extends ActiveMQTestBase {

   @Test
   /*
    * This tests that the broker doesnt fall over when it tries to delete any autocreated addresses/queues in a clustered environment
    * If the undeploy fails then bridges etc can stop working, we need to make sure if undeploy fails on anything the broker is still live
    * */
   public void testRedeployAutoCreateAddress() throws Exception {
      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      URL url1 = RedeployTest.class.getClassLoader().getResource("reload-test-autocreateaddress.xml");
      URL url2 = RedeployTest.class.getClassLoader().getResource("reload-test-autocreateaddress-reload.xml");
      Files.copy(url1.openStream(), brokerXML);

      EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
      embeddedActiveMQ.setConfigResourcePath(brokerXML.toUri().toString());
      embeddedActiveMQ.start();

      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession();
         Queue queue = session.createQueue("autoQueue");
         MessageProducer producer = session.createProducer(queue);
         producer.send(session.createTextMessage("text"));
         connection.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue("autoQueue"));
         Assert.assertNotNull("Address wasn't autocreated accordingly", consumer.receive(5000));
      }

      Assert.assertNotNull(getQueue(embeddedActiveMQ, "autoQueue"));

      // this simulates a remote queue or other type being added that wouldnt get deleted, its not valid to have this happen but it can happen when addresses and queues are auto created in a clustered env
      embeddedActiveMQ.getActiveMQServer().getPostOffice().addBinding(new RemoteQueueBindingImpl(5L,
              new SimpleString("autoQueue"),
              new SimpleString("uniqueName"),
              new SimpleString("routingName"),
              6L,
              null,
              new FakeQueue(new SimpleString("foo"), 6L),
              new SimpleString("bridge"),
              1,
              MessageLoadBalancingType.OFF));

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
         Files.copy(url2.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
         brokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
         latch.setCount(1);
         embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(tick);
         latch.await(10, TimeUnit.SECONDS);

         Assert.assertTrue(tryConsume());

         Assert.assertNotNull(getQueue(embeddedActiveMQ, "autoQueue"));

         factory = new ActiveMQConnectionFactory();
         try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession();
            Queue queue = session.createQueue("autoQueue");
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("text"));
            connection.start();
            MessageConsumer consumer = session.createConsumer(session.createQueue("autoQueue"));
            Assert.assertNotNull("autoQueue redeployed accordingly", consumer.receive(5000));
         }

      } finally {
         embeddedActiveMQ.stop();
      }
   }

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
            Queue queue = session.createQueue("myFilterQueue");
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createMessage();
            message.setStringProperty("x", "x");
            producer.send(message);
            MessageConsumer consumer = session.createConsumer(queue);
            assertNotNull(consumer.receive(5000));
            consumer.close();
         }

         //Send a message that should remain in the queue (this ensures config change is non-destructive)
         try (ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
              Connection connection = factory.createConnection();
              Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE)) {
            connection.start();
            Queue queue = session.createQueue("myFilterQueue");
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createTextMessage("hello");
            message.setStringProperty("x", "x");
            producer.send(message);
         }

         Binding binding = embeddedActiveMQ.getActiveMQServer().getPostOffice().getBinding(SimpleString.toSimpleString("myFilterQueue"));

         Files.copy(url2.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
         brokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
         latch.setCount(1);
         embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(tick);
         latch.await(10, TimeUnit.SECONDS);

         Binding bindingAfterChange = embeddedActiveMQ.getActiveMQServer().getPostOffice().getBinding(SimpleString.toSimpleString("myFilterQueue"));

         assertTrue("Instance should be the same (as should be non destructive)", binding == bindingAfterChange);
         assertEquals(binding.getID(), bindingAfterChange.getID());

         //Check that after the config change we can still consume a message that was sent before, ensuring config change was non-destructive of the queue.
         try (ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
              Connection connection = factory.createConnection();
              Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE)) {
            connection.start();
            Queue queue = session.createQueue("myFilterQueue");
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals("hello", ((TextMessage)message).getText());
            consumer.close();
         }

         try (ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
              Connection connection = factory.createConnection();
              Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE)) {
            connection.start();
            Queue queue = session.createQueue("myFilterQueue");
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createMessage();
            message.setStringProperty("x", "y");
            producer.send(message);
            MessageConsumer consumer = session.createConsumer(queue);
            assertNotNull(consumer.receive(2000));
            consumer.close();
         }

      } finally {
         embeddedActiveMQ.stop();
      }
   }

   private void deployBrokerConfig(EmbeddedActiveMQ server, URL configFile) throws Exception {

      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      Files.copy(configFile.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
      brokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
      final ReusableLatch latch = new ReusableLatch(1);
      Runnable tick = latch::countDown;
      server.getActiveMQServer().getReloadManager().setTick(tick);

      latch.await(10, TimeUnit.SECONDS);
   }

   private void doTestRemoveFilter(URL testConfiguration) throws Exception {

      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");

      URL baseConfig = RedeployTest.class.getClassLoader().getResource("reload-queue-filter.xml");

      Files.copy(baseConfig.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);

      EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
      embeddedActiveMQ.setConfigResourcePath(brokerXML.toUri().toString());
      embeddedActiveMQ.start();

      deployBrokerConfig(embeddedActiveMQ, baseConfig);

      try {

         try (ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
              Connection connection = factory.createConnection();
              Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE)) {

            connection.start();
            Queue queue = session.createQueue("myFilterQueue");

            // Test that the original filter has been set up
            LocalQueueBinding queueBinding = (LocalQueueBinding) embeddedActiveMQ.getActiveMQServer().getPostOffice()
                    .getBinding(new SimpleString("myFilterQueue"));
            // The "x = 'x'" value is found in "reload-queue-filter.xml"
            assertEquals("x = 'x'", queueBinding.getFilter().getFilterString().toString());

            MessageProducer producer = session.createProducer(queue);

            // Test that the original filter affects the flow
            Message passingMessage = session.createMessage();
            passingMessage.setStringProperty("x", "x");
            producer.send(passingMessage);

            Message filteredMessage = session.createMessage();
            filteredMessage.setStringProperty("x", "y");
            producer.send(filteredMessage);

            MessageConsumer consumer = session.createConsumer(queue);
            Message receivedMessage = consumer.receive(2000);
            assertNotNull(receivedMessage);
            assertEquals("x", receivedMessage.getStringProperty("x"));

            assertNull(consumer.receive(2000));

            consumer.close();
         }

         deployBrokerConfig(embeddedActiveMQ, testConfiguration);

         try (ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
              Connection connection = factory.createConnection();
              Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE)) {

            connection.start();
            Queue queue = session.createQueue("myFilterQueue");

            // Test that the filter has been removed
            LocalQueueBinding queueBinding = (LocalQueueBinding) embeddedActiveMQ.getActiveMQServer().getPostOffice()
                    .getBinding(new SimpleString("myFilterQueue"));
            assertNull(queueBinding.getFilter());

            MessageProducer producer = session.createProducer(queue);

            // Test that the original filter no longer affects the flow
            Message message1 = session.createMessage();
            message1.setStringProperty("x", "x");
            producer.send(message1);

            Message message2 = session.createMessage();
            message2.setStringProperty("x", "y");
            producer.send(message2);

            MessageConsumer consumer = session.createConsumer(queue);
            assertNotNull(consumer.receive(2000));
            assertNotNull(consumer.receive(2000));

            consumer.close();
         }

      } finally {
         embeddedActiveMQ.stop();
      }
   }

   @Test
   public void testRedeployRemoveFilter() throws Exception {
      doTestRemoveFilter(RedeployTest.class.getClassLoader().getResource("reload-queue-filter-updated-empty.xml"));
      doTestRemoveFilter(RedeployTest.class.getClassLoader().getResource("reload-queue-filter-removed.xml"));
   }

   /**
    * This one is here just to make sure it's possible to change queue parameters one by one without setting the others
    * to <code>null</code>.
    * @throws Exception
    */
   @Test
   public void testQueuePartialReconfiguration() throws Exception {

      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      URL url = RedeployTest.class.getClassLoader().getResource("reload-empty.xml");
      Files.copy(url.openStream(), brokerXML);

      EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
      embeddedActiveMQ.setConfigResourcePath(brokerXML.toUri().toString());
      embeddedActiveMQ.start();

      try {

         embeddedActiveMQ.getActiveMQServer().createQueue(new QueueConfiguration("virtualQueue").setUser("bob"));
         embeddedActiveMQ.getActiveMQServer().updateQueue(new QueueConfiguration("virtualQueue").setFilterString("foo"));

         LocalQueueBinding queueBinding = (LocalQueueBinding) embeddedActiveMQ.getActiveMQServer().getPostOffice()
                 .getBinding(new SimpleString("virtualQueue"));
         org.apache.activemq.artemis.core.server.Queue queue = queueBinding.getQueue();

         assertEquals(new SimpleString("bob"), queue.getUser());
         assertEquals(new SimpleString("foo"), queue.getFilter().getFilterString());

      } finally {
         embeddedActiveMQ.stop();
      }

   }

   @Test
   public void testRedeployQueueDefaults() throws Exception {

      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      URL baseConfig = RedeployTest.class.getClassLoader().getResource("reload-queue-defaults-before.xml");
      URL newConfig = RedeployTest.class.getClassLoader().getResource("reload-queue-defaults-after.xml");
      Files.copy(baseConfig.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
      EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
      embeddedActiveMQ.setConfigResourcePath(brokerXML.toUri().toString());
      embeddedActiveMQ.start();

      try {
         LocalQueueBinding queueBinding = (LocalQueueBinding) embeddedActiveMQ.getActiveMQServer().getPostOffice()
                 .getBinding(new SimpleString("myQueue"));
         org.apache.activemq.artemis.core.server.Queue queue = queueBinding.getQueue();

         assertNotEquals(ActiveMQDefaultConfiguration.getDefaultMaxQueueConsumers(), queue.getMaxConsumers());
         assertNotEquals(RoutingType.MULTICAST, queue.getRoutingType());
         assertNotEquals(ActiveMQDefaultConfiguration.getDefaultPurgeOnNoConsumers(), queue.isPurgeOnNoConsumers());
         assertNotEquals(ActiveMQDefaultConfiguration.getDefaultEnabled(), queue.isEnabled());
         assertNotEquals(ActiveMQDefaultConfiguration.getDefaultExclusive(), queue.isExclusive());
         assertNotEquals(ActiveMQDefaultConfiguration.getDefaultGroupRebalance(), queue.isGroupRebalance());
         assertNotEquals(ActiveMQDefaultConfiguration.getDefaultGroupBuckets(), queue.getGroupBuckets());
         assertNotEquals(ActiveMQDefaultConfiguration.getDefaultGroupFirstKey(), queue.getGroupFirstKey());
         assertNotEquals(ActiveMQDefaultConfiguration.getDefaultNonDestructive(), queue.isNonDestructive());
         assertNotEquals(ActiveMQDefaultConfiguration.getDefaultConsumersBeforeDispatch(), queue.getConsumersBeforeDispatch());
         assertNotEquals(ActiveMQDefaultConfiguration.getDefaultDelayBeforeDispatch(), queue.getDelayBeforeDispatch());
         assertNotNull(queue.getFilter());
         assertEquals(new SimpleString("jdoe"), queue.getUser());
         assertNotEquals(ActiveMQDefaultConfiguration.getDefaultRingSize(), queue.getRingSize());

         deployBrokerConfig(embeddedActiveMQ, newConfig);

         assertEquals(ActiveMQDefaultConfiguration.getDefaultMaxQueueConsumers(), queue.getMaxConsumers());
         assertEquals(RoutingType.MULTICAST, queue.getRoutingType());
         assertEquals(ActiveMQDefaultConfiguration.getDefaultPurgeOnNoConsumers(), queue.isPurgeOnNoConsumers());
         assertEquals(ActiveMQDefaultConfiguration.getDefaultEnabled(), queue.isEnabled());
         assertEquals(ActiveMQDefaultConfiguration.getDefaultExclusive(), queue.isExclusive());
         assertEquals(ActiveMQDefaultConfiguration.getDefaultGroupRebalance(), queue.isGroupRebalance());
         assertEquals(ActiveMQDefaultConfiguration.getDefaultGroupBuckets(), queue.getGroupBuckets());
         assertEquals(ActiveMQDefaultConfiguration.getDefaultGroupFirstKey(), queue.getGroupFirstKey());
         assertEquals(ActiveMQDefaultConfiguration.getDefaultNonDestructive(), queue.isNonDestructive());
         assertEquals(ActiveMQDefaultConfiguration.getDefaultConsumersBeforeDispatch(), queue.getConsumersBeforeDispatch());
         assertEquals(ActiveMQDefaultConfiguration.getDefaultDelayBeforeDispatch(), queue.getDelayBeforeDispatch());
         assertNull(queue.getFilter());
         assertNull(queue.getUser());
         assertEquals(ActiveMQDefaultConfiguration.getDefaultRingSize(), queue.getRingSize());

      } finally {
         embeddedActiveMQ.stop();
      }
   }

   @Test
   public void testUndeployDivert() throws Exception {

      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      URL baseConfig = RedeployTest.class.getClassLoader().getResource("reload-divert-undeploy-before.xml");
      URL newConfig = RedeployTest.class.getClassLoader().getResource("reload-divert-undeploy-after.xml");
      Files.copy(baseConfig.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
      EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
      embeddedActiveMQ.setConfigResourcePath(brokerXML.toUri().toString());
      embeddedActiveMQ.start();

      try {
         DivertBinding divertBinding = (DivertBinding) embeddedActiveMQ.getActiveMQServer().getPostOffice()
                 .getBinding(new SimpleString("divert"));
         assertNotNull(divertBinding);

         Queue sourceQueue = (Queue) ActiveMQDestination.createDestination("queue://source", ActiveMQDestination.TYPE.QUEUE);
         Queue targetQueue = (Queue) ActiveMQDestination.createDestination("queue://target", ActiveMQDestination.TYPE.QUEUE);

         try (ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
              Connection connection = factory.createConnection();
              Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
              MessageProducer sourceProducer = session.createProducer(sourceQueue);
              MessageConsumer sourceConsumer = session.createConsumer(sourceQueue);
              MessageConsumer targetConsumer = session.createConsumer(targetQueue)) {

            connection.start();
            Message message = session.createTextMessage("Hello world");
            sourceProducer.send(message);
            assertNotNull(sourceConsumer.receive(2000));
            assertNotNull(targetConsumer.receive(2000));
         }

         deployBrokerConfig(embeddedActiveMQ, newConfig);

         Wait.waitFor(() -> embeddedActiveMQ.getActiveMQServer().getPostOffice()
                         .getBinding(new SimpleString("divert")) == null);
         divertBinding = (DivertBinding) embeddedActiveMQ.getActiveMQServer().getPostOffice()
                 .getBinding(new SimpleString("divert"));
         assertNull(divertBinding);

         try (ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
              Connection connection = factory.createConnection();
              Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
              MessageProducer sourceProducer = session.createProducer(sourceQueue);
              MessageConsumer sourceConsumer = session.createConsumer(sourceQueue);
              MessageConsumer targetConsumer = session.createConsumer(targetQueue)) {

            connection.start();
            Message message = session.createTextMessage("Hello world");
            sourceProducer.send(message);
            assertNotNull(sourceConsumer.receive(2000));
            assertNull(targetConsumer.receiveNoWait());
         }
      } finally {
         embeddedActiveMQ.stop();
      }
   }

   @Test
   public void testRedeployWithFailover() throws Exception {
      Set<Role> original = new HashSet<>();
      original.add(new Role("a", false, true, false, false, false, false, false, false, false, false));
      Set<Role> changed = new HashSet<>();
      changed.add(new Role("b", false, true, false, false, false, false, false, false, false, false));



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

         assertEquals("Test address settings original - live", AddressFullMessagePolicy.BLOCK, live.getActiveMQServer().getAddressSettingsRepository().getMatch("myQueue").getAddressFullMessagePolicy());
         assertEquals("Test address settings original - backup", AddressFullMessagePolicy.BLOCK, backup.getActiveMQServer().getAddressSettingsRepository().getMatch("myQueue").getAddressFullMessagePolicy());
         assertEquals("Test security settings original - live", original, live.getActiveMQServer().getSecurityRepository().getMatch("myQueue"));
         assertEquals("Test security settings original - backup", original, backup.getActiveMQServer().getSecurityRepository().getMatch("myQueue"));

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
            producer.send(session.createTextMessage("text1"));
         }

         assertFalse(backup.getActiveMQServer().isActive());
         assertEquals("Test address settings redeploy - live", AddressFullMessagePolicy.PAGE, live.getActiveMQServer().getAddressSettingsRepository().getMatch("myQueue").getAddressFullMessagePolicy());
         assertEquals("Test security settings redeploy - live", changed, live.getActiveMQServer().getSecurityRepository().getMatch("myQueue"));

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
         assertEquals("Test security settings redeploy - backup", changed, backup.getActiveMQServer().getSecurityRepository().getMatch("myQueue"));
         assertEquals("Test address settings redeploy - backup", AddressFullMessagePolicy.PAGE, backup.getActiveMQServer().getAddressSettingsRepository().getMatch("myQueue").getAddressFullMessagePolicy());
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

      ConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
      try (JMSContext jmsContext = connectionFactory.createContext()) {
         jmsContext.createSharedDurableConsumer(jmsContext.createTopic("config_test_consumer_created_queues"),"mySub").receive(100);
      }

      try {
         latch.await(10, TimeUnit.SECONDS);
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_consumer_created_queues").contains("mySub"));

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

         //Ensure queues created by clients (NOT by broker.xml are not removed when we reload).
         Assert.assertTrue(listQueuesNamesForAddress(embeddedActiveMQ, "config_test_consumer_created_queues").contains("mySub"));

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

      embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(latch::countDown);

      try {
         ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://0.0.0.0:61616");
         try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(context.createQueue("myAddress"), "hello");
         }

         latch.await(10, TimeUnit.SECONDS);
         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "myAddress"));
         Assert.assertEquals(RoutingType.ANYCAST, getQueue(embeddedActiveMQ, "myQueue").getRoutingType());

         Files.copy(url2.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
         brokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
         latch.setCount(1);
         embeddedActiveMQ.getActiveMQServer().getReloadManager().setTick(latch::countDown);
         Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

         Assert.assertNotNull(getAddressInfo(embeddedActiveMQ, "myAddress"));
         Assert.assertEquals(RoutingType.MULTICAST, getQueue(embeddedActiveMQ, "myQueue").getRoutingType());

         //Ensures the queue isnt detroyed by checking message sent before change is consumable after (e.g. no message loss)
         try (JMSContext context = connectionFactory.createContext()) {
            Message message = context.createSharedDurableConsumer(context.createTopic("myAddress"), "myQueue").receive();
            assertEquals("hello", ((TextMessage) message).getText());
         }

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
         Assert.assertEquals(true, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").isEnabled());

         Assert.assertEquals(true, getQueue(embeddedActiveMQ, "config_test_queue_change_queue_defaults").isPurgeOnNoConsumers());
         Assert.assertEquals(false, getQueue(embeddedActiveMQ, "config_test_queue_change_queue_defaults").isEnabled());

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
         Assert.assertEquals(false, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").isEnabled());

         Assert.assertEquals(false, getQueue(embeddedActiveMQ, "config_test_queue_change_queue_defaults").isPurgeOnNoConsumers());
         Assert.assertEquals(true, getQueue(embeddedActiveMQ, "config_test_queue_change_queue_defaults").isEnabled());

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
         Assert.assertEquals(false, getQueue(embeddedActiveMQ, "config_test_queue_change_queue").isEnabled());

         Assert.assertEquals(false, getQueue(embeddedActiveMQ, "config_test_queue_change_queue_defaults").isPurgeOnNoConsumers());
         Assert.assertEquals(true, getQueue(embeddedActiveMQ, "config_test_queue_change_queue_defaults").isEnabled());
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

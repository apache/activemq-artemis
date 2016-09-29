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

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.AddressSettingsInfo;
import org.apache.activemq.artemis.api.core.management.BridgeControl;
import org.apache.activemq.artemis.api.core.management.DivertControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.RoleInfo;
import org.apache.activemq.artemis.core.client.impl.ClientSessionImpl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.messagecounter.impl.MessageCounterManagerImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.settings.impl.SlowConsumerPolicy;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ActiveMQServerControlTest extends ManagementTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ActiveMQServer server;

   private Configuration conf;

   private TransportConfiguration connectorConfig;

   // Static --------------------------------------------------------

   private static boolean contains(final String name, final String[] strings) {
      boolean found = false;
      for (String str : strings) {
         IntegrationTestLogger.LOGGER.info("Does " + str + " match " + name);
         if (name.equals(str)) {
            found = true;
            break;
         }
      }
      return found;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public boolean usingCore() {
      return false;
   }

   @Test
   public void testGetAttributes() throws Exception {
      ActiveMQServerControl serverControl = createManagementControl();

      Assert.assertEquals(server.getVersion().getFullVersion(), serverControl.getVersion());

      Assert.assertEquals(conf.isClustered(), serverControl.isClustered());
      Assert.assertEquals(conf.isPersistDeliveryCountBeforeDelivery(), serverControl.isPersistDeliveryCountBeforeDelivery());
      Assert.assertEquals(conf.getScheduledThreadPoolMaxSize(), serverControl.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(conf.getThreadPoolMaxSize(), serverControl.getThreadPoolMaxSize());
      Assert.assertEquals(conf.getSecurityInvalidationInterval(), serverControl.getSecurityInvalidationInterval());
      Assert.assertEquals(conf.isSecurityEnabled(), serverControl.isSecurityEnabled());
      Assert.assertEquals(conf.isAsyncConnectionExecutionEnabled(), serverControl.isAsyncConnectionExecutionEnabled());
      Assert.assertEquals(conf.getIncomingInterceptorClassNames().size(), serverControl.getIncomingInterceptorClassNames().length);
      Assert.assertEquals(conf.getIncomingInterceptorClassNames().size(), serverControl.getIncomingInterceptorClassNames().length);
      Assert.assertEquals(conf.getOutgoingInterceptorClassNames().size(), serverControl.getOutgoingInterceptorClassNames().length);
      Assert.assertEquals(conf.getConnectionTTLOverride(), serverControl.getConnectionTTLOverride());
      //Assert.assertEquals(conf.getBackupConnectorName(), serverControl.getBackupConnectorName());
      Assert.assertEquals(conf.getManagementAddress().toString(), serverControl.getManagementAddress());
      Assert.assertEquals(conf.getManagementNotificationAddress().toString(), serverControl.getManagementNotificationAddress());
      Assert.assertEquals(conf.getIDCacheSize(), serverControl.getIDCacheSize());
      Assert.assertEquals(conf.isPersistIDCache(), serverControl.isPersistIDCache());
      Assert.assertEquals(conf.getBindingsDirectory(), serverControl.getBindingsDirectory());
      Assert.assertEquals(conf.getJournalDirectory(), serverControl.getJournalDirectory());
      Assert.assertEquals(conf.getJournalType().toString(), serverControl.getJournalType());
      Assert.assertEquals(conf.isJournalSyncTransactional(), serverControl.isJournalSyncTransactional());
      Assert.assertEquals(conf.isJournalSyncNonTransactional(), serverControl.isJournalSyncNonTransactional());
      Assert.assertEquals(conf.getJournalFileSize(), serverControl.getJournalFileSize());
      Assert.assertEquals(conf.getJournalMinFiles(), serverControl.getJournalMinFiles());
      if (LibaioContext.isLoaded()) {
         Assert.assertEquals(conf.getJournalMaxIO_AIO(), serverControl.getJournalMaxIO());
         Assert.assertEquals(conf.getJournalBufferSize_AIO(), serverControl.getJournalBufferSize());
         Assert.assertEquals(conf.getJournalBufferTimeout_AIO(), serverControl.getJournalBufferTimeout());
      }
      Assert.assertEquals(conf.isCreateBindingsDir(), serverControl.isCreateBindingsDir());
      Assert.assertEquals(conf.isCreateJournalDir(), serverControl.isCreateJournalDir());
      Assert.assertEquals(conf.getPagingDirectory(), serverControl.getPagingDirectory());
      Assert.assertEquals(conf.getLargeMessagesDirectory(), serverControl.getLargeMessagesDirectory());
      Assert.assertEquals(conf.isWildcardRoutingEnabled(), serverControl.isWildcardRoutingEnabled());
      Assert.assertEquals(conf.getTransactionTimeout(), serverControl.getTransactionTimeout());
      Assert.assertEquals(conf.isMessageCounterEnabled(), serverControl.isMessageCounterEnabled());
      Assert.assertEquals(conf.getTransactionTimeoutScanPeriod(), serverControl.getTransactionTimeoutScanPeriod());
      Assert.assertEquals(conf.getMessageExpiryScanPeriod(), serverControl.getMessageExpiryScanPeriod());
      Assert.assertEquals(conf.getMessageExpiryThreadPriority(), serverControl.getMessageExpiryThreadPriority());
      Assert.assertEquals(conf.getJournalCompactMinFiles(), serverControl.getJournalCompactMinFiles());
      Assert.assertEquals(conf.getJournalCompactPercentage(), serverControl.getJournalCompactPercentage());
      Assert.assertEquals(conf.isPersistenceEnabled(), serverControl.isPersistenceEnabled());
   }

   @Test
   public void testGetConnectors() throws Exception {
      ActiveMQServerControl serverControl = createManagementControl();

      Object[] connectorData = serverControl.getConnectors();
      Assert.assertNotNull(connectorData);
      Assert.assertEquals(1, connectorData.length);

      Object[] config = (Object[]) connectorData[0];

      Assert.assertEquals(connectorConfig.getName(), config[0]);
   }

   @Test
   public void testGetConnectorsAsJSON() throws Exception {
      ActiveMQServerControl serverControl = createManagementControl();

      String jsonString = serverControl.getConnectorsAsJSON();
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(1, array.size());
      JsonObject data = array.getJsonObject(0);
      Assert.assertEquals(connectorConfig.getName(), data.getString("name"));
      Assert.assertEquals(connectorConfig.getFactoryClassName(), data.getString("factoryClassName"));
      Assert.assertEquals(connectorConfig.getParams().size(), data.getJsonObject("params").size());
   }

   @Test
   public void testCreateAndDestroyQueue() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString());

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(true, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
   }

   @Test
   public void testCreateAndDestroyQueue_2() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      String filter = "color = 'green'";
      boolean durable = true;

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString(), filter, durable);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertEquals(filter, queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
   }

   @Test
   public void testCreateAndDestroyQueue_3() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      boolean durable = true;

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString(), durable);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
   }

   @Test
   public void testCreateAndDestroyQueueWithNullFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      String filter = null;
      boolean durable = true;

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString(), filter, durable);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
   }

   @Test
   public void testCreateAndDestroyQueueWithEmptyStringForFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      String filter = "";
      boolean durable = true;

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString(), filter, durable);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name));
   }

   @Test
   public void testGetQueueNames() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      ActiveMQServerControl serverControl = createManagementControl();

      // due to replication, there can be another queue created for replicating
      // management operations

      Assert.assertFalse(ActiveMQServerControlTest.contains(name.toString(), serverControl.getQueueNames()));

      serverControl.createQueue(address.toString(), name.toString());
      Assert.assertTrue(ActiveMQServerControlTest.contains(name.toString(), serverControl.getQueueNames()));

      serverControl.destroyQueue(name.toString());
      Assert.assertFalse(ActiveMQServerControlTest.contains(name.toString(), serverControl.getQueueNames()));
   }

   @Test
   public void testGetAddressNames() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      ActiveMQServerControl serverControl = createManagementControl();

      // due to replication, there can be another queue created for replicating
      // management operations

      Assert.assertFalse(ActiveMQServerControlTest.contains(address.toString(), serverControl.getAddressNames()));

      serverControl.createQueue(address.toString(), name.toString());
      Assert.assertTrue(ActiveMQServerControlTest.contains(address.toString(), serverControl.getAddressNames()));

      serverControl.destroyQueue(name.toString());
      Assert.assertFalse(ActiveMQServerControlTest.contains(address.toString(), serverControl.getAddressNames()));
   }

   @Test
   public void testMessageCounterMaxDayCount() throws Exception {
      ActiveMQServerControl serverControl = createManagementControl();

      Assert.assertEquals(MessageCounterManagerImpl.DEFAULT_MAX_DAY_COUNT, serverControl.getMessageCounterMaxDayCount());

      int newCount = 100;
      serverControl.setMessageCounterMaxDayCount(newCount);

      Assert.assertEquals(newCount, serverControl.getMessageCounterMaxDayCount());

      try {
         serverControl.setMessageCounterMaxDayCount(-1);
         Assert.fail();
      } catch (Exception e) {
      }

      try {
         serverControl.setMessageCounterMaxDayCount(0);
         Assert.fail();
      } catch (Exception e) {
      }

      Assert.assertEquals(newCount, serverControl.getMessageCounterMaxDayCount());
   }

   @Test
   public void testGetMessageCounterSamplePeriod() throws Exception {
      ActiveMQServerControl serverControl = createManagementControl();

      Assert.assertEquals(MessageCounterManagerImpl.DEFAULT_SAMPLE_PERIOD, serverControl.getMessageCounterSamplePeriod());

      long newSample = 20000;
      serverControl.setMessageCounterSamplePeriod(newSample);

      Assert.assertEquals(newSample, serverControl.getMessageCounterSamplePeriod());

      try {
         serverControl.setMessageCounterSamplePeriod(-1);
         Assert.fail();
      } catch (Exception e) {
      }

      try {
         serverControl.setMessageCounterSamplePeriod(0);
         Assert.fail();
      } catch (Exception e) {
      }

      try {
         serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD - 1);
         Assert.fail();
      } catch (Exception e) {
      }

      Assert.assertEquals(newSample, serverControl.getMessageCounterSamplePeriod());
   }

   protected void restartServer() throws Exception {
      server.stop();
      server.start();
   }

   @Test
   public void testSecuritySettings() throws Exception {
      ActiveMQServerControl serverControl = createManagementControl();
      String addressMatch = "test.#";
      String exactAddress = "test.whatever";

      assertEquals(1, serverControl.getRoles(addressMatch).length);
      serverControl.addSecuritySettings(addressMatch, "foo", "foo, bar", "foo", "bar", "foo, bar", "", "", "bar");

      // Restart the server. Those settings should be persisted

      restartServer();

      serverControl = createManagementControl();

      String rolesAsJSON = serverControl.getRolesAsJSON(exactAddress);
      RoleInfo[] roleInfos = RoleInfo.from(rolesAsJSON);
      assertEquals(2, roleInfos.length);
      RoleInfo fooRole = null;
      RoleInfo barRole = null;
      if (roleInfos[0].getName().equals("foo")) {
         fooRole = roleInfos[0];
         barRole = roleInfos[1];
      } else {
         fooRole = roleInfos[1];
         barRole = roleInfos[0];
      }
      assertTrue(fooRole.isSend());
      assertTrue(fooRole.isConsume());
      assertTrue(fooRole.isCreateDurableQueue());
      assertFalse(fooRole.isDeleteDurableQueue());
      assertTrue(fooRole.isCreateNonDurableQueue());
      assertFalse(fooRole.isDeleteNonDurableQueue());
      assertFalse(fooRole.isManage());
      assertFalse(fooRole.isBrowse());

      assertFalse(barRole.isSend());
      assertTrue(barRole.isConsume());
      assertFalse(barRole.isCreateDurableQueue());
      assertTrue(barRole.isDeleteDurableQueue());
      assertTrue(barRole.isCreateNonDurableQueue());
      assertFalse(barRole.isDeleteNonDurableQueue());
      assertFalse(barRole.isManage());
      assertTrue(barRole.isBrowse());

      serverControl.removeSecuritySettings(addressMatch);
      assertEquals(1, serverControl.getRoles(exactAddress).length);
   }

   @Test
   public void testAddressSettings() throws Exception {
      ActiveMQServerControl serverControl = createManagementControl();
      String addressMatch = "test.#";
      String exactAddress = "test.whatever";

      String DLA = "someDLA";
      String expiryAddress = "someExpiry";
      long expiryDelay = -1;
      boolean lastValueQueue = true;
      int deliveryAttempts = 1;
      long maxSizeBytes = 20;
      int pageSizeBytes = 10;
      int pageMaxCacheSize = 7;
      long redeliveryDelay = 4;
      double redeliveryMultiplier = 1;
      long maxRedeliveryDelay = 1000;
      long redistributionDelay = 5;
      boolean sendToDLAOnNoRoute = true;
      String addressFullMessagePolicy = "PAGE";
      long slowConsumerThreshold = 5;
      long slowConsumerCheckPeriod = 10;
      String slowConsumerPolicy = SlowConsumerPolicy.KILL.toString();
      boolean autoCreateJmsQueues = false;
      boolean autoDeleteJmsQueues = false;
      boolean autoCreateJmsTopics = false;
      boolean autoDeleteJmsTopics = false;

      serverControl.addAddressSettings(addressMatch, DLA, expiryAddress, expiryDelay, lastValueQueue, deliveryAttempts, maxSizeBytes, pageSizeBytes, pageMaxCacheSize, redeliveryDelay, redeliveryMultiplier, maxRedeliveryDelay, redistributionDelay, sendToDLAOnNoRoute, addressFullMessagePolicy, slowConsumerThreshold, slowConsumerCheckPeriod, slowConsumerPolicy, autoCreateJmsQueues, autoDeleteJmsQueues, autoCreateJmsTopics, autoDeleteJmsTopics);

      boolean ex = false;
      try {
         serverControl.addAddressSettings(addressMatch, DLA, expiryAddress, expiryDelay, lastValueQueue, deliveryAttempts, 100, 1000, pageMaxCacheSize, redeliveryDelay, redeliveryMultiplier, maxRedeliveryDelay, redistributionDelay, sendToDLAOnNoRoute, addressFullMessagePolicy, slowConsumerThreshold, slowConsumerCheckPeriod, slowConsumerPolicy, autoCreateJmsQueues, autoDeleteJmsQueues, autoCreateJmsTopics, autoDeleteJmsTopics);
      } catch (Exception expected) {
         ex = true;
      }

      assertTrue("Exception expected", ex);
      //restartServer();
      serverControl = createManagementControl();

      String jsonString = serverControl.getAddressSettingsAsJSON(exactAddress);
      AddressSettingsInfo info = AddressSettingsInfo.from(jsonString);

      assertEquals(DLA, info.getDeadLetterAddress());
      assertEquals(expiryAddress, info.getExpiryAddress());
      assertEquals(lastValueQueue, info.isLastValueQueue());
      assertEquals(deliveryAttempts, info.getMaxDeliveryAttempts());
      assertEquals(maxSizeBytes, info.getMaxSizeBytes());
      assertEquals(pageMaxCacheSize, info.getPageCacheMaxSize());
      assertEquals(pageSizeBytes, info.getPageSizeBytes());
      assertEquals(redeliveryDelay, info.getRedeliveryDelay());
      assertEquals(redeliveryMultiplier, info.getRedeliveryMultiplier(), 0.000001);
      assertEquals(maxRedeliveryDelay, info.getMaxRedeliveryDelay());
      assertEquals(redistributionDelay, info.getRedistributionDelay());
      assertEquals(sendToDLAOnNoRoute, info.isSendToDLAOnNoRoute());
      assertEquals(addressFullMessagePolicy, info.getAddressFullMessagePolicy());
      assertEquals(slowConsumerThreshold, info.getSlowConsumerThreshold());
      assertEquals(slowConsumerCheckPeriod, info.getSlowConsumerCheckPeriod());
      assertEquals(slowConsumerPolicy, info.getSlowConsumerPolicy());
      assertEquals(autoCreateJmsQueues, info.isAutoCreateJmsQueues());
      assertEquals(autoDeleteJmsQueues, info.isAutoDeleteJmsQueues());
      assertEquals(autoCreateJmsTopics, info.isAutoCreateJmsTopics());
      assertEquals(autoDeleteJmsTopics, info.isAutoDeleteJmsTopics());

      serverControl.addAddressSettings(addressMatch, DLA, expiryAddress, expiryDelay, lastValueQueue, deliveryAttempts, -1, 1000, pageMaxCacheSize, redeliveryDelay, redeliveryMultiplier, maxRedeliveryDelay, redistributionDelay, sendToDLAOnNoRoute, addressFullMessagePolicy, slowConsumerThreshold, slowConsumerCheckPeriod, slowConsumerPolicy, autoCreateJmsQueues, autoDeleteJmsQueues, autoCreateJmsTopics, autoDeleteJmsTopics);

      jsonString = serverControl.getAddressSettingsAsJSON(exactAddress);
      info = AddressSettingsInfo.from(jsonString);

      assertEquals(DLA, info.getDeadLetterAddress());
      assertEquals(expiryAddress, info.getExpiryAddress());
      assertEquals(lastValueQueue, info.isLastValueQueue());
      assertEquals(deliveryAttempts, info.getMaxDeliveryAttempts());
      assertEquals(-1, info.getMaxSizeBytes());
      assertEquals(pageMaxCacheSize, info.getPageCacheMaxSize());
      assertEquals(1000, info.getPageSizeBytes());
      assertEquals(redeliveryDelay, info.getRedeliveryDelay());
      assertEquals(redeliveryMultiplier, info.getRedeliveryMultiplier(), 0.000001);
      assertEquals(maxRedeliveryDelay, info.getMaxRedeliveryDelay());
      assertEquals(redistributionDelay, info.getRedistributionDelay());
      assertEquals(sendToDLAOnNoRoute, info.isSendToDLAOnNoRoute());
      assertEquals(addressFullMessagePolicy, info.getAddressFullMessagePolicy());
      assertEquals(slowConsumerThreshold, info.getSlowConsumerThreshold());
      assertEquals(slowConsumerCheckPeriod, info.getSlowConsumerCheckPeriod());
      assertEquals(slowConsumerPolicy, info.getSlowConsumerPolicy());
      assertEquals(autoCreateJmsQueues, info.isAutoCreateJmsQueues());
      assertEquals(autoDeleteJmsQueues, info.isAutoDeleteJmsQueues());
      assertEquals(autoCreateJmsTopics, info.isAutoCreateJmsTopics());
      assertEquals(autoDeleteJmsTopics, info.isAutoDeleteJmsTopics());

      ex = false;
      try {
         serverControl.addAddressSettings(addressMatch, DLA, expiryAddress, expiryDelay, lastValueQueue, deliveryAttempts, -2, 1000, pageMaxCacheSize, redeliveryDelay, redeliveryMultiplier, maxRedeliveryDelay, redistributionDelay, sendToDLAOnNoRoute, addressFullMessagePolicy, slowConsumerThreshold, slowConsumerCheckPeriod, slowConsumerPolicy, autoCreateJmsQueues, autoDeleteJmsQueues, autoCreateJmsTopics, autoDeleteJmsTopics);
      } catch (Exception e) {
         ex = true;
      }

      assertTrue("Supposed to have an exception called", ex);

   }

   @Test
   public void testNullRouteNameOnDivert() throws Exception {
      String address = RandomUtil.randomString();
      String name = RandomUtil.randomString();
      String forwardingAddress = RandomUtil.randomString();

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name));
      assertEquals(0, serverControl.getDivertNames().length);

      serverControl.createDivert(name.toString(), null, address, forwardingAddress, true, null, null);

      checkResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name));
   }

   @Test
   public void testCreateAndDestroyDivert() throws Exception {
      String address = RandomUtil.randomString();
      String name = RandomUtil.randomString();
      String routingName = RandomUtil.randomString();
      String forwardingAddress = RandomUtil.randomString();

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name));
      assertEquals(0, serverControl.getDivertNames().length);

      serverControl.createDivert(name.toString(), routingName, address, forwardingAddress, true, null, null);

      checkResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name));
      DivertControl divertControl = ManagementControlHelper.createDivertControl(name.toString(), mbeanServer);
      assertEquals(name.toString(), divertControl.getUniqueName());
      assertEquals(address, divertControl.getAddress());
      assertEquals(forwardingAddress, divertControl.getForwardingAddress());
      assertEquals(routingName, divertControl.getRoutingName());
      assertTrue(divertControl.isExclusive());
      assertNull(divertControl.getFilter());
      assertNull(divertControl.getTransformerClassName());
      String[] divertNames = serverControl.getDivertNames();
      assertEquals(1, divertNames.length);
      assertEquals(name, divertNames[0]);

      // check that a message sent to the address is diverted exclusively
      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession();

      String divertQueue = RandomUtil.randomString();
      String queue = RandomUtil.randomString();
      session.createQueue(forwardingAddress, divertQueue);
      session.createQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);
      String text = RandomUtil.randomString();
      message.putStringProperty("prop", text);
      producer.send(message);

      ClientConsumer consumer = session.createConsumer(queue);
      ClientConsumer divertedConsumer = session.createConsumer(divertQueue);

      session.start();

      assertNull(consumer.receiveImmediate());
      message = divertedConsumer.receive(5000);
      assertNotNull(message);
      assertEquals(text, message.getStringProperty("prop"));

      serverControl.destroyDivert(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name));
      assertEquals(0, serverControl.getDivertNames().length);

      // check that a message is no longer diverted
      message = session.createMessage(false);
      String text2 = RandomUtil.randomString();
      message.putStringProperty("prop", text2);
      producer.send(message);

      assertNull(divertedConsumer.receiveImmediate());
      message = consumer.receive(5000);
      assertNotNull(message);
      assertEquals(text2, message.getStringProperty("prop"));

      consumer.close();
      divertedConsumer.close();
      session.deleteQueue(queue);
      session.deleteQueue(divertQueue);
      session.close();

      locator.close();

   }

   @Test
   public void testCreateAndDestroyBridge() throws Exception {
      String name = RandomUtil.randomString();
      String sourceAddress = RandomUtil.randomString();
      String sourceQueue = RandomUtil.randomString();
      String targetAddress = RandomUtil.randomString();
      String targetQueue = RandomUtil.randomString();

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(name));
      assertEquals(0, serverControl.getBridgeNames().length);

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession();

      session.createQueue(sourceAddress, sourceQueue);
      session.createQueue(targetAddress, targetQueue);

      serverControl.createBridge(name, sourceQueue, targetAddress, null, // forwardingAddress
                                 null, // filterString
                                 ActiveMQClient.DEFAULT_RETRY_INTERVAL, ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER, ActiveMQClient.INITIAL_CONNECT_ATTEMPTS, ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS, false, // duplicateDetection
                                 1, // confirmationWindowSize
                                 -1, // producerWindowSize
                                 ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD, connectorConfig.getName(), // liveConnector
                                 false, false, null, null);

      checkResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(name));
      String[] bridgeNames = serverControl.getBridgeNames();
      assertEquals(1, bridgeNames.length);
      assertEquals(name, bridgeNames[0]);

      BridgeControl bridgeControl = ManagementControlHelper.createBridgeControl(name, mbeanServer);
      assertEquals(name, bridgeControl.getName());
      assertTrue(bridgeControl.isStarted());

      // check that a message sent to the sourceAddress is put in the tagetQueue
      ClientProducer producer = session.createProducer(sourceAddress);
      ClientMessage message = session.createMessage(false);
      String text = RandomUtil.randomString();
      message.putStringProperty("prop", text);
      producer.send(message);

      session.start();

      ClientConsumer targetConsumer = session.createConsumer(targetQueue);
      message = targetConsumer.receive(5000);
      assertNotNull(message);
      assertEquals(text, message.getStringProperty("prop"));

      ClientConsumer sourceConsumer = session.createConsumer(sourceQueue);
      assertNull(sourceConsumer.receiveImmediate());

      serverControl.destroyBridge(name);

      checkNoResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(name));
      assertEquals(0, serverControl.getBridgeNames().length);

      // check that a message is no longer diverted
      message = session.createMessage(false);
      String text2 = RandomUtil.randomString();
      message.putStringProperty("prop", text2);
      producer.send(message);

      assertNull(targetConsumer.receiveImmediate());
      message = sourceConsumer.receive(5000);
      assertNotNull(message);
      assertEquals(text2, message.getStringProperty("prop"));

      sourceConsumer.close();
      targetConsumer.close();

      session.deleteQueue(sourceQueue);
      session.deleteQueue(targetQueue);

      session.close();

      locator.close();
   }

   @Test
   public void testListPreparedTransactionDetails() throws Exception {
      SimpleString atestq = new SimpleString("BasicXaTestq");
      Xid xid = newXID();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession clientSession = csf.createSession(true, false, false);
      clientSession.createQueue(atestq, atestq, null, true);

      ClientMessage m1 = createTextMessage(clientSession, "");
      ClientMessage m2 = createTextMessage(clientSession, "");
      ClientMessage m3 = createTextMessage(clientSession, "");
      ClientMessage m4 = createTextMessage(clientSession, "");
      m1.putStringProperty("m1", "m1");
      m2.putStringProperty("m2", "m2");
      m3.putStringProperty("m3", "m3");
      m4.putStringProperty("m4", "m4");
      ClientProducer clientProducer = clientSession.createProducer(atestq);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m1);
      clientProducer.send(m2);
      clientProducer.send(m3);
      clientProducer.send(m4);
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);

      ActiveMQServerControl serverControl = createManagementControl();

      JsonArray jsonArray = JsonUtil.readJsonArray(serverControl.listProducersInfoAsJSON());

      assertEquals(1, jsonArray.size());
      assertEquals(4, ((JsonObject) jsonArray.get(0)).getInt("msgSent"));

      clientSession.close();
      locator.close();

      String txDetails = serverControl.listPreparedTransactionDetailsAsJSON();

      Assert.assertTrue(txDetails.matches(".*m1.*"));
      Assert.assertTrue(txDetails.matches(".*m2.*"));
      Assert.assertTrue(txDetails.matches(".*m3.*"));
      Assert.assertTrue(txDetails.matches(".*m4.*"));
   }

   @Test
   public void testListPreparedTransactionDetailsAsHTML() throws Exception {
      SimpleString atestq = new SimpleString("BasicXaTestq");
      Xid xid = newXID();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession clientSession = csf.createSession(true, false, false);
      clientSession.createQueue(atestq, atestq, null, true);

      ClientMessage m1 = createTextMessage(clientSession, "");
      ClientMessage m2 = createTextMessage(clientSession, "");
      ClientMessage m3 = createTextMessage(clientSession, "");
      ClientMessage m4 = createTextMessage(clientSession, "");
      m1.putStringProperty("m1", "m1");
      m2.putStringProperty("m2", "m2");
      m3.putStringProperty("m3", "m3");
      m4.putStringProperty("m4", "m4");
      ClientProducer clientProducer = clientSession.createProducer(atestq);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m1);
      clientProducer.send(m2);
      clientProducer.send(m3);
      clientProducer.send(m4);
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);

      clientSession.close();
      locator.close();

      ActiveMQServerControl serverControl = createManagementControl();
      String html = serverControl.listPreparedTransactionDetailsAsHTML();

      Assert.assertTrue(html.matches(".*m1.*"));
      Assert.assertTrue(html.matches(".*m2.*"));
      Assert.assertTrue(html.matches(".*m3.*"));
      Assert.assertTrue(html.matches(".*m4.*"));
   }

   @Test
   public void testCommitPreparedTransactions() throws Exception {
      SimpleString recQueue = new SimpleString("BasicXaTestqRec");
      SimpleString sendQueue = new SimpleString("BasicXaTestqSend");

      byte[] globalTransactionId = UUIDGenerator.getInstance().generateStringUUID().getBytes();
      Xid xid = new XidImpl("xa1".getBytes(), 1, globalTransactionId);
      Xid xid2 = new XidImpl("xa2".getBytes(), 1, globalTransactionId);
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession clientSession = csf.createSession(true, false, false);
      clientSession.createQueue(recQueue, recQueue, null, true);
      clientSession.createQueue(sendQueue, sendQueue, null, true);
      ClientMessage m1 = createTextMessage(clientSession, "");
      m1.putStringProperty("m1", "m1");
      ClientProducer clientProducer = clientSession.createProducer(recQueue);
      clientProducer.send(m1);
      locator.close();

      ServerLocator receiveLocator = createInVMNonHALocator();
      ClientSessionFactory receiveCsf = createSessionFactory(receiveLocator);
      ClientSession receiveClientSession = receiveCsf.createSession(true, false, false);
      ClientConsumer consumer = receiveClientSession.createConsumer(recQueue);

      ServerLocator sendLocator = createInVMNonHALocator();
      ClientSessionFactory sendCsf = createSessionFactory(sendLocator);
      ClientSession sendClientSession = sendCsf.createSession(true, false, false);
      ClientProducer producer = sendClientSession.createProducer(sendQueue);

      receiveClientSession.start(xid, XAResource.TMNOFLAGS);
      receiveClientSession.start();
      sendClientSession.start(xid2, XAResource.TMNOFLAGS);

      ClientMessage m = consumer.receive(5000);
      assertNotNull(m);

      producer.send(m);

      receiveClientSession.end(xid, XAResource.TMSUCCESS);
      sendClientSession.end(xid2, XAResource.TMSUCCESS);

      receiveClientSession.prepare(xid);
      sendClientSession.prepare(xid2);

      ActiveMQServerControl serverControl = createManagementControl();

      sendLocator.close();
      receiveLocator.close();

      boolean success = serverControl.commitPreparedTransaction(XidImpl.toBase64String(xid));

      success = serverControl.commitPreparedTransaction(XidImpl.toBase64String(xid));

      System.out.println("ActiveMQServerControlTest.testCommitPreparedTransactions");
   }

   @Test
   public void testScaleDownWithConnector() throws Exception {
      scaleDown(new ScaleDownHandler() {
         @Override
         public void scaleDown(ActiveMQServerControl control) throws Exception {
            control.scaleDown("server2-connector");
         }
      });
   }

   @Test
   public void testScaleDownWithOutConnector() throws Exception {
      scaleDown(new ScaleDownHandler() {
         @Override
         public void scaleDown(ActiveMQServerControl control) throws Exception {
            control.scaleDown(null);
         }
      });
   }

   @Test
   public void testForceFailover() throws Exception {
      ActiveMQServerControl serverControl = createManagementControl();
      try {
         serverControl.forceFailover();
      } catch (Exception e) {
         if (!usingCore()) {
            fail(e.getMessage());
         }
      }
      assertFalse(server.isStarted());
   }

   @Test
   public void testTotalMessageCount() throws Exception {
      String random1 = RandomUtil.randomString();
      String random2 = RandomUtil.randomString();

      ActiveMQServerControl serverControl = createManagementControl();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession();

      session.createQueue(random1, random1);
      session.createQueue(random2, random2);

      ClientProducer producer1 = session.createProducer(random1);
      ClientProducer producer2 = session.createProducer(random2);
      ClientMessage message = session.createMessage(true);
      producer1.send(message);
      producer2.send(message);

      session.commit();

      assertEquals(2, serverControl.getTotalMessageCount());

      session.deleteQueue(random1);
      session.deleteQueue(random2);

      session.close();

      locator.close();
   }

   @Test
   public void testTotalConnectionCount() throws Exception {
      final int CONNECTION_COUNT = 100;

      ActiveMQServerControl serverControl = createManagementControl();

      ServerLocator locator = createInVMNonHALocator();
      for (int i = 0; i < CONNECTION_COUNT; i++) {
         createSessionFactory(locator).close();
      }

      assertEquals(CONNECTION_COUNT + (usingCore() ? 1 : 0), serverControl.getTotalConnectionCount());
      assertEquals((usingCore() ? 1 : 0), serverControl.getConnectionCount());

      locator.close();
   }

   @Test
   public void testTotalMessagesAdded() throws Exception {
      String random1 = RandomUtil.randomString();
      String random2 = RandomUtil.randomString();

      ActiveMQServerControl serverControl = createManagementControl();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession();

      session.createQueue(random1, random1);
      session.createQueue(random2, random2);

      ClientProducer producer1 = session.createProducer(random1);
      ClientProducer producer2 = session.createProducer(random2);
      ClientMessage message = session.createMessage(false);
      producer1.send(message);
      producer2.send(message);

      session.commit();

      ClientConsumer consumer1 = session.createConsumer(random1);
      ClientConsumer consumer2 = session.createConsumer(random2);

      session.start();

      assertNotNull(consumer1.receive().acknowledge());
      assertNotNull(consumer2.receive().acknowledge());

      session.commit();

      assertEquals(2, serverControl.getTotalMessagesAdded());
      assertEquals(0, serverControl.getTotalMessageCount());

      consumer1.close();
      consumer2.close();

      session.deleteQueue(random1);
      session.deleteQueue(random2);

      session.close();

      locator.close();
   }

   @Test
   public void testTotalMessagesAcknowledged() throws Exception {
      String random1 = RandomUtil.randomString();
      String random2 = RandomUtil.randomString();

      ActiveMQServerControl serverControl = createManagementControl();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession();

      session.createQueue(random1, random1);
      session.createQueue(random2, random2);

      ClientProducer producer1 = session.createProducer(random1);
      ClientProducer producer2 = session.createProducer(random2);
      ClientMessage message = session.createMessage(false);
      producer1.send(message);
      producer2.send(message);

      session.commit();

      ClientConsumer consumer1 = session.createConsumer(random1);
      ClientConsumer consumer2 = session.createConsumer(random2);

      session.start();

      assertNotNull(consumer1.receive().acknowledge());
      assertNotNull(consumer2.receive().acknowledge());

      session.commit();

      assertEquals(2, serverControl.getTotalMessagesAcknowledged());
      assertEquals(0, serverControl.getTotalMessageCount());

      consumer1.close();
      consumer2.close();

      session.deleteQueue(random1);
      session.deleteQueue(random2);

      session.close();

      locator.close();
   }

   @Test
   public void testTotalConsumerCount() throws Exception {
      String random1 = RandomUtil.randomString();
      String random2 = RandomUtil.randomString();

      ActiveMQServerControl serverControl = createManagementControl();
      QueueControl queueControl1 = ManagementControlHelper.createQueueControl(SimpleString.toSimpleString(random1), SimpleString.toSimpleString(random1), mbeanServer);
      QueueControl queueControl2 = ManagementControlHelper.createQueueControl(SimpleString.toSimpleString(random2), SimpleString.toSimpleString(random2), mbeanServer);

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession();

      session.createQueue(random1, random1);
      session.createQueue(random2, random2);

      ClientConsumer consumer1 = session.createConsumer(random1);
      ClientConsumer consumer2 = session.createConsumer(random2);

      assertEquals(usingCore() ? 3 : 2, serverControl.getTotalConsumerCount());
      assertEquals(1, queueControl1.getConsumerCount());
      assertEquals(1, queueControl2.getConsumerCount());

      consumer1.close();
      consumer2.close();

      session.deleteQueue(random1);
      session.deleteQueue(random2);

      session.close();

      locator.close();
   }

   @Test
   public void testListConnectionsAsJSON() throws Exception {
      ActiveMQServerControl serverControl = createManagementControl();
      List<ClientSessionFactory> factories = new ArrayList<>();

      ServerLocator locator = createInVMNonHALocator();
      factories.add(createSessionFactory(locator));
      Thread.sleep(200);
      factories.add(createSessionFactory(locator));
      addClientSession(factories.get(1).createSession());

      String jsonString = serverControl.listConnectionsAsJSON();
      IntegrationTestLogger.LOGGER.info(jsonString);
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(usingCore() ? 3 : 2, array.size());

      String key = "creationTime";
      JsonObject[] sorted = new JsonObject[array.size()];
      for (int i = 0; i < array.size(); i++) {
         sorted[i] = array.getJsonObject(i);
      }

      if (sorted[0].getJsonNumber(key).longValue() > sorted[1].getJsonNumber(key).longValue()) {
         JsonObject o = sorted[1];
         sorted[1] = sorted[0];
         sorted[0] = o;
      }
      if (usingCore()) {
         if (sorted[1].getJsonNumber(key).longValue() > sorted[2].getJsonNumber(key).longValue()) {
            JsonObject o = sorted[2];
            sorted[2] = sorted[1];
            sorted[1] = o;
         }
         if (sorted[0].getJsonNumber(key).longValue() > sorted[1].getJsonNumber(key).longValue()) {
            JsonObject o = sorted[1];
            sorted[1] = sorted[0];
            sorted[0] = o;
         }
      }

      JsonObject first = sorted[0];
      JsonObject second = sorted[1];

      Assert.assertTrue(first.getString("connectionID").length() > 0);
      Assert.assertTrue(first.getString("clientAddress").length() > 0);
      Assert.assertTrue(first.getJsonNumber("creationTime").longValue() > 0);
      Assert.assertEquals(0, first.getJsonNumber("sessionCount").longValue());

      Assert.assertTrue(second.getString("connectionID").length() > 0);
      Assert.assertTrue(second.getString("clientAddress").length() > 0);
      Assert.assertTrue(second.getJsonNumber("creationTime").longValue() > 0);
      Assert.assertEquals(1, second.getJsonNumber("sessionCount").longValue());
   }

   @Test
   public void testListConsumersAsJSON() throws Exception {
      SimpleString queueName = new SimpleString(UUID.randomUUID().toString());
      final String filter = "x = 1";
      ActiveMQServerControl serverControl = createManagementControl();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session = addClientSession(factory.createSession());
      server.createQueue(queueName, queueName, null, false, false);
      addClientConsumer(session.createConsumer(queueName));
      addClientConsumer(session.createConsumer(queueName, SimpleString.toSimpleString(filter), true));

      String jsonString = serverControl.listConsumersAsJSON(factory.getConnection().getID().toString());
      IntegrationTestLogger.LOGGER.info(jsonString);
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(2, array.size());
      JsonObject first;
      JsonObject second;
      if (array.getJsonObject(0).getJsonNumber("creationTime").longValue() < array.getJsonObject(1).getJsonNumber("creationTime").longValue()) {
         first = array.getJsonObject(0);
         second = array.getJsonObject(1);
      } else {
         first = array.getJsonObject(1);
         second = array.getJsonObject(0);
      }

      Assert.assertNotNull(first.getJsonNumber("consumerID").longValue());
      Assert.assertTrue(first.getString("connectionID").length() > 0);
      Assert.assertEquals(factory.getConnection().getID().toString(), first.getString("connectionID"));
      Assert.assertTrue(first.getString("sessionID").length() > 0);
      Assert.assertEquals(((ClientSessionImpl) session).getName(), first.getString("sessionID"));
      Assert.assertTrue(first.getString("queueName").length() > 0);
      Assert.assertEquals(queueName.toString(), first.getString("queueName"));
      Assert.assertEquals(false, first.getBoolean("browseOnly"));
      Assert.assertTrue(first.getJsonNumber("creationTime").longValue() > 0);
      Assert.assertEquals(0, first.getJsonNumber("deliveringCount").longValue());

      Assert.assertNotNull(second.getJsonNumber("consumerID").longValue());
      Assert.assertTrue(second.getString("connectionID").length() > 0);
      Assert.assertEquals(factory.getConnection().getID().toString(), second.getString("connectionID"));
      Assert.assertTrue(second.getString("sessionID").length() > 0);
      Assert.assertEquals(((ClientSessionImpl) session).getName(), second.getString("sessionID"));
      Assert.assertTrue(second.getString("queueName").length() > 0);
      Assert.assertEquals(queueName.toString(), second.getString("queueName"));
      Assert.assertEquals(true, second.getBoolean("browseOnly"));
      Assert.assertTrue(second.getJsonNumber("creationTime").longValue() > 0);
      Assert.assertEquals(0, second.getJsonNumber("deliveringCount").longValue());
      Assert.assertTrue(second.getString("filter").length() > 0);
      Assert.assertEquals(filter, second.getString("filter"));
   }

   @Test
   public void testListAllConsumersAsJSON() throws Exception {
      SimpleString queueName = new SimpleString(UUID.randomUUID().toString());
      ActiveMQServerControl serverControl = createManagementControl();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session = addClientSession(factory.createSession());

      ServerLocator locator2 = createInVMNonHALocator();
      ClientSessionFactory factory2 = createSessionFactory(locator2);
      ClientSession session2 = addClientSession(factory2.createSession());

      server.createQueue(queueName, queueName, null, false, false);

      addClientConsumer(session.createConsumer(queueName));
      Thread.sleep(200);
      addClientConsumer(session2.createConsumer(queueName));

      String jsonString = serverControl.listAllConsumersAsJSON();
      IntegrationTestLogger.LOGGER.info(jsonString);
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(usingCore() ? 3 : 2, array.size());

      String key = "creationTime";
      JsonObject[] sorted = new JsonObject[array.size()];
      for (int i = 0; i < array.size(); i++) {
         sorted[i] = array.getJsonObject(i);
      }

      if (sorted[0].getJsonNumber(key).longValue() > sorted[1].getJsonNumber(key).longValue()) {
         JsonObject o = sorted[1];
         sorted[1] = sorted[0];
         sorted[0] = o;
      }
      if (usingCore()) {
         if (sorted[1].getJsonNumber(key).longValue() > sorted[2].getJsonNumber(key).longValue()) {
            JsonObject o = sorted[2];
            sorted[2] = sorted[1];
            sorted[1] = o;
         }
         if (sorted[0].getJsonNumber(key).longValue() > sorted[1].getJsonNumber(key).longValue()) {
            JsonObject o = sorted[1];
            sorted[1] = sorted[0];
            sorted[0] = o;
         }
      }

      JsonObject first = sorted[0];
      JsonObject second = sorted[1];

      Assert.assertTrue(first.getJsonNumber("creationTime").longValue() > 0);
      Assert.assertNotNull(first.getJsonNumber("consumerID").longValue());
      Assert.assertTrue(first.getString("connectionID").length() > 0);
      Assert.assertEquals(factory.getConnection().getID().toString(), first.getString("connectionID"));
      Assert.assertTrue(first.getString("sessionID").length() > 0);
      Assert.assertEquals(((ClientSessionImpl) session).getName(), first.getString("sessionID"));
      Assert.assertTrue(first.getString("queueName").length() > 0);
      Assert.assertEquals(queueName.toString(), first.getString("queueName"));
      Assert.assertEquals(false, first.getBoolean("browseOnly"));
      Assert.assertEquals(0, first.getJsonNumber("deliveringCount").longValue());

      Assert.assertTrue(second.getJsonNumber("creationTime").longValue() > 0);
      Assert.assertNotNull(second.getJsonNumber("consumerID").longValue());
      Assert.assertTrue(second.getString("connectionID").length() > 0);
      Assert.assertEquals(factory2.getConnection().getID().toString(), second.getString("connectionID"));
      Assert.assertTrue(second.getString("sessionID").length() > 0);
      Assert.assertEquals(((ClientSessionImpl) session2).getName(), second.getString("sessionID"));
      Assert.assertTrue(second.getString("queueName").length() > 0);
      Assert.assertEquals(queueName.toString(), second.getString("queueName"));
      Assert.assertEquals(false, second.getBoolean("browseOnly"));
      Assert.assertEquals(0, second.getJsonNumber("deliveringCount").longValue());
   }

   @Test
   public void testListSessionsAsJSON() throws Exception {
      SimpleString queueName = new SimpleString(UUID.randomUUID().toString());
      server.createQueue(queueName, queueName, null, false, false);
      ActiveMQServerControl serverControl = createManagementControl();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session1 = addClientSession(factory.createSession());
      Thread.sleep(5);
      ClientSession session2 = addClientSession(factory.createSession("myUser", "myPass", false, false, false, false, 0));
      session2.createConsumer(queueName);

      String jsonString = serverControl.listSessionsAsJSON(factory.getConnection().getID().toString());
      IntegrationTestLogger.LOGGER.info(jsonString);
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(2, array.size());
      JsonObject first;
      JsonObject second;
      if (array.getJsonObject(0).getJsonNumber("creationTime").longValue() < array.getJsonObject(1).getJsonNumber("creationTime").longValue()) {
         first = array.getJsonObject(0);
         second = array.getJsonObject(1);
      } else {
         first = array.getJsonObject(1);
         second = array.getJsonObject(0);
      }

      Assert.assertTrue(first.getString("sessionID").length() > 0);
      Assert.assertEquals(((ClientSessionImpl) session1).getName(), first.getString("sessionID"));
      Assert.assertTrue(first.getString("principal").length() > 0);
      Assert.assertEquals("guest", first.getString("principal"));
      Assert.assertTrue(first.getJsonNumber("creationTime").longValue() > 0);
      Assert.assertEquals(0, first.getJsonNumber("consumerCount").longValue());

      Assert.assertTrue(second.getString("sessionID").length() > 0);
      Assert.assertEquals(((ClientSessionImpl) session2).getName(), second.getString("sessionID"));
      Assert.assertTrue(second.getString("principal").length() > 0);
      Assert.assertEquals("myUser", second.getString("principal"));
      Assert.assertTrue(second.getJsonNumber("creationTime").longValue() > 0);
      Assert.assertEquals(1, second.getJsonNumber("consumerCount").longValue());
   }

   protected void scaleDown(ScaleDownHandler handler) throws Exception {
      SimpleString address = new SimpleString("testQueue");
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SERVER_ID_PROP_NAME, "2");
      Configuration config = createDefaultInVMConfig(2).clearAcceptorConfigurations().addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName(), params));
      ActiveMQServer server2 = addServer(ActiveMQServers.newActiveMQServer(config, null, true));

      this.conf.clearConnectorConfigurations().addConnectorConfiguration("server2-connector", new TransportConfiguration(INVM_CONNECTOR_FACTORY, params));

      server2.start();
      server.createQueue(address, address, null, true, false);
      server2.createQueue(address, address, null, true, false);
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession();
      ClientProducer producer = session.createProducer(address);
      for (int i = 0; i < 100; i++) {
         ClientMessage message = session.createMessage(true);
         message.getBodyBuffer().writeString("m" + i);
         producer.send(message);
      }

      ActiveMQServerControl managementControl = createManagementControl();
      handler.scaleDown(managementControl);
      locator.close();
      locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(INVM_CONNECTOR_FACTORY, params)));
      csf = createSessionFactory(locator);
      session = csf.createSession();
      session.start();
      ClientConsumer consumer = session.createConsumer(address);
      for (int i = 0; i < 100; i++) {
         ClientMessage m = consumer.receive(5000);
         assertNotNull(m);
      }
   }

   // Package protected ---------------------------------------------
   interface ScaleDownHandler {

      void scaleDown(ActiveMQServerControl control) throws Exception;
   }
   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      connectorConfig = new TransportConfiguration(INVM_CONNECTOR_FACTORY);

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
      role.add(new Role("guest", true, true, true, true, true, true, true, true));
      server.getSecurityRepository().addMatch("#", role);
   }

   protected ActiveMQServerControl createManagementControl() throws Exception {
      return ManagementControlHelper.createActiveMQServerControl(mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}


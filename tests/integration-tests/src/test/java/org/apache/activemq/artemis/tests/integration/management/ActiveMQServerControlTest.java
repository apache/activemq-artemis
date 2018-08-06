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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
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
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.AddressSettingsInfo;
import org.apache.activemq.artemis.api.core.management.BridgeControl;
import org.apache.activemq.artemis.api.core.management.DivertControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.RoleInfo;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq.artemis.core.client.impl.ClientSessionImpl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.messagecounter.impl.MessageCounterManagerImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.SlowConsumerPolicy;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.unit.core.config.impl.fakes.FakeConnectorServiceFactory;
import org.apache.activemq.artemis.tests.util.Wait;
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
   public void testIsReplicaSync() throws Exception {
      Assert.assertFalse(createManagementControl().isReplicaSync());
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

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      serverControl.createAddress(address.toString(), "ANYCAST");
      serverControl.createQueue(address.toString(), "ANYCAST", name.toString(), null, true, -1, false, false);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, RoutingType.ANYCAST, mbeanServer);
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(true, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
   }

   @Test
   public void testCreateQueueWithNullAddress() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = address;

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      serverControl.createQueue(null, name.toString(), "ANYCAST");

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(name, name, RoutingType.ANYCAST));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, RoutingType.ANYCAST, mbeanServer);
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(true, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
   }

   @Test
   public void testCreateAndDestroyQueue_2() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      String filter = "color = 'green'";
      boolean durable = true;

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      serverControl.createAddress(address.toString(), "ANYCAST");
      serverControl.createQueue(address.toString(), "ANYCAST", name.toString(), filter, durable, -1, false, false);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, RoutingType.ANYCAST, mbeanServer);
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertEquals(filter, queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
   }

   @Test
   public void testCreateAndDestroyQueue_3() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      boolean durable = true;

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      serverControl.createAddress(address.toString(), "ANYCAST");
      serverControl.createQueue(address.toString(), "ANYCAST", name.toString(), null, durable, -1, false, false);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, RoutingType.ANYCAST, mbeanServer);
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
   }

   @Test
   public void testCreateAndDestroyQueue_4() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      boolean durable = RandomUtil.randomBoolean();
      boolean purgeOnNoConsumers = RandomUtil.randomBoolean();
      boolean autoCreateAddress = true;
      int maxConsumers = RandomUtil.randomInt();

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      serverControl.createQueue(address.toString(), RoutingType.ANYCAST.toString(), name.toString(), null, durable, maxConsumers, purgeOnNoConsumers, autoCreateAddress);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, RoutingType.ANYCAST, mbeanServer);
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(purgeOnNoConsumers, queueControl.isPurgeOnNoConsumers());
      Assert.assertEquals(maxConsumers, queueControl.getMaxConsumers());
      Assert.assertEquals(false, queueControl.isTemporary());

      checkResource(ObjectNameBuilder.DEFAULT.getAddressObjectName(address));
      AddressControl addressControl = ManagementControlHelper.createAddressControl(address, mbeanServer);
      Assert.assertEquals(address.toString(), addressControl.getAddress());

      serverControl.destroyQueue(name.toString(), true, true);

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      checkNoResource(ObjectNameBuilder.DEFAULT.getAddressObjectName(address));
   }

   @Test
   public void testCreateAndDestroyQueueClosingConsumers() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      boolean durable = true;

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      serverControl.createAddress(address.toString(), "ANYCAST");
      serverControl.createQueue(address.toString(), "ANYCAST", name.toString(), null, durable, -1, false, false);

      ServerLocator receiveLocator = createInVMNonHALocator();
      ClientSessionFactory receiveCsf = createSessionFactory(receiveLocator);
      ClientSession receiveClientSession = receiveCsf.createSession(true, false, false);
      final ClientConsumer consumer = receiveClientSession.createConsumer(name);

      Assert.assertFalse(consumer.isClosed());

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      serverControl.destroyQueue(name.toString(), true);
      Wait.waitFor(new Wait.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return consumer.isClosed();
         }
      }, 1000, 100);
      Assert.assertTrue(consumer.isClosed());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
   }

   @Test
   public void testCreateAndDestroyQueueWithNullFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      String filter = null;
      boolean durable = true;

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      serverControl.createAddress(address.toString(), "ANYCAST");
      serverControl.createQueue(address.toString(), "ANYCAST", name.toString(), filter, durable, -1, false, false);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, RoutingType.ANYCAST, mbeanServer);
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
   }

   @Test
   public void testCreateAndDestroyQueueWithEmptyStringForFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      String filter = "";
      boolean durable = true;

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      serverControl.createAddress(address.toString(), "ANYCAST");
      serverControl.createQueue(address.toString(), "ANYCAST", name.toString(), filter, durable, -1, false, false);

      checkResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, RoutingType.ANYCAST, mbeanServer);
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(name.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
   }

   @Test
   public void testGetQueueNames() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      ActiveMQServerControl serverControl = createManagementControl();

      // due to replication, there can be another queue created for replicating
      // management operations

      Assert.assertFalse(ActiveMQServerControlTest.contains(name.toString(), serverControl.getQueueNames()));
      serverControl.createAddress(address.toString(), "ANYCAST");
      serverControl.createQueue(address.toString(), "ANYCAST", name.toString(), null, true, -1, false, false);
      Assert.assertTrue(ActiveMQServerControlTest.contains(name.toString(), serverControl.getQueueNames()));

      serverControl.destroyQueue(name.toString());
      Assert.assertFalse(ActiveMQServerControlTest.contains(name.toString(), serverControl.getQueueNames()));
   }

   @Test
   public void testGetQueueNamesWithRoutingType() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString anycastName = RandomUtil.randomSimpleString();
      SimpleString multicastName = RandomUtil.randomSimpleString();

      ActiveMQServerControl serverControl = createManagementControl();

      // due to replication, there can be another queue created for replicating
      // management operations

      Assert.assertFalse(ActiveMQServerControlTest.contains(anycastName.toString(), serverControl.getQueueNames()));
      Assert.assertFalse(ActiveMQServerControlTest.contains(multicastName.toString(), serverControl.getQueueNames()));

      serverControl.createQueue(address.toString(), RoutingType.ANYCAST.toString(), anycastName.toString(), null, true, -1, false, true);
      Assert.assertTrue(ActiveMQServerControlTest.contains(anycastName.toString(), serverControl.getQueueNames(RoutingType.ANYCAST.toString())));
      Assert.assertFalse(ActiveMQServerControlTest.contains(anycastName.toString(), serverControl.getQueueNames(RoutingType.MULTICAST.toString())));

      serverControl.createQueue(address.toString(), RoutingType.MULTICAST.toString(), multicastName.toString(), null, true, -1, false, true);
      Assert.assertTrue(ActiveMQServerControlTest.contains(multicastName.toString(), serverControl.getQueueNames(RoutingType.MULTICAST.toString())));
      Assert.assertFalse(ActiveMQServerControlTest.contains(multicastName.toString(), serverControl.getQueueNames(RoutingType.ANYCAST.toString())));

      serverControl.destroyQueue(anycastName.toString());
      serverControl.destroyQueue(multicastName.toString());
      Assert.assertFalse(ActiveMQServerControlTest.contains(anycastName.toString(), serverControl.getQueueNames()));
      Assert.assertFalse(ActiveMQServerControlTest.contains(multicastName.toString(), serverControl.getQueueNames()));
   }

   @Test
   public void testGetAddressNames() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      ActiveMQServerControl serverControl = createManagementControl();

      // due to replication, there can be another queue created for replicating
      // management operations

      Assert.assertFalse(ActiveMQServerControlTest.contains(address.toString(), serverControl.getAddressNames()));
      serverControl.createQueue(address.toString(), "ANYCAST", name.toString(), null, true, -1, false, true);
      Assert.assertTrue(ActiveMQServerControlTest.contains(address.toString(), serverControl.getAddressNames()));

      serverControl.destroyQueue(name.toString(), true, true);
      Assert.assertFalse(ActiveMQServerControlTest.contains(address.toString(), serverControl.getAddressNames()));
   }

   @Test
   public void testGetAddressDeletedFromJournal() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      ActiveMQServerControl serverControl = createManagementControl();

      // due to replication, there can be another queue created for replicating
      // management operations

      Assert.assertFalse(ActiveMQServerControlTest.contains(address.toString(), serverControl.getAddressNames()));
      serverControl.createAddress(address.toString(), "ANYCAST");
      Assert.assertTrue(ActiveMQServerControlTest.contains(address.toString(), serverControl.getAddressNames()));

      restartServer();

      serverControl.deleteAddress(address.toString());

      restartServer();

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

      //this only gets warning now and won't cause exception.
      serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD - 1);

      Assert.assertEquals(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD - 1, serverControl.getMessageCounterSamplePeriod());
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
      serverControl.addSecuritySettings(addressMatch, "foo", "foo, bar", "foo", "bar", "foo, bar", "", "", "bar", "foo", "foo");

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
      assertTrue(fooRole.isCreateAddress());

      assertFalse(barRole.isSend());
      assertTrue(barRole.isConsume());
      assertFalse(barRole.isCreateDurableQueue());
      assertTrue(barRole.isDeleteDurableQueue());
      assertTrue(barRole.isCreateNonDurableQueue());
      assertFalse(barRole.isDeleteNonDurableQueue());
      assertFalse(barRole.isManage());
      assertTrue(barRole.isBrowse());
      assertFalse(barRole.isCreateAddress());

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
      //      assertEquals(autoCreateJmsTopics, info.isAutoCreateAddresses());
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
      //      assertEquals(autoCreateJmsTopics, info.isAutoCreateAddresses());
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

      checkNoResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name, address));
      assertEquals(0, serverControl.getDivertNames().length);

      serverControl.createDivert(name.toString(), null, address, forwardingAddress, true, null, null);

      checkResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name, address));
   }

   @Test
   public void testCreateAndDestroyDivert() throws Exception {
      String address = RandomUtil.randomString();
      String name = RandomUtil.randomString();
      String routingName = RandomUtil.randomString();
      String forwardingAddress = RandomUtil.randomString();

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name, address));
      assertEquals(0, serverControl.getDivertNames().length);

      serverControl.createDivert(name.toString(), routingName, address, forwardingAddress, true, null, null);

      checkResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name, address));
      DivertControl divertControl = ManagementControlHelper.createDivertControl(name.toString(), address, mbeanServer);
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
      session.createQueue(forwardingAddress, RoutingType.ANYCAST, divertQueue);
      session.createQueue(address, RoutingType.ANYCAST, queue);

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

      checkNoResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(name, address));
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

      session.createQueue(sourceAddress, RoutingType.ANYCAST, sourceQueue);
      session.createQueue(targetAddress, RoutingType.ANYCAST, targetQueue);

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

      // add another TX, but don't prepare it
      ClientMessage m5 = createTextMessage(clientSession, "");
      ClientMessage m6 = createTextMessage(clientSession, "");
      ClientMessage m7 = createTextMessage(clientSession, "");
      ClientMessage m8 = createTextMessage(clientSession, "");
      m5.putStringProperty("m5", "m5");
      m6.putStringProperty("m6", "m6");
      m7.putStringProperty("m7", "m7");
      m8.putStringProperty("m8", "m8");
      Xid xid2 = newXID();
      clientSession.start(xid2, XAResource.TMNOFLAGS);
      clientProducer.send(m5);
      clientProducer.send(m6);
      clientProducer.send(m7);
      clientProducer.send(m8);
      clientSession.end(xid2, XAResource.TMSUCCESS);

      ActiveMQServerControl serverControl = createManagementControl();

      JsonArray jsonArray = JsonUtil.readJsonArray(serverControl.listProducersInfoAsJSON());

      assertEquals(1, jsonArray.size());
      assertEquals(8, ((JsonObject) jsonArray.get(0)).getInt("msgSent"));

      clientSession.close();
      locator.close();

      String txDetails = serverControl.listPreparedTransactionDetailsAsJSON();

      Assert.assertTrue(txDetails.matches(".*m1.*"));
      Assert.assertTrue(txDetails.matches(".*m2.*"));
      Assert.assertTrue(txDetails.matches(".*m3.*"));
      Assert.assertTrue(txDetails.matches(".*m4.*"));
      Assert.assertFalse(txDetails.matches(".*m5.*"));
      Assert.assertFalse(txDetails.matches(".*m6.*"));
      Assert.assertFalse(txDetails.matches(".*m7.*"));
      Assert.assertFalse(txDetails.matches(".*m8.*"));
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
      Wait.waitFor(() -> !server.isStarted());
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

      session.createQueue(random1, RoutingType.ANYCAST, random1);
      session.createQueue(random2, RoutingType.ANYCAST, random2);

      ClientProducer producer1 = session.createProducer(random1);
      ClientProducer producer2 = session.createProducer(random2);
      ClientMessage message = session.createMessage(true);
      producer1.send(message);
      producer2.send(message);

      session.commit();

      // flush executors on queues so we can get precise number of messages
      Queue queue1 = server.locateQueue(SimpleString.toSimpleString(random1));
      queue1.flushExecutor();
      Queue queue2 = server.locateQueue(SimpleString.toSimpleString(random1));
      queue2.flushExecutor();

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

      session.createQueue(random1, RoutingType.ANYCAST, random1);
      session.createQueue(random2, RoutingType.ANYCAST, random2);

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

      session.createQueue(random1, RoutingType.ANYCAST, random1);
      session.createQueue(random2, RoutingType.ANYCAST, random2);

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
      QueueControl queueControl1 = ManagementControlHelper.createQueueControl(SimpleString.toSimpleString(random1), SimpleString.toSimpleString(random1), RoutingType.ANYCAST, mbeanServer);
      QueueControl queueControl2 = ManagementControlHelper.createQueueControl(SimpleString.toSimpleString(random2), SimpleString.toSimpleString(random2), RoutingType.ANYCAST, mbeanServer);

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession();

      session.createQueue(random1, RoutingType.ANYCAST, random1);
      session.createQueue(random2, RoutingType.ANYCAST, random2);

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
      server.addAddressInfo(new AddressInfo(queueName, RoutingType.ANYCAST));
      server.createQueue(queueName, RoutingType.ANYCAST, queueName, null, false, false);
      addClientConsumer(session.createConsumer(queueName));
      Thread.sleep(100); // We check the timestamp for the creation time. We need to make sure it's different
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
      serverControl.createAddress(queueName.toString(), "ANYCAST");
      server.createQueue(queueName, RoutingType.ANYCAST, queueName, null, false, false);

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
      server.addAddressInfo(new AddressInfo(queueName, RoutingType.ANYCAST));
      server.createQueue(queueName, RoutingType.ANYCAST, queueName, null, false, false);
      ActiveMQServerControl serverControl = createManagementControl();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session1 = addClientSession(factory.createSession());
      ClientSession session2 = addClientSession(factory.createSession("myUser", "myPass", false, false, false, false, 0));
      session2.createConsumer(queueName);

      String jsonString = serverControl.listSessionsAsJSON(factory.getConnection().getID().toString());
      IntegrationTestLogger.LOGGER.info(jsonString);
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(2, array.size());
      JsonObject first = lookupSession(array, session1);
      JsonObject second = lookupSession(array, session2);

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

   private JsonObject lookupSession(JsonArray jsonArray, ClientSession session) throws Exception {
      String name = ((ClientSessionImpl)session).getName();

      for (int i = 0; i < jsonArray.size(); i++) {
         JsonObject obj = jsonArray.getJsonObject(i);
         String sessionID = obj.getString("sessionID");
         Assert.assertNotNull(sessionID);

         if (sessionID.equals(name)) {
            return obj;
         }
      }

      Assert.fail("Sesison not found for session id " + name);

      // not going to happen, fail will throw an exception but it won't compile without this
      return null;
   }

   @Test
   public void testListAllSessionsAsJSON() throws Exception {
      SimpleString queueName = new SimpleString(UUID.randomUUID().toString());
      server.addAddressInfo(new AddressInfo(queueName, RoutingType.ANYCAST));
      server.createQueue(queueName, RoutingType.ANYCAST, queueName, null, false, false);
      ActiveMQServerControl serverControl = createManagementControl();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = createSessionFactory(locator);
      ServerLocator locator2 = createInVMNonHALocator();
      ClientSessionFactory factory2 = createSessionFactory(locator2);
      ClientSession session1 = addClientSession(factory.createSession());
      Thread.sleep(5);
      ClientSession session2 = addClientSession(factory2.createSession("myUser", "myPass", false, false, false, false, 0));
      session2.addMetaData("foo", "bar");
      session2.addMetaData("bar", "baz");
      session2.createConsumer(queueName);

      String jsonString = serverControl.listAllSessionsAsJSON();
      IntegrationTestLogger.LOGGER.info(jsonString);
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(2 + (usingCore() ? 1 : 0), array.size());
      JsonObject first = lookupSession(array, session1);
      JsonObject second = lookupSession(array, session2);

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
      Assert.assertEquals(second.getJsonObject("metadata").getJsonString("foo").getString(), "bar");
      Assert.assertEquals(second.getJsonObject("metadata").getJsonString("bar").getString(), "baz");
   }

   @Test
   public void testListAllSessionsAsJSONWithJMS() throws Exception {
      SimpleString queueName = new SimpleString(UUID.randomUUID().toString());
      server.addAddressInfo(new AddressInfo(queueName, RoutingType.ANYCAST));
      server.createQueue(queueName, RoutingType.ANYCAST, queueName, null, false, false);
      ActiveMQServerControl serverControl = createManagementControl();

      ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactory("vm://0", "cf");
      Connection con = cf.createConnection();
      String clientID = UUID.randomUUID().toString();
      con.setClientID(clientID);

      String jsonString = serverControl.listAllSessionsAsJSON();
      IntegrationTestLogger.LOGGER.info(jsonString);
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(1 + (usingCore() ? 1 : 0), array.size());
      JsonObject obj = lookupSession(array, ((ActiveMQConnection)con).getInitialSession());
      Assert.assertEquals(obj.getJsonObject("metadata").getJsonString(ClientSession.JMS_SESSION_CLIENT_ID_PROPERTY).getString(), clientID);
      Assert.assertNotNull(obj.getJsonObject("metadata").getJsonString(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY));
   }

   @Test
   public void testListQueues() throws Exception {
      SimpleString queueName1 = new SimpleString("my_queue_one");
      SimpleString queueName2 = new SimpleString("my_queue_two");
      SimpleString queueName3 = new SimpleString("other_queue_three");

      ActiveMQServerControl serverControl = createManagementControl();

      server.addAddressInfo(new AddressInfo(queueName1, RoutingType.ANYCAST));
      server.createQueue(queueName1, RoutingType.ANYCAST, queueName1, null, false, false);

      server.addAddressInfo(new AddressInfo(queueName2, RoutingType.ANYCAST));
      server.createQueue(queueName2, RoutingType.ANYCAST, queueName2, null, false, false);

      server.addAddressInfo(new AddressInfo(queueName3, RoutingType.ANYCAST));
      server.createQueue(queueName3, RoutingType.ANYCAST, queueName3, null, false, false);

      //test with filter that matches 2 queues
      String filterString = createJsonFilter("name", "CONTAINS", "my_queue");

      String queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

      JsonObject queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
      JsonArray array = (JsonArray) queuesAsJsonObject.get("data");

      Assert.assertEquals("number of queues returned from query", 2, array.size());
      Assert.assertTrue(array.getJsonObject(0).getString("name").contains("my_queue"));
      Assert.assertTrue(array.getJsonObject(1).getString("name").contains("my_queue"));

      //test with an empty filter
      filterString = createJsonFilter("", "", "");

      queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

      queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
      array = (JsonArray) queuesAsJsonObject.get("data");

      // at least 3 queues or more
      Assert.assertTrue("number of queues returned from query", 3 <= array.size());

      //test with small page size
      queuesAsJsonString = serverControl.listQueues(filterString, 1, 1);

      queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
      array = (JsonArray) queuesAsJsonObject.get("data");

      Assert.assertEquals("number of queues returned from query", 1, array.size());
      //check all field names are available
      Assert.assertNotEquals("name", "", array.getJsonObject(0).getString("name"));
      Assert.assertNotEquals("id", "", array.getJsonObject(0).getString("id"));
      Assert.assertNotEquals("address", "", array.getJsonObject(0).getString("address"));
      Assert.assertEquals("filter", "", array.getJsonObject(0).getString("filter"));
      Assert.assertNotEquals("rate", "", array.getJsonObject(0).getString("rate"));
      Assert.assertEquals("durable", "false", array.getJsonObject(0).getString("durable"));
      Assert.assertEquals("paused", "false", array.getJsonObject(0).getString("paused"));
      Assert.assertNotEquals("temporary", "", array.getJsonObject(0).getString("temporary"));
      Assert.assertEquals("purgeOnNoConsumers", "false", array.getJsonObject(0).getString("purgeOnNoConsumers"));
      Assert.assertNotEquals("consumerCount", "", array.getJsonObject(0).getString("consumerCount"));
      Assert.assertEquals("maxConsumers", "-1", array.getJsonObject(0).getString("maxConsumers"));
      Assert.assertEquals("autoCreated", "false", array.getJsonObject(0).getString("autoCreated"));
      Assert.assertEquals("user", "", array.getJsonObject(0).getString("user"));
      Assert.assertNotEquals("routingType", "", array.getJsonObject(0).getString("routingType"));
      Assert.assertEquals("messagesAdded", "0", array.getJsonObject(0).getString("messagesAdded"));
      Assert.assertEquals("messageCount", "0", array.getJsonObject(0).getString("messageCount"));
      Assert.assertEquals("messagesAcked", "0", array.getJsonObject(0).getString("messagesAcked"));
      Assert.assertEquals("deliveringCount", "0", array.getJsonObject(0).getString("deliveringCount"));
      Assert.assertEquals("messagesKilled", "0", array.getJsonObject(0).getString("messagesKilled"));
      Assert.assertEquals("deliverDeliver", "true", array.getJsonObject(0).getString("deliverDeliver"));

   }

   @Test
   public void testListQueuesOrder() throws Exception {
      SimpleString queueName1 = new SimpleString("my_queue_1");
      SimpleString queueName2 = new SimpleString("my_queue_2");
      SimpleString queueName3 = new SimpleString("my_queue_3");

      ActiveMQServerControl serverControl = createManagementControl();

      server.addAddressInfo(new AddressInfo(queueName1, RoutingType.ANYCAST));
      server.createQueue(queueName1, RoutingType.ANYCAST, queueName1, new SimpleString("filter1"),null,true,
                         false, false,20,false,false);
      Thread.sleep(500);
      server.addAddressInfo(new AddressInfo(queueName2, RoutingType.ANYCAST));
      server.createQueue(queueName2, RoutingType.ANYCAST, queueName2, new SimpleString("filter3"), null,true,
                         false, true,40,false,false);
      Thread.sleep(500);
      server.addAddressInfo(new AddressInfo(queueName3, RoutingType.ANYCAST));
      server.createQueue(queueName3, RoutingType.ANYCAST, queueName3,  new SimpleString("filter0"),null,true,
                         false, false,10,false,false);

      //test default order
      String filterString = createJsonFilter("name", "CONTAINS", "my_queue");
      String queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);
      JsonObject queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
      JsonArray array = (JsonArray) queuesAsJsonObject.get("data");

      Assert.assertEquals("number of queues returned from query", 3, array.size());
      Assert.assertEquals("queue1 default Order", queueName1.toString(), array.getJsonObject(0).getString("name"));
      Assert.assertEquals("queue2 default Order", queueName2.toString(), array.getJsonObject(1).getString("name"));
      Assert.assertEquals("queue3 default Order", queueName3.toString(), array.getJsonObject(2).getString("name"));

      //test ordered by id desc
      filterString = createJsonFilter("name", "CONTAINS", "my_queue", "id", "desc");
      queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);
      queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
      array = (JsonArray) queuesAsJsonObject.get("data");

      Assert.assertEquals("number of queues returned from query", 3, array.size());
      Assert.assertEquals("queue3 ordered by id", queueName3.toString(), array.getJsonObject(0).getString("name"));
      Assert.assertEquals("queue2 ordered by id", queueName2.toString(), array.getJsonObject(1).getString("name"));
      Assert.assertEquals("queue1 ordered by id", queueName1.toString(), array.getJsonObject(2).getString("name"));

      //ordered by address desc
      filterString = createJsonFilter("name", "CONTAINS", "my_queue", "address", "desc");
      queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);
      queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
      array = (JsonArray) queuesAsJsonObject.get("data");

      Assert.assertEquals("number of queues returned from query", 3, array.size());
      Assert.assertEquals("queue3 ordered by address", queueName3.toString(), array.getJsonObject(0).getString("name"));
      Assert.assertEquals("queue2 ordered by address", queueName2.toString(), array.getJsonObject(1).getString("name"));
      Assert.assertEquals("queue1 ordered by address", queueName1.toString(), array.getJsonObject(2).getString("name"));

      //ordered by auto create desc
      filterString = createJsonFilter("name", "CONTAINS", "my_queue", "autoCreated", "asc");
      queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);
      queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
      array = (JsonArray) queuesAsJsonObject.get("data");

      Assert.assertEquals("number of queues returned from query", 3, array.size());
      Assert.assertEquals("pos1 ordered by autocreate", "false", array.getJsonObject(0).getString("autoCreated"));
      Assert.assertEquals("pos2 ordered by autocreate", "false", array.getJsonObject(1).getString("autoCreated"));
      Assert.assertEquals("pos3 ordered by autocreate", "true", array.getJsonObject(2).getString("autoCreated"));

      //ordered by filter desc
      filterString = createJsonFilter("name", "CONTAINS", "my_queue", "filter", "desc");
      queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);
      queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
      array = (JsonArray) queuesAsJsonObject.get("data");

      Assert.assertEquals("number of queues returned from query", 3, array.size());
      Assert.assertEquals("queue2 ordered by filter", queueName2.toString(), array.getJsonObject(0).getString("name"));
      Assert.assertEquals("queue1 ordered by filter", queueName1.toString(), array.getJsonObject(1).getString("name"));
      Assert.assertEquals("queue3 ordered by filter", queueName3.toString(), array.getJsonObject(2).getString("name"));

      //ordered by max consumers asc
      filterString = createJsonFilter("name", "CONTAINS", "my_queue", "maxConsumers", "asc");
      queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);
      queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
      array = (JsonArray) queuesAsJsonObject.get("data");

      Assert.assertEquals("number of queues returned from query", 3, array.size());
      Assert.assertEquals("queue3 ordered by filter", queueName3.toString(), array.getJsonObject(0).getString("name"));
      Assert.assertEquals("queue1 ordered by filter", queueName1.toString(), array.getJsonObject(1).getString("name"));
      Assert.assertEquals("queue2 ordered by filter", queueName2.toString(), array.getJsonObject(2).getString("name"));

      //ordering between the pages
      //page 1
      filterString = createJsonFilter("name", "CONTAINS", "my_queue", "address", "desc");
      queuesAsJsonString = serverControl.listQueues(filterString, 1, 1);
      queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
      array = (JsonArray) queuesAsJsonObject.get("data");
      Assert.assertEquals("number of queues returned from query", 1, array.size());
      Assert.assertEquals("queue3 ordered by page", queueName3.toString(), array.getJsonObject(0).getString("name"));

      //page 2
      filterString = createJsonFilter("name", "CONTAINS", "my_queue", "address", "desc");
      queuesAsJsonString = serverControl.listQueues(filterString, 2, 1);
      queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
      array = (JsonArray) queuesAsJsonObject.get("data");
      Assert.assertEquals("number of queues returned from query", 1, array.size());
      Assert.assertEquals("queue2 ordered by page", queueName2.toString(), array.getJsonObject(0).getString("name"));

      //page 3
      filterString = createJsonFilter("name", "CONTAINS", "my_queue", "address", "desc");
      queuesAsJsonString = serverControl.listQueues(filterString, 3, 1);
      queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
      array = (JsonArray) queuesAsJsonObject.get("data");
      Assert.assertEquals("number of queues returned from query", 1, array.size());
      Assert.assertEquals("queue1 ordered by page", queueName1.toString(), array.getJsonObject(0).getString("name"));

   }

   @Test
   public void testListQueuesNumericFilter() throws Exception {
      SimpleString queueName1 = new SimpleString("my_queue_one");
      SimpleString queueName2 = new SimpleString("my_queue_two");
      SimpleString queueName3 = new SimpleString("one_consumer_queue_three");
      SimpleString queueName4 = new SimpleString("my_queue_four");

      ActiveMQServerControl serverControl = createManagementControl();

      server.addAddressInfo(new AddressInfo(queueName1, RoutingType.ANYCAST));
      server.createQueue(queueName1, RoutingType.ANYCAST, queueName1, null, false, false);

      server.addAddressInfo(new AddressInfo(queueName2, RoutingType.ANYCAST));
      server.createQueue(queueName2, RoutingType.ANYCAST, queueName2, null, false, false);

      server.addAddressInfo(new AddressInfo(queueName3, RoutingType.ANYCAST));
      server.createQueue(queueName3, RoutingType.ANYCAST, queueName3, null, false, false, 10, false, false);

      server.addAddressInfo(new AddressInfo(queueName4, RoutingType.ANYCAST));
      server.createQueue(queueName4, RoutingType.ANYCAST, queueName4, null, false, false);

      // create some consumers
      try (ServerLocator locator = createInVMNonHALocator(); ClientSessionFactory csf = createSessionFactory(locator); ClientSession session = csf.createSession()) {

         session.start();
         ClientConsumer consumer1_q1 = session.createConsumer(queueName1);
         ClientConsumer consumer2_q1 = session.createConsumer(queueName1);
         ClientConsumer consumer1_q2 = session.createConsumer(queueName2);
         ClientConsumer consumer2_q2 = session.createConsumer(queueName2);
         ClientConsumer consumer3_q2 = session.createConsumer(queueName2);
         ClientConsumer consumer1_q3 = session.createConsumer(queueName3);

         ClientProducer clientProducer = session.createProducer(queueName1);
         ClientMessage message = session.createMessage(false);
         for (int i = 0; i < 10; i++) {
            clientProducer.send(message);
         }

         //consume one message
         ClientMessage messageReceived = consumer1_q1.receive(100);
         if (messageReceived == null) {
            fail("should have received a message");
         }
         messageReceived.acknowledge();
         session.commit();

         //test with CONTAINS returns nothing for numeric field
         String filterString = createJsonFilter("CONSUMER_COUNT", "CONTAINS", "0");
         String queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         JsonObject queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         JsonArray array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from query", 0, array.size());

         //test with LESS_THAN returns 1 queue
         filterString = createJsonFilter("CONSUMER_COUNT", "LESS_THAN", "1");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from LESS_THAN query", 1, array.size());
         Assert.assertEquals("correct queue returned from query", queueName4.toString(), array.getJsonObject(0).getString("name"));

         //test with GREATER_THAN returns 2 queue
         filterString = createJsonFilter("CONSUMER_COUNT", "GREATER_THAN", "2");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from GREATER_THAN query", 1, array.size());
         Assert.assertEquals("correct queue returned from query", queueName2.toString(), array.getJsonObject(0).getString("name"));

         //test with GREATER_THAN returns 2 queue
         filterString = createJsonFilter("CONSUMER_COUNT", "EQUALS", "3");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from EQUALS query", 1, array.size());
         Assert.assertEquals("correct queue returned from query", queueName2.toString(), array.getJsonObject(0).getString("name"));

         //test with MESSAGE_COUNT returns 2 queue
         filterString = createJsonFilter("MESSAGE_COUNT", "GREATER_THAN", "5");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from MESSAGE_COUNT query", 1, array.size());
         Assert.assertEquals("correct queue returned from query", queueName1.toString(), array.getJsonObject(0).getString("name"));

         //test with MESSAGE_ADDED returns 1 queue
         filterString = createJsonFilter("MESSAGES_ADDED", "GREATER_THAN", "5");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from MESSAGE_COUNT query", 1, array.size());
         Assert.assertEquals("correct queue returned from query", queueName1.toString(), array.getJsonObject(0).getString("name"));

         //test with DELIVERING_COUNT returns 1 queue
         filterString = createJsonFilter("DELIVERING_COUNT", "GREATER_THAN", "5");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from DELIVERING_COUNT query", 1, array.size());
         Assert.assertEquals("correct queue returned from query", queueName1.toString(), array.getJsonObject(0).getString("name"));

         //test with MESSAGE_ACKED returns 1 queue
         filterString = createJsonFilter("MESSAGES_ACKED", "GREATER_THAN", "0");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from MESSAGES_ACKED query", 1, array.size());
         Assert.assertEquals("correct queue returned from query", queueName1.toString(), array.getJsonObject(0).getString("name"));

         //test with MAX_CONSUMERS returns 1 queue
         filterString = createJsonFilter("MAX_CONSUMERS", "GREATER_THAN", "9");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from MAX_CONSUMERS query", 1, array.size());
         Assert.assertEquals("correct queue returned from query", queueName3.toString(), array.getJsonObject(0).getString("name"));

         //test with MESSAGES_KILLED returns 0 queue
         filterString = createJsonFilter("MESSAGES_KILLED", "GREATER_THAN", "0");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from MESSAGES_KILLED query", 0, array.size());

      }

   }

   @Test
   public void testListQueuesNumericFilterInvalid() throws Exception {
      SimpleString queueName1 = new SimpleString("my_queue_one");
      SimpleString queueName2 = new SimpleString("one_consumer_queue_two");
      SimpleString queueName3 = new SimpleString("one_consumer_queue_three");
      SimpleString queueName4 = new SimpleString("my_queue_four");

      ActiveMQServerControl serverControl = createManagementControl();

      server.addAddressInfo(new AddressInfo(queueName1, RoutingType.ANYCAST));
      server.createQueue(queueName1, RoutingType.ANYCAST, queueName1, null, false, false);

      server.addAddressInfo(new AddressInfo(queueName2, RoutingType.ANYCAST));
      server.createQueue(queueName2, RoutingType.ANYCAST, queueName2, null, false, false);

      server.addAddressInfo(new AddressInfo(queueName3, RoutingType.ANYCAST));
      server.createQueue(queueName3, RoutingType.ANYCAST, queueName3, null, false, false, 10, false, false);

      server.addAddressInfo(new AddressInfo(queueName4, RoutingType.ANYCAST));
      server.createQueue(queueName4, RoutingType.ANYCAST, queueName4, null, false, false);

      // create some consumers
      try (ServerLocator locator = createInVMNonHALocator(); ClientSessionFactory csf = createSessionFactory(locator); ClientSession session = csf.createSession()) {

         session.start();
         ClientConsumer consumer1_q1 = session.createConsumer(queueName1);
         ClientConsumer consumer2_q1 = session.createConsumer(queueName1);

         //test with CONTAINS returns nothing for numeric field
         String filterString = createJsonFilter("CONSUMER_COUNT", "CONTAINS", "NOT_NUMBER");
         String queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         JsonObject queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         JsonArray array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from query", 0, array.size());

         //test with LESS_THAN and not a number
         filterString = createJsonFilter("CONSUMER_COUNT", "LESS_THAN", "NOT_NUMBER");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from LESS_THAN query", 0, array.size());

         //test with GREATER_THAN and not a number
         filterString = createJsonFilter("CONSUMER_COUNT", "GREATER_THAN", "NOT_NUMBER");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from GREATER_THAN query", 0, array.size());

         //test with EQUALS and not number
         filterString = createJsonFilter("CONSUMER_COUNT", "EQUALS", "NOT_NUMBER");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from EQUALS query", 0, array.size());

         //test with LESS_THAN on string value returns no queue
         filterString = createJsonFilter("name", "LESS_THAN", "my_queue");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from LESS_THAN on non numeric field", 0, array.size());

         //test with GREATER_THAN on string value returns no queue
         filterString = createJsonFilter("name", "GREATER_THAN", "my_queue");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from GREATER_THAN on non numeric field", 0, array.size());

         //test with GREATER_THAN and empty string
         filterString = createJsonFilter("CONSUMER_COUNT", "GREATER_THAN", " ");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from GREATER_THAN query", 0, array.size());

         //test with CONSUMER_COUNT against a float value
         filterString = createJsonFilter("CONSUMER_COUNT", "GREATER_THAN", "0.12");
         queuesAsJsonString = serverControl.listQueues(filterString, 1, 50);

         queuesAsJsonObject = JsonUtil.readJsonObject(queuesAsJsonString);
         array = (JsonArray) queuesAsJsonObject.get("data");
         Assert.assertEquals("number of queues returned from GREATER_THAN query", 0, array.size());

      }

   }

   @Test
   public void testListAddresses() throws Exception {
      SimpleString queueName1 = new SimpleString("my_queue_one");
      SimpleString queueName2 = new SimpleString("my_queue_two");
      SimpleString queueName3 = new SimpleString("other_queue_three");
      SimpleString queueName4 = new SimpleString("other_queue_four");

      SimpleString addressName1 = new SimpleString("my_address_one");
      SimpleString addressName2 = new SimpleString("my_address_two");
      SimpleString addressName3 = new SimpleString("other_address_three");

      ActiveMQServerControl serverControl = createManagementControl();

      server.addAddressInfo(new AddressInfo(addressName1, RoutingType.ANYCAST));
      server.createQueue(addressName1, RoutingType.ANYCAST, queueName1, null, false, false);
      server.addAddressInfo(new AddressInfo(addressName2, RoutingType.ANYCAST));
      server.createQueue(addressName2, RoutingType.ANYCAST, queueName2, null, false, false);
      server.addAddressInfo(new AddressInfo(addressName3, RoutingType.ANYCAST));
      server.createQueue(addressName3, RoutingType.ANYCAST, queueName3, null, false, false);
      server.createQueue(addressName3, RoutingType.ANYCAST, queueName4, null, false, false);

      //test with CONTAINS filter
      String filterString = createJsonFilter("name", "CONTAINS", "my_address");
      String addressesAsJsonString = serverControl.listAddresses(filterString, 1, 50);
      JsonObject addressesAsJsonObject = JsonUtil.readJsonObject(addressesAsJsonString);
      JsonArray array = (JsonArray) addressesAsJsonObject.get("data");

      Assert.assertEquals("number of addresses returned from query", 2, array.size());
      Assert.assertTrue("address name check", array.getJsonObject(0).getString("name").contains("my_address"));
      Assert.assertTrue("address name check", array.getJsonObject(1).getString("name").contains("my_address"));

      //test with EQUALS filter
      filterString = createJsonFilter("name", "EQUALS", addressName1.toString());
      addressesAsJsonString = serverControl.listAddresses(filterString, 1, 50);
      addressesAsJsonObject = JsonUtil.readJsonObject(addressesAsJsonString);
      array = (JsonArray) addressesAsJsonObject.get("data");

      Assert.assertEquals("number of addresses returned from query", 1, array.size());
      //check all field names
      Assert.assertEquals("address name check", addressName1.toString(), array.getJsonObject(0).getString("name"));
      Assert.assertNotEquals("id", "", array.getJsonObject(0).getString("id"));
      Assert.assertTrue("routingTypes", array.getJsonObject(0).getString("routingTypes").contains(RoutingType.ANYCAST.name()));
      Assert.assertEquals("queueCount", "1", array.getJsonObject(0).getString("queueCount"));

      //test with empty filter - all addresses should be returned
      filterString = createJsonFilter("", "", "");
      addressesAsJsonString = serverControl.listAddresses(filterString, 1, 50);
      addressesAsJsonObject = JsonUtil.readJsonObject(addressesAsJsonString);
      array = (JsonArray) addressesAsJsonObject.get("data");

      Assert.assertTrue("number of addresses returned from query", 3 <= array.size());

      //test with small page size
      addressesAsJsonString = serverControl.listAddresses(filterString, 1, 1);
      addressesAsJsonObject = JsonUtil.readJsonObject(addressesAsJsonString);
      array = (JsonArray) addressesAsJsonObject.get("data");

      Assert.assertEquals("number of queues returned from query", 1, array.size());

      //test with QUEUE_COUNT with GREATER_THAN filter
      filterString = createJsonFilter("QUEUE_COUNT", "GREATER_THAN", "1");
      addressesAsJsonString = serverControl.listAddresses(filterString, 1, 50);
      addressesAsJsonObject = JsonUtil.readJsonObject(addressesAsJsonString);
      array = (JsonArray) addressesAsJsonObject.get("data");

      Assert.assertEquals("number of addresses returned from query", 1, array.size());
      Assert.assertEquals("address name check", addressName3.toString(), array.getJsonObject(0).getString("name"));

      //test with QUEUE_COUNT with LESS_THAN filter
      filterString = createJsonFilter("QUEUE_COUNT", "LESS_THAN", "0");
      addressesAsJsonString = serverControl.listAddresses(filterString, 1, 50);
      addressesAsJsonObject = JsonUtil.readJsonObject(addressesAsJsonString);
      array = (JsonArray) addressesAsJsonObject.get("data");

      Assert.assertEquals("number of addresses returned from query", 0, array.size());

   }

   @Test
   public void testListAddressOrder() throws Exception {

      SimpleString queueName1 = new SimpleString("my_queue_one");
      SimpleString queueName2 = new SimpleString("my_queue_two");
      SimpleString queueName3 = new SimpleString("other_queue_three");
      SimpleString queueName4 = new SimpleString("other_queue_four");

      SimpleString addressName1 = new SimpleString("my_address_1");
      SimpleString addressName2 = new SimpleString("my_address_2");
      SimpleString addressName3 = new SimpleString("my_address_3");

      ActiveMQServerControl serverControl = createManagementControl();

      server.addAddressInfo(new AddressInfo(addressName1, RoutingType.ANYCAST));
      server.createQueue(addressName1, RoutingType.ANYCAST, queueName1, null, false, false);
      server.addAddressInfo(new AddressInfo(addressName2, RoutingType.ANYCAST));
      server.addAddressInfo(new AddressInfo(addressName3, RoutingType.ANYCAST));
      server.createQueue(addressName3, RoutingType.ANYCAST, queueName3, null, false, false);
      server.createQueue(addressName3, RoutingType.ANYCAST, queueName4, null, false, false);

      //test default order
      String filterString = createJsonFilter("name", "CONTAINS", "my");
      String addressesAsJsonString = serverControl.listAddresses(filterString, 1, 50);
      JsonObject addressesAsJsonObject = JsonUtil.readJsonObject(addressesAsJsonString);
      JsonArray array = (JsonArray) addressesAsJsonObject.get("data");

      Assert.assertEquals("number addresses returned", 3, array.size());
      Assert.assertEquals("address1 default order", addressName1.toString(), array.getJsonObject(0).getString("name"));
      Assert.assertEquals("address2 default order", addressName2.toString(), array.getJsonObject(1).getString("name"));
      Assert.assertEquals("address3 default order", addressName3.toString(), array.getJsonObject(2).getString("name"));

      //test  ordered by name desc
      filterString = createJsonFilter("name", "CONTAINS", "my", "name", "desc");
      addressesAsJsonString = serverControl.listAddresses(filterString, 1, 50);
      addressesAsJsonObject = JsonUtil.readJsonObject(addressesAsJsonString);
      array = (JsonArray) addressesAsJsonObject.get("data");

      Assert.assertEquals("number addresses returned", 3, array.size());
      Assert.assertEquals("address3 ordered by name", addressName3.toString(), array.getJsonObject(0).getString("name"));
      Assert.assertEquals("address2 ordered by name", addressName2.toString(), array.getJsonObject(1).getString("name"));
      Assert.assertEquals("address1 ordered by name", addressName1.toString(), array.getJsonObject(2).getString("name"));

      //test  ordered by queue count asc
      filterString = createJsonFilter("name", "CONTAINS", "my", "queueCount", "asc");
      addressesAsJsonString = serverControl.listAddresses(filterString, 1, 50);
      addressesAsJsonObject = JsonUtil.readJsonObject(addressesAsJsonString);
      array = (JsonArray) addressesAsJsonObject.get("data");

      Assert.assertEquals("number addresses returned", 3, array.size());
      Assert.assertEquals("address2 ordered by queue count", addressName2.toString(), array.getJsonObject(0).getString("name"));
      Assert.assertEquals("address1 ordered by queue count", addressName1.toString(), array.getJsonObject(1).getString("name"));
      Assert.assertEquals("address3 ordered by queue count", addressName3.toString(), array.getJsonObject(2).getString("name"));
   }

   @Test
   public void testListConsumers() throws Exception {
      SimpleString queueName1 = new SimpleString("my_queue_one");
      SimpleString queueName2 = new SimpleString("my_queue_two");
      SimpleString queueName3 = new SimpleString("other_queue_three");
      SimpleString addressName1 = new SimpleString("my_address_one");
      SimpleString addressName2 = new SimpleString("my_address_two");

      ActiveMQServerControl serverControl = createManagementControl();

      server.addAddressInfo(new AddressInfo(addressName1, RoutingType.ANYCAST));
      server.createQueue(addressName1, RoutingType.ANYCAST, queueName1, null, false, false);

      server.addAddressInfo(new AddressInfo(addressName2, RoutingType.ANYCAST));
      server.createQueue(addressName2, RoutingType.ANYCAST, queueName2, null, false, false);
      server.createQueue(addressName2, RoutingType.ANYCAST, queueName3, null, false, false);

      // create some consumers
      try (ServerLocator locator = createInVMNonHALocator(); ClientSessionFactory csf = createSessionFactory(locator); ClientSession session = csf.createSession()) {

         ClientConsumer consumer1_q1 = session.createConsumer(queueName1);
         ClientConsumer consumer2_q1 = session.createConsumer(queueName1);
         ClientConsumer consumer1_q2 = session.createConsumer(queueName2);
         ClientConsumer consumer1_q3 = session.createConsumer(queueName3);

         //test with filter  EQUALS
         String filterString = createJsonFilter("queue", "EQUALS", queueName1.toString());
         String consumersAsJsonString = serverControl.listConsumers(filterString, 1, 50);
         JsonObject consumersAsJsonObject = JsonUtil.readJsonObject(consumersAsJsonString);
         JsonArray array = (JsonArray) consumersAsJsonObject.get("data");

         Assert.assertEquals("number of consumers returned from query", 2, array.size());
         Assert.assertEquals("check consumer's queue", queueName1.toString(), array.getJsonObject(0).getString("queue"));
         Assert.assertEquals("check consumer's queue", queueName1.toString(), array.getJsonObject(0).getString("queue"));

         // test with a CONTAINS operation
         filterString = createJsonFilter("queue", "CONTAINS", "my_queue");
         consumersAsJsonString = serverControl.listConsumers(filterString, 1, 50);
         consumersAsJsonObject = JsonUtil.readJsonObject(consumersAsJsonString);
         array = (JsonArray) consumersAsJsonObject.get("data");

         Assert.assertEquals("number of consumers returned from query", 3, array.size());

         // filter by address
         filterString = createJsonFilter("address", "EQUALS", addressName1.toString());
         consumersAsJsonString = serverControl.listConsumers(filterString, 1, 50);
         consumersAsJsonObject = JsonUtil.readJsonObject(consumersAsJsonString);
         array = (JsonArray) consumersAsJsonObject.get("data");

         Assert.assertEquals("number of consumers returned from query", 2, array.size());
         Assert.assertEquals("check consumers address", addressName1.toString(), array.getJsonObject(0).getString("address"));
         Assert.assertEquals("check consumers address", addressName1.toString(), array.getJsonObject(1).getString("address"));

         //test with empty filter - all consumers should be returned
         filterString = createJsonFilter("", "", "");
         consumersAsJsonString = serverControl.listConsumers(filterString, 1, 50);
         consumersAsJsonObject = JsonUtil.readJsonObject(consumersAsJsonString);
         array = (JsonArray) consumersAsJsonObject.get("data");

         Assert.assertTrue("at least 4 consumers returned from query", 4 <= array.size());

         //test with small page size
         consumersAsJsonString = serverControl.listConsumers(filterString, 1, 1);
         consumersAsJsonObject = JsonUtil.readJsonObject(consumersAsJsonString);
         array = (JsonArray) consumersAsJsonObject.get("data");

         Assert.assertEquals("number of consumers returned from query", 1, array.size());

         //test contents of returned consumer
         filterString = createJsonFilter("queue", "EQUALS", queueName3.toString());
         consumersAsJsonString = serverControl.listConsumers(filterString, 1, 50);
         consumersAsJsonObject = JsonUtil.readJsonObject(consumersAsJsonString);
         array = (JsonArray) consumersAsJsonObject.get("data");

         Assert.assertEquals("number of consumers returned from query", 1, array.size());
         JsonObject jsonConsumer = array.getJsonObject(0);
         Assert.assertEquals("queue name in consumer", queueName3.toString(), jsonConsumer.getString("queue"));
         Assert.assertEquals("address name in consumer", addressName2.toString(), jsonConsumer.getString("address"));
         Assert.assertEquals("consumer protocol ", "CORE", jsonConsumer.getString("protocol"));
         Assert.assertEquals("queue type", "anycast", jsonConsumer.getString("queueType"));
         Assert.assertNotEquals("id", "", jsonConsumer.getString("id"));
         Assert.assertNotEquals("session", "", jsonConsumer.getString("session"));
         Assert.assertEquals("clientID", "", jsonConsumer.getString("clientID"));
         Assert.assertEquals("user", "", jsonConsumer.getString("user"));
         Assert.assertNotEquals("localAddress", "", jsonConsumer.getString("localAddress"));
         Assert.assertNotEquals("remoteAddress", "", jsonConsumer.getString("remoteAddress"));
         Assert.assertNotEquals("creationTime", "", jsonConsumer.getString("creationTime"));
      }

   }

   @Test
   public void testListConsumersOrder() throws Exception {
      SimpleString queueName1 = new SimpleString("my_queue_one");
      SimpleString addressName1 = new SimpleString("my_address_one");

      ActiveMQServerControl serverControl = createManagementControl();

      server.addAddressInfo(new AddressInfo(addressName1, RoutingType.ANYCAST));
      server.createQueue(addressName1, RoutingType.ANYCAST, queueName1, null, false, false);

      try (ServerLocator locator = createInVMNonHALocator(); ClientSessionFactory csf = createSessionFactory(locator);) {

         ClientSessionImpl session1 = (ClientSessionImpl) csf.createSession();
         ClientSessionImpl session2 = (ClientSessionImpl) csf.createSession();
         ClientSessionImpl session3 = (ClientSessionImpl) csf.createSession();

         //sleep - test compares creationTimes
         ClientConsumer consumer_s1 = session1.createConsumer(queueName1);
         Thread.sleep(500);
         ClientConsumer consumer_s2 = session2.createConsumer(queueName1);
         Thread.sleep(500);
         ClientConsumer consumer_s3 = session3.createConsumer(queueName1);

         //test default Order
         String filterString = createJsonFilter("queue", "EQUALS", queueName1.toString());
         String consumersAsJsonString = serverControl.listConsumers(filterString, 1, 50);
         JsonObject consumersAsJsonObject = JsonUtil.readJsonObject(consumersAsJsonString);
         JsonArray array = (JsonArray) consumersAsJsonObject.get("data");
         Assert.assertEquals("number of consumers returned from query", 3, array.size());

         Assert.assertEquals("Consumer1 default order", session1.getName(), array.getJsonObject(0).getString("session"));
         Assert.assertEquals("Consumer2 default order", session2.getName(), array.getJsonObject(1).getString("session"));
         Assert.assertEquals("Consumer3 default order", session3.getName(), array.getJsonObject(2).getString("session"));

         //test ordered by creationTime
         filterString = createJsonFilter("queue", "EQUALS", queueName1.toString(), "creationTime", "desc");
         consumersAsJsonString = serverControl.listConsumers(filterString, 1, 50);
         consumersAsJsonObject = JsonUtil.readJsonObject(consumersAsJsonString);
         array = (JsonArray) consumersAsJsonObject.get("data");
         Assert.assertEquals("number of consumers returned from query", 3, array.size());

         Assert.assertEquals("Consumer3 creation time", session3.getName(), array.getJsonObject(0).getString("session"));
         Assert.assertEquals("Consumer2 creation time", session2.getName(), array.getJsonObject(1).getString("session"));
         Assert.assertEquals("Consumer1 creation time", session1.getName(), array.getJsonObject(2).getString("session"));

      }
   }

   @Test
   public void testListSessions() throws Exception {
      SimpleString queueName1 = new SimpleString("my_queue_one");
      SimpleString addressName1 = new SimpleString("my_address_one");

      ActiveMQServerControl serverControl = createManagementControl();

      server.addAddressInfo(new AddressInfo(addressName1, RoutingType.ANYCAST));
      server.createQueue(addressName1, RoutingType.ANYCAST, queueName1, null, false, false);

      // create some consumers
      try (ServerLocator locator = createInVMNonHALocator(); ClientSessionFactory csf = createSessionFactory(locator);) {

         ClientSessionImpl session1 = (ClientSessionImpl) csf.createSession();
         Thread.sleep(500);
         ClientSessionImpl session2 = (ClientSessionImpl) csf.createSession();
         Thread.sleep(500);
         ClientSessionImpl session3 = (ClientSessionImpl) csf.createSession();
         ClientConsumer consumer1_s1 = session1.createConsumer(queueName1);
         ClientConsumer consumer2_s1 = session1.createConsumer(queueName1);

         ClientConsumer consumer1_s2 = session2.createConsumer(queueName1);
         ClientConsumer consumer2_s2 = session2.createConsumer(queueName1);
         ClientConsumer consumer3_s2 = session2.createConsumer(queueName1);
         ClientConsumer consumer4_s2 = session2.createConsumer(queueName1);

         ClientConsumer consumer1_s3 = session3.createConsumer(queueName1);
         ClientConsumer consumer2_s3 = session3.createConsumer(queueName1);
         ClientConsumer consumer3_s3 = session3.createConsumer(queueName1);

         String filterString = createJsonFilter("CONSUMER_COUNT", "GREATER_THAN", "1");
         String sessionsAsJsonString = serverControl.listSessions(filterString, 1, 50);
         JsonObject sessionsAsJsonObject = JsonUtil.readJsonObject(sessionsAsJsonString);
         JsonArray array = (JsonArray) sessionsAsJsonObject.get("data");


         Assert.assertEquals("number of sessions returned from query", 3, array.size());
         JsonObject jsonSession = array.getJsonObject(0);

         //check all fields
         Assert.assertNotEquals("id", "", jsonSession.getString("id"));
         Assert.assertEquals("user", "", jsonSession.getString("user"));
         Assert.assertNotEquals("creationTime", "", jsonSession.getString("creationTime"));
         Assert.assertEquals("consumerCount", 2, jsonSession.getInt("consumerCount"));
         Assert.assertTrue("producerCount", 0 <= jsonSession.getInt("producerCount"));
         Assert.assertNotEquals("connectionID", "", jsonSession.getString("connectionID"));

         //check default order
         Assert.assertEquals("session1 location", session1.getName(), array.getJsonObject(0).getString("id"));
         Assert.assertEquals("session2 location", session2.getName(), array.getJsonObject(1).getString("id"));
         Assert.assertEquals("session3 location", session3.getName(), array.getJsonObject(2).getString("id"));

         //bring back session ordered by consumer count
         filterString = createJsonFilter("CONSUMER_COUNT", "GREATER_THAN", "1", "consumerCount", "asc");
         sessionsAsJsonString = serverControl.listSessions(filterString, 1, 50);
         sessionsAsJsonObject = JsonUtil.readJsonObject(sessionsAsJsonString);
         array = (JsonArray) sessionsAsJsonObject.get("data");

         Assert.assertTrue("number of sessions returned from query", 3 == array.size());
         Assert.assertEquals("session1 ordered by consumer", session1.getName(), array.getJsonObject(0).getString("id"));
         Assert.assertEquals("session3 ordered by consumer", session3.getName(), array.getJsonObject(1).getString("id"));
         Assert.assertEquals("session2 ordered by consumer", session2.getName(), array.getJsonObject(2).getString("id"));

         //bring back session ordered by consumer Count
         filterString = createJsonFilter("CONSUMER_COUNT", "GREATER_THAN", "1", "consumerCount", "asc");
         sessionsAsJsonString = serverControl.listSessions(filterString, 1, 50);
         sessionsAsJsonObject = JsonUtil.readJsonObject(sessionsAsJsonString);
         array = (JsonArray) sessionsAsJsonObject.get("data");

         Assert.assertTrue("number of sessions returned from query", 3 == array.size());
         Assert.assertEquals("session1 ordered by consumer", session1.getName(), array.getJsonObject(0).getString("id"));
         Assert.assertEquals("session3 ordered by consumer", session3.getName(), array.getJsonObject(1).getString("id"));
         Assert.assertEquals("session2 ordered by consumer", session2.getName(), array.getJsonObject(2).getString("id"));

         //bring back session ordered by creation time (desc)
         filterString = createJsonFilter("CONSUMER_COUNT", "GREATER_THAN", "1", "creationTime", "desc");
         sessionsAsJsonString = serverControl.listSessions(filterString, 1, 50);
         sessionsAsJsonObject = JsonUtil.readJsonObject(sessionsAsJsonString);
         array = (JsonArray) sessionsAsJsonObject.get("data");

         Assert.assertTrue("number of sessions returned from query", 3 == array.size());
         Assert.assertEquals("session3 ordered by creationTime", session3.getName(), array.getJsonObject(0).getString("id"));
         Assert.assertEquals("session2 ordered by creationTime", session2.getName(), array.getJsonObject(1).getString("id"));
         Assert.assertEquals("session1 ordered by creationTime", session1.getName(), array.getJsonObject(2).getString("id"));

      }
   }

   @Test
   public void testListConnections() throws Exception {
      SimpleString queueName1 = new SimpleString("my_queue_one");
      SimpleString addressName1 = new SimpleString("my_address_one");

      ActiveMQServerControl serverControl = createManagementControl();

      server.addAddressInfo(new AddressInfo(addressName1, RoutingType.ANYCAST));
      server.createQueue(addressName1, RoutingType.ANYCAST, queueName1, null, false, false);

      ClientSessionFactoryImpl csf = null;
      ClientSessionFactoryImpl csf2 = null;
      ClientSessionFactoryImpl csf3 = null;

      // create some consumers
      try (ServerLocator locator = createInVMNonHALocator()) {

         //sleep as test compares creationTime
         csf = (ClientSessionFactoryImpl) createSessionFactory(locator);
         Thread.sleep(500);
         csf2 = (ClientSessionFactoryImpl) createSessionFactory(locator);
         Thread.sleep(500);
         csf3 = (ClientSessionFactoryImpl) createSessionFactory(locator);

         ClientSession session1_c1 = csf.createSession();
         ClientSession session2_c1 = csf.createSession();

         ClientSession session1_c2 = csf2.createSession();
         ClientSession session2_c2 = csf2.createSession();
         ClientSession session3_c2 = csf2.createSession();
         ClientSession session4_c2 = csf2.createSession();

         ClientSession session1_c4 = csf3.createSession();
         ClientSession session2_c4 = csf3.createSession();
         ClientSession session3_c4 = csf3.createSession();

         String filterString = createJsonFilter("SESSION_COUNT", "GREATER_THAN", "1");
         String connectionsAsJsonString = serverControl.listConnections(filterString, 1, 50);
         JsonObject connectionsAsJsonObject = JsonUtil.readJsonObject(connectionsAsJsonString);
         JsonArray array = (JsonArray) connectionsAsJsonObject.get("data");

         Assert.assertEquals("number of connections returned from query", 3, array.size());
         JsonObject jsonConnection = array.getJsonObject(0);

         //check all fields
         Assert.assertNotEquals("connectionID", "", jsonConnection.getString("connectionID"));
         Assert.assertNotEquals("remoteAddress", "", jsonConnection.getString("remoteAddress"));
         Assert.assertEquals("users", "", jsonConnection.getString("users"));
         Assert.assertNotEquals("creationTime", "", jsonConnection.getString("creationTime"));
         Assert.assertNotEquals("implementation", "", jsonConnection.getString("implementation"));
         Assert.assertNotEquals("protocol", "", jsonConnection.getString("protocol"));
         Assert.assertEquals("clientID", "", jsonConnection.getString("clientID"));
         Assert.assertNotEquals("localAddress", "", jsonConnection.getString("localAddress"));
         Assert.assertEquals("sessionCount", 2, jsonConnection.getInt("sessionCount"));

         //check default order
         Assert.assertEquals("connection1 default Order", csf.getConnection().getID(), array.getJsonObject(0).getString("connectionID"));
         Assert.assertEquals("connection2 default Order", csf2.getConnection().getID(), array.getJsonObject(1).getString("connectionID"));
         Assert.assertEquals("connection3 session Order", csf3.getConnection().getID(), array.getJsonObject(2).getString("connectionID"));

         //check order by session count desc
         filterString = createJsonFilter("SESSION_COUNT", "GREATER_THAN", "1", "sessionCount", "desc");
         connectionsAsJsonString = serverControl.listConnections(filterString, 1, 50);
         connectionsAsJsonObject = JsonUtil.readJsonObject(connectionsAsJsonString);
         array = (JsonArray) connectionsAsJsonObject.get("data");

         Assert.assertEquals("number of connections returned from query", 3, array.size());
         Assert.assertEquals("connection2 session Order", csf2.getConnection().getID(), array.getJsonObject(0).getString("connectionID"));
         Assert.assertEquals("connection3 session Order", csf3.getConnection().getID(), array.getJsonObject(1).getString("connectionID"));
         Assert.assertEquals("connection1 session Order", csf.getConnection().getID(), array.getJsonObject(2).getString("connectionID"));

         //check order by creationTime desc
         filterString = createJsonFilter("SESSION_COUNT", "GREATER_THAN", "1", "creationTime", "desc");
         connectionsAsJsonString = serverControl.listConnections(filterString, 1, 50);
         connectionsAsJsonObject = JsonUtil.readJsonObject(connectionsAsJsonString);
         array = (JsonArray) connectionsAsJsonObject.get("data");

         Assert.assertEquals("number of connections returned from query", 3, array.size());
         Assert.assertEquals("connection3 creationTime Order", csf3.getConnection().getID(), array.getJsonObject(0).getString("connectionID"));
         Assert.assertEquals("connection2 creationTime Order", csf2.getConnection().getID(), array.getJsonObject(1).getString("connectionID"));
         Assert.assertEquals("connection1 creationTime Order", csf.getConnection().getID(), array.getJsonObject(2).getString("connectionID"));
      } finally {
         if (csf != null) {
            csf.close();
         }
         if (csf2 != null) {
            csf.close();
         }
         if (csf3 != null) {
            csf.close();
         }
      }
   }

   @Test
   public void testListProducers() throws Exception {
      SimpleString queueName1 = new SimpleString("my_queue_one");
      SimpleString addressName1 = new SimpleString("my_address_one");

      ActiveMQServerControl serverControl = createManagementControl();

      server.addAddressInfo(new AddressInfo(addressName1, RoutingType.ANYCAST));
      server.createQueue(addressName1, RoutingType.ANYCAST, queueName1, null, false, false);

      // create some consumers
      try (ServerLocator locator = createInVMNonHALocator(); ClientSessionFactory csf = createSessionFactory(locator);) {

         ClientSession session1 = csf.createSession();
         ClientSession session2 = csf.createSession();

         ClientProducer producer1 = session1.createProducer(addressName1);
         ClientProducer producer2 = session1.createProducer(addressName1);

         //bring back all producers
         String filterString = createJsonFilter("", "", "");
         String producersAsJsonString = serverControl.listProducers(filterString, 1, 50);
         JsonObject producersAsJsonObject = JsonUtil.readJsonObject(producersAsJsonString);
         JsonArray array = (JsonArray) producersAsJsonObject.get("data");

         Assert.assertTrue("number of producers returned from query", 2 <= array.size());
         JsonObject jsonSession = array.getJsonObject(0);

         //check all fields
         Assert.assertNotEquals("id", "", jsonSession.getString("id"));
         Assert.assertNotEquals("session", "", jsonSession.getString("session"));
         Assert.assertEquals("clientID", "", jsonSession.getString("clientID"));
         Assert.assertEquals("user", "", jsonSession.getString("user"));
         Assert.assertNotEquals("protocol", "", jsonSession.getString("protocol"));
         Assert.assertEquals("address", "", jsonSession.getString("address"));
         Assert.assertNotEquals("localAddress", "", jsonSession.getString("localAddress"));
         Assert.assertNotEquals("remoteAddress", "", jsonSession.getString("remoteAddress"));
         Assert.assertNotEquals("creationTime", "", jsonSession.getString("creationTime"));

      }
   }

   @Test
   public void testMemoryUsagePercentage() throws Exception {
      //messages size 100K
      final int MESSAGE_SIZE = 100000;
      String name1 = "messageUsagePercentage.test.1";

      server.stop();
      //no globalMaxSize set
      server.getConfiguration().setGlobalMaxSize(-1);
      server.start();

      ActiveMQServerControl serverControl = createManagementControl();
      // check before adding messages
      assertEquals("Memory Usage before adding messages", 0, serverControl.getAddressMemoryUsage());
      assertEquals("MemoryUsagePercentage", 0, serverControl.getAddressMemoryUsagePercentage());

      try (ServerLocator locator = createInVMNonHALocator(); ClientSessionFactory csf = createSessionFactory(locator); ClientSession session = csf.createSession()) {
         session.createQueue(name1, RoutingType.ANYCAST, name1);
         ClientProducer producer1 = session.createProducer(name1);
         sendMessagesWithPredefinedSize(30, session, producer1, MESSAGE_SIZE);

         //it is hard to predict an exact number so checking if it falls in a certain range: totalSizeOfMessageSent < X > totalSizeofMessageSent + 100k
         assertTrue("Memory Usage within range ", ((30 * MESSAGE_SIZE) < serverControl.getAddressMemoryUsage()) && (serverControl.getAddressMemoryUsage() < ((30 * MESSAGE_SIZE) + 100000)));
         // no globalMaxSize set so it should return zero
         assertEquals("MemoryUsagePercentage", 0, serverControl.getAddressMemoryUsagePercentage());
      }
   }

   @Test
   public void testMemoryUsage() throws Exception {
      //messages size 100K
      final int MESSAGE_SIZE = 100000;
      String name1 = "messageUsage.test.1";
      String name2 = "messageUsage.test.2";

      server.stop();
      // set to 5 MB
      server.getConfiguration().setGlobalMaxSize(5000000);
      server.start();

      ActiveMQServerControl serverControl = createManagementControl();
      // check before adding messages
      assertEquals("Memory Usage before adding messages", 0, serverControl.getAddressMemoryUsage());
      assertEquals("MemoryUsagePercentage", 0, serverControl.getAddressMemoryUsagePercentage());

      try (ServerLocator locator = createInVMNonHALocator(); ClientSessionFactory csf = createSessionFactory(locator); ClientSession session = csf.createSession()) {
         session.createQueue(name1, RoutingType.ANYCAST, name1);
         session.createQueue(name2, RoutingType.ANYCAST, name2);
         ClientProducer producer1 = session.createProducer(name1);
         ClientProducer producer2 = session.createProducer(name2);
         sendMessagesWithPredefinedSize(10, session, producer1, MESSAGE_SIZE);
         sendMessagesWithPredefinedSize(10, session, producer2, MESSAGE_SIZE);

         //it is hard to predict an exact number so checking if it falls in a certain range: totalSizeOfMessageSent < X > totalSizeofMessageSent + 100k
         assertTrue("Memory Usage within range ", ((20 * MESSAGE_SIZE) < serverControl.getAddressMemoryUsage()) && (serverControl.getAddressMemoryUsage() < ((20 * MESSAGE_SIZE) + 100000)));
         assertTrue("MemoryUsagePercentage", (40 <= serverControl.getAddressMemoryUsagePercentage()) && (42 >= serverControl.getAddressMemoryUsagePercentage()));
      }
   }

   @Test
   public void testConnectorServiceManagement() throws Exception {
      ActiveMQServerControl managementControl = createManagementControl();
      managementControl.createConnectorService("myconn", FakeConnectorServiceFactory.class.getCanonicalName(), new HashMap<String, Object>());

      Assert.assertEquals(1, server.getConnectorsService().getConnectors().size());

      managementControl.createConnectorService("myconn2", FakeConnectorServiceFactory.class.getCanonicalName(), new HashMap<String, Object>());
      Assert.assertEquals(2, server.getConnectorsService().getConnectors().size());

      managementControl.destroyConnectorService("myconn");
      Assert.assertEquals(1, server.getConnectorsService().getConnectors().size());
      Assert.assertEquals("myconn2", managementControl.getConnectorServices()[0]);
   }

   @Test
   public void testCloseCOREclient() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      boolean durable = true;

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getQueueObjectName(address, name, RoutingType.ANYCAST));
      serverControl.createAddress(address.toString(), "ANYCAST");
      serverControl.createQueue(address.toString(), "ANYCAST", name.toString(), null, durable, -1, false, false);

      ServerLocator receiveLocator = createInVMNonHALocator();
      ClientSessionFactory receiveCsf = createSessionFactory(receiveLocator);
      ClientSession receiveClientSession = receiveCsf.createSession(true, false, false);
      final ClientConsumer COREclient = receiveClientSession.createConsumer(name);

      ServerSession ss = server.getSessions().iterator().next();
      ServerConsumer sc = ss.getServerConsumers().iterator().next();

      Assert.assertFalse(COREclient.isClosed());
      serverControl.closeConsumerWithID(((ClientSessionImpl)receiveClientSession).getName(), Long.toString(sc.sequentialID()));
      Wait.waitFor(() -> COREclient.isClosed());
      Assert.assertTrue(COREclient.isClosed());
   }

   @Test
   public void testCloseJMSclient() throws Exception {
      ActiveMQServerControl serverControl = createManagementControl();
      ConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      Connection conn = cf.createConnection();
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      javax.jms.Topic topic = ActiveMQJMSClient.createTopic("ConsumerTestTopic");

      MessageConsumer JMSclient = session.createConsumer(topic, "test1");


      long clientID = -1;
      String sessionID = ((ClientSessionImpl)(((ActiveMQSession)session).getCoreSession())).getName();

      Set<ServerSession> sessions = server.getSessions();
      for (ServerSession sess : sessions) {
         if (sess.getName().equals(sessionID.toString())) {
            Set<ServerConsumer> serverConsumers = sess.getServerConsumers();
            for (ServerConsumer serverConsumer : serverConsumers) {
               clientID = serverConsumer.sequentialID();
            }
         }
      }

      Assert.assertFalse(((org.apache.activemq.artemis.jms.client.ActiveMQMessageConsumer)JMSclient).isClosed());
      serverControl.closeConsumerWithID(sessionID, Long.toString(clientID));
      Wait.waitFor(() -> ((org.apache.activemq.artemis.jms.client.ActiveMQMessageConsumer)JMSclient).isClosed());
      Assert.assertTrue(((org.apache.activemq.artemis.jms.client.ActiveMQMessageConsumer)JMSclient).isClosed());
   }


   protected void scaleDown(ScaleDownHandler handler) throws Exception {
      SimpleString address = new SimpleString("testQueue");
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SERVER_ID_PROP_NAME, "2");
      Configuration config = createDefaultInVMConfig(2).clearAcceptorConfigurations().addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName(), params)).setSecurityEnabled(false);
      ActiveMQServer server2 = addServer(ActiveMQServers.newActiveMQServer(config, null, true));

      this.conf.clearConnectorConfigurations().addConnectorConfiguration("server2-connector", new TransportConfiguration(INVM_CONNECTOR_FACTORY, params));

      server2.start();
      server.addAddressInfo(new AddressInfo(address, RoutingType.ANYCAST));
      server.createQueue(address, RoutingType.ANYCAST, address, null, true, false, -1, false, false);
      server2.addAddressInfo(new AddressInfo(address, RoutingType.ANYCAST));
      server2.createQueue(address, RoutingType.ANYCAST, address, null, true, false, -1, false, false);
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
      role.add(new Role("guest", true, true, true, true, true, true, true, true, true, true));
      server.getSecurityRepository().addMatch("#", role);
   }

   protected ActiveMQServerControl createManagementControl() throws Exception {
      return ManagementControlHelper.createActiveMQServerControl(mbeanServer);
   }

   // Private -------------------------------------------------------

   private String createJsonFilter(String fieldName, String operationName, String value) {
      HashMap<String, Object> filterMap = new HashMap<>();
      filterMap.put("field", fieldName);
      filterMap.put("operation", operationName);
      filterMap.put("value", value);
      JsonObject jsonFilterObject = JsonUtil.toJsonObject(filterMap);
      return jsonFilterObject.toString();
   }

   private String createJsonFilter(String fieldName, String operationName, String value,String sortColumn, String sortOrder) {
      HashMap<String, Object> filterMap = new HashMap<>();
      filterMap.put("field", fieldName);
      filterMap.put("operation", operationName);
      filterMap.put("value", value);
      filterMap.put("sortColumn", sortColumn);
      filterMap.put("sortOrder", sortOrder);
      JsonObject jsonFilterObject = JsonUtil.toJsonObject(filterMap);
      return jsonFilterObject.toString();
   }

   private void sendMessagesWithPredefinedSize(int numberOfMessages,
                                               ClientSession session,
                                               ClientProducer producer,
                                               int messageSize) throws Exception {
      ClientMessage message;
      final byte[] body = new byte[messageSize];
      ByteBuffer bb = ByteBuffer.wrap(body);
      for (int i = 1; i <= messageSize; i++) {
         bb.put(getSamplebyte(i));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);
         ActiveMQBuffer bodyLocal = message.getBodyBuffer();
         bodyLocal.writeBytes(body);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }
      session.commit();
   }
   // Inner classes -------------------------------------------------

}


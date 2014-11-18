/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.tests.integration.management;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.api.core.management.AddressSettingsInfo;
import org.apache.activemq.api.core.management.BridgeControl;
import org.apache.activemq.api.core.management.DivertControl;
import org.apache.activemq.api.core.management.ActiveMQServerControl;
import org.apache.activemq.api.core.management.ObjectNameBuilder;
import org.apache.activemq.api.core.management.QueueControl;
import org.apache.activemq.api.core.management.RoleInfo;
import org.apache.activemq.core.asyncio.impl.AsynchronousFileImpl;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.messagecounter.impl.MessageCounterManagerImpl;
import org.apache.activemq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.core.settings.impl.SlowConsumerPolicy;
import org.apache.activemq.core.transaction.impl.XidImpl;
import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.UnitTestCase;
import org.apache.activemq.utils.UUIDGenerator;
import org.apache.activemq.utils.json.JSONArray;
import org.apache.activemq.utils.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A QueueControlTest
 *
 * @author jmesnil
 *         <p/>
 *         Created 26 nov. 2008 14:18:48
 */
public class ActiveMQServerControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ActiveMQServer server;

   private Configuration conf;

   private TransportConfiguration connectorConfig;

   // Static --------------------------------------------------------

   private static boolean contains(final String name, final String[] strings)
   {
      boolean found = false;
      for (String str : strings)
      {
         if (name.equals(str))
         {
            found = true;
            break;
         }
      }
      return found;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testGetAttributes() throws Exception
   {
      ActiveMQServerControl serverControl = createManagementControl();

      Assert.assertEquals(server.getVersion().getFullVersion(), serverControl.getVersion());

      Assert.assertEquals(conf.isClustered(), serverControl.isClustered());
      Assert.assertEquals(conf.isPersistDeliveryCountBeforeDelivery(),
                          serverControl.isPersistDeliveryCountBeforeDelivery());
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
      Assert.assertEquals(conf.getManagementNotificationAddress().toString(),
                          serverControl.getManagementNotificationAddress());
      Assert.assertEquals(conf.getIDCacheSize(), serverControl.getIDCacheSize());
      Assert.assertEquals(conf.isPersistIDCache(), serverControl.isPersistIDCache());
      Assert.assertEquals(conf.getBindingsDirectory(), serverControl.getBindingsDirectory());
      Assert.assertEquals(conf.getJournalDirectory(), serverControl.getJournalDirectory());
      Assert.assertEquals(conf.getJournalType().toString(), serverControl.getJournalType());
      Assert.assertEquals(conf.isJournalSyncTransactional(), serverControl.isJournalSyncTransactional());
      Assert.assertEquals(conf.isJournalSyncNonTransactional(), serverControl.isJournalSyncNonTransactional());
      Assert.assertEquals(conf.getJournalFileSize(), serverControl.getJournalFileSize());
      Assert.assertEquals(conf.getJournalMinFiles(), serverControl.getJournalMinFiles());
      if (AsynchronousFileImpl.isLoaded())
      {
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
   public void testGetConnectors() throws Exception
   {
      ActiveMQServerControl serverControl = createManagementControl();

      Object[] connectorData = serverControl.getConnectors();
      Assert.assertNotNull(connectorData);
      Assert.assertEquals(1, connectorData.length);

      Object[] config = (Object[]) connectorData[0];

      Assert.assertEquals(connectorConfig.getName(), config[0]);
   }

   @Test
   public void testGetConnectorsAsJSON() throws Exception
   {
      ActiveMQServerControl serverControl = createManagementControl();

      String jsonString = serverControl.getConnectorsAsJSON();
      Assert.assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      Assert.assertEquals(1, array.length());
      JSONObject data = array.getJSONObject(0);
      Assert.assertEquals(connectorConfig.getName(), data.optString("name"));
      Assert.assertEquals(connectorConfig.getFactoryClassName(), data.optString("factoryClassName"));
      Assert.assertEquals(connectorConfig.getParams().size(), data.getJSONObject("params").length());
   }

   @Test
   public void testCreateAndDestroyQueue() throws Exception
   {
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
   public void testCreateAndDestroyQueue_2() throws Exception
   {
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
   public void testCreateAndDestroyQueue_3() throws Exception
   {
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
   public void testCreateAndDestroyQueueWithNullFilter() throws Exception
   {
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
   public void testCreateAndDestroyQueueWithEmptyStringForFilter() throws Exception
   {
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
   public void testGetQueueNames() throws Exception
   {
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
   public void testGetAddressNames() throws Exception
   {
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
   public void testMessageCounterMaxDayCount() throws Exception
   {
      ActiveMQServerControl serverControl = createManagementControl();

      Assert.assertEquals(MessageCounterManagerImpl.DEFAULT_MAX_DAY_COUNT, serverControl.getMessageCounterMaxDayCount());

      int newCount = 100;
      serverControl.setMessageCounterMaxDayCount(newCount);

      Assert.assertEquals(newCount, serverControl.getMessageCounterMaxDayCount());

      try
      {
         serverControl.setMessageCounterMaxDayCount(-1);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

      try
      {
         serverControl.setMessageCounterMaxDayCount(0);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

      Assert.assertEquals(newCount, serverControl.getMessageCounterMaxDayCount());
   }

   @Test
   public void testGetMessageCounterSamplePeriod() throws Exception
   {
      ActiveMQServerControl serverControl = createManagementControl();

      Assert.assertEquals(MessageCounterManagerImpl.DEFAULT_SAMPLE_PERIOD,
                          serverControl.getMessageCounterSamplePeriod());

      long newSample = 20000;
      serverControl.setMessageCounterSamplePeriod(newSample);

      Assert.assertEquals(newSample, serverControl.getMessageCounterSamplePeriod());

      try
      {
         serverControl.setMessageCounterSamplePeriod(-1);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

      try
      {
         serverControl.setMessageCounterSamplePeriod(0);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

      try
      {
         serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD - 1);
         Assert.fail();
      }
      catch (Exception e)
      {
      }

      Assert.assertEquals(newSample, serverControl.getMessageCounterSamplePeriod());
   }

   protected void restartServer() throws Exception
   {
      server.stop();
      server.start();
   }

   @Test
   public void testSecuritySettings() throws Exception
   {
      ActiveMQServerControl serverControl = createManagementControl();
      String addressMatch = "test.#";
      String exactAddress = "test.whatever";

      assertEquals(0, serverControl.getRoles(addressMatch).length);
      serverControl.addSecuritySettings(addressMatch, "foo", "foo, bar", "foo", "bar", "foo, bar", "", "");

      // Restart the server. Those settings should be persisted

      restartServer();

      serverControl = createManagementControl();

      String rolesAsJSON = serverControl.getRolesAsJSON(exactAddress);
      RoleInfo[] roleInfos = RoleInfo.from(rolesAsJSON);
      assertEquals(2, roleInfos.length);
      RoleInfo fooRole = null;
      RoleInfo barRole = null;
      if (roleInfos[0].getName().equals("foo"))
      {
         fooRole = roleInfos[0];
         barRole = roleInfos[1];
      }
      else
      {
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

      assertFalse(barRole.isSend());
      assertTrue(barRole.isConsume());
      assertFalse(barRole.isCreateDurableQueue());
      assertTrue(barRole.isDeleteDurableQueue());
      assertTrue(barRole.isCreateNonDurableQueue());
      assertFalse(barRole.isDeleteNonDurableQueue());
      assertFalse(barRole.isManage());

      serverControl.removeSecuritySettings(addressMatch);
      assertEquals(0, serverControl.getRoles(exactAddress).length);
   }

   @Test
   public void testAddressSettings() throws Exception
   {
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

      serverControl.addAddressSettings(addressMatch,
                                       DLA,
                                       expiryAddress,
                                       expiryDelay,
                                       lastValueQueue,
                                       deliveryAttempts,
                                       maxSizeBytes,
                                       pageSizeBytes,
                                       pageMaxCacheSize,
                                       redeliveryDelay,
                                       redeliveryMultiplier,
                                       maxRedeliveryDelay,
                                       redistributionDelay,
                                       sendToDLAOnNoRoute,
                                       addressFullMessagePolicy,
                                       slowConsumerThreshold,
                                       slowConsumerCheckPeriod,
                                       slowConsumerPolicy);


      boolean ex = false;
      try
      {
         serverControl.addAddressSettings(addressMatch,
                                          DLA,
                                          expiryAddress,
                                          expiryDelay,
                                          lastValueQueue,
                                          deliveryAttempts,
                                          100,
                                          1000,
                                          pageMaxCacheSize,
                                          redeliveryDelay,
                                          redeliveryMultiplier,
                                          maxRedeliveryDelay,
                                          redistributionDelay,
                                          sendToDLAOnNoRoute,
                                          addressFullMessagePolicy,
                                          slowConsumerThreshold,
                                          slowConsumerCheckPeriod,
                                          slowConsumerPolicy);
      }
      catch (Exception expected)
      {
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

      serverControl.addAddressSettings(addressMatch,
                                       DLA,
                                       expiryAddress,
                                       expiryDelay,
                                       lastValueQueue,
                                       deliveryAttempts,
                                       -1,
                                       1000,
                                       pageMaxCacheSize,
                                       redeliveryDelay,
                                       redeliveryMultiplier,
                                       maxRedeliveryDelay,
                                       redistributionDelay,
                                       sendToDLAOnNoRoute,
                                       addressFullMessagePolicy,
                                       slowConsumerThreshold,
                                       slowConsumerCheckPeriod,
                                       slowConsumerPolicy);


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


      ex = false;
      try
      {
         serverControl.addAddressSettings(addressMatch,
                                          DLA,
                                          expiryAddress,
                                          expiryDelay,
                                          lastValueQueue,
                                          deliveryAttempts,
                                          -2,
                                          1000,
                                          pageMaxCacheSize,
                                          redeliveryDelay,
                                          redeliveryMultiplier,
                                          maxRedeliveryDelay,
                                          redistributionDelay,
                                          sendToDLAOnNoRoute,
                                          addressFullMessagePolicy,
                                          slowConsumerThreshold,
                                          slowConsumerCheckPeriod,
                                          slowConsumerPolicy);
      }
      catch (Exception e)
      {
         ex = true;
      }


      assertTrue("Supposed to have an exception called", ex);


   }

   @Test
   public void testCreateAndDestroyDivert() throws Exception
   {
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
      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));

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
   public void testCreateAndDestroyBridge() throws Exception
   {
      String name = RandomUtil.randomString();
      String sourceAddress = RandomUtil.randomString();
      String sourceQueue = RandomUtil.randomString();
      String targetAddress = RandomUtil.randomString();
      String targetQueue = RandomUtil.randomString();

      ActiveMQServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(name));
      assertEquals(0, serverControl.getBridgeNames().length);

      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession();

      session.createQueue(sourceAddress, sourceQueue);
      session.createQueue(targetAddress, targetQueue);

      serverControl.createBridge(name,
                                 sourceQueue,
                                 targetAddress,
                                 null, // forwardingAddress
                                 null, // filterString
                                 ActiveMQClient.DEFAULT_RETRY_INTERVAL,
                                 ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                 ActiveMQClient.INITIAL_CONNECT_ATTEMPTS,
                                 ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS,
                                 false, // duplicateDetection
                                 1, // confirmationWindowSize
                                 ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                 connectorConfig.getName(), // liveConnector
                                 false,
                                 false,
                                 null,
                                 null);

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
   public void testListPreparedTransactionDetails() throws Exception
   {
      SimpleString atestq = new SimpleString("BasicXaTestq");
      Xid xid = newXID();

      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
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

      JSONArray jsonArray = new JSONArray(serverControl.listProducersInfoAsJSON());

      assertEquals(1, jsonArray.length());
      assertEquals(4, ((JSONObject) jsonArray.get(0)).getInt("msgSent"));

      clientSession.close();
      locator.close();

      String txDetails = serverControl.listPreparedTransactionDetailsAsJSON();

      Assert.assertTrue(txDetails.matches(".*m1.*"));
      Assert.assertTrue(txDetails.matches(".*m2.*"));
      Assert.assertTrue(txDetails.matches(".*m3.*"));
      Assert.assertTrue(txDetails.matches(".*m4.*"));
   }

   @Test
   public void testListPreparedTransactionDetailsAsHTML() throws Exception
   {
      SimpleString atestq = new SimpleString("BasicXaTestq");
      Xid xid = newXID();

      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
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
   public void testCommitPreparedTransactions() throws Exception
   {
      SimpleString recQueue = new SimpleString("BasicXaTestqRec");
      SimpleString sendQueue = new SimpleString("BasicXaTestqSend");

      byte[] globalTransactionId = UUIDGenerator.getInstance().generateStringUUID().getBytes();
      Xid xid = new XidImpl("xa1".getBytes(), 1, globalTransactionId);
      Xid xid2 = new XidImpl("xa2".getBytes(), 1, globalTransactionId);
      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession clientSession = csf.createSession(true, false, false);
      clientSession.createQueue(recQueue, recQueue, null, true);
      clientSession.createQueue(sendQueue, sendQueue, null, true);
      ClientMessage m1 = createTextMessage(clientSession, "");
      m1.putStringProperty("m1", "m1");
      ClientProducer clientProducer = clientSession.createProducer(recQueue);
      clientProducer.send(m1);
      locator.close();


      ServerLocator receiveLocator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory receiveCsf = createSessionFactory(receiveLocator);
      ClientSession receiveClientSession = receiveCsf.createSession(true, false, false);
      ClientConsumer consumer = receiveClientSession.createConsumer(recQueue);


      ServerLocator sendLocator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
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
   public void testScaleDownWithConnector() throws Exception
   {
      scaleDown(new ScaleDownHandler()
      {
         @Override
         public void scaleDown(ActiveMQServerControl control) throws Exception
         {
            control.scaleDown("server2-connector");
         }
      });
   }

   @Test
   public void testScaleDownWithOutConnector() throws Exception
   {
      scaleDown(new ScaleDownHandler()
      {
         @Override
         public void scaleDown(ActiveMQServerControl control) throws Exception
         {
            control.scaleDown(null);
         }
      });
   }

   protected void scaleDown(ScaleDownHandler handler) throws Exception
   {
      SimpleString address = new SimpleString("testQueue");
      Configuration conf = createDefaultConfig(false, 2);
      conf.setSecurityEnabled(false);
      conf.getAcceptorConfigurations().clear();
      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put("server-id", "2");
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName(), params));
      ActiveMQServer server2 = ActiveMQServers.newActiveMQServer(conf, null, true);
      this.conf.getConnectorConfigurations().clear();
      this.conf.getConnectorConfigurations().put("server2-connector", new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY, params));
      try
      {
         server2.start();
         server.createQueue(address, address, null, true, false);
         server2.createQueue(address, address, null, true, false);
         ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
         ClientSessionFactory csf = createSessionFactory(locator);
         ClientSession session = csf.createSession();
         ClientProducer producer = session.createProducer(address);
         for (int i = 0; i < 100; i++)
         {
            ClientMessage message = session.createMessage(true);
            message.getBodyBuffer().writeString("m" + i);
            producer.send(message);
         }

         ActiveMQServerControl managementControl = createManagementControl();
         handler.scaleDown(managementControl);
         locator.close();
         locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY, params));
         csf = createSessionFactory(locator);
         session = csf.createSession();
         session.start();
         ClientConsumer consumer = session.createConsumer(address);
         for (int i = 0; i < 100; i++)
         {
            ClientMessage m = consumer.receive(5000);
            assertNotNull(m);
         }
      }
      finally
      {
         server2.stop();
      }
   }
   // Package protected ---------------------------------------------
   interface ScaleDownHandler
   {
      void scaleDown(ActiveMQServerControl control) throws Exception;
   }
   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Map<String, Object> params = new HashMap<String, Object>();
      //params.put(RandomUtil.randomString(), RandomUtil.randomBoolean());
      connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                   params,
                                                   RandomUtil.randomString());

      conf = createDefaultConfig(false)
         .setSecurityEnabled(false)
         .setJMXManagementEnabled(true)
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()))
         .addConnectorConfiguration(connectorConfig.getName(), connectorConfig);
      server = ActiveMQServers.newActiveMQServer(conf, mbeanServer, true);
      server.start();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (server != null)
      {
         server.stop();
      }

      server = null;

      connectorConfig = null;

      super.tearDown();
   }

   protected ActiveMQServerControl createManagementControl() throws Exception
   {
      return ManagementControlHelper.createActiveMQServerControl(mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}


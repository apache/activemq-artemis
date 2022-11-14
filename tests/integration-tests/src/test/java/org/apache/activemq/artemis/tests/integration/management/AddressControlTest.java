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
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.core.management.RoleInfo;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.cluster.RemoteQueueBinding;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.cluster.impl.RemoteQueueBindingImpl;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonString;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.activemq.artemis.tests.util.RandomUtil.randomString;

public class AddressControlTest extends ManagementTestBase {

   private ActiveMQServer server;
   protected ClientSession session;
   private ServerLocator locator;
   private ClientSessionFactory sf;

   public boolean usingCore() {
      return false;
   }

   @Test
   public void testManagementAddressAlwaysExists() throws Exception {
      ClientSession.AddressQuery query = session.addressQuery(new SimpleString("activemq.management"));
      assertTrue(query.isExists());
   }

   @Test
   public void testGetAddress() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(new QueueConfiguration(queue).setAddress(address).setDurable(false));

      AddressControl addressControl = createManagementControl(address);

      Assert.assertEquals(address.toString(), addressControl.getAddress());

      session.deleteQueue(queue);
   }

   @Test
   public void testIsRetroactiveResource() throws Exception {
      SimpleString baseAddress = RandomUtil.randomSimpleString();
      SimpleString address = ResourceNames.getRetroactiveResourceAddressName(server.getInternalNamingPrefix(), server.getConfiguration().getWildcardConfiguration().getDelimiterString(), baseAddress);

      session.createAddress(address, RoutingType.MULTICAST, false);

      AddressControl addressControl = createManagementControl(address);

      Assert.assertTrue(addressControl.isRetroactiveResource());
   }

   @Test
   public void testGetLocalQueueNames() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString anotherQueue = RandomUtil.randomSimpleString();

      session.createQueue(new QueueConfiguration(queue).setAddress(address));

      // add a fake RemoteQueueBinding to simulate being in a cluster; we don't want this binding to be returned by getQueueNames()
      RemoteQueueBinding binding = new RemoteQueueBindingImpl(server.getStorageManager().generateID(), address, RandomUtil.randomSimpleString(), RandomUtil.randomSimpleString(), RandomUtil.randomLong(), null, null, RandomUtil.randomSimpleString(), RandomUtil.randomInt() + 1, MessageLoadBalancingType.OFF);
      server.getPostOffice().addBinding(binding);

      AddressControl addressControl = createManagementControl(address);
      String[] queueNames = addressControl.getQueueNames();
      Assert.assertEquals(1, queueNames.length);
      Assert.assertEquals(queue.toString(), queueNames[0]);

      session.createQueue(new QueueConfiguration(anotherQueue).setAddress(address).setDurable(false));
      queueNames = addressControl.getQueueNames();
      Assert.assertEquals(2, queueNames.length);

      session.deleteQueue(queue);

      queueNames = addressControl.getQueueNames();
      Assert.assertEquals(1, queueNames.length);
      Assert.assertEquals(anotherQueue.toString(), queueNames[0]);

      session.deleteQueue(anotherQueue);
   }

   @Test
   public void testGetRemoteQueueNames() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createAddress(address, RoutingType.MULTICAST, false);

      // add a fake RemoteQueueBinding to simulate being in a cluster; this should be returned by getRemoteQueueNames()
      RemoteQueueBinding binding = new RemoteQueueBindingImpl(server.getStorageManager().generateID(), address, queue, RandomUtil.randomSimpleString(), RandomUtil.randomLong(), null, null, RandomUtil.randomSimpleString(), RandomUtil.randomInt() + 1, MessageLoadBalancingType.OFF);
      server.getPostOffice().addBinding(binding);

      AddressControl addressControl = createManagementControl(address);
      String[] queueNames = addressControl.getRemoteQueueNames();
      Assert.assertEquals(1, queueNames.length);
      Assert.assertEquals(queue.toString(), queueNames[0]);
   }

   @Test
   public void testGetAllQueueNames() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString anotherQueue = RandomUtil.randomSimpleString();
      SimpleString remoteQueue = RandomUtil.randomSimpleString();

      session.createQueue(new QueueConfiguration(queue).setAddress(address));

      // add a fake RemoteQueueBinding to simulate being in a cluster
      RemoteQueueBinding binding = new RemoteQueueBindingImpl(server.getStorageManager().generateID(), address, remoteQueue, RandomUtil.randomSimpleString(), RandomUtil.randomLong(), null, null, RandomUtil.randomSimpleString(), RandomUtil.randomInt() + 1, MessageLoadBalancingType.OFF);
      server.getPostOffice().addBinding(binding);

      AddressControl addressControl = createManagementControl(address);
      String[] queueNames = addressControl.getAllQueueNames();
      Assert.assertEquals(2, queueNames.length);
      Assert.assertTrue(Arrays.asList(queueNames).contains(queue.toString()));
      Assert.assertTrue(Arrays.asList(queueNames).contains(remoteQueue.toString()));

      session.createQueue(new QueueConfiguration(anotherQueue).setAddress(address).setDurable(false));
      queueNames = addressControl.getAllQueueNames();
      Assert.assertEquals(3, queueNames.length);
      Assert.assertTrue(Arrays.asList(queueNames).contains(anotherQueue.toString()));

      session.deleteQueue(queue);

      queueNames = addressControl.getAllQueueNames();
      Assert.assertEquals(2, queueNames.length);
      Assert.assertTrue(Arrays.asList(queueNames).contains(anotherQueue.toString()));
      Assert.assertFalse(Arrays.asList(queueNames).contains(queue.toString()));

      session.deleteQueue(anotherQueue);
   }

   @Test
   public void testGetBindingNames() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      String divertName = RandomUtil.randomString();

      session.createQueue(new QueueConfiguration(queue).setAddress(address).setDurable(false));

      AddressControl addressControl = createManagementControl(address);
      String[] bindingNames = addressControl.getBindingNames();
      assertEquals(1, bindingNames.length);
      assertEquals(queue.toString(), bindingNames[0]);

      server.getActiveMQServerControl().createDivert(divertName, randomString(), address.toString(), RandomUtil.randomString(), false, null, null);

      bindingNames = addressControl.getBindingNames();
      Assert.assertEquals(2, bindingNames.length);

      session.deleteQueue(queue);

      bindingNames = addressControl.getBindingNames();
      assertEquals(1, bindingNames.length);
      assertEquals(divertName.toString(), bindingNames[0]);
   }

   @Test
   public void testGetRoles() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      Role role = new Role(RandomUtil.randomString(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean());

      session.createQueue(new QueueConfiguration(queue).setAddress(address));

      AddressControl addressControl = createManagementControl(address);
      Object[] roles = addressControl.getRoles();
      Assert.assertEquals(0, roles.length);

      Set<Role> newRoles = new HashSet<>();
      newRoles.add(role);
      server.getSecurityRepository().addMatch(address.toString(), newRoles);

      roles = addressControl.getRoles();
      Assert.assertEquals(1, roles.length);
      Object[] r = (Object[]) roles[0];
      Assert.assertEquals(role.getName(), r[0]);
      Assert.assertEquals(CheckType.SEND.hasRole(role), (boolean)r[1]);
      Assert.assertEquals(CheckType.CONSUME.hasRole(role), (boolean)r[2]);
      Assert.assertEquals(CheckType.CREATE_DURABLE_QUEUE.hasRole(role), (boolean)r[3]);
      Assert.assertEquals(CheckType.DELETE_DURABLE_QUEUE.hasRole(role), (boolean)r[4]);
      Assert.assertEquals(CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role), (boolean)r[5]);
      Assert.assertEquals(CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role), (boolean)r[6]);
      Assert.assertEquals(CheckType.MANAGE.hasRole(role), (boolean)r[7]);
      Assert.assertEquals(CheckType.BROWSE.hasRole(role), (boolean)r[8]);
      Assert.assertEquals(CheckType.CREATE_ADDRESS.hasRole(role), (boolean)r[9]);
      Assert.assertEquals(CheckType.DELETE_ADDRESS.hasRole(role), (boolean)r[10]);

      Assert.assertEquals(CheckType.values().length + 1, r.length);

      session.deleteQueue(queue);
   }

   @Test
   public void testGetRolesAsJSON() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      Role role = new Role(RandomUtil.randomString(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean());

      session.createQueue(new QueueConfiguration(queue).setAddress(address));

      AddressControl addressControl = createManagementControl(address);
      String jsonString = addressControl.getRolesAsJSON();
      Assert.assertNotNull(jsonString);
      RoleInfo[] roles = RoleInfo.from(jsonString);
      Assert.assertEquals(0, roles.length);

      Set<Role> newRoles = new HashSet<>();
      newRoles.add(role);
      server.getSecurityRepository().addMatch(address.toString(), newRoles);

      jsonString = addressControl.getRolesAsJSON();
      Assert.assertNotNull(jsonString);
      roles = RoleInfo.from(jsonString);
      Assert.assertEquals(1, roles.length);
      RoleInfo r = roles[0];
      Assert.assertEquals(role.getName(), roles[0].getName());
      Assert.assertEquals(role.isSend(), r.isSend());
      Assert.assertEquals(role.isConsume(), r.isConsume());
      Assert.assertEquals(role.isCreateDurableQueue(), r.isCreateDurableQueue());
      Assert.assertEquals(role.isDeleteDurableQueue(), r.isDeleteDurableQueue());
      Assert.assertEquals(role.isCreateNonDurableQueue(), r.isCreateNonDurableQueue());
      Assert.assertEquals(role.isDeleteNonDurableQueue(), r.isDeleteNonDurableQueue());
      Assert.assertEquals(role.isManage(), r.isManage());

      session.deleteQueue(queue);
   }

   @Test
   public void testGetNumberOfPages() throws Exception {
      session.close();
      server.stop();
      server.getConfiguration().setPersistenceEnabled(true);

      SimpleString address = RandomUtil.randomSimpleString();

      AddressSettings addressSettings = new AddressSettings().setPageSizeBytes(1024).setMaxSizeBytes(10 * 1024);
      final int NUMBER_MESSAGES_BEFORE_PAGING = 7;

      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);
      server.start();
      ServerLocator locator2 = createInVMNonHALocator();
      addServerLocator(locator2);
      ClientSessionFactory sf2 = createSessionFactory(locator2);

      session = sf2.createSession(false, true, false);
      session.start();
      session.createQueue(new QueueConfiguration(address));

      QueueImpl serverQueue = (QueueImpl) server.locateQueue(address);

      ClientProducer producer = session.createProducer(address);

      for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++) {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(new byte[896]);
         producer.send(msg);
      }
      session.commit();

      AddressControl addressControl = createManagementControl(address);
      Assert.assertEquals(0, addressControl.getNumberOfPages());

      ClientMessage msg = session.createMessage(true);
      msg.getBodyBuffer().writeBytes(new byte[896]);
      producer.send(msg);

      session.commit();
      Assert.assertEquals(1, addressControl.getNumberOfPages());

      msg = session.createMessage(true);
      msg.getBodyBuffer().writeBytes(new byte[896]);
      producer.send(msg);

      session.commit();
      Assert.assertEquals(1, addressControl.getNumberOfPages());

      msg = session.createMessage(true);
      msg.getBodyBuffer().writeBytes(new byte[896]);
      producer.send(msg);

      session.commit();

      Assert.assertEquals("# of pages is 2", 2, addressControl.getNumberOfPages());

      Assert.assertEquals(serverQueue.getPageSubscription().getPagingStore().getAddressSize(), addressControl.getAddressSize());
   }


   @Test
   public void testScheduleCleanup() throws Exception {
      server.getConfiguration().setPersistenceEnabled(true);

      SimpleString address = RandomUtil.randomSimpleString();

      AddressSettings addressSettings = new AddressSettings().setPageSizeBytes(1024).setMaxSizeBytes(10 * 1024);

      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      server.addAddressInfo(new AddressInfo(address).addRoutingType(RoutingType.MULTICAST));
      server.createQueue(new QueueConfiguration(address).setName(address).setDurable(true));


      AddressControl addressControl = createManagementControl(address);

      Queue queue = server.locateQueue(address);
      queue.getPagingStore().startPaging();

      Assert.assertTrue(addressControl.isPaging());

      addressControl.schedulePageCleanup();

      Wait.assertFalse(addressControl::isPaging, 2000, 10);
      Wait.assertFalse(queue.getPagingStore()::isPaging);
   }

   @Test
   public void testGetNumberOfBytesPerPage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createQueue(new QueueConfiguration(address));

      AddressControl addressControl = createManagementControl(address);
      Assert.assertEquals(AddressSettings.DEFAULT_PAGE_SIZE, addressControl.getNumberOfBytesPerPage());

      session.close();
      server.stop();

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setPageSizeBytes(1024);

      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);
      server.start();
      ServerLocator locator2 = createInVMNonHALocator();
      ClientSessionFactory sf2 = createSessionFactory(locator2);

      session = sf2.createSession(false, true, false);
      Assert.assertEquals(1024, addressControl.getNumberOfBytesPerPage());
   }

   @Test
   public void testGetRoutingTypes() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      AddressControl addressControl = createManagementControl(address);
      String[] routingTypes = addressControl.getRoutingTypes();
      Assert.assertEquals(1, routingTypes.length);
      Assert.assertEquals(RoutingType.ANYCAST.toString(), routingTypes[0]);

      address = RandomUtil.randomSimpleString();
      EnumSet<RoutingType> types = EnumSet.of(RoutingType.ANYCAST, RoutingType.MULTICAST);
      session.createAddress(address, types, false);

      addressControl = createManagementControl(address);
      routingTypes = addressControl.getRoutingTypes();
      Set<String> strings = new HashSet<>(Arrays.asList(routingTypes));
      Assert.assertEquals(2, strings.size());
      Assert.assertTrue(strings.contains(RoutingType.ANYCAST.toString()));
      Assert.assertTrue(strings.contains(RoutingType.MULTICAST.toString()));
   }

   @Test
   public void testGetRoutingTypesAsJSON() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      AddressControl addressControl = createManagementControl(address);
      JsonArray jsonArray = JsonUtil.readJsonArray(addressControl.getRoutingTypesAsJSON());

      assertEquals(1, jsonArray.size());
      assertEquals(RoutingType.ANYCAST.toString(), ((JsonString) jsonArray.get(0)).getString());
   }

   @Test
   public void testGetMessageCount() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      AddressControl addressControl = createManagementControl(address);
      assertEquals(0, addressControl.getMessageCount());

      ClientProducer producer = session.createProducer(address.toString());
      producer.send(session.createMessage(false));
      assertEquals(0, addressControl.getMessageCount());

      session.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));
      producer.send(session.createMessage(false));
      assertTrue(Wait.waitFor(() -> addressControl.getMessageCount() == 1, 2000, 100));

      session.createQueue(new QueueConfiguration(address.concat('2')).setAddress(address).setRoutingType(RoutingType.ANYCAST));
      producer.send(session.createMessage(false));
      assertTrue(Wait.waitFor(() -> addressControl.getMessageCount() == 2, 2000, 100));
   }

   @Test
   public void testNumberOfMessages() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      AddressControl addressControl = createManagementControl(address);
      assertEquals(0, addressControl.getNumberOfMessages());

      ClientProducer producer = session.createProducer(address.toString());
      producer.send(session.createMessage(false));
      assertEquals(0, addressControl.getNumberOfMessages());

      session.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));
      producer.send(session.createMessage(false));
      Wait.assertTrue(() -> addressControl.getNumberOfMessages() == 1, 2000, 100);

      RemoteQueueBinding binding = Mockito.mock(RemoteQueueBinding.class);
      Mockito.when(binding.getAddress()).thenReturn(address);
      Queue queue = Mockito.mock(Queue.class);
      Mockito.when(queue.getMessageCount()).thenReturn((long) 999);
      Mockito.when(binding.getQueue()).thenReturn(queue);
      Mockito.when(binding.getUniqueName()).thenReturn(RandomUtil.randomSimpleString());
      Mockito.when(binding.getRoutingName()).thenReturn(RandomUtil.randomSimpleString());
      Mockito.when(binding.getClusterName()).thenReturn(RandomUtil.randomSimpleString());
      Mockito.when(binding.getType()).thenReturn(BindingType.REMOTE_QUEUE);
      server.getPostOffice().addBinding(binding);

      assertEquals(1, addressControl.getNumberOfMessages());
   }

   @Test
   public void testGetRoutedMessageCounts() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      AddressControl addressControl = createManagementControl(address);
      assertEquals(0, addressControl.getMessageCount());

      ClientProducer producer = session.createProducer(address.toString());
      producer.send(session.createMessage(false));
      assertTrue(Wait.waitFor(() -> addressControl.getRoutedMessageCount() == 0, 2000, 100));
      assertTrue(Wait.waitFor(() -> addressControl.getUnRoutedMessageCount() == 1, 2000, 100));

      session.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));
      producer.send(session.createMessage(false));
      assertTrue(Wait.waitFor(() -> addressControl.getRoutedMessageCount() == 1, 2000, 100));
      assertTrue(Wait.waitFor(() -> addressControl.getUnRoutedMessageCount() == 1, 2000, 100));

      session.createQueue(new QueueConfiguration(address.concat('2')).setAddress(address).setRoutingType(RoutingType.ANYCAST));
      producer.send(session.createMessage(false));
      assertTrue(Wait.waitFor(() -> addressControl.getRoutedMessageCount() == 2, 2000, 100));
      assertTrue(Wait.waitFor(() -> addressControl.getUnRoutedMessageCount() == 1, 2000, 100));

      session.deleteQueue(address);
      session.deleteQueue(address.concat('2'));
      producer.send(session.createMessage(false));
      assertTrue(Wait.waitFor(() -> addressControl.getRoutedMessageCount() == 2, 2000, 100));
      assertTrue(Wait.waitFor(() -> addressControl.getUnRoutedMessageCount() == 2, 2000, 100));
   }

   @Test
   public void testSendMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      AddressControl addressControl = createManagementControl(address);
      Assert.assertEquals(0, addressControl.getQueueNames().length);
      session.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));
      Assert.assertEquals(1, addressControl.getQueueNames().length);
      addressControl.sendMessage(null, Message.BYTES_TYPE, Base64.encodeBytes("test".getBytes()), false, null, null);

      Wait.waitFor(() -> addressControl.getMessageCount() == 1);
      Assert.assertEquals(1, addressControl.getMessageCount());

      ClientConsumer consumer = session.createConsumer(address);
      ClientMessage message = consumer.receive(500);
      assertNotNull(message);
      byte[] buffer = new byte[message.getBodyBuffer().readableBytes()];
      message.getBodyBuffer().readBytes(buffer);
      assertEquals("test", new String(buffer));
   }

   @Test
   public void testSendMessageWithProperties() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      AddressControl addressControl = createManagementControl(address);
      Assert.assertEquals(0, addressControl.getQueueNames().length);
      session.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));
      Assert.assertEquals(1, addressControl.getQueueNames().length);
      Map<String, String> headers = new HashMap<>();
      headers.put("myProp1", "myValue1");
      headers.put("myProp2", "myValue2");
      addressControl.sendMessage(headers, Message.BYTES_TYPE, Base64.encodeBytes("test".getBytes()), false, null, null);

      Wait.waitFor(() -> addressControl.getMessageCount() == 1);
      Assert.assertEquals(1, addressControl.getMessageCount());

      ClientConsumer consumer = session.createConsumer(address);
      ClientMessage message = consumer.receive(500);
      assertNotNull(message);
      byte[] buffer = new byte[message.getBodyBuffer().readableBytes()];
      message.getBodyBuffer().readBytes(buffer);
      assertEquals("test", new String(buffer));
      assertEquals("myValue1", message.getStringProperty("myProp1"));
      assertEquals("myValue2", message.getStringProperty("myProp2"));
   }

   @Test
   public void testSendMessageWithMessageId() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      AddressControl addressControl = createManagementControl(address);
      Assert.assertEquals(0, addressControl.getQueueNames().length);
      session.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));
      Assert.assertEquals(1, addressControl.getQueueNames().length);
      addressControl.sendMessage(null, Message.BYTES_TYPE, Base64.encodeBytes("test".getBytes()), false, null, null, true);
      addressControl.sendMessage(null, Message.BYTES_TYPE, Base64.encodeBytes("test".getBytes()), false, null, null, false);

      Wait.waitFor(() -> addressControl.getMessageCount() == 2);
      Assert.assertEquals(2, addressControl.getMessageCount());

      ClientConsumer consumer = session.createConsumer(address);
      ClientMessage message = consumer.receive(500);
      assertNotNull(message);
      assertNotNull(message.getUserID());
      byte[] buffer = new byte[message.getBodyBuffer().readableBytes()];
      message.getBodyBuffer().readBytes(buffer);
      assertEquals("test", new String(buffer));message = consumer.receive(500);
      assertNotNull(message);
      assertNull(message.getUserID());
      buffer = new byte[message.getBodyBuffer().readableBytes()];
      message.getBodyBuffer().readBytes(buffer);
      assertEquals("test", new String(buffer));
   }

   @Test
   public void testGetCurrentDuplicateIdCacheSize() throws Exception {
      internalDuplicateIdTest(false);
   }

   @Test
   public void testClearDuplicateIdCache() throws Exception {
      internalDuplicateIdTest(true);
   }

   private void internalDuplicateIdTest(boolean clear) throws Exception {
      server.getConfiguration().setPersistIDCache(false);
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      AddressControl addressControl = createManagementControl(address);
      Assert.assertEquals(0, addressControl.getQueueNames().length);
      session.createQueue(address, RoutingType.ANYCAST, address);
      Assert.assertEquals(1, addressControl.getQueueNames().length);
      Map<String, String> headers = new HashMap<>();
      headers.put(Message.HDR_DUPLICATE_DETECTION_ID.toString(), UUID.randomUUID().toString());
      addressControl.sendMessage(headers, Message.BYTES_TYPE, Base64.encodeBytes("test".getBytes()), false, null, null);
      addressControl.sendMessage(headers, Message.BYTES_TYPE, Base64.encodeBytes("test".getBytes()), false, null, null);
      headers.clear();
      headers.put(Message.HDR_DUPLICATE_DETECTION_ID.toString(), UUID.randomUUID().toString());
      addressControl.sendMessage(headers, Message.BYTES_TYPE, Base64.encodeBytes("test".getBytes()), false, null, null);

      Wait.assertTrue(() -> addressControl.getCurrentDuplicateIdCacheSize() == 2);

      if (clear) {
         assertTrue(addressControl.clearDuplicateIdCache());
         Wait.assertTrue(() -> addressControl.getCurrentDuplicateIdCacheSize() == 0);
      }
   }

   @Test
   public void testPurge() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      AddressControl addressControl = createManagementControl(address);
      Assert.assertNotNull(addressControl);
      assertEquals(0, addressControl.getMessageCount());

      ClientProducer producer = session.createProducer(address.toString());
      producer.send(session.createMessage(false));
      assertEquals(0, addressControl.getMessageCount());

      session.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));
      producer.send(session.createMessage(false));
      assertTrue(Wait.waitFor(() -> addressControl.getMessageCount() == 1, 2000, 100));

      session.createQueue(new QueueConfiguration(address.concat('2')).setAddress(address).setRoutingType(RoutingType.ANYCAST));
      producer.send(session.createMessage(false));
      assertTrue(Wait.waitFor(() -> addressControl.getMessageCount() == 2, 2000, 100));

      assertEquals(2L, addressControl.purge());

      Wait.assertEquals(0L, () -> addressControl.getMessageCount(), 2000, 100);
   }

   @Test
   public void testReplayWithoutDate() throws Exception {
      testReplaySimple(false);
   }

   @Test
   public void testReplayWithDate() throws Exception {
      testReplaySimple(true);
   }

   private void testReplaySimple(boolean useDate) throws Exception {

      String queue = "testQueue" + RandomUtil.randomString();
      server.addAddressInfo(new AddressInfo(queue).addRoutingType(RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(queue).setRoutingType(RoutingType.ANYCAST).setAddress(queue));
      AddressControl addressControl = createManagementControl(SimpleString.toSimpleString(queue));

      ConnectionFactory factory = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue jmsQueue = session.createQueue(queue);
         MessageProducer producer = session.createProducer(jmsQueue);
         producer.send(session.createTextMessage("before"));

         connection.start();
         MessageConsumer consumer = session.createConsumer(jmsQueue);
         Assert.assertNotNull(consumer.receive(5000));
         Assert.assertNull(consumer.receiveNoWait());

         addressControl.replay(queue, null);
         Assert.assertNotNull(consumer.receive(5000));
         Assert.assertNull(consumer.receiveNoWait());

         if (useDate) {
            addressControl.replay("dontexist", null); // just to force a move next file, and copy stuff into place
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
            Thread.sleep(1000); // waiting a second just to have the timestamp change
            String dateEnd = format.format(new Date());
            Thread.sleep(1000); // waiting a second just to have the timestamp change
            String dateStart = "19800101000000";


            for (int i = 0; i < 100; i++) {
               producer.send(session.createTextMessage("after receiving"));
            }
            for (int i = 0; i < 100; i++) {
               Assert.assertNotNull(consumer.receive());
            }
            Assert.assertNull(consumer.receiveNoWait());
            addressControl.replay(dateStart, dateEnd, queue, null);
            for (int i = 0; i < 2; i++) { // replay of the replay will contain two messages
               TextMessage message = (TextMessage) consumer.receive(5000);
               Assert.assertNotNull(message);
               Assert.assertEquals("before", message.getText());
            }
            Assert.assertNull(consumer.receiveNoWait());
         } else {
            addressControl.replay(queue, null);

            // replay of the replay, there will be two messages
            for (int i = 0; i < 2; i++) {
               Assert.assertNotNull(consumer.receive(5000));
            }
            Assert.assertNull(consumer.receiveNoWait());
         }
      }
   }

   @Test
   public void testReplayFilter() throws Exception {

      String queue = "testQueue" + RandomUtil.randomString();
      server.addAddressInfo(new AddressInfo(queue).addRoutingType(RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(queue).setRoutingType(RoutingType.ANYCAST).setAddress(queue));

      AddressControl addressControl = createManagementControl(SimpleString.toSimpleString(queue));

      ConnectionFactory factory = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue jmsQueue = session.createQueue(queue);
         MessageProducer producer = session.createProducer(jmsQueue);
         for (int i = 0; i < 10; i++) {
            TextMessage message = session.createTextMessage("message " + i);
            message.setIntProperty("i", i);
            producer.send(message);
         }

         connection.start();
         MessageConsumer consumer = session.createConsumer(jmsQueue);
         for (int i = 0; i < 10; i++) {
            Assert.assertNotNull(consumer.receive(5000));
         }
         Assert.assertNull(consumer.receiveNoWait());

         addressControl.replay(queue, "i=5");
         TextMessage message = (TextMessage)consumer.receive(5000);
         Assert.assertNotNull(message);
         Assert.assertEquals(5, message.getIntProperty("i"));
         Assert.assertEquals("message 5", message.getText());
         Assert.assertNull(consumer.receiveNoWait());
      }
   }

   @Test
   public void testAddressSizeAfterRestart() throws Exception {
      session.close();
      server.stop();
      server.getConfiguration().setPersistenceEnabled(true);

      SimpleString address = RandomUtil.randomSimpleString();

      server.start();
      ServerLocator locator2 = createInVMNonHALocator();
      addServerLocator(locator2);
      ClientSessionFactory sf2 = createSessionFactory(locator2);

      session = sf2.createSession(false, true, false);
      session.start();
      session.createQueue(new QueueConfiguration(address));

      ClientProducer producer = session.createProducer(address);

      final int numMessages = 10;
      final int payLoadSize = 896;
      for (int i = 0; i < numMessages; i++) {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(new byte[payLoadSize]);
         producer.send(msg);
      }
      session.commit();

      AddressControl addressControl = createManagementControl(address);
      Assert.assertTrue(addressControl.getAddressSize() > numMessages * payLoadSize );

      // restart to reload journal
      server.stop();
      server.start();

      addressControl = createManagementControl(address);
      Assert.assertTrue(addressControl.getAddressSize() > numMessages * payLoadSize );
   }


   @Test
   public void testAddressSizeAfterRestartWithPaging() throws Exception {
      session.close();
      server.stop();
      server.getConfiguration().setPersistenceEnabled(true);

      final int payLoadSize = 896;
      final int pageLimitNumberOfMessages = 4;
      SimpleString address = RandomUtil.randomSimpleString();
      AddressSettings addressSettings = new AddressSettings().setPageSizeBytes(payLoadSize * 2).setMaxSizeBytes(payLoadSize * pageLimitNumberOfMessages);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      server.start();
      ServerLocator locator2 = createInVMNonHALocator();
      addServerLocator(locator2);
      ClientSessionFactory sf2 = createSessionFactory(locator2);

      session = sf2.createSession(false, true, false);
      session.start();
      session.createQueue(new QueueConfiguration(address));

      ClientProducer producer = session.createProducer(address);

      final int numMessages = 8;
      for (int i = 0; i < numMessages; i++) {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(new byte[payLoadSize]);
         producer.send(msg);
      }
      session.commit();

      AddressControl addressControl = createManagementControl(address);
      Assert.assertTrue(addressControl.getAddressSize() > pageLimitNumberOfMessages * payLoadSize );

      final long exactSizeValueBeforeRestart = addressControl.getAddressSize();
      final int exactPercentBeforeRestart = addressControl.getAddressLimitPercent();

      // restart to reload journal
      server.stop();
      server.start();

      addressControl = createManagementControl(address);
      Assert.assertTrue(addressControl.getAddressSize() > pageLimitNumberOfMessages * payLoadSize );
      Assert.assertEquals(exactSizeValueBeforeRestart, addressControl.getAddressSize());
      Assert.assertEquals(exactPercentBeforeRestart, addressControl.getAddressLimitPercent());
   }


   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      Configuration config = createDefaultNettyConfig().setJMXManagementEnabled(true);
      config.setJournalRetentionDirectory(config.getJournalDirectory() + "_ret"); // needed for replay tests
      server = createServer(true, config);
      server.setMBeanServer(mbeanServer);
      server.start();

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true);
      sf = createSessionFactory(locator);
      session = sf.createSession(false, true, false);
      session.start();
      addClientSession(session);
   }

   protected AddressControl createManagementControl(final SimpleString address) throws Exception {
      return ManagementControlHelper.createAddressControl(address, mbeanServer);
   }




}

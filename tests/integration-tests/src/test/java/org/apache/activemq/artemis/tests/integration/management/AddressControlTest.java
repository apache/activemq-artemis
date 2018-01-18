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
import javax.json.JsonString;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.RoleInfo;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

      session.createQueue(address, queue, false);

      AddressControl addressControl = createManagementControl(address);

      Assert.assertEquals(address.toString(), addressControl.getAddress());

      session.deleteQueue(queue);
   }

   @Test
   public void testGetQueueNames() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString anotherQueue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
      String[] queueNames = addressControl.getQueueNames();
      Assert.assertEquals(1, queueNames.length);
      Assert.assertEquals(queue.toString(), queueNames[0]);

      session.createQueue(address, anotherQueue, false);
      queueNames = addressControl.getQueueNames();
      Assert.assertEquals(2, queueNames.length);

      session.deleteQueue(queue);

      queueNames = addressControl.getQueueNames();
      Assert.assertEquals(1, queueNames.length);
      Assert.assertEquals(anotherQueue.toString(), queueNames[0]);

      session.deleteQueue(anotherQueue);
   }

   @Test
   public void testGetBindingNames() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      String divertName = RandomUtil.randomString();

      session.createQueue(address, queue, false);

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

      session.createQueue(address, queue, true);

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
      Assert.assertEquals(CheckType.SEND.hasRole(role), r[1]);
      Assert.assertEquals(CheckType.CONSUME.hasRole(role), r[2]);
      Assert.assertEquals(CheckType.CREATE_DURABLE_QUEUE.hasRole(role), r[3]);
      Assert.assertEquals(CheckType.DELETE_DURABLE_QUEUE.hasRole(role), r[4]);
      Assert.assertEquals(CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role), r[5]);
      Assert.assertEquals(CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role), r[6]);
      Assert.assertEquals(CheckType.MANAGE.hasRole(role), r[7]);

      session.deleteQueue(queue);
   }

   @Test
   public void testGetRolesAsJSON() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      Role role = new Role(RandomUtil.randomString(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean());

      session.createQueue(address, queue, true);

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
      session.createQueue(address, address, true);

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

      System.out.println("Address size=" + addressControl.getAddressSize());

      Assert.assertEquals(serverQueue.getPageSubscription().getPagingStore().getAddressSize(), addressControl.getAddressSize());
   }

   @Test
   public void testGetNumberOfBytesPerPage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createQueue(address, address, true);

      AddressControl addressControl = createManagementControl(address);
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalFileSize(), addressControl.getNumberOfBytesPerPage());

      session.close();
      server.stop();

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setPageSizeBytes(1024);

      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);
      server.start();
      ServerLocator locator2 = createInVMNonHALocator();
      ClientSessionFactory sf2 = createSessionFactory(locator2);

      session = sf2.createSession(false, true, false);
      session.createQueue(address, address, true);
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

      session.createQueue(address, RoutingType.ANYCAST, address);
      producer.send(session.createMessage(false));
      assertTrue(Wait.waitFor(() -> addressControl.getMessageCount() == 1, 2000, 100));

      session.createQueue(address, RoutingType.ANYCAST, address.concat('2'));
      producer.send(session.createMessage(false));
      assertTrue(Wait.waitFor(() -> addressControl.getMessageCount() == 2, 2000, 100));
   }

   @Test
   public void testSendMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      AddressControl addressControl = createManagementControl(address);
      Assert.assertEquals(0, addressControl.getQueueNames().length);
      session.createQueue(address, RoutingType.ANYCAST, address);
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      Configuration config = createDefaultInVMConfig().setJMXManagementEnabled(true);
      server = createServer(false, config);
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

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

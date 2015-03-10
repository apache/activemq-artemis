/**
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
package org.apache.activemq.tests.integration.management;

import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.api.core.management.AddressControl;
import org.apache.activemq.api.core.management.RoleInfo;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.core.security.CheckType;
import org.apache.activemq.core.security.Role;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.impl.QueueImpl;
import org.apache.activemq.core.settings.impl.AddressSettings;
import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.activemq.tests.util.RandomUtil.randomString;

public class AddressControlTest extends ManagementTestBase
{

   private ActiveMQServer server;
   protected ClientSession session;
   private ServerLocator locator;
   private ClientSessionFactory sf;

   @Test
   public void testGetAddress() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, false);

      AddressControl addressControl = createManagementControl(address);

      Assert.assertEquals(address.toString(), addressControl.getAddress());

      session.deleteQueue(queue);
   }

   @Test
   public void testGetQueueNames() throws Exception
   {
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
   public void testGetBindingNames() throws Exception
   {
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
   public void testGetRoles() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      Role role = new Role(RandomUtil.randomString(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean());

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
      Object[] roles = addressControl.getRoles();
      Assert.assertEquals(0, roles.length);

      Set<Role> newRoles = new HashSet<Role>();
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
   public void testGetRolesAsJSON() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      Role role = new Role(RandomUtil.randomString(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean());

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
      String jsonString = addressControl.getRolesAsJSON();
      Assert.assertNotNull(jsonString);
      RoleInfo[] roles = RoleInfo.from(jsonString);
      Assert.assertEquals(0, roles.length);

      Set<Role> newRoles = new HashSet<Role>();
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
   public void testGetNumberOfPages() throws Exception
   {
      session.close();
      server.stop();
      server.getConfiguration().setPersistenceEnabled(true);

      SimpleString address = RandomUtil.randomSimpleString();

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setPageSizeBytes(1024);
      addressSettings.setMaxSizeBytes(10 * 1024);
      final int NUMBER_MESSAGES_BEFORE_PAGING = 5;

      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);
      server.start();
      ServerLocator locator2 =
         ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(
            UnitTestCase.INVM_CONNECTOR_FACTORY));
      addServerLocator(locator2);
      ClientSessionFactory sf2 = createSessionFactory(locator2);

      session = sf2.createSession(false, true, false);
      session.start();
      session.createQueue(address, address, true);

      QueueImpl serverQueue = (QueueImpl) server.locateQueue(address);

      ClientProducer producer = session.createProducer(address);

      for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(new byte[512]);
         producer.send(msg);
      }
      session.commit();

      AddressControl addressControl = createManagementControl(address);
      Assert.assertEquals(0, addressControl.getNumberOfPages());

      ClientMessage msg = session.createMessage(true);
      msg.getBodyBuffer().writeBytes(new byte[512]);
      producer.send(msg);

      session.commit();
      Assert.assertEquals(1, addressControl.getNumberOfPages());

      msg = session.createMessage(true);
      msg.getBodyBuffer().writeBytes(new byte[512]);
      producer.send(msg);

      session.commit();
      Assert.assertEquals(1, addressControl.getNumberOfPages());

      msg = session.createMessage(true);
      msg.getBodyBuffer().writeBytes(new byte[512]);
      producer.send(msg);

      session.commit();

      Assert.assertEquals("# of pages is 2", 2, addressControl.getNumberOfPages());

      System.out.println("Address size=" + addressControl.getAddressSize());

      Assert.assertEquals(serverQueue.getPageSubscription().getPagingStore().getAddressSize(), addressControl.getAddressSize());
   }

   @Test
   public void testGetNumberOfBytesPerPage() throws Exception
   {
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
      ServerLocator locator2 = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(
         UnitTestCase.INVM_CONNECTOR_FACTORY));
      addServerLocator(locator2);
      ClientSessionFactory sf2 = createSessionFactory(locator2);

      session = sf2.createSession(false, true, false);
      session.createQueue(address, address, true);
      Assert.assertEquals(1024, addressControl.getNumberOfBytesPerPage());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createBasicConfig()
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      server = createServer(false, conf, mbeanServer);
      server.start();

      locator = createInVMNonHALocator();
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnNonDurableSend(true);
      sf = createSessionFactory(locator);
      session = sf.createSession(false, true, false);
      session.start();
      addClientSession(session);
   }

   protected AddressControl createManagementControl(final SimpleString address) throws Exception
   {
      return ManagementControlHelper.createAddressControl(address, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

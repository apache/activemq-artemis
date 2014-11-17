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

import org.junit.Test;

import org.junit.Assert;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.management.AddressControl;
import org.apache.activemq.api.core.management.ManagementHelper;
import org.apache.activemq.api.core.management.QueueControl;
import org.apache.activemq.api.core.management.ResourceNames;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.HornetQServers;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.core.server.impl.ServerMessageImpl;
import org.apache.activemq.core.server.management.impl.ManagementServiceImpl;
import org.apache.activemq.tests.integration.server.FakeStorageManager;
import org.apache.activemq.tests.unit.core.postoffice.impl.FakeQueue;
import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.UnitTestCase;

/*
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ManagementServiceImplTest extends UnitTestCase
{
   @Test
   public void testHandleManagementMessageWithOperation() throws Exception
   {
      String queue = RandomUtil.randomString();
      String address = RandomUtil.randomString();

      Configuration conf = createBasicConfig()
         .setJMXManagementEnabled(false);

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);
      ManagementHelper.putOperationInvocation(message, ResourceNames.CORE_SERVER, "createQueue", queue, address);

      ServerMessage reply = server.getManagementService().handleMessage(message);

      Assert.assertTrue(ManagementHelper.hasOperationSucceeded(reply));

      server.stop();
   }

   @Test
   public void testHandleManagementMessageWithOperationWhichFails() throws Exception
   {
      Configuration conf = createBasicConfig()
         .setJMXManagementEnabled(false);

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);
      ManagementHelper.putOperationInvocation(message, ResourceNames.CORE_SERVER, "thereIsNoSuchOperation");

      ServerMessage reply = server.getManagementService().handleMessage(message);

      Assert.assertFalse(ManagementHelper.hasOperationSucceeded(reply));
      Assert.assertNotNull(ManagementHelper.getResult(reply));
      server.stop();
   }

   @Test
   public void testHandleManagementMessageWithUnknowResource() throws Exception
   {
      Configuration conf = createBasicConfig()
         .setJMXManagementEnabled(false);

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);
      ManagementHelper.putOperationInvocation(message, "Resouce.Does.Not.Exist", "toString");

      ServerMessage reply = server.getManagementService().handleMessage(message);

      Assert.assertFalse(ManagementHelper.hasOperationSucceeded(reply));
      Assert.assertNotNull(ManagementHelper.getResult(reply));
      server.stop();
   }

   @Test
   public void testHandleManagementMessageWithUnknownAttribute() throws Exception
   {
      Configuration conf = createBasicConfig()
         .setJMXManagementEnabled(false);

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);

      ManagementHelper.putAttribute(message, ResourceNames.CORE_SERVER, "started");

      ServerMessage reply = server.getManagementService().handleMessage(message);

      Assert.assertTrue(ManagementHelper.hasOperationSucceeded(reply));
      Assert.assertTrue((Boolean)ManagementHelper.getResult(reply));
      server.stop();
   }

   @Test
   public void testHandleManagementMessageWithKnownAttribute() throws Exception
   {
      Configuration conf = createBasicConfig()
         .setJMXManagementEnabled(false);

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);

      ManagementHelper.putAttribute(message, ResourceNames.CORE_SERVER, "attribute.Does.Not.Exist");

      ServerMessage reply = server.getManagementService().handleMessage(message);

      Assert.assertFalse(ManagementHelper.hasOperationSucceeded(reply));
      Assert.assertNotNull(ManagementHelper.getResult(reply));
      server.stop();
   }

   @Test
   public void testGetResources() throws Exception
   {
      Configuration conf = createBasicConfig()
         .setJMXManagementEnabled(false);
      ManagementServiceImpl managementService = new ManagementServiceImpl(null, conf);
      managementService.setStorageManager(new NullStorageManager());

      SimpleString address = RandomUtil.randomSimpleString();
      managementService.registerAddress(address);
      Queue queue = new FakeQueue(RandomUtil.randomSimpleString());
      managementService.registerQueue(queue, RandomUtil.randomSimpleString(), new FakeStorageManager());

      Object[] addresses = managementService.getResources(AddressControl.class);
      Assert.assertEquals(1, addresses.length);
      Assert.assertTrue(addresses[0] instanceof AddressControl);
      AddressControl addressControl = (AddressControl)addresses[0];
      Assert.assertEquals(address.toString(), addressControl.getAddress());

      Object[] queues = managementService.getResources(QueueControl.class);
      Assert.assertEquals(1, queues.length);
      Assert.assertTrue(queues[0] instanceof QueueControl);
      QueueControl queueControl = (QueueControl)queues[0];
      Assert.assertEquals(queue.getName().toString(), queueControl.getName());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

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
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.LastValueQueue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SessionCreateAndDeleteQueueTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   private final SimpleString address = new SimpleString("address");

   private final SimpleString queueName = new SimpleString("queue");

   private ServerLocator locator;

   @Test
   public void testDurableFalse() throws Exception {
      ClientSession session = createSessionFactory(locator).createSession(false, true, true);
      session.createQueue(address, queueName, false);
      Binding binding = server.getPostOffice().getBinding(queueName);
      Queue q = (Queue) binding.getBindable();
      Assert.assertFalse(q.isDurable());

      session.close();
   }

   @Test
   public void testDurableTrue() throws Exception {
      ClientSession session = createSessionFactory(locator).createSession(false, true, true);
      session.createQueue(address, queueName, true);
      Binding binding = server.getPostOffice().getBinding(queueName);
      Queue q = (Queue) binding.getBindable();
      Assert.assertTrue(q.isDurable());

      session.close();
   }

   @Test
   public void testTemporaryFalse() throws Exception {
      ClientSession session = createSessionFactory(locator).createSession(false, true, true);
      session.createQueue(address, queueName, false);
      Binding binding = server.getPostOffice().getBinding(queueName);
      Queue q = (Queue) binding.getBindable();
      Assert.assertFalse(q.isTemporary());

      session.close();
   }

   @Test
   public void testTemporaryTrue() throws Exception {
      ClientSession session = createSessionFactory(locator).createSession(false, true, true);
      session.createTemporaryQueue(address, queueName);
      Binding binding = server.getPostOffice().getBinding(queueName);
      Queue q = (Queue) binding.getBindable();
      Assert.assertTrue(q.isTemporary());

      session.close();
   }

   @Test
   public void testcreateWithFilter() throws Exception {
      ClientSession session = createSessionFactory(locator).createSession(false, true, true);
      SimpleString filterString = new SimpleString("x=y");
      session.createQueue(address, queueName, filterString, false);
      Binding binding = server.getPostOffice().getBinding(queueName);
      Queue q = (Queue) binding.getBindable();
      Assert.assertEquals(q.getFilter().getFilterString(), filterString);

      session.close();
   }

   @Test
   public void testAddressSettingUSed() throws Exception {
      server.getAddressSettingsRepository().addMatch(address.toString(), new AddressSettings().setDefaultLastValueQueue(true));
      ClientSession session = createSessionFactory(locator).createSession(false, true, true);
      SimpleString filterString = new SimpleString("x=y");
      session.createQueue(address, queueName, filterString, false);
      Binding binding = server.getPostOffice().getBinding(queueName);
      Assert.assertTrue(binding.getBindable() instanceof LastValueQueue);

      session.close();
   }

   @Test
   public void testDeleteQueue() throws Exception {
      ClientSession session = createSessionFactory(locator).createSession(false, true, true);
      session.createQueue(address, queueName, false);
      Binding binding = server.getPostOffice().getBinding(queueName);
      Assert.assertNotNull(binding);
      session.deleteQueue(queueName);
      binding = server.getPostOffice().getBinding(queueName);
      Assert.assertNull(binding);
      session.close();
   }

   @Test
   public void testDeleteQueueNotExist() throws Exception {
      ClientSession session = createSessionFactory(locator).createSession(false, true, true);
      try {
         session.deleteQueue(queueName);
         Assert.fail("should throw exception");
      } catch (ActiveMQNonExistentQueueException neqe) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(false);
      server.start();
      locator = createInVMNonHALocator();
   }
}

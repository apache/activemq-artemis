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

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class PersistentManagementTest extends ManagementTestBase {

   private ClientSession session;
   private ServerLocator locator;
   private ClientSessionFactory sf;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      createManageableServer(true, true);

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true);
      sf = createSessionFactory(locator);
      session = sf.createSession(false, true, false);
      session.start();
      addClientSession(session);
   }

   @Test
   public void testSendOverSizeMessageOverQueueControl() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, true);

      QueueControl queueControl = createManagementControl(address, queue);

      int bodySize = server.getConfiguration().getJournalBufferSize_AIO();
      byte[] bigData = createBytesData(bodySize);

      try {
         queueControl.sendMessage(new HashMap<String, String>(), Message.BYTES_TYPE, Base64.encodeBytes(bigData), true, "myUser", "myPassword");
         fail("Expecting message being rejected.");
      } catch (Exception e) {
         //got rejected. ok
      }
   }

   @Test
   public void testSendOverSizeMessageOverAddressControl() throws Exception {

      SimpleString address = RandomUtil.randomSimpleString();
      session.createAddress(address, RoutingType.ANYCAST, false);

      AddressControl addressControl = createManagementControl(address);
      session.createQueue(address, RoutingType.ANYCAST, address);

      int bodySize = server.getConfiguration().getJournalBufferSize_AIO();
      byte[] bigData = createBytesData(bodySize);

      try {
         addressControl.sendMessage(null, Message.BYTES_TYPE, Base64.encodeBytes(bigData), false, null, null);
         fail("Expecting message being rejected.");
      } catch (Exception e) {
         //got rejected. ok
      }
   }

   byte[] createBytesData(int nbytes) {
      byte[] result = new byte[nbytes];
      for (int i = 0; i < nbytes; i++) {
         result[i] = RandomUtil.randomByte();
      }
      return result;
   }
}

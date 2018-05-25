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
package org.apache.activemq.artemis.tests.integration.server;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.junit.Wait;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MessageExpirationTest extends ActiveMQTestBase {

   private static final int EXPIRATION = 200;

   private ActiveMQServer server;

   private ClientSession session;

   private ClientSessionFactory sf;

   private ServerLocator locator;

   @Test
   public void testMessagesExpiredNoBindings() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString expiryAddress = RandomUtil.randomSimpleString();

      AddressSettings addressSettings = new AddressSettings().setExpiryAddress(expiryAddress);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      session.createQueue(address, RoutingType.MULTICAST, queue, null, true);

      Assert.assertEquals(0, server.locateQueue(queue).getMessagesExpired());

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(true);
      message.setExpiration(System.currentTimeMillis() + EXPIRATION);
      producer.send(message);

      Assert.assertTrue(Wait.waitFor(() -> server.locateQueue(queue).getMessagesExpired() == 1, 2000, 100));

      assertEquals(0, server.locateQueue(queue).getMessageCount());
      assertEquals(0, server.locateQueue(queue).getDeliveringCount());

      session.deleteQueue(queue);
   }

   @Test
   public void testMessagesExpiredNoExpiryAddress() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, true);

      Assert.assertEquals(0, server.locateQueue(queue).getMessagesExpired());

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(true);
      message.setExpiration(System.currentTimeMillis() + EXPIRATION);
      producer.send(message);

      Assert.assertTrue(Wait.waitFor(() -> server.locateQueue(queue).getMessagesExpired() == 1, 2000, 100));

      assertEquals(0, server.locateQueue(queue).getMessageCount());
      assertEquals(0, server.locateQueue(queue).getDeliveringCount());

      session.deleteQueue(queue);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false);

      server.getConfiguration().setMessageExpiryScanPeriod(500);

      server.start();
      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));
   }
}

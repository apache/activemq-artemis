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
package org.apache.activemq.artemis.tests.extras.byteman;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.activemq.artemis.api.core.Message.HDR_ROUTE_TO_IDS;

@RunWith(BMUnitRunner.class)
public class MessageRerouteTest extends ActiveMQTestBase {

   public static final SimpleString ADDRESS = new SimpleString("SimpleAddress");
   private static boolean poisonMessage = false;
   private static final String INTERNAL_PROP = HDR_ROUTE_TO_IDS + "sf.my-clusterxxxxxx";

   protected ActiveMQServer server = null;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(createDefaultNettyConfig());
      server.getConfiguration().setJMXManagementEnabled(true);

      server.start();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      if (server != null) {
         server.stop();
      }
      super.tearDown();
   }

   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "insert a internal property to message",
                     targetClass = "org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl",
                     targetMethod = "processRoute(org.apache.activemq.artemis.api.core.Message, org.apache.activemq.artemis.core.server.RoutingContext, boolean)",
                     targetLocation = "AT ENTRY",
                     action = "org.apache.activemq.artemis.tests.extras.byteman.MessageRerouteTest.insertProperty($1);"

                  )
            }
      )
   // this test inserts a fake prop to a message before PostOffice.processRoute()
   // then stop the server and restart again
   // attach a consumer to receive the message, make sure the fake prop is gone.
   public void testInternalPropertyRemoved() throws Exception {
      ServerLocator locator = createNettyNonHALocator();
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);

      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession("guest", null, false, true, true, false, 0);

      session.createQueue(ADDRESS, RoutingType.ANYCAST, ADDRESS, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeBytes(new byte[24]);

      poisonMessage = true;
      final int num = 20;
      for (int i = 0; i < num; i++) {
         producer.send(message);
      }

      poisonMessage = false;
      session.close();
      server.stop();

      server.start();
      waitForServerToStart(server);
      locator = createNettyNonHALocator();

      sf = createSessionFactory(locator);
      session = sf.createSession("guest", null, false, true, true, false, 0);
      session.start();
      ClientConsumer consumer = session.createConsumer(ADDRESS);
      for (int i = 0; i < num; i++) {
         ClientMessage message1 = consumer.receive(2000);
         System.out.println("message received: " + message1.getPropertyNames());
         assertFalse(message1.getPropertyNames().contains(new SimpleString(INTERNAL_PROP)));
      }

   }

   public static void insertProperty(Message serverMessage) {
      if (poisonMessage) {
         byte[] ids = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
         serverMessage.putBytesProperty(INTERNAL_PROP, ids);
      }
   }

}

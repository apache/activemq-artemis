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

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class CreateQueueTest extends ActiveMQTestBase {

   public final SimpleString addressA = new SimpleString("addressA");
   public final SimpleString addressB = new SimpleString("addressB");
   public final SimpleString queueA = new SimpleString("queueA");
   public final SimpleString queueB = new SimpleString("queueB");
   public final SimpleString queueC = new SimpleString("queueC");
   public final SimpleString queueD = new SimpleString("queueD");

   private ServerLocator locator;
   private ActiveMQServer server;
   private ClientSessionFactory cf;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      locator = createInVMNonHALocator();
      server = createServer(false);

      server.start();
      cf = createSessionFactory(locator);
   }

   @Test
   public void testUnsupportedRoutingType() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      server.getAddressSettingsRepository().addMatch(addressA.toString(), new AddressSettings().setAutoCreateAddresses(false));
      server.getAddressSettingsRepository().addMatch(addressB.toString(), new AddressSettings().setAutoCreateAddresses(false));

      EnumSet<RoutingType> routingTypes = EnumSet.of(RoutingType.ANYCAST);
      sendSession.createAddress(addressA, routingTypes, false);
      try {
         sendSession.createQueue(addressA, RoutingType.MULTICAST, queueA);
         fail("Creating a queue here should fail since the queue routing type differs from what is supported on the address.");
      } catch (Exception e) {
         assertTrue(e instanceof ActiveMQException);
         ActiveMQException ae = (ActiveMQException) e;
         assertEquals(ActiveMQExceptionType.INTERNAL_ERROR, ae.getType());
      }

      routingTypes = EnumSet.of(RoutingType.MULTICAST);
      sendSession.createAddress(addressB, routingTypes, false);
      try {
         sendSession.createQueue(addressB, RoutingType.ANYCAST, queueB);
         fail("Creating a queue here should fail since the queue routing type differs from what is supported on the address.");
      } catch (Exception e) {
         assertTrue(e instanceof ActiveMQException);
         ActiveMQException ae = (ActiveMQException) e;
         assertEquals(ActiveMQExceptionType.INTERNAL_ERROR, ae.getType());
      }
      sendSession.close();
   }

   @Test
   public void testAddressDoesNotExist() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      server.getAddressSettingsRepository().addMatch(addressA.toString(), new AddressSettings().setAutoCreateAddresses(false));
      Set<RoutingType> routingTypes = new HashSet<>();
      routingTypes.add(RoutingType.ANYCAST);
      try {
         sendSession.createQueue(addressA, RoutingType.MULTICAST, queueA);
         fail("Creating a queue here should fail since the queue's address doesn't exist and auto-create-addresses = false.");
      } catch (Exception e) {
         assertTrue(e instanceof ActiveMQException);
         ActiveMQException ae = (ActiveMQException) e;
         assertEquals(ActiveMQExceptionType.ADDRESS_DOES_NOT_EXIST, ae.getType());
      }
      sendSession.close();
   }
}

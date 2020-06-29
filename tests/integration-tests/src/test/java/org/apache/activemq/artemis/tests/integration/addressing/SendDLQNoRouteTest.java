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
package org.apache.activemq.artemis.tests.integration.addressing;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class SendDLQNoRouteTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   private ClientSessionFactory sessionFactory;

   @Before
   public void setup() throws Exception {
      server = createServer(true);
      server.start();
   }


   @Test(timeout = 20_000)
   public void testDLQNoRoute() throws Exception {
      AddressSettings addressSettings = new AddressSettings().setSendToDLAOnNoRoute(true);
      addressSettings.setDeadLetterAddress(SimpleString.toSimpleString("DLA"));
      server.getAddressSettingsRepository().addMatch("#", addressSettings);

      AddressInfo info = new AddressInfo(SimpleString.toSimpleString("info")).addRoutingType(RoutingType.MULTICAST);
      server.addAddressInfo(info);

      AddressInfo dla = new AddressInfo(SimpleString.toSimpleString("DLA")).addRoutingType(RoutingType.MULTICAST);
      server.addAddressInfo(dla);


      ServerLocator locator = createNonHALocator(false);

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(true, true);
      ClientProducer producer = session.createProducer("info");

      producer.send(session.createMessage(true));

      session.commit();


   }
}

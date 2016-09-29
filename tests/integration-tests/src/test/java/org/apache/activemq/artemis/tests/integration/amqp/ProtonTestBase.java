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
package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Before;

public class ProtonTestBase extends ActiveMQTestBase {

   protected String brokerName = "my-broker";
   protected ActiveMQServer server;

   protected String tcpAmqpConnectionUri = "tcp://localhost:5672";
   protected String userName = "guest";
   protected String password = "guest";

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = this.createServer(true, true);
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PORT_PROP_NAME, "5672");
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, "AMQP");
      HashMap<String, Object> amqpParams = new HashMap<>();
      configureAmqp(amqpParams);
      TransportConfiguration transportConfiguration = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, "amqp-acceptor", amqpParams);

      server.getConfiguration().getAcceptorConfigurations().add(transportConfiguration);
      server.getConfiguration().setName(brokerName);

      // Default Page
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      server.getConfiguration().getAddressesSettings().put("#", addressSettings);

      server.start();
   }

   protected void configureAmqp(Map<String, Object> params) {
   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         server.stop();
      } finally {
         super.tearDown();
      }
   }
}

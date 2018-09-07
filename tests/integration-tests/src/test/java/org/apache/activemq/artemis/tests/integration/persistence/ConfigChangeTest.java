/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.persistence;

import java.util.ArrayList;
import java.util.List;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.TextMessage;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Test;

public class ConfigChangeTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   @Test
   public void testChangeQueueRoutingTypeOnRestart() throws Exception {
      Configuration configuration = createDefaultInVMConfig();
      configuration.addAddressesSetting("#", new AddressSettings());

      List addressConfigurations = new ArrayList();
      CoreAddressConfiguration addressConfiguration = new CoreAddressConfiguration()
         .setName("myAddress")
         .addRoutingType(RoutingType.ANYCAST)
         .addQueueConfiguration(new CoreQueueConfiguration()
                                   .setName("myQueue")
                                   .setAddress("myAddress")
                                   .setRoutingType(RoutingType.ANYCAST));
      addressConfigurations.add(addressConfiguration);
      configuration.setAddressConfigurations(addressConfigurations);
      server = createServer(true, configuration);
      server.start();


      ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://0");
      try (JMSContext context = connectionFactory.createContext()) {
         context.createProducer().send(context.createQueue("myAddress"), "hello");
      }


      server.stop();

      addressConfiguration = new CoreAddressConfiguration()
         .setName("myAddress")
         .addRoutingType(RoutingType.MULTICAST)
         .addQueueConfiguration(new CoreQueueConfiguration()
                                   .setName("myQueue")
                                   .setAddress("myAddress")
                                   .setRoutingType(RoutingType.MULTICAST));
      addressConfigurations.clear();
      addressConfigurations.add(addressConfiguration);
      configuration.setAddressConfigurations(addressConfigurations);
      server.start();
      assertEquals(RoutingType.MULTICAST, server.getAddressInfo(SimpleString.toSimpleString("myAddress")).getRoutingType());
      assertEquals(RoutingType.MULTICAST, server.locateQueue(SimpleString.toSimpleString("myQueue")).getRoutingType());

      //Ensures the queue isnt detroyed by checking message sent before change is consumable after (e.g. no message loss)
      try (JMSContext context = connectionFactory.createContext()) {
         Message message = context.createSharedDurableConsumer(context.createTopic("myAddress"), "myQueue").receive();
         assertEquals("hello", ((TextMessage) message).getText());
      }

      server.stop();
   }
}

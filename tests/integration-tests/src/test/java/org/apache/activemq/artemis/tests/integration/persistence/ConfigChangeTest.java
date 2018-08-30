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

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.DeletionPolicy;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Test;

public class ConfigChangeTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   @Test
   public void testChangeQueueRoutingTypeOnRestart() throws Exception {
      internalTestChangeQueueRoutingTypeOnRestart(false);
   }

   @Test
   public void testChangeQueueRoutingTypeOnRestartNegative() throws Exception {
      internalTestChangeQueueRoutingTypeOnRestart(true);
   }

   public void internalTestChangeQueueRoutingTypeOnRestart(boolean negative) throws Exception {
      // if negative == true then the queue's routing type should *not* change

      Configuration configuration = createDefaultInVMConfig();
      configuration.addAddressesSetting("#", new AddressSettings()
         .setConfigDeleteQueues(negative ? DeletionPolicy.OFF : DeletionPolicy.FORCE)
         .setConfigDeleteAddresses(negative ? DeletionPolicy.OFF : DeletionPolicy.FORCE));

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
      assertEquals(negative ? RoutingType.ANYCAST : RoutingType.MULTICAST, server.getAddressInfo(SimpleString.toSimpleString("myAddress")).getRoutingType());
      assertEquals(negative ? RoutingType.ANYCAST : RoutingType.MULTICAST, server.locateQueue(SimpleString.toSimpleString("myQueue")).getRoutingType());
      server.stop();
   }
}

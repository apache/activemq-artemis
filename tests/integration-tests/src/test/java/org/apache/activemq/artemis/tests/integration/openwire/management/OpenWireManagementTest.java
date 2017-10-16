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
package org.apache.activemq.artemis.tests.integration.openwire.management;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.ConsumerEventSource;
import org.apache.activemq.advisory.ProducerEventSource;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.artemis.tests.integration.openwire.OpenWireTestBase;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Session;

public class OpenWireManagementTest extends OpenWireTestBase {

   private ActiveMQServerControl serverControl;
   private SimpleString queueName1 = new SimpleString("queue1");
   private SimpleString queueName2 = new SimpleString("queue2");;
   private SimpleString queueName3 = new SimpleString("queue3");;

   private ConnectionFactory factory;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      serverControl = (ActiveMQServerControl) ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getActiveMQServerObjectName(), ActiveMQServerControl.class, mbeanServer);
      factory = new ActiveMQConnectionFactory(urlString);
   }

   @Override
   protected void extraServerConfig(Configuration serverConfig) {
      serverConfig.setJMXManagementEnabled(true);
   }

   @Test
   public void testHiddenInternalAddress() throws Exception {
      server.createQueue(queueName1, RoutingType.ANYCAST, queueName1, null, true, false, -1, false, true);
      server.createQueue(queueName2, RoutingType.ANYCAST, queueName2, null, true, false, -1, false, true);
      server.createQueue(queueName3, RoutingType.ANYCAST, queueName3, null, true, false, -1, false, true);


      String[] addresses = serverControl.getAddressNames();
      assertEquals(3, addresses.length);
      for (String addr : addresses) {
         assertFalse(addr.startsWith(AddressInfo.ADVISORY_TOPIC.toString()));
      }

      try (Connection connection = factory.createConnection()) {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination destination = session.createQueue(queueName1.toString());

         ConsumerEventSource consumerEventSource = new ConsumerEventSource(connection, destination);
         consumerEventSource.setConsumerListener(consumerEvent -> {
         });
         consumerEventSource.start();

         ProducerEventSource producerEventSource = new ProducerEventSource(connection, destination);
         producerEventSource.setProducerListener(producerEvent -> {
         });
         producerEventSource.start();

         //after that point several advisory addresses are created.
         //make sure they are not accessible via management api.
         addresses = serverControl.getAddressNames();
         for (String addr : addresses) {
            assertFalse(addr.startsWith(AddressInfo.ADVISORY_TOPIC.toString()));
         }
         consumerEventSource.stop();
         producerEventSource.stop();
      }
   }
}

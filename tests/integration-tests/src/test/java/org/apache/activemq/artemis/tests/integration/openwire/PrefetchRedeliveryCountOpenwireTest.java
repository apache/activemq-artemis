/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.openwire;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.Map;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Assert;
import org.junit.Test;

public class PrefetchRedeliveryCountOpenwireTest extends OpenWireTestBase {

   @Override
   public void setUp() throws Exception {
      realStore = true;
      super.setUp();
   }

   @Override
   protected void configureAddressSettings(Map<String, AddressSettings> addressSettingsMap) {
      super.configureAddressSettings(addressSettingsMap);
      // force send to dlq early
      addressSettingsMap.get("#").setMaxDeliveryAttempts(2);
   }

   @Test(timeout = 60_000)
   public void testConsumerSingleMessageLoop() throws Exception {
      Connection exConn = null;

      SimpleString durableQueue = new SimpleString("exampleQueue");
      this.server.createQueue(new QueueConfiguration(durableQueue).setRoutingType(RoutingType.ANYCAST).setExclusive(true));

      try {
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();
         exFact.setWatchTopicAdvisories(false);

         Queue queue = new ActiveMQQueue("exampleQueue");

         exConn = exFact.createConnection();

         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(queue);

         TextMessage message = session.createTextMessage("This is a text message");

         int numMessages = 20;
         for (int i = 0; i < numMessages; i++) {
            producer.send(message);
         }

         for (int i = 0; i < numMessages; i++) {
            // consumer per message
            MessageConsumer messageConsumer = session.createConsumer(queue);

            TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);
            Assert.assertNotNull(messageReceived);

            assertEquals("This is a text message", messageReceived.getText());
            messageConsumer.close();
         }
      } finally {
         if (exConn != null) {
            exConn.close();
         }
      }
   }
}

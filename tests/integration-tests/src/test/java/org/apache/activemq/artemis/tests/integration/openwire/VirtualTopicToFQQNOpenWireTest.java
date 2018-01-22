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
package org.apache.activemq.artemis.tests.integration.openwire;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Set;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.junit.Test;

public class VirtualTopicToFQQNOpenWireTest extends OpenWireTestBase {

   @Override
   protected void extraServerConfig(Configuration serverConfig) {
      Set<TransportConfiguration> acceptors = server.getConfiguration().getAcceptorConfigurations();
      for (TransportConfiguration tc : acceptors) {
         if (tc.getName().equals("netty")) {
            tc.getExtraParams().put("virtualTopicConsumerWildcards", "Consumer.*.>;2");
            tc.getExtraParams().put("virtualTopicConsumerLruCacheMax", "10000");

         }
      }
   }

   @Test
   public void testAutoVirtualTopicFQQN() throws Exception {
      Connection connection = null;

      SimpleString topic = new SimpleString("VirtualTopic.Orders");
      this.server.getAddressSettingsRepository().getMatch("VirtualTopic.#").setAutoCreateQueues(true);
      this.server.getAddressSettingsRepository().getMatch("VirtualTopic.#").setAutoCreateAddresses(true);

      try {
         ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(urlString);
         activeMQConnectionFactory.setWatchTopicAdvisories(false);
         connection = activeMQConnectionFactory.createConnection();
         connection.start();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination destination = session.createTopic(topic.toString());

         MessageConsumer messageConsumerA = session.createConsumer(session.createQueue("Consumer.A." + topic.toString()));
         MessageConsumer messageConsumerB = session.createConsumer(session.createQueue("Consumer.B." + topic.toString()));

         MessageProducer producer = session.createProducer(destination);
         TextMessage message = session.createTextMessage("This is a text message");
         producer.send(message);

         TextMessage messageReceivedA = (TextMessage) messageConsumerA.receive(2000);
         TextMessage messageReceivedB = (TextMessage) messageConsumerB.receive(2000);

         assertTrue((messageReceivedA != null && messageReceivedB != null));
         String text = messageReceivedA.getText();
         assertEquals("This is a text message", text);

         messageConsumerA.close();
         messageConsumerB.close();

      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }
}

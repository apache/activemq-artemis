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
package org.apache.activemq.artemis.tests.integration.openwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

public class OpenWireDivertNonExclusiveTest extends OpenWireDivertTestBase {

   @Override
   protected boolean isExclusive() {
      return false;
   }

   @Test
   //core sending, openwire receiving
   public void testSingleNonExclusiveDivert() throws Exception {
      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession coreSession = sf.createSession(false, true, true);

      final SimpleString queueName1 = SimpleString.of("queue1");

      final SimpleString queueName2 = SimpleString.of("queue2");

      coreSession.createQueue(QueueConfiguration.of(queueName1).setAddress(forwardAddress).setDurable(false));

      coreSession.createQueue(QueueConfiguration.of(queueName2).setAddress(testAddress).setDurable(false));

      ClientProducer producer = coreSession.createProducer(SimpleString.of(testAddress));
      final int numMessages = 1;

      final SimpleString propKey = SimpleString.of("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = coreSession.createMessage(false);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      coreSession.close();

      //use openwire to receive
      factory = new ActiveMQConnectionFactory(urlString);
      Connection openwireConnection = factory.createConnection();

      try {
         Session session = openwireConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         openwireConnection.start();

         Queue q1 = session.createQueue(CompositeAddress.toFullyQualified(testAddress, "queue1"));
         Queue q2 = session.createQueue(CompositeAddress.toFullyQualified(forwardAddress, "queue2"));

         MessageConsumer consumer1 = session.createConsumer(q1);
         MessageConsumer consumer2 = session.createConsumer(q2);

         for (int i = 0; i < numMessages; i++) {
            Message message = consumer1.receive(TIMEOUT);

            assertNotNull(message);

            assertEquals(i, message.getObjectProperty(propKey.toString()));

            message.acknowledge();
         }

         assertNull(consumer1.receive(50));

         for (int i = 0; i < numMessages; i++) {
            Message message = consumer2.receive(TIMEOUT);

            assertNotNull(message);

            assertEquals(i, message.getObjectProperty(propKey.toString()));

            message.acknowledge();
         }

         assertNull(consumer2.receive(50));
      } finally {
         if (openwireConnection != null) {
            openwireConnection.close();
         }
      }
   }

   @Test
   //openwire sending, openwire receiving
   public void testSingleNonExclusiveDivertOpenWirePublisher() throws Exception {
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession coreSession = sf.createSession(false, true, true);

      final SimpleString queueName1 = SimpleString.of("queue1");
      final SimpleString queueName2 = SimpleString.of("queue2");

      coreSession.createQueue(QueueConfiguration.of(queueName1).setAddress(forwardAddress).setRoutingType(RoutingType.ANYCAST).setDurable(false));
      coreSession.createQueue(QueueConfiguration.of(queueName2).setAddress(testAddress).setRoutingType(RoutingType.ANYCAST).setDurable(false));
      coreSession.close();

      //use openwire to receive
      factory = new ActiveMQConnectionFactory(urlString);
      Connection openwireConnection = factory.createConnection();

      try {
         Session session = openwireConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         openwireConnection.start();

         MessageProducer producer = session.createProducer(session.createQueue(testAddress));

         final int numMessages = 10;
         final String propKey = "testkey";

         for (int i = 0; i < numMessages; i++) {
            Message message = session.createMessage();
            message.setIntProperty(propKey, i);
            producer.send(message);
         }

         Queue q1 = session.createQueue(CompositeAddress.toFullyQualified(testAddress, "queue1"));
         Queue q2 = session.createQueue(CompositeAddress.toFullyQualified(forwardAddress, "queue2"));

         MessageConsumer consumer1 = session.createConsumer(q1);
         MessageConsumer consumer2 = session.createConsumer(q2);

         for (int i = 0; i < numMessages; i++) {
            Message message = consumer1.receive(TIMEOUT);
            assertNotNull(message);
            assertEquals(i, message.getObjectProperty(propKey.toString()));
            message.acknowledge();
         }

         assertNull(consumer1.receive(50));

         for (int i = 0; i < numMessages; i++) {
            Message message = consumer2.receive(TIMEOUT);
            assertNotNull(message);
            assertEquals(i, message.getObjectProperty(propKey.toString()));
            message.acknowledge();
         }

         assertNull(consumer2.receive(50));
      } finally {
         if (openwireConnection != null) {
            openwireConnection.close();
         }
      }
   }

}

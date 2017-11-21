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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.junit.Before;
import org.junit.Test;

public class OpenWireLargeMessageTest extends BasicOpenWireTest {

   public OpenWireLargeMessageTest() {
      super();
   }

   public SimpleString lmAddress = new SimpleString("LargeMessageAddress");

   @Override
   @Before
   public void setUp() throws Exception {
      this.realStore = true;
      super.setUp();
      server.createQueue(lmAddress, RoutingType.ANYCAST, lmAddress, null, true, false);
   }

   @Test
   public void testSendLargeMessage() throws Exception {
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(lmAddress.toString());
         MessageProducer producer = session.createProducer(queue);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);

         // Create 1MB Message
         int size = 1024 * 1024;
         byte[] bytes = new byte[size];
         BytesMessage message = session.createBytesMessage();
         message.writeBytes(bytes);
         producer.send(message);
      }
   }

   @Test
   public void testSendReceiveLargeMessage() throws Exception {
      // Create 1MB Message
      int size = 1024 * 1024;

      byte[] bytes = new byte[size];

      try (Connection connection = factory.createConnection()) {
         connection.start();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(lmAddress.toString());
         MessageProducer producer = session.createProducer(queue);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);

         bytes[0] = 1;

         BytesMessage message = session.createBytesMessage();
         message.writeBytes(bytes);
         producer.send(message);
      }

      server.stop();
      server.start();

      try (Connection connection = factory.createConnection()) {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(lmAddress.toString());


         MessageConsumer consumer = session.createConsumer(queue);
         BytesMessage m = (BytesMessage) consumer.receive();
         assertNotNull(m);

         byte[] body = new byte[size];
         m.readBytes(body);

         assertArrayEquals(body, bytes);
      }
   }
}

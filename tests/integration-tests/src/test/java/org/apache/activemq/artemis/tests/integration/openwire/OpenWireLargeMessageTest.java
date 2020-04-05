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
import java.util.Map;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OpenWireLargeMessageTest extends BasicOpenWireTest {

   public OpenWireLargeMessageTest() {
      super();
   }

   public SimpleString lmAddress = new SimpleString("LargeMessageAddress");
   public SimpleString lmDropAddress = new SimpleString("LargeMessageDropAddress");

   @Override
   @Before
   public void setUp() throws Exception {
      this.realStore = true;
      super.setUp();
      server.createQueue(new QueueConfiguration(lmAddress).setRoutingType(RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(lmDropAddress).setRoutingType(RoutingType.ANYCAST));
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

   @Override
   protected void configureAddressSettings(Map<String, AddressSettings> addressSettingsMap) {
      addressSettingsMap.put("#", new AddressSettings().setAutoCreateQueues(false).setAutoCreateAddresses(false).setDeadLetterAddress(new SimpleString("ActiveMQ.DLQ")).setAutoCreateAddresses(true));
      addressSettingsMap.put(lmDropAddress.toString(),
                             new AddressSettings()
                                .setMaxSizeBytes(100 * 1024)
                                .setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP)
                                .setMessageCounterHistoryDayLimit(10)
                                .setRedeliveryDelay(0)
                                .setMaxDeliveryAttempts(0));
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

   @Test
   public void testFastLargeMessageProducerDropOnPaging() throws Exception {
      AssertionLoggerHandler.startCapture();
      try {
         // Create 100K Message
         int size = 100 * 1024;

         final byte[] bytes = new byte[size];

         try (Connection connection = factory.createConnection()) {
            connection.start();

            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
               Queue queue = session.createQueue(lmDropAddress.toString());
               try (MessageProducer producer = session.createProducer(queue)) {
                  producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                  bytes[0] = 1;

                  BytesMessage message = session.createBytesMessage();
                  message.writeBytes(bytes);

                  final PagingStore pageStore = server.getPagingManager().getPageStore(lmDropAddress);
                  while (!pageStore.isPaging()) {
                     producer.send(message);
                  }
                  for (int i = 0; i < 10; i++) {
                     producer.send(message);
                  }
                  final long messageCount = server.locateQueue(lmDropAddress).getMessageCount();
                  Assert.assertTrue("The queue cannot be empty", messageCount > 0);
                  try (MessageConsumer messageConsumer = session.createConsumer(queue)) {
                     for (long m = 0; m < messageCount; m++) {
                        if (messageConsumer.receive(2000) == null) {
                           Assert.fail("The messages are not finished yet");
                        }
                     }
                  }
               }
            }
         }
         server.stop();
         Assert.assertFalse(AssertionLoggerHandler.findText("NullPointerException"));
         Assert.assertFalse(AssertionLoggerHandler.findText("It was not possible to delete message"));
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }
}

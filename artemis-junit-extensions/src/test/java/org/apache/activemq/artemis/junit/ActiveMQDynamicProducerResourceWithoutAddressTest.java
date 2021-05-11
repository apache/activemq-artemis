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
package org.apache.activemq.artemis.junit;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class ActiveMQDynamicProducerResourceWithoutAddressTest {

   static final SimpleString TEST_QUEUE_ONE = new SimpleString("test.queue.one");
   static final SimpleString TEST_QUEUE_TWO = new SimpleString("test.queue.two");
   static final String TEST_BODY = "Test Message";
   static final Map<String, Object> TEST_PROPERTIES;

   static final String ASSERT_SENT_FORMAT = "Message should have been sent to %s";
   static final String ASSERT_RECEIVED_FORMAT = "Message should have been received from %s";
   static final String ASSERT_COUNT_FORMAT = "Unexpected message count in queue %s";

   static {
      TEST_PROPERTIES = new HashMap<String, Object>(2);
      TEST_PROPERTIES.put("PropertyOne", "Property Value 1");
      TEST_PROPERTIES.put("PropertyTwo", "Property Value 2");
   }

   @RegisterExtension
   @Order(1)
   public EmbeddedActiveMQExtension server = new EmbeddedActiveMQExtension();

   @RegisterExtension
   @Order(2)
   public ActiveMQDynamicProducerExtension producer = new ActiveMQDynamicProducerExtension(server.getVmURL());

   ClientMessage sentOne = null;
   ClientMessage sentTwo = null;

   @BeforeAll
   public void setUp() {
      producer.setAutoCreateQueue(false);
      server.createQueue(TEST_QUEUE_ONE, TEST_QUEUE_ONE);
      server.createQueue(TEST_QUEUE_TWO, TEST_QUEUE_TWO);
   }

   @AfterAll
   public void tearDown() {
      assertNotNull(sentOne, String.format(ASSERT_SENT_FORMAT, TEST_QUEUE_ONE));
      assertNotNull(sentTwo, String.format(ASSERT_SENT_FORMAT, TEST_QUEUE_TWO));

      ClientMessage receivedOne = server.receiveMessage(TEST_QUEUE_ONE);
      assertNotNull(receivedOne, String.format(ASSERT_RECEIVED_FORMAT, TEST_QUEUE_ONE));

      ClientMessage receivedTwo = server.receiveMessage(TEST_QUEUE_TWO);
      assertNotNull(receivedTwo, String.format(ASSERT_RECEIVED_FORMAT, TEST_QUEUE_TWO));
   }

   @Test
   public void testSendBytes() {
      sentOne = producer.sendMessage(TEST_QUEUE_ONE, TEST_BODY.getBytes());
      sentTwo = producer.sendMessage(TEST_QUEUE_TWO, TEST_BODY.getBytes());
   }

   @Test
   public void testSendString() {
      sentOne = producer.sendMessage(TEST_QUEUE_ONE, TEST_BODY);
      sentTwo = producer.sendMessage(TEST_QUEUE_TWO, TEST_BODY);
   }

   @Test
   public void testSendBytesAndProperties() {
      sentOne = producer.sendMessage(TEST_QUEUE_ONE, TEST_BODY.getBytes(), TEST_PROPERTIES);
      sentTwo = producer.sendMessage(TEST_QUEUE_TWO, TEST_BODY.getBytes(), TEST_PROPERTIES);
   }

   @Test
   public void testSendStringAndProperties() {
      sentOne = producer.sendMessage(TEST_QUEUE_ONE, TEST_BODY, TEST_PROPERTIES);
      sentTwo = producer.sendMessage(TEST_QUEUE_TWO, TEST_BODY, TEST_PROPERTIES);
   }

}

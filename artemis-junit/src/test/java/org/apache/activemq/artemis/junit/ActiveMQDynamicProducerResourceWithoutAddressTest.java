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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

      ThreadLeakCheckRule.addKownThread("MemoryPoolMXBean notification dispatcher");
      ThreadLeakCheckRule.addKownThread("threadDeathWatcher");
   }

   EmbeddedActiveMQResource server = new EmbeddedActiveMQResource();

   ActiveMQDynamicProducerResource producer = new ActiveMQDynamicProducerResource(server.getVmURL());

   @Rule
   public RuleChain ruleChain = RuleChain.outerRule(new ThreadLeakCheckRule()).around(server).around(producer);

   ClientMessage sentOne = null;
   ClientMessage sentTwo = null;

   @Before
   public void setUp() throws Exception {
      producer.setAutoCreateQueue(false);
      server.createQueue(TEST_QUEUE_ONE, TEST_QUEUE_ONE);
      server.createQueue(TEST_QUEUE_TWO, TEST_QUEUE_TWO);
   }

   @After
   public void tearDown() throws Exception {
      assertNotNull(String.format(ASSERT_SENT_FORMAT, TEST_QUEUE_ONE), sentOne);
      assertNotNull(String.format(ASSERT_SENT_FORMAT, TEST_QUEUE_TWO), sentTwo);
      Wait.waitFor(new Wait.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return server.getMessageCount(TEST_QUEUE_ONE) == 1 && server.getMessageCount(TEST_QUEUE_TWO) == 1;
         }
      }, 5000, 100);
      assertEquals(String.format(ASSERT_COUNT_FORMAT, TEST_QUEUE_ONE), 1, server.getMessageCount(TEST_QUEUE_ONE));
      assertEquals(String.format(ASSERT_COUNT_FORMAT, TEST_QUEUE_TWO), 1, server.getMessageCount(TEST_QUEUE_TWO));

      ClientMessage receivedOne = server.receiveMessage(TEST_QUEUE_ONE);
      assertNotNull(String.format(ASSERT_RECEIVED_FORMAT, TEST_QUEUE_ONE), receivedOne);

      ClientMessage receivedTwo = server.receiveMessage(TEST_QUEUE_TWO);
      assertNotNull(String.format(ASSERT_RECEIVED_FORMAT, TEST_QUEUE_TWO), receivedTwo);

      server.stop();
   }

   @Test
   public void testSendBytes() throws Exception {
      sentOne = producer.sendMessage(TEST_QUEUE_ONE, TEST_BODY.getBytes());
      sentTwo = producer.sendMessage(TEST_QUEUE_TWO, TEST_BODY.getBytes());
   }

   @Test
   public void testSendString() throws Exception {
      sentOne = producer.sendMessage(TEST_QUEUE_ONE, TEST_BODY);
      sentTwo = producer.sendMessage(TEST_QUEUE_TWO, TEST_BODY);
   }

   @Test
   public void testSendBytesAndProperties() throws Exception {
      sentOne = producer.sendMessage(TEST_QUEUE_ONE, TEST_BODY.getBytes(), TEST_PROPERTIES);
      sentTwo = producer.sendMessage(TEST_QUEUE_TWO, TEST_BODY.getBytes(), TEST_PROPERTIES);
   }

   @Test
   public void testSendStringAndProperties() throws Exception {
      sentOne = producer.sendMessage(TEST_QUEUE_ONE, TEST_BODY, TEST_PROPERTIES);
      sentTwo = producer.sendMessage(TEST_QUEUE_TWO, TEST_BODY, TEST_PROPERTIES);
   }

}

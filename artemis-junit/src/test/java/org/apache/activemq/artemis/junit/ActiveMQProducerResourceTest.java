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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import static org.junit.Assert.assertNotNull;

public class ActiveMQProducerResourceTest {

   static final SimpleString TEST_QUEUE = new SimpleString("test.queue");
   static final SimpleString TEST_ADDRESS = new SimpleString("test.queue");
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

   ActiveMQProducerResource producer = new ActiveMQProducerResource(server.getVmURL(), TEST_ADDRESS);

   @Rule
   public RuleChain ruleChain = RuleChain.outerRule(new ThreadLeakCheckRule()).around(server).around(producer);


   ClientMessage sent = null;

   @After
   public void checkResults() throws Exception {
      assertNotNull(String.format(ASSERT_SENT_FORMAT, TEST_ADDRESS), sent);

      ClientMessage received = server.receiveMessage(TEST_QUEUE);
      assertNotNull(String.format(ASSERT_RECEIVED_FORMAT, TEST_QUEUE), received);
   }

   @Test
   public void testSendBytes() throws Exception {
      sent = producer.sendMessage(TEST_BODY.getBytes());
   }

   @Test
   public void testSendString() throws Exception {
      sent = producer.sendMessage(TEST_BODY);
   }

   @Test
   public void testSendBytesAndProperties() throws Exception {
      sent = producer.sendMessage(TEST_BODY.getBytes(), TEST_PROPERTIES);
   }

   @Test
   public void testSendStringAndProperties() throws Exception {
      sent = producer.sendMessage(TEST_BODY, TEST_PROPERTIES);
   }

}

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
package org.apache.activemq.artemis.tests.integration.jms.multiprotocol;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.artemis.utils.CompositeAddress;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Test;

public class JMSFQQNConsumerTest extends MultiprotocolJMSClientTestSupport {

   @Test
   public void testFQQNTopicConsumerWithSelectorAMQP() throws Exception {
      testFQQNTopicConsumerWithSelector(AMQPConnection);
   }

   @Test
   public void testFQQNTopicConsumerWithSelectorOpenWire() throws Exception {
      testFQQNTopicConsumerWithSelector(OpenWireConnection);
   }

   @Test
   public void testFQQNTopicConsumerWithSelectorCore() throws Exception {
      testFQQNTopicConsumerWithSelector(CoreConnection);
   }

   private void testFQQNTopicConsumerWithSelector(ConnectionSupplier supplier) throws Exception {
      final String queue = "queue";
      final String address = "address";
      final String filter = "prop='value'";
      try (Connection c = supplier.createConnection()) {
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic t = s.createTopic(CompositeAddress.toFullyQualified(address, queue));
         MessageConsumer mc = s.createConsumer(t, filter);
         Wait.assertTrue(() -> server.locateQueue(queue) != null, 2000, 100);
         assertNotNull(server.locateQueue(queue).getFilter());
         assertEquals(filter, server.locateQueue(queue).getFilter().getFilterString().toString());
      }
   }

   @Test
   public void testFQQNQueueConsumerWithSelectorAMQP() throws Exception {
      testFQQNQueueConsumerWithSelector(AMQPConnection);
   }

   @Test
   public void testFQQNQueueConsumerWithSelectorOpenWire() throws Exception {
      testFQQNQueueConsumerWithSelector(OpenWireConnection);
   }

   @Test
   public void testFQQNQueueConsumerWithSelectorCore() throws Exception {
      testFQQNQueueConsumerWithSelector(CoreConnection);
   }

   private void testFQQNQueueConsumerWithSelector(ConnectionSupplier supplier) throws Exception {
      final String queue = "queue";
      final String address = "address";
      final String prop = "prop";
      final String value = RandomUtil.randomString();
      final String filter = prop + "='" + value + "'";
      try (Connection c = supplier.createConnection()) {
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue q = s.createQueue(CompositeAddress.toFullyQualified(address, queue));
         MessageConsumer mc = s.createConsumer(q, filter);
         Wait.assertTrue(() -> server.locateQueue(queue) != null, 2000, 100);
         assertNull(server.locateQueue(queue).getFilter());
         MessageProducer p = s.createProducer(q);
         Message m = s.createMessage();
         m.setStringProperty(prop, value);
         p.send(m);
         c.start();
         assertNotNull(mc.receive(1000));
         m = s.createMessage();
         m.setStringProperty(prop, RandomUtil.randomString());
         assertNull(mc.receive(1000));
      }
   }
}
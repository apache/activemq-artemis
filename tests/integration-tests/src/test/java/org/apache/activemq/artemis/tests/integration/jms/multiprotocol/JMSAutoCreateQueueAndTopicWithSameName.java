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
import javax.jms.Session;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JMSAutoCreateQueueAndTopicWithSameName extends MultiprotocolJMSClientTestSupport {

   @Test
   public void testAutoCreateTopicThenQueueCore() throws Exception {
      testAutoCreateTopicThenQueue(createCoreConnection());
   }

   @Test
   public void testAutoCreateTopicThenQueueOpenWire() throws Exception {
      testAutoCreateTopicThenQueue(createOpenWireConnection());
   }

   @Disabled // disable for now since AMQP doesn't support this at all
   @Test
   public void testAutoCreateTopicThenQueueAMQP() throws Exception {
      testAutoCreateTopicThenQueue(createConnection());
   }

   private void testAutoCreateTopicThenQueue(Connection c) throws Exception {
      Session s = c.createSession();
      s.createConsumer(s.createTopic(getName()));
      s.createConsumer(s.createQueue(getName()));
      verifyBindings(SimpleString.of(getName()));
      c.close();
   }

   @Test
   public void testAutoCreateQueueThenTopicCore() throws Exception {
      testAutoCreateQueueThenTopic(createCoreConnection());
   }

   @Test
   public void testAutoCreateQueueThenTopicOpenWire() throws Exception {
      testAutoCreateQueueThenTopic(createOpenWireConnection());
   }

   @Disabled // disable for now since AMQP doesn't support this at all
   @Test
   public void testAutoCreateQueueThenTopicAMQP() throws Exception {
      testAutoCreateQueueThenTopic(createConnection());
   }

   private void testAutoCreateQueueThenTopic(Connection c) throws Exception {
      Session s = c.createSession();
      s.createConsumer(s.createQueue(getName()));
      s.createConsumer(s.createTopic(getName()));
      verifyBindings(SimpleString.of(getName()));
      c.close();
   }

   private void verifyBindings(SimpleString address) throws Exception {
      Bindings bindings = server.getPostOffice().getBindingsForAddress(address);
      assertEquals(2, bindings.size());
      int multicastCount = 0;
      int anycastCount = 0;
      for (Binding binding : bindings.getBindings()) {
         assertTrue(binding instanceof LocalQueueBinding);
         if (((LocalQueueBinding) binding).getQueue().getRoutingType() == RoutingType.ANYCAST) {
            anycastCount++;
            assertEquals(address, ((LocalQueueBinding) binding).getQueue().getName());
         } else {
            multicastCount++;
         }
      }
      assertEquals(1, multicastCount);
      assertEquals(1, anycastCount);
   }
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.mqtt.imported;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTConnectionManager;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTSession;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTFQQNTest extends MQTTTestSupport {

   private static final Logger LOG = LoggerFactory.getLogger(MQTTFQQNTest.class);

   @Override
   @Before
   public void setUp() throws Exception {
      Field sessions = MQTTSession.class.getDeclaredField("SESSIONS");
      sessions.setAccessible(true);
      sessions.set(null, new ConcurrentHashMap<>());

      Field connectedClients = MQTTConnectionManager.class.getDeclaredField("CONNECTED_CLIENTS");
      connectedClients.setAccessible(true);
      connectedClients.set(null, new ConcurrentHashSet<>());
      super.setUp();

   }

   @Override
   @After
   public void tearDown() throws Exception {
      super.tearDown();

   }

   @Test
   public void testMQTTSubNames() throws Exception {
      final MQTTClientProvider subscriptionProvider = getMQTTClientProvider();
      initializeConnection(subscriptionProvider);

      try {
         subscriptionProvider.subscribe("foo/bah", AT_MOST_ONCE);

         Map<SimpleString, Binding> allBindings = server.getPostOffice().getAllBindings();
         assertEquals(1, allBindings.size());
         Binding b = allBindings.values().iterator().next();
         //check that query using bare queue name works as before
         QueueQueryResult result = server.queueQuery(b.getUniqueName());
         assertTrue(result.isExists());
         assertEquals(result.getAddress(), new SimpleString("foo.bah"));
         assertEquals(b.getUniqueName(), result.getName());
         //check that queue query using FQQN returns FQQN
         result = server.queueQuery(new SimpleString("foo.bah::" + b.getUniqueName()));
         assertTrue(result.isExists());
         assertEquals(new SimpleString("foo.bah"), result.getAddress());
         assertEquals(new SimpleString("foo.bah::" + b.getUniqueName()), result.getName());
      } finally {
         subscriptionProvider.disconnect();
      }
   }
}

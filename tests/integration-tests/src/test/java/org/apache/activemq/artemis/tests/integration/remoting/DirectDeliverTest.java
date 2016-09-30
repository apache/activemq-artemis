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
package org.apache.activemq.artemis.tests.integration.remoting;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class DirectDeliverTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.DIRECT_DELIVER, true);

      TransportConfiguration tc = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);

      Configuration config = createBasicConfig().addAcceptorConfiguration(tc);
      server = createServer(false, config);
      server.start();

      locator = createNettyNonHALocator();
      addServerLocator(locator);
   }

   @Test
   public void testDirectDeliver() throws Exception {
      final String foo = "foo";

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession();

      session.createQueue(foo, foo);

      Binding binding = server.getPostOffice().getBinding(new SimpleString(foo));

      Queue queue = (Queue) binding.getBindable();

      assertTrue(queue.isDirectDeliver());

      ClientProducer prod = session.createProducer(foo);

      ClientConsumer cons = session.createConsumer(foo);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage msg = session.createMessage(true);

         prod.send(msg);
      }

      queue.flushExecutor();

      //Consumer is not started so should go queued
      assertFalse(queue.isDirectDeliver());

      session.start();

      for (int i = 0; i < numMessages; i++) {
         ClientMessage msg = cons.receive(10000);

         assertNotNull(msg);

         msg.acknowledge();
      }

      Thread.sleep((long) (QueueImpl.CHECK_QUEUE_SIZE_PERIOD * 1.5));

      //Add another message, should go direct
      ClientMessage msg = session.createMessage(true);

      prod.send(msg);

      queue.flushExecutor();

      assertTrue(queue.isDirectDeliver());

      //Send some more
      for (int i = 0; i < numMessages; i++) {
         msg = session.createMessage(true);

         prod.send(msg);
      }

      for (int i = 0; i < numMessages + 1; i++) {
         msg = cons.receive(10000);

         assertNotNull(msg);

         msg.acknowledge();
      }

      assertTrue(queue.isDirectDeliver());

      session.stop();

      for (int i = 0; i < numMessages; i++) {
         msg = session.createMessage(true);

         prod.send(msg);
      }

      assertFalse(queue.isDirectDeliver());
   }

}

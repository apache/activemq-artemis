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

package org.apache.activemq.artemis.tests.integration.client;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;

public class UpdateQueueTest extends ActiveMQTestBase {

   @Test
   public void testUpdateQueue() throws Exception {
      ActiveMQServer server = createServer(true, true);

      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();

      server.start();


      SimpleString ADDRESS = SimpleString.toSimpleString("queue.0");

      server.createQueue(ADDRESS, RoutingType.ANYCAST, ADDRESS, null,
                         null, true, false);

      Connection conn = factory.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = session.createProducer(session.createQueue(ADDRESS.toString()));

      for (int i = 0; i < 100; i++) {
         prod.send(session.createTextMessage("message " + i));
      }

      server.updateQueue(ADDRESS.toString(), RoutingType.ANYCAST, 1, false);

      conn.close();
      factory.close();

      server.stop();

      server.start();

      Queue queue = server.locateQueue(ADDRESS);

      Assert.assertNotNull("queue not found", queue);

      factory = new ActiveMQConnectionFactory();

      conn = factory.createConnection();
      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer consumer = session.createConsumer(session.createQueue(ADDRESS.toString()));

      conn.start();
      for (int i = 0; i < 100; i++) {
         Assert.assertNotNull(consumer.receive(5000));
      }

      Assert.assertNull(consumer.receiveNoWait());

      Assert.assertEquals(1, queue.getMaxConsumers());

      conn.close();

      server.stop();

   }
}

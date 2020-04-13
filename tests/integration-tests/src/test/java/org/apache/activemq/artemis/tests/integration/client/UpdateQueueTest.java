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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;

public class UpdateQueueTest extends ActiveMQTestBase {

   @Test
   public void testUpdateQueueWithNullUser() throws Exception {
      ActiveMQServer server = createServer(true, true);

      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();

      server.start();

      SimpleString ADDRESS = SimpleString.toSimpleString("queue.0");

      final SimpleString user = new SimpleString("newUser");

      Queue queue = server.createQueue(new QueueConfiguration(ADDRESS).setRoutingType(RoutingType.ANYCAST).setUser(user));

      long originalID = queue.getID();

      Assert.assertEquals(user, queue.getUser());

      Connection conn = factory.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = session.createProducer(session.createQueue(ADDRESS.toString()));

      for (int i = 0; i < 100; i++) {
         prod.send(session.createTextMessage("message " + i));
      }

      server.updateQueue(new QueueConfiguration(ADDRESS).setRoutingType(RoutingType.ANYCAST).setMaxConsumers(1).setExclusive(false));

      conn.close();
      factory.close();

      server.stop();
      server.start();

      validateBindingRecords(server, JournalRecordIds.QUEUE_BINDING_RECORD, 2);

      queue = server.locateQueue(ADDRESS);

      Assert.assertNotNull("queue not found", queue);

      Assert.assertEquals("newUser", user, queue.getUser());

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

      Assert.assertEquals(originalID, server.locateQueue(ADDRESS).getID());

      // stopping, restarting to make sure the system will not create an extra record without an udpate
      server.stop();
      server.start();
      validateBindingRecords(server, JournalRecordIds.QUEUE_BINDING_RECORD, 2);
      server.stop();

   }

   @Test
   public void testUpdateQueue() throws Exception {
      ActiveMQServer server = createServer(true, true);

      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();

      server.start();

      SimpleString ADDRESS = SimpleString.toSimpleString("queue.0");

      Queue queue = server.createQueue(new QueueConfiguration(ADDRESS).setRoutingType(RoutingType.ANYCAST));

      long originalID = queue.getID();

      Assert.assertNull(queue.getUser());

      Connection conn = factory.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = session.createProducer(session.createQueue(ADDRESS.toString()));

      for (int i = 0; i < 100; i++) {
         prod.send(session.createTextMessage("message " + i));
      }

      server.updateQueue(new QueueConfiguration(ADDRESS.toString())
                            .setRoutingType(RoutingType.ANYCAST)
                            .setMaxConsumers(1)
                            .setPurgeOnNoConsumers(false)
                            .setExclusive(true)
                            .setGroupRebalance(true)
                            .setGroupBuckets(5)
                            .setGroupFirstKey("gfk")
                            .setNonDestructive(true)
                            .setConsumersBeforeDispatch(1)
                            .setDelayBeforeDispatch(10L)
                            .setUser("newUser")
                            .setRingSize(180L));

      conn.close();
      factory.close();

      server.stop();
      server.start();

      validateBindingRecords(server, JournalRecordIds.QUEUE_BINDING_RECORD, 2);

      queue = server.locateQueue(ADDRESS);

      Assert.assertNotNull("queue not found", queue);
      Assert.assertEquals(1, queue.getMaxConsumers());
      Assert.assertEquals(false, queue.isPurgeOnNoConsumers());
      Assert.assertEquals(true, queue.isExclusive());
      Assert.assertEquals(true, queue.isGroupRebalance());
      Assert.assertEquals(5, queue.getGroupBuckets());
      Assert.assertEquals("gfk", queue.getGroupFirstKey().toString());
      Assert.assertEquals(true, queue.isNonDestructive());
      Assert.assertEquals(1, queue.getConsumersBeforeDispatch());
      Assert.assertEquals(10L, queue.getDelayBeforeDispatch());
      Assert.assertEquals("newUser", queue.getUser().toString());
      Assert.assertEquals(180L, queue.getRingSize());

      factory = new ActiveMQConnectionFactory();

      conn = factory.createConnection();
      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer consumer = session.createConsumer(session.createQueue(ADDRESS.toString()));

      conn.start();
      for (int i = 0; i < 100; i++) {
         Assert.assertNotNull(consumer.receive(5000));
      }

      Assert.assertNull(consumer.receiveNoWait());

      conn.close();

      Assert.assertEquals(originalID, server.locateQueue(ADDRESS).getID());

      // stopping, restarting to make sure the system will not create an extra record without an udpate
      server.stop();
      server.start();
      validateBindingRecords(server, JournalRecordIds.QUEUE_BINDING_RECORD, 2);
      server.stop();

   }

   private void validateBindingRecords(ActiveMQServer server, byte type, int expected) throws Exception {
      HashMap<Integer, AtomicInteger> counts = countBindingJournal(server.getConfiguration());
      // if this fails, don't ignore it.. it means something is sending a new record on the journal
      // something is creating new records upon restart of the server.
      // I really meant to have this fix, so don't ignore it if it fails
      Assert.assertEquals(expected, counts.get((int) type).intValue());
   }

   @Test
   public void testUpdateAddress() throws Exception {
      ActiveMQServer server = createServer(true, true);

      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();

      server.start();

      SimpleString ADDRESS = SimpleString.toSimpleString("queue.0");

      AddressInfo infoAdded = new AddressInfo(ADDRESS, RoutingType.ANYCAST);

      server.addAddressInfo(infoAdded);

      server.updateAddressInfo(ADDRESS, infoAdded.getRoutingTypes());

      server.stop();
      server.start();

      AddressInfo infoAfterRestart = server.getPostOffice().getAddressInfo(ADDRESS);

      Assert.assertEquals(infoAdded.getId(), infoAfterRestart.getId());

      EnumSet<RoutingType> completeSet = EnumSet.allOf(RoutingType.class);

      server.updateAddressInfo(ADDRESS, completeSet);

      server.stop();
      server.start();

      infoAfterRestart = server.getPostOffice().getAddressInfo(ADDRESS);

      // it was changed.. so new ID
      Assert.assertNotEquals(infoAdded.getId(), infoAfterRestart.getId());
   }
}

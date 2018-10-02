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
package org.apache.activemq.artemis.tests.integration.openwire.cluster;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TemporaryQueueClusterTest extends OpenWireJMSClusteredTestBase {

   public static final String QUEUE_NAME = "target";

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      createAddressInfo(0, QUEUE_NAME, RoutingType.ANYCAST, -1, false);
      createAddressInfo(1, QUEUE_NAME, RoutingType.ANYCAST, -1, false);
      createQueue(0, QUEUE_NAME, QUEUE_NAME, null, true, null, null, RoutingType.ANYCAST);
      createQueue(1, QUEUE_NAME, QUEUE_NAME, null, true, null, null, RoutingType.ANYCAST);

      waitForBindings(0, QUEUE_NAME, 1, 0, true);
      waitForBindings(0, QUEUE_NAME, 1, 0, false);
      waitForBindings(1, QUEUE_NAME, 1, 0, true);
      waitForBindings(1, QUEUE_NAME, 1, 0, false);
   }

   @Test
   public void testClusteredQueue() throws Exception {

      System.out.println("creating connections");
      Connection conn1 = openWireCf1.createConnection();
      Connection conn2 = openWireCf2.createConnection();

      conn1.start();
      conn2.start();

      try {
         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue targetQueue1 = session1.createQueue(QUEUE_NAME);

         Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue targetQueue2 = session2.createQueue(QUEUE_NAME);


         this.waitForBindings(servers[0], QUEUE_NAME, true, 1, 0, 2000);
         this.waitForBindings(servers[1], QUEUE_NAME, true, 1, 0, 2000);
         this.waitForBindings(servers[1], QUEUE_NAME, false, 1, 0, 2000);
         this.waitForBindings(servers[0], QUEUE_NAME, false, 1, 0, 2000);


         MessageProducer prod1 = session1.createProducer(targetQueue1);
         MessageConsumer cons2 = session2.createConsumer(targetQueue2);

         TextMessage msg = session1.createTextMessage("hello");

         prod1.send(msg);

         TextMessage msgReceived = (TextMessage) cons2.receive(5000);

         assertNotNull(msgReceived);
         assertEquals(msgReceived.getText(), msg.getText());

      } finally {
         conn1.close();
         conn2.close();
      }
   }

   @Test
   public void testTemporaryQueue() throws Exception {

      Connection conn1 = openWireCf1.createConnection();
      Connection conn2 = openWireCf2.createConnection();

      conn1.start();
      conn2.start();

      try {
         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue targetQueue1 = session1.createQueue(QUEUE_NAME);
         Queue tempQueue = session1.createTemporaryQueue();
         System.out.println("temp queue is " + tempQueue.getQueueName());
         Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue targetQueue2 = session2.createQueue(QUEUE_NAME);

         waitForBindings(0, QUEUE_NAME, 1, 0, true);
         waitForBindings(0, tempQueue.getQueueName(), 1, 0, true);
         waitForBindings(0, QUEUE_NAME, 1, 0, false);

         waitForBindings(1, QUEUE_NAME, 1, 0, true);
         waitForBindings(1, QUEUE_NAME, 1, 0, false);
         waitForBindings(1, tempQueue.getQueueName(), 1, 0, false);

         MessageProducer prod1 = session1.createProducer(targetQueue1);
         MessageConsumer cons2 = session2.createConsumer(targetQueue2);
         MessageConsumer tempCons1 = session1.createConsumer(tempQueue);

         waitForBindings(0, tempQueue.getQueueName(), 1, 1, true);
         waitForBindings(1, QUEUE_NAME, 1, 1, true);

         for (int i = 0; i < 10; i++) {
            TextMessage message = session1.createTextMessage("" + i);
            message.setJMSReplyTo(tempQueue);
            prod1.send(message);
         }

         for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
               TextMessage received = (TextMessage) cons2.receive(5000);
               System.out.println(received.getText());
               System.out.println("check temp queue on server #2");
               MessageProducer tempProducer = session2.createProducer(received.getJMSReplyTo());
               tempProducer.send(session2.createTextMessage(">>> " + received.getText()));
               tempProducer.close();
            }
         }

         for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
               TextMessage received = (TextMessage) tempCons1.receive(5000);
               assertNotNull(received);
               System.out.println(received.getText());
            }
         }
      } finally {
         conn1.close();
         conn2.close();
      }
   }

   @Test
   public void testAdvisoryTopicTempQueueIsNotClustered() throws Exception {
      final Connection connNode1 = openWireCf1.createConnection();
      connNode1.start();
      final Connection connNode2 = openWireCf2.createConnection();
      connNode2.start();
      try {
         final Session sessionNode1 = connNode1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue tempQueue = sessionNode1.createTemporaryQueue();
         waitForBindings(0, tempQueue.getQueueName(), 1, 0, true);
         final ActiveMQTopic advisoryTopic = AdvisorySupport.getDestinationAdvisoryTopic(tempQueue);
         waitForBindings(0, advisoryTopic.getTopicName(), 1, 1, true);
         final Session session2 = connNode2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final MessageConsumer consumer = session2.createConsumer(advisoryTopic);
         final Queue tempQueue2 = sessionNode1.createTemporaryQueue();
         waitForBindings(0, tempQueue2.getQueueName(), 1, 0, true);
         final Message receive = consumer.receive(1000);
         Assert.assertNull(receive);
      } finally {
         connNode1.close();
         connNode2.close();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

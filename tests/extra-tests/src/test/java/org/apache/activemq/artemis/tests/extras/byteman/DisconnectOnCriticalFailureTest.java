/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.extras.byteman;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(BMUnitRunner.class)
public class DisconnectOnCriticalFailureTest extends JMSTestBase {

   private static AtomicBoolean corruptPacket = new AtomicBoolean(false);

   @After
   @Override
   public void tearDown() throws Exception {
      corruptPacket.set(false);
      super.tearDown();
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "Corrupt Decoding",
         targetClass = "org.apache.activemq.artemis.core.protocol.core.impl.PacketDecoder",
         targetMethod = "decode(byte)",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.DisconnectOnCriticalFailureTest.doThrow();")})
   public void testSendDisconnect() throws Exception {
      createQueue("queue1");
      final Connection producerConnection = nettyCf.createConnection();
      final CountDownLatch latch = new CountDownLatch(1);

      try {
         producerConnection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException e) {
               latch.countDown();
            }
         });

         corruptPacket.set(true);
         producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         assertTrue(latch.await(5, TimeUnit.SECONDS));
      } finally {
         corruptPacket.set(false);

         if (producerConnection != null) {
            producerConnection.close();
         }
      }
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "Corrupt Decoding",
         targetClass = "org.apache.activemq.artemis.core.protocol.ClientPacketDecoder",
         targetMethod = "decode(org.apache.activemq.artemis.api.core.ActiveMQBuffer)",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.DisconnectOnCriticalFailureTest.doThrow($1);")})
   public void testClientDisconnect() throws Exception {
      Queue q1 = createQueue("queue1");
      final Connection connection = nettyCf.createConnection();
      final CountDownLatch latch = new CountDownLatch(1);

      try {
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException e) {
               latch.countDown();
            }
         });

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(q1);
         TextMessage m = session.createTextMessage("hello");
         producer.send(m);
         connection.start();

         corruptPacket.set(true);
         MessageConsumer consumer = session.createConsumer(q1);
         consumer.receive(2000);

         assertTrue(latch.await(5, TimeUnit.SECONDS));
      } finally {
         corruptPacket.set(false);

         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 60000)
   @BMRules(
      rules = {@BMRule(
         name = "Corrupt Decoding",
         targetClass = "org.apache.activemq.artemis.core.protocol.ClientPacketDecoder",
         targetMethod = "decode(org.apache.activemq.artemis.api.core.ActiveMQBuffer)",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.DisconnectOnCriticalFailureTest.doThrow($1);")})
   public void testClientDisconnectLarge() throws Exception {
      Queue q1 = createQueue("queue1");
      final Connection connection = nettyCf.createConnection();
      final CountDownLatch latch = new CountDownLatch(1);
      ServerLocator locator = ((ActiveMQConnectionFactory)nettyCf).getServerLocator();
      int minSize = locator.getMinLargeMessageSize();
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < minSize; i++) {
         builder.append("a");
      }

      try {
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException e) {
               latch.countDown();
            }
         });

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(q1);
         TextMessage m = session.createTextMessage(builder.toString());
         producer.send(m);
         connection.start();

         corruptPacket.set(true);
         MessageConsumer consumer = session.createConsumer(q1);
         Message lm = consumer.receive(2000);

         //first receive won't crash because the packet
         //is SESS_RECEIVE_LARGE_MSG
         assertNotNull(lm);

         //second receive will force server to send a
         //"forced delivery" message, and will cause
         //the exception to be thrown.
         lm = consumer.receive(5000);
         assertNull(lm);

         assertTrue(latch.await(5, TimeUnit.SECONDS));
      } finally {
         corruptPacket.set(false);

         if (connection != null) {
            connection.close();
         }
      }
   }

   public static void doThrow(ActiveMQBuffer buff) {
      byte type = buff.getByte(buff.readerIndex());
      if (corruptPacket.get() && type == PacketImpl.SESS_RECEIVE_MSG) {
         corruptPacket.set(false);
         throw new IllegalArgumentException("Invalid type: -84");
      }
   }

   public static void doThrow() {
      if (corruptPacket.get()) {
         corruptPacket.set(false);
         throw new IllegalArgumentException("Invalid type: -84");
      }
   }
}

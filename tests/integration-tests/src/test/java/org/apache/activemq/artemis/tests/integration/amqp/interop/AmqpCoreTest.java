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
package org.apache.activemq.artemis.tests.integration.amqp.interop;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
import org.apache.activemq.artemis.junit.Wait;
import org.apache.activemq.artemis.tests.integration.amqp.JMSClientTestSupport;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsTopic;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;

public class AmqpCoreTest extends JMSClientTestSupport {

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   @Override
   public void setUp() throws Exception {
      super.setUp();
   }

   @Test
   public void testMultipleCoreReceiving() throws Exception {

      Connection coreJmsConn = this.createCoreConnection();

      final int total = 100;

      try {
         Session session = coreJmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         ClientSession coreSession = ((ActiveMQSession) session).getCoreSession();
         coreSession.createQueue("exampleQueueAddress", RoutingType.MULTICAST, "exampleQueue1");
         coreSession.createQueue("exampleQueueAddress", RoutingType.MULTICAST, "exampleQueue2");
         coreSession.createQueue("exampleQueueAddress", RoutingType.MULTICAST, "exampleQueue3");

         ClientConsumer consumer1 = coreSession.createConsumer("exampleQueue1");
         CoreMessageHandler handler1 = new CoreMessageHandler(1);
         consumer1.setMessageHandler(handler1);
         ClientConsumer consumer2 = coreSession.createConsumer("exampleQueue2");
         CoreMessageHandler handler2 = new CoreMessageHandler(2);
         consumer2.setMessageHandler(handler2);
         CoreMessageHandler handler3 = new CoreMessageHandler(3);
         ClientConsumer consumer3 = coreSession.createConsumer("exampleQueue3");
         consumer3.setMessageHandler(handler3);

         sendAmqpMessages("exampleQueueAddress", total);

         assertTrue("not enough message received: " + handler1.getNumMsg() + " expected: " + total, handler1.waitForMessages(total));
         assertTrue("not enough message received: " + handler2.getNumMsg() + " expected: " + total, handler2.waitForMessages(total));
         assertTrue("not enough message received: " + handler3.getNumMsg() + " expected: " + total, handler3.waitForMessages(total));


      } finally {
         coreJmsConn.close();
      }
   }

   private void sendAmqpMessages(String address, int total) throws Exception {
      ConnectionFactory cfAMQP = new JmsConnectionFactory("amqp://127.0.0.1:" + AMQP_PORT);
      Connection connectionAMQP = cfAMQP.createConnection();
      try {
         connectionAMQP.start();
         Session sessionAMQP = connectionAMQP.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer sender = sessionAMQP.createProducer(new JmsTopic(address));

         for (int i = 0; i < total; i++) {
            sender.send(sessionAMQP.createTextMessage("hello"));
         }
      } finally {
         connectionAMQP.close();
      }
   }

   private class CoreMessageHandler implements MessageHandler {
      int id;
      int numMsg = 0;
      volatile boolean zeroLen = false;

      CoreMessageHandler(int id) {
         this.id = id;
      }

      @Override
      public void onMessage(ClientMessage message) {
         System.out.println("received: " + message.getBodySize());
         if (message.getBodySize() == 0) {
            System.out.println("xxx found zero len message!");
            zeroLen = true;
         }
         addMessage(message);
      }

      private synchronized void addMessage(ClientMessage message) {
         numMsg++;
         System.out.println("[receiver " + id + "] recieved: " + numMsg);
      }

      public synchronized boolean waitForMessages(int num) throws Exception {
         assertFalse(zeroLen);
         return Wait.waitFor(() -> numMsg == num, 30000);
      }

      public int getNumMsg() {
         return numMsg;
      }
   }

}

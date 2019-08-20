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

package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.ArrayList;
import java.util.List;


import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.Test;

/**
 * Test consumer behavior for Transacted Session Consumers.
 */
public class JmsTransactedConsumerTest extends JMSClientTestSupport {
   public static final String MESSAGE_NUMBER = "MessageNumber";

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   @Test(timeout = 60000)
   public void testReceiveSomeThenRollbackOpenWire() throws Exception {
      try (Connection connection = createOpenWireConnection()) {
         testReceiveSomeThenRollback(connection);
      }
   }

   @Test(timeout = 60000)
   public void testReceiveSomeThenRollbackCore() throws Exception {
      try (Connection connection = createCoreConnection()) {
         testReceiveSomeThenRollback(connection);
      }
   }

   @Test(timeout = 60000)
   public void testReceiveSomeThenRollbackAMQP() throws Exception {
      try (Connection connection = createConnection()) {
         testReceiveSomeThenRollback(connection);
      }
   }

   public void testReceiveSomeThenRollback(Connection connection) throws Exception {
      connection.start();

      int totalCount = 5;
      int consumeBeforeRollback = 2;
      sendToAmqQueue(totalCount);


      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      Queue queue = session.createQueue(name.getMethodName());
      MessageConsumer consumer = session.createConsumer(queue);

      for (int i = 1; i <= consumeBeforeRollback; i++) {
         Message message = consumer.receive(3000);
         assertNotNull(message);
         assertEquals("Unexpected message number", i, message.getIntProperty(MESSAGE_NUMBER));
      }

      session.rollback();

      // Consume again.. the previously consumed messages should get delivered
      // again after the rollback and then the remainder should follow
      List<Integer> messageNumbers = new ArrayList<>();
      for (int i = 1; i <= totalCount; i++) {
         Message message = consumer.receive(3000);
         assertNotNull("Failed to receive message: " + i, message);
         int msgNum = message.getIntProperty(MESSAGE_NUMBER);
         messageNumbers.add(msgNum);
      }

      session.commit();

      assertEquals("Unexpected size of list", totalCount, messageNumbers.size());
      for (int i = 0; i < messageNumbers.size(); i++) {
         assertEquals("Unexpected order of messages: " + messageNumbers, Integer.valueOf(i + 1), messageNumbers.get(i));
      }

   }

   protected void sendToAmqQueue(int count) throws Exception {
      Connection activemqConnection = createCoreConnection();
      Session amqSession = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue amqTestQueue = amqSession.createQueue(name.getMethodName());
      sendMessages(activemqConnection, amqTestQueue, count);
      activemqConnection.close();
   }

   public void sendMessages(Connection connection, Destination destination, int count) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(destination);

      for (int i = 1; i <= count; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("TextMessage: " + i);
         message.setIntProperty(MESSAGE_NUMBER, i);
         p.send(message);
      }

      session.close();

   }
}
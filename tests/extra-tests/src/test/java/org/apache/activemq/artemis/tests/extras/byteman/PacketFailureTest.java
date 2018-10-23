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
package org.apache.activemq.artemis.tests.extras.byteman;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQMessageConsumer;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class PacketFailureTest extends JMSTestBase {

   private Queue queue;
   static boolean pause = false;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

      queue = createQueue("TestQueue");
   }

   @Test(timeout = 20000)
   @BMRule(
      name = "blow-up",
      targetClass = "org.apache.activemq.artemis.core.protocol.core.impl.ActiveMQSessionContext",
      targetMethod = "handleReceivedMessagePacket(org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionReceiveMessage)",
      targetLocation = "ENTRY",
      action = "throw new Exception()")
   public void testFailureToHandlePacket() throws Exception {
      final int MESSAGE_COUNT = 20;
      Connection sendConnection = null;
      Connection connection = null;
      try {
         ((ActiveMQConnectionFactory)cf).setReconnectAttempts(0);
         sendConnection = cf.createConnection();
         final Session sendSession = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final MessageProducer producer = sendSession.createProducer(queue);

         for (int j = 0; j < MESSAGE_COUNT; j++) {
            TextMessage message = sendSession.createTextMessage();

            message.setText("Message" + j);

            producer.send(message);
         }

         producer.close();
         sendSession.close();

         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();

         Message message = consumer.receive(1000);
         assertNull(message);
         assertTrue(((ActiveMQMessageConsumer) consumer).isClosed());
      } finally {
         if (connection != null) {
            connection.close();
         }
         if (sendConnection != null) {
            sendConnection.close();
         }
      }
   }
}

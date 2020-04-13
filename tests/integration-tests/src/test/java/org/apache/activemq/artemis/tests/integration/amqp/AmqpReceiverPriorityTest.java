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

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test various behaviors of AMQP receivers with the broker.
 */
public class AmqpReceiverPriorityTest extends AmqpClientTestSupport {

   @Test(timeout = 30000)
   public void testPriority() throws Exception {

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      Map<Symbol, Object> properties1 = new HashMap<>();
      properties1.put(Symbol.getSymbol("priority"), 5);
      AmqpReceiver receiver1 = session.createReceiver(getQueueName(), null, false, false, properties1);
      receiver1.flow(100);

      Map<Symbol, Object> properties2 = new HashMap<>();
      properties2.put(Symbol.getSymbol("priority"), 50);
      AmqpReceiver receiver2 = session.createReceiver(getQueueName(), null, false, false, properties2);
      receiver2.flow(100);

      Map<Symbol, Object> properties3 = new HashMap<>();
      properties3.put(Symbol.getSymbol("priority"), 10);
      AmqpReceiver receiver3 = session.createReceiver(getQueueName(), null, false, false, properties3);
      receiver3.flow(100);

      sendMessages(getQueueName(), 5);


      for (int i = 0; i < 5; i++) {
         AmqpMessage message1 = receiver1.receiveNoWait();
         AmqpMessage message2 = receiver2.receive(250, TimeUnit.MILLISECONDS);
         AmqpMessage message3 = receiver3.receiveNoWait();
         assertNotNull("did not receive message first time", message2);
         assertEquals("MessageID:" + i, message2.getMessageId());
         message2.accept();
         assertNull("message is not meant to goto lower priority receiver", message1);
         assertNull("message is not meant to goto lower priority receiver", message3);
      }
      assertNoMessage(receiver1);
      assertNoMessage(receiver3);

      //Close the high priority receiver
      receiver2.close();

      sendMessages(getQueueName(), 5);

      //Check messages now goto next priority receiver
      for (int i = 0; i < 5; i++) {
         AmqpMessage message1 = receiver1.receiveNoWait();
         AmqpMessage message3 = receiver3.receive(250, TimeUnit.MILLISECONDS);
         assertNotNull("did not receive message first time", message3);
         assertEquals("MessageID:" + i, message3.getMessageId());
         message3.accept();
         assertNull("message is not meant to goto lower priority receiver", message1);
      }
      assertNoMessage(receiver1);


      connection.close();
   }

   public void assertNoMessage(AmqpReceiver receiver) throws Exception {
      //A check to make sure no messages
      AmqpMessage message = receiver.receive(250, TimeUnit.MILLISECONDS);
      assertNull("message is not meant to goto lower priority receiver", message);
   }

   @Test(timeout = 30000)
   public void testPriorityProvidedAsByte() throws Exception {
      testPriorityNumber((byte) 5);
   }

   @Test(timeout = 30000)
   public void testPriorityProvidedAsUnsignedInteger() throws Exception {
      testPriorityNumber(UnsignedInteger.valueOf(5));
   }


   private void testPriorityNumber(Number number) throws Exception {

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      Map<Symbol, Object> properties1 = new HashMap<>();
      properties1.put(Symbol.getSymbol("priority"), number);
      AmqpReceiver receiver1 = session.createReceiver(getQueueName(), null, false, false, properties1);
      receiver1.flow(100);

      sendMessages(getQueueName(), 2);


      for (int i = 0; i < 2; i++) {
         AmqpMessage message1 = receiver1.receive(3000, TimeUnit.MILLISECONDS);
         assertNotNull("did not receive message" + i, message1);
         assertEquals("MessageID:" + i, message1.getMessageId());
         message1.accept();
      }
      connection.close();
   }
}

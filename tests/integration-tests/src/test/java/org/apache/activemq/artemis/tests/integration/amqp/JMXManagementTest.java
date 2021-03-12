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
package org.apache.activemq.artemis.tests.integration.amqp;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.Binary;
import org.junit.Assert;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.json.JsonArray;
import javax.json.JsonObject;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JMXManagementTest extends JMSClientTestSupport {

   @Test
   public void testListDeliveringMessages() throws Exception {
      SimpleString queue = new SimpleString(getQueueName());

      Connection connection1 = createConnection();
      Connection connection2 = createConnection();
      Session prodSession = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session consSession = connection2.createSession(true, Session.SESSION_TRANSACTED);

      javax.jms.Queue jmsQueue = prodSession.createQueue(queue.toString());

      QueueControl queueControl = createManagementControl(queue, queue);

      MessageProducer producer = prodSession.createProducer(jmsQueue);
      final int num = 20;

      for (int i = 0; i < num; i++) {
         TextMessage message = prodSession.createTextMessage("hello" + i);
         producer.send(message);
      }

      connection2.start();
      MessageConsumer consumer = consSession.createConsumer(jmsQueue);

      for (int i = 0; i < num; i++) {
         TextMessage msgRec = (TextMessage) consumer.receive(5000);
         assertNotNull(msgRec);
         assertEquals(msgRec.getText(), "hello" + i);
      }

      //before commit
      assertEquals(num, queueControl.getDeliveringCount());

      Map<String, Map<String, Object>[]> result = queueControl.listDeliveringMessages();
      assertEquals(1, result.size());

      Map<String, Object>[] msgMaps = result.entrySet().iterator().next().getValue();

      assertEquals(num, msgMaps.length);

      consSession.commit();
      result = queueControl.listDeliveringMessages();

      assertEquals(0, result.size());

      consSession.close();
      prodSession.close();

      connection1.close();
      connection2.close();
   }

   @Test
   public void testGetFirstMessage() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getQueueName());

         session.begin();
         AmqpMessage message = new AmqpMessage();
         message.setApplicationProperty("TEST_BINARY", new Binary("TEST".getBytes()));

         final String oneK = new String(new char[1024]).replace("\0", "$");
         message.setApplicationProperty("TEST_BIG_BINARY", new Binary(oneK.getBytes(StandardCharsets.UTF_8)));
         message.setApplicationProperty("TEST_STRING", oneK);
         message.setText("NOT_VISIBLE");
         sender.send(message);
         session.commit();

         SimpleString queue = new SimpleString(getQueueName());
         QueueControl queueControl = createManagementControl(queue, queue);
         String firstMessageAsJSON = queueControl.getFirstMessageAsJSON();
         Assert.assertNotNull(firstMessageAsJSON);

         // Json is still bulky!
         Assert.assertTrue(firstMessageAsJSON.length() < 1500);
         Assert.assertFalse(firstMessageAsJSON.contains("NOT_VISIBLE"));

         // composite data limits
         Map<String, Object>[] result = queueControl.listMessages("");
         assertEquals(1, result.length);

         final Map<String, Object> msgMap = result[0];
         Assert.assertTrue(msgMap.get("TEST_STRING").toString().length() < 512);

      } finally {
         connection.close();
      }
   }

   @Test
   public void testAddressSizeOnDelete() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getQueueName());

         session.begin();
         AmqpMessage message = new AmqpMessage();
         message.setApplicationProperty("TEST_STRING", "TEST");
         message.setTimeToLive(100);
         message.setText("TEST");
         // send 2 so we can verify getFirstMessage and List
         sender.send(message);
         sender.send(message);
         session.commit();

         PagingStore targetPagingStore = server.getPagingManager().getPageStore(SimpleString.toSimpleString(getQueueName()));
         assertNotNull(targetPagingStore);

         assertTrue(targetPagingStore.getAddressSize() > 0);

         SimpleString queue = new SimpleString(getQueueName());
         QueueControl queueControl = createManagementControl(queue, queue);

         Wait.assertEquals(2, queueControl::getMessageCount);

         JsonArray array = JsonUtil.readJsonArray(queueControl.getFirstMessageAsJSON());
         JsonObject object = (JsonObject) array.get(0);
         queueControl.removeMessage(object.getJsonNumber("messageID").longValue());

         Wait.assertEquals(1, queueControl::getMessageCount);

         Map<String, Object>[] messages = queueControl.listMessages("");
         Assert.assertEquals(1, messages.length);
         queueControl.removeMessage((Long) messages[0].get("messageID"));

         Assert.assertEquals(0, queueControl.getMessageCount());
         Wait.assertEquals(0L, targetPagingStore::getAddressSize);

      } finally {
         connection.close();
      }
   }

   protected QueueControl createManagementControl(final SimpleString address,
                                                  final SimpleString queue) throws Exception {
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, queue, RoutingType.ANYCAST, this.mBeanServer);

      return queueControl;
   }
}

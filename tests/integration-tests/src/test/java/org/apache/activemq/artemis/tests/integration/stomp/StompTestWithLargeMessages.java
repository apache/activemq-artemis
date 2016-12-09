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
package org.apache.activemq.artemis.tests.integration.stomp;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.largemessage.LargeMessageTestBase;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StompTestWithLargeMessages extends StompTestBase {

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }

   @Override
   public boolean isCompressLargeMessages() {
      return true;
   }

   @Override
   public boolean isPersistenceEnabled() {
      return true;
   }

   @Override
   public Integer getStompMinLargeMessageSize() {
      return 2048;
   }

   //stomp sender -> large -> stomp receiver
   @Test
   public void testSendReceiveLargePersistentMessages() throws Exception {
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection("1.0", hostname, port);
      conn.connect(defUser, defPass);

      int count = 10;
      int msgSize = 1024 * 1024;
      char[] contents = new char[msgSize];
      for (int i = 0; i < msgSize; i++) {
         contents[i] = 'A';
      }
      String body = new String(contents);

      for (int i = 0; i < count; i++) {
         ClientStompFrame frame = conn.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("persistent", "true");
         frame.setBody(body);
         conn.sendFrame(frame);
      }

      ClientStompFrame subFrame = conn.createFrame("SUBSCRIBE");
      subFrame.addHeader("subscription-type", "ANYCAST");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");
      conn.sendFrame(subFrame);

      for (int i = 0; i < count; i++) {
         ClientStompFrame frame = conn.receiveFrame(60000);
         Assert.assertNotNull(frame);
         System.out.println("part of frame: " + frame.getBody().substring(0, 200));
         Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
         Assert.assertTrue(frame.getHeader("destination").equals(getQueuePrefix() + getQueueName()));
         int index = frame.getBody().indexOf("AAAA");
         assertEquals(msgSize, (frame.getBody().length() - index));
      }

      ClientStompFrame unsubFrame = conn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      unsubFrame.addHeader("receipt", "567");
      ClientStompFrame response = conn.sendFrame(unsubFrame);
      assertNotNull(response);
      assertNotNull(response.getCommand().equals("RECEIPT"));

      conn.disconnect();
   }

   //core sender -> large -> stomp receiver
   @Test
   public void testReceiveLargePersistentMessagesFromCore() throws Exception {
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection("1.0", hostname, port);
      conn.connect(defUser, defPass);

      int msgSize = 3 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
      char[] contents = new char[msgSize];
      for (int i = 0; i < msgSize; i++) {
         contents[i] = 'B';
      }
      String msg = new String(contents);

      int count = 10;
      for (int i = 0; i < count; i++) {
         this.sendJmsMessage(msg);
      }

      ClientStompFrame subFrame = conn.createFrame("SUBSCRIBE");
      subFrame.addHeader("subscription-type", "ANYCAST");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");
      conn.sendFrame(subFrame);

      for (int i = 0; i < count; i++) {
         ClientStompFrame frame = conn.receiveFrame(60000);
         Assert.assertNotNull(frame);
         System.out.println("part of frame: " + frame.getBody().substring(0, 200));
         Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
         Assert.assertTrue(frame.getHeader("destination").equals(getQueuePrefix() + getQueueName()));
         int index = frame.getBody().indexOf("BBB");
         assertEquals(msgSize, (frame.getBody().length() - index));
      }

      ClientStompFrame unsubFrame = conn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      unsubFrame.addHeader("receipt", "567");
      ClientStompFrame response = conn.sendFrame(unsubFrame);
      assertNotNull(response);
      assertNotNull(response.getCommand().equals("RECEIPT"));

      conn.disconnect();
   }

   //stomp v12 sender -> large -> stomp v12 receiver
   @Test
   public void testSendReceiveLargePersistentMessagesV12() throws Exception {
      StompClientConnection connV12 = StompClientConnectionFactory.createClientConnection("1.2", "localhost", port);
      connV12.connect(defUser, defPass);

      int count = 10;
      int szBody = 1024 * 1024;
      char[] contents = new char[szBody];
      for (int i = 0; i < szBody; i++) {
         contents[i] = 'A';
      }
      String body = new String(contents);

      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination-type", "ANYCAST");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("persistent", "true");
      frame.setBody(body);

      for (int i = 0; i < count; i++) {
         connV12.sendFrame(frame);
      }

      ClientStompFrame subFrame = connV12.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("subscription-type", "ANYCAST");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      connV12.sendFrame(subFrame);

      for (int i = 0; i < count; i++) {
         ClientStompFrame receiveFrame = connV12.receiveFrame(30000);

         Assert.assertNotNull(receiveFrame);
         System.out.println("part of frame: " + receiveFrame.getBody().substring(0, 20));
         Assert.assertTrue(receiveFrame.getCommand().equals("MESSAGE"));
         Assert.assertEquals(receiveFrame.getHeader("destination"), getQueuePrefix() + getQueueName());
         assertEquals(szBody, receiveFrame.getBody().length());
      }

      // remove susbcription
      ClientStompFrame unsubFrame = connV12.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV12.sendFrame(unsubFrame);

      connV12.disconnect();
   }

   //core sender -> large -> stomp v12 receiver
   @Test
   public void testReceiveLargePersistentMessagesFromCoreV12() throws Exception {
      int msgSize = 3 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
      char[] contents = new char[msgSize];
      for (int i = 0; i < msgSize; i++) {
         contents[i] = 'B';
      }
      String msg = new String(contents);

      int count = 10;
      for (int i = 0; i < count; i++) {
         this.sendJmsMessage(msg);
      }

      StompClientConnection connV12 = StompClientConnectionFactory.createClientConnection("1.2", "localhost", port);
      connV12.connect(defUser, defPass);

      ClientStompFrame subFrame = connV12.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("subscription-type", "ANYCAST");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");
      connV12.sendFrame(subFrame);

      for (int i = 0; i < count; i++) {
         ClientStompFrame receiveFrame = connV12.receiveFrame(30000);

         Assert.assertNotNull(receiveFrame);
         System.out.println("part of frame: " + receiveFrame.getBody().substring(0, 20));
         Assert.assertTrue(receiveFrame.getCommand().equals("MESSAGE"));
         Assert.assertEquals(receiveFrame.getHeader("destination"), getQueuePrefix() + getQueueName());
         assertEquals(msgSize, receiveFrame.getBody().length());
      }

      // remove susbcription
      ClientStompFrame unsubFrame = connV12.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV12.sendFrame(unsubFrame);

      connV12.disconnect();
   }

   //core sender -> large (compressed regular) -> stomp v10 receiver
   @Test
   public void testReceiveLargeCompressedToRegularPersistentMessagesFromCore() throws Exception {
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection("1.0", hostname, port);
      conn.connect(defUser, defPass);

      LargeMessageTestBase.TestLargeMessageInputStream input = new LargeMessageTestBase.TestLargeMessageInputStream(ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, true);
      LargeMessageTestBase.adjustLargeCompression(true, input, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      char[] contents = input.toArray();
      String msg = new String(contents);

      String leadingPart = msg.substring(0, 100);

      int count = 10;
      for (int i = 0; i < count; i++) {
         this.sendJmsMessage(msg);
      }

      ClientStompFrame subFrame = conn.createFrame("SUBSCRIBE");
      subFrame.addHeader("subscription-type", "ANYCAST");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");
      conn.sendFrame(subFrame);

      for (int i = 0; i < count; i++) {
         ClientStompFrame receiveFrame = conn.receiveFrame(30000);
         Assert.assertNotNull(receiveFrame);
         System.out.println("part of frame: " + receiveFrame.getBody().substring(0, 250));
         Assert.assertTrue(receiveFrame.getCommand().equals("MESSAGE"));
         Assert.assertEquals(receiveFrame.getHeader("destination"), getQueuePrefix() + getQueueName());
         int index = receiveFrame.getBody().indexOf(leadingPart);
         assertEquals(msg.length(), (receiveFrame.getBody().length() - index));
      }

      // remove suscription
      ClientStompFrame unsubFrame = conn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      unsubFrame.addHeader("receipt", "567");
      ClientStompFrame response = conn.sendFrame(unsubFrame);
      assertNotNull(response);
      assertNotNull(response.getCommand().equals("RECEIPT"));

      conn.disconnect();
   }

   //core sender -> large (compressed regular) -> stomp v12 receiver
   @Test
   public void testReceiveLargeCompressedToRegularPersistentMessagesFromCoreV12() throws Exception {
      LargeMessageTestBase.TestLargeMessageInputStream input = new LargeMessageTestBase.TestLargeMessageInputStream(ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, true);
      LargeMessageTestBase.adjustLargeCompression(true, input, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      char[] contents = input.toArray();
      String msg = new String(contents);

      int count = 10;
      for (int i = 0; i < count; i++) {
         this.sendJmsMessage(msg);
      }

      StompClientConnection connV12 = StompClientConnectionFactory.createClientConnection("1.2", "localhost", port);
      connV12.connect(defUser, defPass);

      ClientStompFrame subFrame = connV12.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("subscription-type", "ANYCAST");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      connV12.sendFrame(subFrame);

      for (int i = 0; i < count; i++) {
         ClientStompFrame receiveFrame = connV12.receiveFrame(30000);

         Assert.assertNotNull(receiveFrame);
         System.out.println("part of frame: " + receiveFrame.getBody().substring(0, 20));
         Assert.assertTrue(receiveFrame.getCommand().equals("MESSAGE"));
         Assert.assertEquals(receiveFrame.getHeader("destination"), getQueuePrefix() + getQueueName());
         assertEquals(contents.length, receiveFrame.getBody().length());
      }

      // remove susbcription
      ClientStompFrame unsubFrame = connV12.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV12.sendFrame(unsubFrame);

      connV12.disconnect();
   }

   //core sender -> large (compressed large) -> stomp v12 receiver
   @Test
   public void testReceiveLargeCompressedToLargePersistentMessagesFromCoreV12() throws Exception {
      LargeMessageTestBase.TestLargeMessageInputStream input = new LargeMessageTestBase.TestLargeMessageInputStream(ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, true);
      input.setSize(10 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);
      LargeMessageTestBase.adjustLargeCompression(false, input, 10 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      char[] contents = input.toArray();
      String msg = new String(contents);

      int count = 10;
      for (int i = 0; i < count; i++) {
         this.sendJmsMessage(msg);
      }

      IntegrationTestLogger.LOGGER.info("Message count for " + getQueueName() + ": " + server.getActiveMQServer().locateQueue(SimpleString.toSimpleString(getQueueName())).getMessageCount());

      StompClientConnection connV12 = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      connV12.connect(defUser, defPass);

      ClientStompFrame subFrame = connV12.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("subscription-type", "ANYCAST");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      connV12.sendFrame(subFrame);

      for (int i = 0; i < count; i++) {
         ClientStompFrame receiveFrame = connV12.receiveFrame(30000);

         Assert.assertNotNull(receiveFrame);
         System.out.println("part of frame: " + receiveFrame.getBody().substring(0, 20));
         Assert.assertTrue(receiveFrame.getCommand().equals("MESSAGE"));
         Assert.assertEquals(receiveFrame.getHeader("destination"), getQueuePrefix() + getQueueName());
         assertEquals(contents.length, receiveFrame.getBody().length());
      }

      // remove susbcription
      ClientStompFrame unsubFrame = connV12.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV12.sendFrame(unsubFrame);

      connV12.disconnect();
   }

   //core sender -> large (compressed large) -> stomp v10 receiver
   @Test
   public void testReceiveLargeCompressedToLargePersistentMessagesFromCore() throws Exception {
      LargeMessageTestBase.TestLargeMessageInputStream input = new LargeMessageTestBase.TestLargeMessageInputStream(ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, true);
      input.setSize(10 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);
      LargeMessageTestBase.adjustLargeCompression(false, input, 10 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      char[] contents = input.toArray();
      String msg = new String(contents);

      String leadingPart = msg.substring(0, 100);

      int count = 10;
      for (int i = 0; i < count; i++) {
         this.sendJmsMessage(msg);
      }

      StompClientConnection conn = StompClientConnectionFactory.createClientConnection("1.0", hostname, port);
      conn.connect(defUser, defPass);

      ClientStompFrame subFrame = conn.createFrame("SUBSCRIBE");
      subFrame.addHeader("subscription-type", "ANYCAST");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");
      conn.sendFrame(subFrame);

      for (int i = 0; i < count; i++) {
         ClientStompFrame frame = conn.receiveFrame(60000);
         Assert.assertNotNull(frame);
         System.out.println("part of frame: " + frame.getBody().substring(0, 250));
         Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
         Assert.assertTrue(frame.getHeader("destination").equals(getQueuePrefix() + getQueueName()));
         int index = frame.getBody().toString().indexOf(leadingPart);
         assertEquals(msg.length(), (frame.getBody().toString().length() - index));
      }

      ClientStompFrame unsubFrame = conn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      unsubFrame.addHeader("receipt", "567");
      conn.sendFrame(unsubFrame);

      conn.disconnect();
   }
}

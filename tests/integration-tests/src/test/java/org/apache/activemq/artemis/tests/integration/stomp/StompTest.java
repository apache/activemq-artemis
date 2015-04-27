/**
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
package org.apache.activemq.tests.integration.stomp;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.management.ResourceNames;
import org.apache.activemq.api.jms.ActiveMQJMSClient;
import org.apache.activemq.core.protocol.stomp.Stomp;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class StompTest extends StompTestBase
{
   private static final transient IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   @Test
   public void testSendManyMessages() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);
      frame = receiveFrame(10000);

      Assert.assertTrue(frame.startsWith("CONNECTED"));
      int count = 1000;
      final CountDownLatch latch = new CountDownLatch(count);
      consumer.setMessageListener(new MessageListener()
      {

         public void onMessage(Message arg0)
         {
            latch.countDown();
         }
      });

      frame = "SEND\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;
      for (int i = 1; i <= count; i++)
      {
         // Thread.sleep(1);
         // System.out.println(">>> " + i);
         sendFrame(frame);
      }

      assertTrue(latch.await(60, TimeUnit.SECONDS));
   }

   @Test
   public void testConnect() throws Exception
   {

      String connect_frame = "CONNECT\n" + "login: brianm\n" +
         "passcode: wombats\n" +
         "request-id: 1\n" +
         "\n" +
         Stomp.NULL;
      sendFrame(connect_frame);

      String f = receiveFrame(10000);
      Assert.assertTrue(f.startsWith("CONNECTED"));
      Assert.assertTrue(f.indexOf("response-id:1") >= 0);
   }

   @Test
   public void testDisconnectAndError() throws Exception
   {

      String connectFrame = "CONNECT\n" + "login: brianm\n" +
         "passcode: wombats\n" +
         "request-id: 1\n" +
         "\n" +
         Stomp.NULL;
      sendFrame(connectFrame);

      String f = receiveFrame(10000);
      Assert.assertTrue(f.startsWith("CONNECTED"));
      Assert.assertTrue(f.indexOf("response-id:1") >= 0);

      String disconnectFrame = "DISCONNECT\n\n" + Stomp.NULL;
      sendFrame(disconnectFrame);

      waitForFrameToTakeEffect();

      // sending a message will result in an error
      String frame = "SEND\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n\n" +
         "Hello World" +
         Stomp.NULL;

      assertChannelClosed();
   }

   @Test
   public void testSendMessage() throws Exception
   {

      MessageConsumer consumer = session.createConsumer(queue);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SEND\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;

      sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      // Assert default priority 4 is used when priority header is not set
      Assert.assertEquals("getJMSPriority", 4, message.getJMSPriority());

      // Make sure that the timestamp is valid - should
      // be very close to the current time.
      long tnow = System.currentTimeMillis();
      long tmsg = message.getJMSTimestamp();
      Assert.assertTrue(Math.abs(tnow - tmsg) < 1000);
   }

   @Test
   public void testSendMessageToNonExistentQueue() throws Exception
   {
      String nonExistantQueue = RandomUtil.randomString();
      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SEND\n" + "destination:" + getQueuePrefix() + nonExistantQueue + "\n\n" + "Hello World" + Stomp.NULL;

      sendFrame(frame);
      receiveFrame(1000);

      MessageConsumer consumer = session.createConsumer(ActiveMQJMSClient.createQueue(nonExistantQueue));
      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      // Assert default priority 4 is used when priority header is not set
      Assert.assertEquals("getJMSPriority", 4, message.getJMSPriority());

      // Make sure that the timestamp is valid - should
      // be very close to the current time.
      long tnow = System.currentTimeMillis();
      long tmsg = message.getJMSTimestamp();
      Assert.assertTrue(Math.abs(tnow - tmsg) < 1500);

      // closing the consumer here should trigger auto-deletion
      assertNotNull(server.getActiveMQServer().getPostOffice().getBinding(new SimpleString(ResourceNames.JMS_QUEUE + nonExistantQueue)));
      consumer.close();
      assertNull(server.getActiveMQServer().getPostOffice().getBinding(new SimpleString(ResourceNames.JMS_QUEUE + nonExistantQueue)));
   }

   /*
    * Some STOMP clients erroneously put a new line \n *after* the terminating NUL char at the end of the frame
    * This means next frame read might have a \n a the beginning.
    * This is contrary to STOMP spec but we deal with it so we can work nicely with crappy STOMP clients
    */
   @Test
   public void testSendMessageWithLeadingNewLine() throws Exception
   {

      MessageConsumer consumer = session.createConsumer(queue);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL + "\n";
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SEND\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n\n" +
         "Hello World" +
         Stomp.NULL +
         "\n";

      sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());

      // Make sure that the timestamp is valid - should
      // be very close to the current time.
      long tnow = System.currentTimeMillis();
      long tmsg = message.getJMSTimestamp();
      Assert.assertTrue(Math.abs(tnow - tmsg) < 1000);
   }

   @Test
   public void testSendMessageWithReceipt() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SEND\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "receipt: 1234\n\n" +
         "Hello World" +
         Stomp.NULL;

      sendFrame(frame);

      String f = receiveFrame(10000);
      Assert.assertTrue(f.startsWith("RECEIPT"));
      Assert.assertTrue(f.indexOf("receipt-id:1234") >= 0);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());

      // Make sure that the timestamp is valid - should
      // be very close to the current time.
      long tnow = System.currentTimeMillis();
      long tmsg = message.getJMSTimestamp();
      Assert.assertTrue(Math.abs(tnow - tmsg) < 1000);
   }

   @Test
   public void testSendMessageWithContentLength() throws Exception
   {

      MessageConsumer consumer = session.createConsumer(queue);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      byte[] data = new byte[]{1, 0, 0, 4};

      frame = "SEND\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "content-length:" +
         data.length +
         "\n\n";
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(frame.getBytes(StandardCharsets.UTF_8));
      baos.write(data);
      baos.write('\0');
      baos.flush();
      sendFrame(new String(baos.toByteArray()));

      BytesMessage message = (BytesMessage) consumer.receive(10000);
      Assert.assertNotNull(message);
      assertEquals(data.length, message.getBodyLength());
      assertEquals(data[0], message.readByte());
      assertEquals(data[1], message.readByte());
      assertEquals(data[2], message.readByte());
      assertEquals(data[3], message.readByte());
   }

   @Test
   public void testJMSXGroupIdCanBeSet() throws Exception
   {

      MessageConsumer consumer = session.createConsumer(queue);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SEND\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "JMSXGroupID: TEST\n\n" +
         "Hello World" +
         Stomp.NULL;

      sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      // differ from StompConnect
      Assert.assertEquals("TEST", message.getStringProperty("JMSXGroupID"));
   }

   @Test
   public void testSendMessageWithCustomHeadersAndSelector() throws Exception
   {

      MessageConsumer consumer = session.createConsumer(queue, "foo = 'abc'");

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SEND\n" + "foo:abc\n" +
         "bar:123\n" +
         "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n\n" +
         "Hello World" +
         Stomp.NULL;

      sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("bar", "123", message.getStringProperty("bar"));
   }

   @Test
   public void testSendMessageWithStandardHeaders() throws Exception
   {

      MessageConsumer consumer = session.createConsumer(queue);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SEND\n" + "correlation-id:c123\n" +
         "persistent:true\n" +
         "priority:3\n" +
         "type:t345\n" +
         "JMSXGroupID:abc\n" +
         "foo:abc\n" +
         "bar:123\n" +
         "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n\n" +
         "Hello World" +
         Stomp.NULL;

      sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("JMSCorrelationID", "c123", message.getJMSCorrelationID());
      Assert.assertEquals("getJMSType", "t345", message.getJMSType());
      Assert.assertEquals("getJMSPriority", 3, message.getJMSPriority());
      Assert.assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("bar", "123", message.getStringProperty("bar"));

      Assert.assertEquals("JMSXGroupID", "abc", message.getStringProperty("JMSXGroupID"));
      // FIXME do we support it?
      // Assert.assertEquals("GroupID", "abc", amqMessage.getGroupID());
   }

   @Test
   public void testSendMessageWithLongHeaders() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      StringBuffer buffer = new StringBuffer();
      for (int i = 0; i < 1024; i++)
      {
         buffer.append("a");
      }
      String longHeader = "longHeader:" + buffer.toString() + "\n";

      frame = "SEND\n" + "correlation-id:c123\n" +
         "persistent:true\n" +
         "priority:3\n" +
         "type:t345\n" +
         "JMSXGroupID:abc\n" +
         "foo:abc\n" +
         longHeader +
         "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n\n" +
         "Hello World" +
         Stomp.NULL;

      sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("JMSCorrelationID", "c123", message.getJMSCorrelationID());
      Assert.assertEquals("getJMSType", "t345", message.getJMSType());
      Assert.assertEquals("getJMSPriority", 3, message.getJMSPriority());
      Assert.assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("longHeader", 1024, message.getStringProperty("longHeader").length());

      Assert.assertEquals("JMSXGroupID", "abc", message.getStringProperty("JMSXGroupID"));
   }

   @Test
   public void testSubscribeWithAutoAck() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:auto\n\nfff" + Stomp.NULL;
      sendFrame(frame);

      sendMessage(getName());

      frame = receiveFrame(10000);
      System.out.println("-------- frame received: " + frame);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      Assert.assertTrue(frame.indexOf("destination:") > 0);
      Assert.assertTrue(frame.indexOf(getName()) > 0);

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      // message should not be received as it was auto-acked
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);

   }

   @Test
   public void testSubscribeWithAutoAckAndBytesMessage() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
      sendFrame(frame);

      byte[] payload = new byte[]{1, 2, 3, 4, 5};
      sendMessage(payload, queue);

      frame = receiveFrame(10000);

      System.out.println("Message: " + frame);

      Assert.assertTrue(frame.startsWith("MESSAGE"));

      Pattern cl = Pattern.compile("Content-length:\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
      Matcher cl_matcher = cl.matcher(frame);
      Assert.assertTrue(cl_matcher.find());
      Assert.assertEquals("5", cl_matcher.group(1));

      Assert.assertFalse(Pattern.compile("type:\\s*null", Pattern.CASE_INSENSITIVE).matcher(frame).find());
      Assert.assertTrue(frame.indexOf(new String(payload)) > -1);

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

   @Test
   public void testSubscribeWithMessageSentWithProperties() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
      sendFrame(frame);

      MessageProducer producer = session.createProducer(queue);
      BytesMessage message = session.createBytesMessage();
      message.setStringProperty("S", "value");
      message.setBooleanProperty("n", false);
      message.setByteProperty("byte", (byte) 9);
      message.setDoubleProperty("d", 2.0);
      message.setFloatProperty("f", (float) 6.0);
      message.setIntProperty("i", 10);
      message.setLongProperty("l", 121);
      message.setShortProperty("s", (short) 12);
      message.writeBytes("Hello World".getBytes(StandardCharsets.UTF_8));
      producer.send(message);

      frame = receiveFrame(10000);
      Assert.assertNotNull(frame);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      Assert.assertTrue(frame.indexOf("S:") > 0);
      Assert.assertTrue(frame.indexOf("n:") > 0);
      Assert.assertTrue(frame.indexOf("byte:") > 0);
      Assert.assertTrue(frame.indexOf("d:") > 0);
      Assert.assertTrue(frame.indexOf("f:") > 0);
      Assert.assertTrue(frame.indexOf("i:") > 0);
      Assert.assertTrue(frame.indexOf("l:") > 0);
      Assert.assertTrue(frame.indexOf("s:") > 0);
      Assert.assertTrue(frame.indexOf("Hello World") > 0);

      // System.out.println("out: "+frame);

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

   @Test
   public void testSubscribeWithID() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "ack:auto\n" +
         "id: mysubid\n\n" +
         Stomp.NULL;
      sendFrame(frame);

      sendMessage(getName());

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      Assert.assertTrue(frame.indexOf("destination:") > 0);
      Assert.assertTrue(frame.indexOf("subscription:") > 0);
      Assert.assertTrue(frame.indexOf(getName()) > 0);

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

   @Test
   public void testBodyWithUTF8() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
      sendFrame(frame);

      String text = "A" + "\u00ea" + "\u00f1" + "\u00fc" + "C";
      System.out.println(text);
      sendMessage(text);

      frame = receiveFrame(10000);
      System.out.println(frame);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      Assert.assertTrue(frame.indexOf("destination:") > 0);
      Assert.assertTrue(frame.indexOf(text) > 0);

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

   @Test
   public void testMessagesAreInOrder() throws Exception
   {
      int ctr = 10;
      String[] data = new String[ctr];

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
      sendFrame(frame);

      for (int i = 0; i < ctr; ++i)
      {
         data[i] = getName() + i;
         sendMessage(data[i]);
      }

      for (int i = 0; i < ctr; ++i)
      {
         frame = receiveFrame(1000);
         Assert.assertTrue("Message not in order", frame.indexOf(data[i]) >= 0);
      }

      // sleep a while before publishing another set of messages
      waitForFrameToTakeEffect();

      for (int i = 0; i < ctr; ++i)
      {
         data[i] = getName() + ":second:" + i;
         sendMessage(data[i]);
      }

      for (int i = 0; i < ctr; ++i)
      {
         frame = receiveFrame(1000);
         Assert.assertTrue("Message not in order", frame.indexOf(data[i]) >= 0);
      }

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

   @Test
   public void testSubscribeWithAutoAckAndSelector() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "selector: foo = 'zzz'\n" +
         "ack:auto\n\n" +
         Stomp.NULL;
      sendFrame(frame);

      sendMessage("Ignored message", "foo", "1234");
      sendMessage("Real message", "foo", "zzz");

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      Assert.assertTrue("Should have received the real message but got: " + frame, frame.indexOf("Real message") > 0);

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

   @Test
   public void testSubscribeWithClientAck() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:client\n\n" + Stomp.NULL;

      sendFrame(frame);

      sendMessage(getName());
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      Pattern cl = Pattern.compile("message-id:\\s*(\\S+)", Pattern.CASE_INSENSITIVE);
      Matcher cl_matcher = cl.matcher(frame);
      Assert.assertTrue(cl_matcher.find());
      String messageID = cl_matcher.group(1);

      frame = "ACK\n" + "message-id: " + messageID + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      // message should not be received since message was acknowledged by the client
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testRedeliveryWithClientAck() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:client\n\n" + Stomp.NULL;

      sendFrame(frame);

      sendMessage(getName());
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      // message should be received since message was not acknowledged
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertTrue(message.getJMSRedelivered());
   }

   @Test
   public void testSubscribeWithClientAckThenConsumingAgainWithAutoAckWithNoDisconnectFrame() throws Exception
   {
      assertSubscribeWithClientAckThenConsumeWithAutoAck(false);
   }

   @Test
   public void testSubscribeWithClientAckThenConsumingAgainWithAutoAckWithExplicitDisconnect() throws Exception
   {
      assertSubscribeWithClientAckThenConsumeWithAutoAck(true);
   }

   protected void assertSubscribeWithClientAckThenConsumeWithAutoAck(boolean sendDisconnect) throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:client\n\n" + Stomp.NULL;

      sendFrame(frame);
      sendMessage(getName());

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));

      log.info("Reconnecting!");

      if (sendDisconnect)
      {
         frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
         sendFrame(frame);
         waitForFrameToTakeEffect();
         reconnect();
      }
      else
      {
         reconnect(100);
         waitForFrameToTakeEffect();
      }

      // message should be received since message was not acknowledged
      frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n\n" + Stomp.NULL;

      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
      waitForFrameToTakeEffect();

      // now let's make sure we don't see the message again
      reconnect();

      frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "receipt: 1234\n\n" +
         Stomp.NULL;

      sendFrame(frame);
      // wait for SUBSCRIBE's receipt
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("RECEIPT"));

      sendMessage("shouldBeNextMessage");

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      System.out.println(frame);
      Assert.assertTrue(frame.contains("shouldBeNextMessage"));
   }

   @Test
   public void testUnsubscribe() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);
      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
      sendFrame(frame);

      // send a message to our queue
      sendMessage("first message");

      // receive message from socket
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));

      // remove suscription
      frame = "UNSUBSCRIBE\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "receipt:567\n" +
         "\n\n" +
         Stomp.NULL;
      sendFrame(frame);
      waitForReceipt();

      // send a message to our queue
      sendMessage("second message");

      frame = receiveFrame(1000);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);
   }

   @Test
   public void testUnsubscribeWithID() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);
      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "id: mysubid\n" +
         "ack:auto\n\n" +
         Stomp.NULL;
      sendFrame(frame);

      // send a message to our queue
      sendMessage("first message");

      // receive message from socket
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));

      // remove suscription
      frame = "UNSUBSCRIBE\n" + "id:mysubid\n" + "receipt: 345\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
      waitForReceipt();

      // send a message to our queue
      sendMessage("second message");

      frame = receiveFrame(1000);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);

   }

   @Test
   public void testTransactionCommit() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      String f = receiveFrame(1000);
      Assert.assertTrue(f.startsWith("CONNECTED"));

      frame = "BEGIN\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = "SEND\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "transaction: tx1\n" +
         "receipt: 123\n" +
         "\n\n" +
         "Hello World" +
         Stomp.NULL;
      sendFrame(frame);
      waitForReceipt();

      // check the message is not committed
      assertNull(consumer.receive(100));

      frame = "COMMIT\n" + "transaction: tx1\n" + "receipt:456\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
      waitForReceipt();

      Message message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);
   }

   @Test
   public void testSuccessiveTransactionsWithSameID() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      String f = receiveFrame(1000);
      Assert.assertTrue(f.startsWith("CONNECTED"));

      // first tx
      frame = "BEGIN\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = "SEND\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "transaction: tx1\n" +
         "\n\n" +
         "Hello World" +
         Stomp.NULL;
      sendFrame(frame);

      frame = "COMMIT\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      Message message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);

      // 2nd tx with same tx ID
      frame = "BEGIN\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = "SEND\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "transaction: tx1\n" +
         "\n\n" +
         "Hello World" +
         Stomp.NULL;
      sendFrame(frame);

      frame = "COMMIT\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);
   }

   @Test
   public void testBeginSameTransactionTwice() throws Exception
   {
      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      String f = receiveFrame(1000);
      Assert.assertTrue(f.startsWith("CONNECTED"));

      frame = "BEGIN\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      // begin the tx a 2nd time
      frame = "BEGIN\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      f = receiveFrame(1000);
      Assert.assertTrue(f.startsWith("ERROR"));

   }

   @Test
   public void testTransactionRollback() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      String f = receiveFrame(1000);
      Assert.assertTrue(f.startsWith("CONNECTED"));

      frame = "BEGIN\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = "SEND\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "transaction: tx1\n" +
         "\n" +
         "first message" +
         Stomp.NULL;
      sendFrame(frame);

      // rollback first message
      frame = "ABORT\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = "BEGIN\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = "SEND\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "transaction: tx1\n" +
         "\n" +
         "second message" +
         Stomp.NULL;
      sendFrame(frame);

      frame = "COMMIT\n" + "transaction: tx1\n" + "receipt:789\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
      waitForReceipt();

      // only second msg should be received since first msg was rolled back
      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("second message", message.getText());
   }

   @Test
   public void testSubscribeToTopic() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" +
         getTopicPrefix() +
         getTopicName() +
         "\n" +
         "receipt: 12\n" +
         "\n\n" +
         Stomp.NULL;
      sendFrame(frame);
      // wait for SUBSCRIBE's receipt
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("RECEIPT"));

      sendMessage(getName(), topic);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      Assert.assertTrue(frame.indexOf("destination:") > 0);
      Assert.assertTrue(frame.indexOf(getName()) > 0);

      frame = "UNSUBSCRIBE\n" + "destination:" +
         getTopicPrefix() +
         getTopicName() +
         "\n" +
         "receipt: 1234\n" +
         "\n\n" +
         Stomp.NULL;
      sendFrame(frame);
      // wait for UNSUBSCRIBE's receipt
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("RECEIPT"));

      sendMessage(getName(), topic);

      frame = receiveFrame(1000);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);


      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

   @Test
   public void testSubscribeToNonExistantQueue() throws Exception
   {
      String nonExistantQueue = RandomUtil.randomString();

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" +
         getQueuePrefix() +
         nonExistantQueue +
         "\n" +
         "receipt: 12\n" +
         "\n\n" +
         Stomp.NULL;
      sendFrame(frame);
      // wait for SUBSCRIBE's receipt
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("RECEIPT"));

      sendMessage(getName(), ActiveMQJMSClient.createQueue(nonExistantQueue));

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      Assert.assertTrue(frame.indexOf("destination:") > 0);
      Assert.assertTrue(frame.indexOf(getName()) > 0);

      assertNotNull(server.getActiveMQServer().getPostOffice().getBinding(new SimpleString(ResourceNames.JMS_QUEUE + nonExistantQueue)));

      frame = "UNSUBSCRIBE\n" + "destination:" +
         getQueuePrefix() +
         nonExistantQueue +
         "\n" +
         "receipt: 1234\n" +
         "\n\n" +
         Stomp.NULL;
      sendFrame(frame);
      // wait for UNSUBSCRIBE's receipt
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("RECEIPT"));

      assertNull(server.getActiveMQServer().getPostOffice().getBinding(new SimpleString(ResourceNames.JMS_QUEUE + nonExistantQueue)));

      sendMessage(getName(), ActiveMQJMSClient.createQueue(nonExistantQueue));

      frame = receiveFrame(1000);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);


      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

   @Test
   public void testDurableSubscriberWithReconnection() throws Exception
   {

      String connectFame = "CONNECT\n" + "login: brianm\n" +
         "passcode: wombats\n" +
         "client-id: myclientid\n\n" +
         Stomp.NULL;
      sendFrame(connectFame);

      String frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      String subscribeFrame = "SUBSCRIBE\n" + "destination:" +
         getTopicPrefix() +
         getTopicName() +
         "\n" +
         "durable-subscriber-name: " +
         getName() +
         "\n" +
         "\n\n" +
         Stomp.NULL;
      sendFrame(subscribeFrame);
      waitForFrameToTakeEffect();

      String disconnectFrame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(disconnectFrame);
      waitForFrameToTakeEffect();

      // send the message when the durable subscriber is disconnected
      sendMessage(getName(), topic);

      reconnect(100);
      sendFrame(connectFame);
      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      sendFrame(subscribeFrame);

      // we must have received the message
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      Assert.assertTrue(frame.indexOf("destination:") > 0);
      Assert.assertTrue(frame.indexOf(getName()) > 0);

      String unsubscribeFrame = "UNSUBSCRIBE\n" + "destination:" +
         getTopicPrefix() +
         getTopicName() +
         "\n" +
         "receipt: 1234\n" +
         "\n\n" +
         Stomp.NULL;
      sendFrame(unsubscribeFrame);
      // wait for UNSUBSCRIBE's receipt
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("RECEIPT"));

      sendFrame(disconnectFrame);
   }

   @Test
   public void testDurableSubscriber() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n" + "client-id: myclientid\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      String subscribeFrame = "SUBSCRIBE\n" + "destination:" +
         getTopicPrefix() +
         getTopicName() +
         "\n" +
         "receipt: 12\n" +
         "durable-subscriber-name: " +
         getName() +
         "\n" +
         "\n\n" +
         Stomp.NULL;
      sendFrame(subscribeFrame);
      // wait for SUBSCRIBE's receipt
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("RECEIPT"));

      // creating a subscriber with the same durable-subscriber-name must fail
      sendFrame(subscribeFrame);
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("ERROR"));

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

   @Test
   public void testSubscribeToTopicWithNoLocal() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" +
         getTopicPrefix() +
         getTopicName() +
         "\n" +
         "receipt: 12\n" +
         "no-local: true\n" +
         "\n\n" +
         Stomp.NULL;
      sendFrame(frame);
      // wait for SUBSCRIBE's receipt
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("RECEIPT"));

      // send a message on the same connection => it should not be received
      frame = "SEND\n" + "destination:" + getTopicPrefix() + getTopicName() + "\n\n" + "Hello World" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(2000);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);


      // send message on another JMS connection => it should be received
      sendMessage(getName(), topic);
      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      Assert.assertTrue(frame.indexOf("destination:") > 0);
      Assert.assertTrue(frame.indexOf(getName()) > 0);

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

   @Test
   public void testClientAckNotPartOfTransaction() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" +
         getQueuePrefix() +
         getQueueName() +
         "\n" +
         "ack:client\n" +
         "\n\n" +
         Stomp.NULL;
      sendFrame(frame);

      sendMessage(getName());

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      Assert.assertTrue(frame.indexOf("destination:") > 0);
      Assert.assertTrue(frame.indexOf(getName()) > 0);
      Assert.assertTrue(frame.indexOf("message-id:") > 0);
      Pattern cl = Pattern.compile("message-id:\\s*(\\S+)", Pattern.CASE_INSENSITIVE);
      Matcher cl_matcher = cl.matcher(frame);
      Assert.assertTrue(cl_matcher.find());
      String messageID = cl_matcher.group(1);

      frame = "BEGIN\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = "ACK\n" + "message-id:" + messageID + "\n" + "transaction: tx1\n" + "\n" + "second message" + Stomp.NULL;
      sendFrame(frame);

      frame = "ABORT\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(1000);
      Assert.assertNull("No message should have been received as the message was acked even though the transaction has been aborted", frame);

      frame = "UNSUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

   // HORNETQ-1007
   @Test
   public void testMultiProtocolConsumers() throws Exception
   {
      final int TIME_OUT = 5000;

      int count = 1000;

      // Create 2 core consumers
      MessageConsumer consumer1 = session.createConsumer(topic);
      MessageConsumer consumer2 = session.createConsumer(topic);

      // connect and subscribe STOMP consumer
      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);
      frame = receiveFrame(TIME_OUT);
      Assert.assertTrue(frame.startsWith("CONNECTED"));
      frame = "SUBSCRIBE\n" + "destination:" +
         getTopicPrefix() +
         getTopicName() +
         "\n" +
         "receipt: 12\n" +
         "\n\n" +
         Stomp.NULL;
      sendFrame(frame);
      // wait for SUBSCRIBE's receipt
      frame = receiveFrame(TIME_OUT);
      Assert.assertTrue(frame.startsWith("RECEIPT"));

      MessageProducer producer = session.createProducer(topic);
      TextMessage message = session.createTextMessage(getName());

      for (int i = 1; i <= count; i++)
      {
         producer.send(message);
         Assert.assertNotNull(consumer1.receive(TIME_OUT));
         Assert.assertNotNull(consumer2.receive(TIME_OUT));
         frame = receiveFrame(TIME_OUT);
         Assert.assertTrue(frame.startsWith("MESSAGE"));
         Assert.assertTrue(frame.indexOf("destination:") > 0);
         Assert.assertTrue(frame.indexOf(getName()) > 0);
      }

      consumer1.close();
      consumer2.close();
      frame = "UNSUBSCRIBE\n" + "destination:" +
         getTopicPrefix() +
         getTopicName() +
         "\n" +
         "receipt: 1234\n" +
         "\n\n" +
         Stomp.NULL;
      sendFrame(frame);
      // wait for UNSUBSCRIBE's receipt
      frame = receiveFrame(TIME_OUT);
      Assert.assertTrue(frame.startsWith("RECEIPT"));

      sendMessage(getName(), topic);

      frame = receiveFrame(TIME_OUT);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

   @Test
   //stomp should return an ERROR when acking a non-existent message
   public void testUnexpectedAck() throws Exception
   {

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      String messageID = "888888";
      frame = "ACK\n" + "message-id:" + messageID + "\n" + "\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(100000);
      assertNotNull(frame);

      System.out.println("received frame: " + frame);
      assertTrue(frame.startsWith("ERROR"));

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
   }

}

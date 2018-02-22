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

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.protocol.stomp.Stomp;
import org.apache.activemq.artemis.core.protocol.stomp.StompProtocolManagerFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.mqtt.imported.FuseMQTTClientProvider;
import org.apache.activemq.artemis.tests.integration.mqtt.imported.MQTTClientProvider;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StompTest extends StompTestBase {

   private static final transient IntegrationTestLogger log = IntegrationTestLogger.LOGGER;
   protected StompClientConnection conn;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      conn = StompClientConnectionFactory.createClientConnection(uri);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         boolean connected = conn != null && conn.isConnected();
         log.debug("Connection 1.0 connected: " + connected);
         if (connected) {
            try {
               conn.disconnect();
            } catch (Exception e) {
               // ignore
            }
         }
      } finally {
         super.tearDown();
         conn.closeTransport();
      }
   }

   @Test
   public void testConnectionTTL() throws Exception {
      int port = 61614;

      URI uri = createStompClientUri(scheme, hostname, port);

      server.getActiveMQServer().getRemotingService().createAcceptor("test", "tcp://127.0.0.1:" + port + "?connectionTtl=1000").start();
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(uri);
      conn.connect("brianm", "wombats");

      Thread.sleep(5000);

      ClientStompFrame frame = conn.receiveFrame();

      assertEquals(Stomp.Responses.ERROR, frame.getCommand());

      assertFalse(conn.isConnected());
   }

   @Test
   public void testSendManyMessages() throws Exception {
      conn.connect(defUser, defPass);

      MessageConsumer consumer = session.createConsumer(queue);

      int count = 1000;
      final CountDownLatch latch = new CountDownLatch(count);
      consumer.setMessageListener(new MessageListener() {

         @Override
         public void onMessage(Message arg0) {
            latch.countDown();
         }
      });

      for (int i = 1; i <= count; i++) {
         send(conn, getQueuePrefix() + getQueueName(), null, "Hello World!");
      }

      assertTrue(latch.await(60, TimeUnit.SECONDS));
   }

   @Test
   public void testSendOverDiskFull() throws Exception {
      AssertionLoggerHandler.startCapture();
      try {
         MessageConsumer consumer = session.createConsumer(queue);

         conn.connect(defUser, defPass);
         int count = 1000;
         final CountDownLatch latch = new CountDownLatch(count);
         consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message arg0) {
               latch.countDown();
            }
         });

         ((ActiveMQServerImpl) server.getActiveMQServer()).getMonitor()
                                                          .setMaxUsage(0)
                                                          .tick();

         // Connection should be closed by broker when disk is full and attempt to send
         Exception e = null;
         try {
            for (int i = 1; i <= count; i++) {
               send(conn, getQueuePrefix() + getQueueName(), null, "Hello World!");
            }
         } catch (Exception se) {
            e = se;
         }
         assertNotNull(e);
         // It should encounter the exception on logs
         AssertionLoggerHandler.findText("AMQ119119");
      } finally {
         AssertionLoggerHandler.clear();
         AssertionLoggerHandler.stopCapture();
      }
   }

   @Test
   public void testConnect() throws Exception {
      ClientStompFrame frame = conn.createFrame(Stomp.Commands.CONNECT)
                                   .addHeader(Stomp.Headers.Connect.LOGIN, defUser)
                                   .addHeader(Stomp.Headers.Connect.PASSCODE, defPass)
                                   .addHeader(Stomp.Headers.Connect.REQUEST_ID, "1");
      ClientStompFrame response = conn.sendFrame(frame);

      Assert.assertTrue(response.getCommand()
                                .equals(Stomp.Responses.CONNECTED));
      Assert.assertTrue(response.getHeader(Stomp.Headers.Connected.RESPONSE_ID)
                                .equals("1"));
   }

   @Test
   public void testDisconnectAndError() throws Exception {
      ClientStompFrame frame = conn.createFrame(Stomp.Commands.CONNECT)
                                   .addHeader(Stomp.Headers.Connect.LOGIN, defUser)
                                   .addHeader(Stomp.Headers.Connect.PASSCODE, defPass)
                                   .addHeader(Stomp.Headers.Connect.REQUEST_ID, "1");
      ClientStompFrame response = conn.sendFrame(frame);

      Assert.assertTrue(response.getCommand()
                                .equals(Stomp.Responses.CONNECTED));
      Assert.assertTrue(response.getHeader(Stomp.Headers.Connected.RESPONSE_ID)
                                .equals("1"));

      conn.disconnect();

      // sending a message will result in an error
      try {
         send(conn, getQueuePrefix() + getQueueName(), null, "Hello World!");
         fail("Should have thrown an exception since connection is disconnected");
      } catch (Exception e) {
         // ignore
      }
   }

   @Test
   public void testSendMessage() throws Exception {

      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      send(conn, getQueuePrefix() + getQueueName(), null, "Hello World");

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
   public void sendSTOMPReceiveMQTT() throws Exception {
      // Set up MQTT Subscription
      MQTTClientProvider clientProvider = new FuseMQTTClientProvider();
      clientProvider.connect("tcp://" + hostname + ":" + port);
      clientProvider.subscribe(getTopicPrefix() + getTopicName(), 0);

      String stompPayload = "This is a test message";

      // Set up STOMP connection and send STOMP Message
      conn.connect(defUser, defPass);
      send(conn, getTopicPrefix() + getTopicName(), null, stompPayload);

      // Receive MQTT Message
      byte[] mqttPayload = clientProvider.receive(10000);
      clientProvider.disconnect();

      assertEquals(stompPayload, new String(mqttPayload, "UTF-8"));
      clientProvider.disconnect();
   }

   @Test
   public void sendMQTTReceiveSTOMP() throws Exception {
      String payload = "This is a test message";

      // Set up STOMP subscription
      conn.connect(defUser, defPass);
      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.AUTO);

      // Send MQTT Message
      MQTTClientProvider clientProvider = new FuseMQTTClientProvider();
      clientProvider.connect("tcp://" + hostname + ":" + port);
      clientProvider.publish(getQueuePrefix() + getQueueName(), payload.getBytes(), 0);
      clientProvider.disconnect();

      // Receive STOMP Message
      ClientStompFrame frame = conn.receiveFrame();
      assertTrue(frame.getBody()
                      .contains(payload));

   }

   @Test
   public void testSendMessageToNonExistentQueue() throws Exception {
      String nonExistentQueue = RandomUtil.randomString();
      conn.connect(defUser, defPass);
      send(conn, getQueuePrefix() + nonExistentQueue, null, "Hello World", true, RoutingType.ANYCAST);

      MessageConsumer consumer = session.createConsumer(ActiveMQJMSClient.createQueue(nonExistentQueue));
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
      assertNotNull(server.getActiveMQServer()
                          .getPostOffice()
                          .getBinding(new SimpleString(nonExistentQueue)));
      consumer.close();
      assertNull(server.getActiveMQServer()
                       .getPostOffice()
                       .getBinding(new SimpleString(nonExistentQueue)));
   }

   @Test
   public void testSendMessageToNonExistentTopic() throws Exception {
      String nonExistentTopic = RandomUtil.randomString();
      conn.connect(defUser, defPass);

      // first send a message to ensure that sending to a non-existent topic won't throw an error
      send(conn, getTopicPrefix() + nonExistentTopic, null, "Hello World", true, RoutingType.MULTICAST);

      // create a subscription on the topic and send/receive another message
      MessageConsumer consumer = session.createConsumer(ActiveMQJMSClient.createTopic(nonExistentTopic));
      send(conn, getTopicPrefix() + nonExistentTopic, null, "Hello World", true);
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

      assertNotNull(server.getActiveMQServer()
                          .getAddressInfo(new SimpleString(nonExistentTopic)));

      // closing the consumer here should trigger auto-deletion of the subscription queue and address
      consumer.close();
      Thread.sleep(200);
      assertNull(server.getActiveMQServer()
                       .getAddressInfo(new SimpleString(nonExistentTopic)));
   }

   /*
    * Some STOMP clients erroneously put a new line \n *after* the terminating NUL char at the end of the frame
    * This means next frame read might have a \n a the beginning.
    * This is contrary to STOMP spec but we deal with it so we can work nicely with crappy STOMP clients
    */
   @Test
   public void testSendMessageWithLeadingNewLine() throws Exception {
      conn.connect(defUser, defPass);
      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, getQueuePrefix() + getQueueName())
                                   .setBody("Hello World");
      conn.sendWickedFrame(frame);

      MessageConsumer consumer = session.createConsumer(queue);
      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());

      send(conn, getQueuePrefix() + getQueueName(), null, "Hello World");
      message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());

      // Make sure that the timestamp is valid - should
      // be very close to the current time.
      long tnow = System.currentTimeMillis();
      long tmsg = message.getJMSTimestamp();
      Assert.assertTrue(Math.abs(tnow - tmsg) < 1000);
   }

   @Test
   public void testSendMessageWithReceipt() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      send(conn, getQueuePrefix() + getQueueName(), null, "Hello World", true);

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
   public void testSendMessageWithContentLength() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);
      conn.connect(defUser, defPass);

      byte[] data = new byte[]{1, 0, 0, 4};
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(data);
      baos.flush();

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader(Stomp.Headers.CONTENT_LENGTH, Integer.toString(data.length))
                                   .setBody(new String(baos.toByteArray()));
      conn.sendFrame(frame);

      BytesMessage message = (BytesMessage) consumer.receive(10000);
      Assert.assertNotNull(message);
      assertEquals(data.length, message.getBodyLength());
      assertEquals(data[0], message.readByte());
      assertEquals(data[1], message.readByte());
      assertEquals(data[2], message.readByte());
      assertEquals(data[3], message.readByte());
   }

   @Test
   public void testJMSXGroupIdCanBeSet() throws Exception {
      final String jmsxGroupID = "JMSXGroupID";
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader("JMSXGroupID", jmsxGroupID)
                                   .setBody("Hello World");
      conn.sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      // differ from StompConnect
      Assert.assertEquals(jmsxGroupID, message.getStringProperty("JMSXGroupID"));
   }

   @Test
   public void testSendMessageWithCustomHeadersAndSelector() throws Exception {

      MessageConsumer consumer = session.createConsumer(queue, "foo = 'abc'");

      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader("foo", "abc")
                                   .addHeader("bar", "123")
                                   .setBody("Hello World");
      conn.sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("bar", "123", message.getStringProperty("bar"));
   }

   @Test
   public void testSendMessageWithCustomHeadersAndHyphenatedSelector() throws Exception {

      MessageConsumer consumer = session.createConsumer(queue, "hyphenated_props:b-ar = '123'");

      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader("foo", "abc")
                                   .addHeader("b-ar", "123")
                                   .setBody("Hello World");
      conn.sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("b-ar", "123", message.getStringProperty("b-ar"));
   }

   @Test
   public void testSendMessageWithStandardHeaders() throws Exception {

      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader("foo", "abc")
                                   .addHeader("bar", "123")
                                   .addHeader("correlation-id", "c123")
                                   .addHeader("persistent", "true")
                                   .addHeader("type", "t345")
                                   .addHeader("JMSXGroupID", "abc")
                                   .addHeader("priority", "3")
                                   .setBody("Hello World");
      conn.sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("JMSCorrelationID", "c123", message.getJMSCorrelationID());
      Assert.assertEquals("getJMSType", "t345", message.getJMSType());
      Assert.assertEquals("getJMSPriority", 3, message.getJMSPriority());
      Assert.assertEquals(javax.jms.DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("bar", "123", message.getStringProperty("bar"));

      Assert.assertEquals("JMSXGroupID", "abc", message.getStringProperty("JMSXGroupID"));
      // FIXME do we support it?
      // Assert.assertEquals("GroupID", "abc", amqMessage.getGroupID());
   }

   @Test
   public void testSendMessageWithLongHeaders() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      StringBuffer buffer = new StringBuffer();
      for (int i = 0; i < 1024; i++) {
         buffer.append("a");
      }

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader("foo", "abc")
                                   .addHeader("bar", "123")
                                   .addHeader("correlation-id", "c123")
                                   .addHeader("persistent", "true")
                                   .addHeader("type", "t345")
                                   .addHeader("JMSXGroupID", "abc")
                                   .addHeader("priority", "3")
                                   .addHeader("longHeader", buffer.toString())
                                   .setBody("Hello World");
      conn.sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("JMSCorrelationID", "c123", message.getJMSCorrelationID());
      Assert.assertEquals("getJMSType", "t345", message.getJMSType());
      Assert.assertEquals("getJMSPriority", 3, message.getJMSPriority());
      Assert.assertEquals(javax.jms.DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("longHeader", 1024, message.getStringProperty("longHeader")
                                                     .length());

      Assert.assertEquals("JMSXGroupID", "abc", message.getStringProperty("JMSXGroupID"));
   }

   @Test
   public void testSendMessageWithDelay() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader("foo", "abc")
                                   .addHeader("bar", "123")
                                   .addHeader("correlation-id", "c123")
                                   .addHeader("persistent", "true")
                                   .addHeader("type", "t345")
                                   .addHeader("JMSXGroupID", "abc")
                                   .addHeader("priority", "3")
                                   .addHeader("AMQ_SCHEDULED_DELAY", "2000")
                                   .setBody("Hello World");
      conn.sendFrame(frame);

      assertNull("Should not receive message yet", consumer.receive(1000));

      TextMessage message = (TextMessage) consumer.receive(4000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("JMSCorrelationID", "c123", message.getJMSCorrelationID());
      Assert.assertEquals("getJMSType", "t345", message.getJMSType());
      Assert.assertEquals("getJMSPriority", 3, message.getJMSPriority());
      Assert.assertEquals(javax.jms.DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("JMSXGroupID", "abc", message.getStringProperty("JMSXGroupID"));
   }

   @Test
   public void testSendMessageWithDeliveryTime() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader("foo", "abc")
                                   .addHeader("bar", "123")
                                   .addHeader("correlation-id", "c123")
                                   .addHeader("persistent", "true")
                                   .addHeader("type", "t345")
                                   .addHeader("JMSXGroupID", "abc")
                                   .addHeader("priority", "3")
                                   .addHeader("AMQ_SCHEDULED_TIME", Long.toString(System.currentTimeMillis() + 2000))
                                   .setBody("Hello World");
      conn.sendFrame(frame);

      assertNull("Should not receive message yet", consumer.receive(1000));

      TextMessage message = (TextMessage) consumer.receive(4000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("JMSCorrelationID", "c123", message.getJMSCorrelationID());
      Assert.assertEquals("getJMSType", "t345", message.getJMSType());
      Assert.assertEquals("getJMSPriority", 3, message.getJMSPriority());
      Assert.assertEquals(javax.jms.DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("JMSXGroupID", "abc", message.getStringProperty("JMSXGroupID"));
   }

   @Test
   public void testSendMessageWithDelayWithBadValue() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader("AMQ_SCHEDULED_DELAY", "foo")
                                   .setBody("Hello World");
      conn.sendFrame(frame);

      assertNull("Should not receive message yet", consumer.receive(1000));

      ClientStompFrame error = conn.receiveFrame();

      Assert.assertNotNull(error);
      Assert.assertTrue(error.getCommand().equals("ERROR"));
   }

   @Test
   public void testSendMessageWithDeliveryTimeWithBadValue() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader("AMQ_SCHEDULED_TIME", "foo")
                                   .setBody("Hello World");
      conn.sendFrame(frame);

      assertNull("Should not receive message yet", consumer.receive(1000));

      ClientStompFrame error = conn.receiveFrame();

      Assert.assertNotNull(error);
      Assert.assertTrue(error.getCommand().equals("ERROR"));
   }

   @Test
   public void testSubscribeWithAutoAck() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.AUTO);

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader(Stomp.Headers.Send.DESTINATION));
      Assert.assertEquals(getName(), frame.getBody());

      Pattern cl = Pattern.compile(Stomp.Headers.CONTENT_LENGTH + ":\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
      Matcher cl_matcher = cl.matcher(frame.toString());
      Assert.assertFalse(cl_matcher.find());

      conn.disconnect();

      // message should not be received as it was auto-acked
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);

   }

   @Test
   public void testSubscribeWithAutoAckAndBytesMessage() throws Exception {
      conn.connect(defUser, defPass);
      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.AUTO);

      byte[] payload = new byte[]{1, 2, 3, 4, 5};

      sendJmsMessage(payload, queue);

      ClientStompFrame frame = conn.receiveFrame(10000);

      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

      Pattern cl = Pattern.compile(Stomp.Headers.CONTENT_LENGTH + ":\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
      Matcher cl_matcher = cl.matcher(frame.toString());
      Assert.assertTrue(cl_matcher.find());
      Assert.assertEquals("5", cl_matcher.group(1));

      Assert.assertFalse(Pattern.compile("type:\\s*null", Pattern.CASE_INSENSITIVE).matcher(frame.toString()).find());
      Assert.assertTrue(frame.getBody().toString().indexOf(new String(payload)) > -1);

      conn.disconnect();
   }

   @Test
   public void testSubscribeWithMessageSentWithProperties() throws Exception {
      conn.connect(defUser, defPass);
      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.AUTO);

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

      ClientStompFrame frame = conn.receiveFrame(10000);
      Assert.assertNotNull(frame);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals("value", frame.getHeader("S"));
      Assert.assertEquals("false", frame.getHeader("n"));
      Assert.assertEquals("9", frame.getHeader("byte"));
      Assert.assertEquals("2.0", frame.getHeader("d"));
      Assert.assertEquals("6.0", frame.getHeader("f"));
      Assert.assertEquals("10", frame.getHeader("i"));
      Assert.assertEquals("121", frame.getHeader("l"));
      Assert.assertEquals("12", frame.getHeader("s"));
      Assert.assertEquals("Hello World", frame.getBody());

      conn.disconnect();
   }

   @Test
   public void testSubscribeWithID() throws Exception {
      conn.connect(defUser, defPass);
      subscribe(conn, "mysubid", Stomp.Headers.Subscribe.AckModeValues.AUTO);

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader(Stomp.Headers.Message.DESTINATION));
      Assert.assertEquals("mysubid", frame.getHeader(Stomp.Headers.Message.SUBSCRIPTION));
      Assert.assertEquals(getName(), frame.getBody());

      conn.disconnect();
   }

   //
   @Test
   public void testBodyWithUTF8() throws Exception {
      conn.connect(defUser, defPass);
      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.AUTO);

      String text = "A" + "\u00ea" + "\u00f1" + "\u00fc" + "C";
      log.info(text);
      sendJmsMessage(text);

      ClientStompFrame frame = conn.receiveFrame(10000);
      log.info(frame);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader(Stomp.Headers.Message.DESTINATION));
      Assert.assertEquals(text, frame.getBody());

      conn.disconnect();
   }

   @Test
   public void testMessagesAreInOrder() throws Exception {
      int ctr = 10;
      String[] data = new String[ctr];

      conn.connect(defUser, defPass);
      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.AUTO);

      for (int i = 0; i < ctr; ++i) {
         data[i] = getName() + i;
         sendJmsMessage(data[i]);
      }

      for (int i = 0; i < ctr; ++i) {
         ClientStompFrame frame = conn.receiveFrame(1000);
         Assert.assertTrue("Message not in order", frame.getBody().equals(data[i]));
      }

      // sleep a while before publishing another set of messages
      Thread.sleep(200);

      for (int i = 0; i < ctr; ++i) {
         data[i] = getName() + Stomp.Headers.SEPARATOR + "second:" + i;
         sendJmsMessage(data[i]);
      }

      for (int i = 0; i < ctr; ++i) {
         ClientStompFrame frame = conn.receiveFrame(1000);
         Assert.assertTrue("Message not in order", frame.getBody().equals(data[i]));
      }

      conn.disconnect();
   }

   @Test
   public void testSubscribeWithAutoAckAndSelector() throws Exception {
      conn.connect(defUser, defPass);
      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.AUTO, null, "foo = 'zzz'");

      sendJmsMessage("Ignored message", "foo", "1234");
      sendJmsMessage("Real message", "foo", "zzz");

      ClientStompFrame frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertTrue("Should have received the real message but got: " + frame, frame.getBody().equals("Real message"));

      conn.disconnect();
   }

   @Test
   public void testSubscribeWithAutoAckAndHyphenatedSelector() throws Exception {
      conn.connect(defUser, defPass);
      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.AUTO, null, "hyphenated_props:foo-bar = 'zzz'");

      ServerLocator serverLocator = addServerLocator(ActiveMQClient.createServerLocator("vm://0"));
      ClientSessionFactory clientSessionFactory = serverLocator.createSessionFactory();
      ClientSession clientSession = clientSessionFactory.createSession(true, true);
      ClientProducer producer = clientSession.createProducer(getQueuePrefix() + getQueueName());

      ClientMessage ignoredMessage = clientSession.createMessage(false);
      ignoredMessage.putStringProperty("foo-bar", "1234");
      ignoredMessage.getBodyBuffer().writeNullableSimpleString(SimpleString.toSimpleString("Ignored message"));

      ClientMessage realMessage = clientSession.createMessage(false);
      realMessage.putStringProperty("foo-bar", "zzz");
      realMessage.getBodyBuffer().writeNullableSimpleString(SimpleString.toSimpleString("Real message"));

      producer.send(ignoredMessage);
      producer.send(realMessage);


      ClientStompFrame frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertTrue("Should have received the real message but got: " + frame, frame.getBody().equals("Real message"));

      conn.disconnect();
   }

   @Test
   public void testSubscribeWithClientAck() throws Exception {
      conn.connect(defUser, defPass);
      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.CLIENT);

      sendJmsMessage(getName());
      ClientStompFrame frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertNotNull(frame.getHeader(Stomp.Headers.Message.MESSAGE_ID));
      ack(conn, null, frame);

      conn.disconnect();

      // message should not be received since message was acknowledged by the client
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testRedeliveryWithClientAck() throws Exception {
      conn.connect(defUser, defPass);
      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.CLIENT);

      sendJmsMessage(getName());
      ClientStompFrame frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

      conn.disconnect();

      // message should be received since message was not acknowledged
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertTrue(message.getJMSRedelivered());
   }

   @Test
   public void testSubscribeWithClientAckThenConsumingAgainWithAutoAckWithNoDisconnectFrame() throws Exception {
      assertSubscribeWithClientAckThenConsumeWithAutoAck(false);
   }

   @Test
   public void testSubscribeWithClientAckThenConsumingAgainWithAutoAckWithExplicitDisconnect() throws Exception {
      assertSubscribeWithClientAckThenConsumeWithAutoAck(true);
   }

   protected void assertSubscribeWithClientAckThenConsumeWithAutoAck(boolean sendDisconnect) throws Exception {
      conn.connect(defUser, defPass);
      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.CLIENT);
      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

      log.info("Reconnecting!");

      if (sendDisconnect) {
         conn.disconnect();
         conn.destroy();
         conn = StompClientConnectionFactory.createClientConnection(uri);
      } else {
         conn.destroy();
         conn = StompClientConnectionFactory.createClientConnection(uri);
      }

      // message should be received since message was not acknowledged
      conn.connect(defUser, defPass);

      subscribe(conn, null);

      frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

      conn.disconnect();
      conn.destroy();

      conn = StompClientConnectionFactory.createClientConnection(uri);

      // now let's make sure we don't see the message again

      conn.connect(defUser, defPass);

      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.AUTO, null, true);

      sendJmsMessage("shouldBeNextMessage");

      frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals("shouldBeNextMessage", frame.getBody());
   }


   @Test
   public void testUnsubscribe() throws Exception {
      conn.connect(defUser, defPass);
      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.AUTO);

      // send a message to our queue
      sendJmsMessage("first message");

      // receive message
      ClientStompFrame frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

      // remove suscription
      unsubscribe(conn, null, getQueuePrefix() + getQueueName(), true, false);

      // send a message to our queue
      sendJmsMessage("second message");

      frame = conn.receiveFrame(1000);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);
   }

   @Test
   public void testUnsubscribeWithID() throws Exception {
      conn.connect(defUser, defPass);
      subscribe(conn, "mysubid", Stomp.Headers.Subscribe.AckModeValues.AUTO);

      // send a message to our queue
      sendJmsMessage("first message");

      // receive message from socket
      ClientStompFrame frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

      // remove suscription
      unsubscribe(conn, "mysubid", null, true, false);

      // send a message to our queue
      sendJmsMessage("second message");

      frame = conn.receiveFrame(1000);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);

   }

   @Test
   public void testTransactionCommit() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);
      conn.connect(defUser, defPass);

      beginTransaction(conn, "tx1");
      send(conn, getQueuePrefix() + getQueueName(), null, "Hello World", true, null, "tx1");

      // check the message is not committed
      assertNull(consumer.receive(100));

      commitTransaction(conn, "tx1", true);

      Message message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);
   }

   @Test
   public void testSuccessiveTransactionsWithSameID() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);
      conn.connect(defUser, defPass);

      // first tx
      beginTransaction(conn, "tx1");
      send(conn, getQueuePrefix() + getQueueName(), null, "Hello World", true, null, "tx1");
      commitTransaction(conn, "tx1");

      Message message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);

      // 2nd tx with same tx ID
      beginTransaction(conn, "tx1");
      send(conn, getQueuePrefix() + getQueueName(), null, "Hello World", true, null, "tx1");
      commitTransaction(conn, "tx1");

      message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);
   }

   @Test
   public void testBeginSameTransactionTwice() throws Exception {
      conn.connect(defUser, defPass);
      beginTransaction(conn, "tx1");
      beginTransaction(conn, "tx1");

      ClientStompFrame frame = conn.receiveFrame(1000);
      Assert.assertEquals(Stomp.Responses.ERROR, frame.getCommand());
   }

   @Test
   public void testTransactionRollback() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);
      String txId = "tx1";

      conn.connect(defUser, defPass);
      beginTransaction(conn, txId);
      send(conn, getQueuePrefix() + getQueueName(), null, "first message", true, null, txId);
      abortTransaction(conn, txId);

      beginTransaction(conn, txId);
      send(conn, getQueuePrefix() + getQueueName(), null, "second message", true, null, txId);
      commitTransaction(conn, txId);

      // only second msg should be received since first msg was rolled back
      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("second message", message.getText());
   }

   @Test
   public void testSubscribeToTopic() throws Exception {
      final int baselineQueueCount = server.getActiveMQServer().getActiveMQServerControl().getQueueNames().length;

      conn.connect(defUser, defPass);

      subscribeTopic(conn, null, null, null, true);

      assertTrue("Subscription queue should be created here", Wait.waitFor(new Wait.Condition() {

         @Override
         public boolean isSatisfied() throws Exception {
            int length = server.getActiveMQServer().getActiveMQServerControl().getQueueNames().length;
            if (length - baselineQueueCount == 1) {
               return true;
            } else {
               log.info("Queue count: " + (length - baselineQueueCount));
               return false;
            }
         }
      }, TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(100)));

      sendJmsMessage(getName(), topic);

      ClientStompFrame frame = conn.receiveFrame(1000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(getTopicPrefix() + getTopicName(), frame.getHeader(Stomp.Headers.Send.DESTINATION));
      Assert.assertEquals(getName(), frame.getBody());

      unsubscribe(conn, null, getTopicPrefix() + getTopicName(), true, false);

      sendJmsMessage(getName(), topic);

      frame = conn.receiveFrame(1000);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);

      assertEquals("Subscription queue should be deleted", 0, server.getActiveMQServer().getActiveMQServerControl().getQueueNames().length - baselineQueueCount);

      conn.disconnect();
   }

   @Test
   public void testSubscribeToQueue() throws Exception {
      final int baselineQueueCount = server.getActiveMQServer().getActiveMQServerControl().getQueueNames().length;

      conn.connect(defUser, defPass);
      subscribe(conn, null, null, null, true);

      assertFalse("Queue should not be created here", Wait.waitFor(new Wait.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            if (server.getActiveMQServer().getActiveMQServerControl().getQueueNames().length - baselineQueueCount == 1) {
               return true;
            } else {
               return false;
            }
         }
      }, TimeUnit.MILLISECONDS.toMillis(1000), TimeUnit.MILLISECONDS.toMillis(100)));

      sendJmsMessage(getName(), queue);

      ClientStompFrame frame = conn.receiveFrame(1000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader(Stomp.Headers.Send.DESTINATION));
      Assert.assertEquals(getName(), frame.getBody());

      unsubscribe(conn, null, getQueuePrefix() + getQueueName(), true, false);

      sendJmsMessage(getName(), queue);

      frame = conn.receiveFrame(1000);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);

      assertEquals("Subscription queue should not be deleted", baselineQueueCount, server.getActiveMQServer().getActiveMQServerControl().getQueueNames().length);

      conn.disconnect();
   }

   @Test
   public void testSubscribeToNonExistentQueue() throws Exception {
      String nonExistentQueue = RandomUtil.randomString();

      conn.connect(defUser, defPass);
      subscribe(conn, null, null, null, null, getQueuePrefix() + nonExistentQueue, true);

      sendJmsMessage(getName(), ActiveMQJMSClient.createQueue(nonExistentQueue));

      ClientStompFrame frame = conn.receiveFrame(1000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(getQueuePrefix() + nonExistentQueue, frame.getHeader(Stomp.Headers.Send.DESTINATION));
      Assert.assertEquals(getName(), frame.getBody());

      assertNotNull(server.getActiveMQServer().getPostOffice().getBinding(new SimpleString(nonExistentQueue)));

      final Queue subscription = ((LocalQueueBinding) server.getActiveMQServer().getPostOffice().getBinding(new SimpleString(nonExistentQueue))).getQueue();

      assertTrue(Wait.waitFor(new Wait.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            if (subscription.getMessageCount() == 0)
               return true;
            else
               return false;
         }
      }, 1000, 50));

      unsubscribe(conn, null, getQueuePrefix() + nonExistentQueue, true, false);

      assertNull(server.getActiveMQServer().getPostOffice().getBinding(new SimpleString(nonExistentQueue)));

      sendJmsMessage(getName(), ActiveMQJMSClient.createQueue(nonExistentQueue));

      frame = conn.receiveFrame(1000);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);

      conn.disconnect();
   }

   @Test
   public void testDurableSubscriberWithReconnection() throws Exception {
      conn.connect(defUser, defPass, "myclientid");
      subscribeTopic(conn, null, null, getName());

      conn.disconnect();

      Thread.sleep(500);

      // send the message when the durable subscriber is disconnected
      sendJmsMessage(getName(), topic);

      conn.destroy();
      conn = StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass, "myclientid");

      subscribeTopic(conn, null, null, getName());

      ClientStompFrame frame = conn.receiveFrame(3000);
      assertNotNull("Should have received a message from the durable subscription", frame);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(getTopicPrefix() + getTopicName(), frame.getHeader(Stomp.Headers.Send.DESTINATION));
      Assert.assertEquals(getName(), frame.getBody());

      unsubscribe(conn, null, getTopicPrefix() + getTopicName(), true, true);

      conn.disconnect();
   }

   @Test
   public void testDurableSubscriberWithReconnectionLegacy() throws Exception {
      conn.connect(defUser, defPass, "myclientid");
      subscribeTopicLegacyActiveMQ(conn, null, null, getName(), true, false);

      conn.disconnect();

      Thread.sleep(500);

      // send the message when the durable subscriber is disconnected
      sendJmsMessage(getName(), topic);

      conn.destroy();
      conn = StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass, "myclientid");

      subscribeTopicLegacyActiveMQ(conn, null, null, getName(), true, false);

      ClientStompFrame frame = conn.receiveFrame(3000);
      assertNotNull("Should have received a message from the durable subscription", frame);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(getTopicPrefix() + getTopicName(), frame.getHeader(Stomp.Headers.Send.DESTINATION));
      Assert.assertEquals(getName(), frame.getBody());

      unsubscribeLegacyActiveMQ(conn, null, getTopicPrefix() + getTopicName(), true, true);

      conn.disconnect();
   }

   @Test
   public void testDurableSubscriber() throws Exception {
      conn.connect(defUser, defPass, "myclientid");
      subscribeTopic(conn, null, null, getName(), true);
      ClientStompFrame response = subscribeTopic(conn, null, null, getName(), true);

      // creating a subscriber with the same durable-subscriber-name must fail
      Assert.assertEquals(Stomp.Responses.ERROR, response.getCommand());

      conn.disconnect();
   }

   @Test
   public void testDurableSubscriberLegacySubscriptionHeader() throws Exception {
      conn.connect(defUser, defPass, "myclientid");
      subscribeTopicLegacyActiveMQ(conn, null, null, getName(), true, false);
      ClientStompFrame response = subscribeTopicLegacyActiveMQ(conn, null, null, getName(), true, false);

      // creating a subscriber with the same durable-subscriber-name must fail
      Assert.assertEquals(Stomp.Responses.ERROR, response.getCommand());

      conn.disconnect();
   }

   @Test
   public void testDurableUnSubscribe() throws Exception {
      conn.connect(defUser, defPass, "myclientid");
      subscribeTopic(conn, null, null, getName(), true);
      conn.disconnect();
      Thread.sleep(500);

      assertNotNull(server.getActiveMQServer().locateQueue(SimpleString.toSimpleString("myclientid." + getName())));

      conn.destroy();
      conn = StompClientConnectionFactory.createClientConnection(uri);

      conn.connect(defUser, defPass, "myclientid");
      unsubscribe(conn, getName(), getTopicPrefix() + getTopicName(), false, true);
      conn.disconnect();
      Thread.sleep(500);

      assertNull(server.getActiveMQServer().locateQueue(SimpleString.toSimpleString("myclientid." + getName())));
   }

   @Test
   public void testDurableUnSubscribeLegacySubscriptionHeader() throws Exception {
      conn.connect(defUser, defPass, "myclientid");
      subscribeTopicLegacyActiveMQ(conn, null, null, getName(), true, false);
      conn.disconnect();
      Thread.sleep(500);

      assertNotNull(server.getActiveMQServer().locateQueue(SimpleString.toSimpleString("myclientid." + getName())));

      conn.destroy();
      conn = StompClientConnectionFactory.createClientConnection(uri);

      conn.connect(defUser, defPass, "myclientid");
      unsubscribeLegacyActiveMQ(conn, getName(), getTopicPrefix() + getTopicName(), false, true);
      conn.disconnect();
      Thread.sleep(500);

      assertNull(server.getActiveMQServer().locateQueue(SimpleString.toSimpleString("myclientid." + getName())));
   }

   @Test
   public void testSubscribeToTopicWithNoLocal() throws Exception {
      conn.connect(defUser, defPass);
      subscribeTopic(conn, null, null, null, true, true);

      // send a message on the same connection => it should not be received is noLocal = true on subscribe
      send(conn, getTopicPrefix() + getTopicName(), null, "Hello World");

      ClientStompFrame frame = conn.receiveFrame(2000);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);

      // send message on another JMS connection => it should be received
      sendJmsMessage(getName(), topic);
      frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(getTopicPrefix() + getTopicName(), frame.getHeader(Stomp.Headers.Send.DESTINATION));
      Assert.assertEquals(getName(), frame.getBody());

      conn.disconnect();
   }

   @Test
   public void testTopicExistsAfterNoUnsubscribeDisconnect() throws Exception {
      conn.connect(defUser, defPass);
      subscribeTopic(conn, null, null, null, true);

      // disconnect, _without unsubscribing_
      conn.disconnect();

      Thread.sleep(500);

      conn.destroy();

      // connect again
      conn = StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass);

      // send a receipted message to the topic
      ClientStompFrame response = send(conn, getTopicPrefix() + getTopicName(), null, "Hello World", true);
      assertEquals(Stomp.Responses.RECEIPT, response.getCommand());

      // ...and nothing else
      ClientStompFrame frame = conn.receiveFrame(2000);
      log.info("Received frame: " + frame);
      Assert.assertNull(frame);

      conn.disconnect();
   }

   @Test
   public void testClientAckNotPartOfTransaction() throws Exception {
      conn.connect(defUser, defPass);
      subscribe(conn, null, Stomp.Headers.Subscribe.AckModeValues.CLIENT);

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame(10000);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader(Stomp.Headers.Send.DESTINATION));
      String messageID = frame.getHeader(Stomp.Headers.Message.MESSAGE_ID);
      Assert.assertNotNull(messageID);
      Assert.assertEquals(getName(), frame.getBody());

      beginTransaction(conn, "tx1");
      ack(conn, null, messageID, "tx1");
      abortTransaction(conn, "tx1");

      frame = conn.receiveFrame(1000);
      Assert.assertNull("No message should have been received as the message was acked even though the transaction has been aborted", frame);

      unsubscribe(conn, null, getQueuePrefix() + getQueueName(), false, false);

      conn.disconnect();
   }

   // HORNETQ-1007
   @Test
   public void testMultiProtocolConsumers() throws Exception {
      final int TIME_OUT = 2000;

      int count = 1000;

      // Create 2 core consumers
      MessageConsumer consumer1 = session.createConsumer(topic);
      MessageConsumer consumer2 = session.createConsumer(topic);

      // connect and subscribe STOMP consumer
      conn.connect(defUser, defPass);
      subscribeTopic(conn, null, null, null, true);

      MessageProducer producer = session.createProducer(topic);
      TextMessage message = session.createTextMessage(getName());

      for (int i = 1; i <= count; i++) {
         producer.send(message);
         Assert.assertNotNull(consumer1.receive(TIME_OUT));
         Assert.assertNotNull(consumer2.receive(TIME_OUT));
         ClientStompFrame frame = conn.receiveFrame(TIME_OUT);
         Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
         Assert.assertEquals(getTopicPrefix() + getTopicName(), frame.getHeader(Stomp.Headers.Send.DESTINATION));
         Assert.assertEquals(getName(), frame.getBody());
      }

      consumer1.close();
      consumer2.close();
      unsubscribe(conn, null, getTopicPrefix() + getTopicName(), true, false);

      sendJmsMessage(getName(), topic);

      ClientStompFrame frame = conn.receiveFrame(TIME_OUT);
      log.info("Received frame: " + frame);
      Assert.assertNull("No message should have been received since subscription was removed", frame);

      conn.disconnect();
   }

   @Test
   //stomp should return an ERROR when acking a non-existent message
   public void testUnexpectedAck() throws Exception {
      String messageID = "888888";

      conn.connect(defUser, defPass);
      ack(conn, null, messageID, null);

      ClientStompFrame frame = conn.receiveFrame(1000);
      assertNotNull(frame);
      assertEquals(Stomp.Responses.ERROR, frame.getCommand());

      conn.disconnect();
   }

   @Test
   public void testDotAnycastPrefixOnSend() throws Exception {
      testPrefix("jms.queue.", RoutingType.ANYCAST, true);
   }

   @Test
   public void testDotMulticastPrefixOnSend() throws Exception {
      testPrefix("jms.topic.", RoutingType.MULTICAST, true);
   }

   @Test
   public void testDotAnycastPrefixOnSubscribe() throws Exception {
      testPrefix("jms.queue.", RoutingType.ANYCAST, false);
   }

   @Test
   public void testDotMulticastPrefixOnSubscribe() throws Exception {
      testPrefix("jms.topic.", RoutingType.MULTICAST, false);
   }

   @Test
   public void testSlashAnycastPrefixOnSend() throws Exception {
      testPrefix("/queue/", RoutingType.ANYCAST, true);
   }

   @Test
   public void testSlashMulticastPrefixOnSend() throws Exception {
      testPrefix("/topic/", RoutingType.MULTICAST, true);
   }

   @Test
   public void testSlashAnycastPrefixOnSubscribe() throws Exception {
      testPrefix("/queue/", RoutingType.ANYCAST, false);
   }

   @Test
   public void testSlashMulticastPrefixOnSubscribe() throws Exception {
      testPrefix("/topic/", RoutingType.MULTICAST, false);
   }

   public void testPrefix(final String prefix, final RoutingType routingType, final boolean send) throws Exception {
      int port = 61614;

      URI uri = createStompClientUri(scheme, hostname, port);

      final String ADDRESS = UUID.randomUUID().toString();
      final String PREFIXED_ADDRESS = prefix + ADDRESS;
      String param = routingType.toString();
      String urlParam = param.toLowerCase() + "Prefix";
      server.getActiveMQServer().getRemotingService().createAcceptor("test", "tcp://" + hostname + ":" + port + "?protocols=" + StompProtocolManagerFactory.STOMP_PROTOCOL_NAME + "&" + urlParam + "=" + prefix).start();
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass);

      // since this queue doesn't exist the broker should create a new address using the routing type matching the prefix
      if (send) {
         send(conn, PREFIXED_ADDRESS, null, "Hello World", true);
      } else {
         String uuid = UUID.randomUUID().toString();

         ClientStompFrame frame = conn.createFrame(Stomp.Commands.SUBSCRIBE)
                                      .addHeader(Stomp.Headers.Subscribe.DESTINATION, PREFIXED_ADDRESS)
                                      .addHeader(Stomp.Headers.RECEIPT_REQUESTED, uuid);

         frame = conn.sendFrame(frame);

         assertEquals(uuid, frame.getHeader(Stomp.Headers.Response.RECEIPT_ID));
      }

      AddressInfo addressInfo = server.getActiveMQServer().getAddressInfo(SimpleString.toSimpleString(ADDRESS));
      assertNotNull("No address was created with the name " + ADDRESS, addressInfo);

      Set<RoutingType> routingTypes = new HashSet<>();
      routingTypes.add(RoutingType.valueOf(param));
      assertEquals(routingTypes, addressInfo.getRoutingTypes());

      conn.disconnect();
   }

   @Test
   public void testDotPrefixedSendAndRecieveAnycast() throws Exception {
      testPrefixedSendAndRecieve("jms.queue.", RoutingType.ANYCAST);
   }

   @Test
   public void testDotPrefixedSendAndRecieveMulticast() throws Exception {
      testPrefixedSendAndRecieve("jms.topic.", RoutingType.MULTICAST);
   }

   @Test
   public void testSlashPrefixedSendAndRecieveAnycast() throws Exception {
      testPrefixedSendAndRecieve("/queue/", RoutingType.ANYCAST);
   }

   @Test
   public void testSlashPrefixedSendAndRecieveMulticast() throws Exception {
      testPrefixedSendAndRecieve("/topic/", RoutingType.MULTICAST);
   }

   public void testPrefixedSendAndRecieve(final String prefix, RoutingType routingType) throws Exception {
      int port = 61614;

      URI uri = createStompClientUri(scheme, hostname, port);

      final String ADDRESS = UUID.randomUUID().toString();
      final String PREFIXED_ADDRESS = prefix + ADDRESS;
      String urlParam = routingType.toString().toLowerCase() + "Prefix";
      server.getActiveMQServer().getRemotingService().createAcceptor("test", "tcp://" + hostname + ":" + port + "?protocols=" + StompProtocolManagerFactory.STOMP_PROTOCOL_NAME + "&" + urlParam + "=" + prefix).start();
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass);
      String uuid = UUID.randomUUID().toString();

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SUBSCRIBE)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, PREFIXED_ADDRESS)
                                   .addHeader(Stomp.Headers.RECEIPT_REQUESTED, uuid);

      frame = conn.sendFrame(frame);
      assertEquals(uuid, frame.getHeader(Stomp.Headers.Response.RECEIPT_ID));

      send(conn, ADDRESS, null, "Hello World", true);

      frame = conn.receiveFrame(10000);
      Assert.assertNotNull("Should have received a message", frame);
      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(ADDRESS, frame.getHeader(Stomp.Headers.Send.DESTINATION));
      Assert.assertEquals("Hello World", frame.getBody());

      conn.disconnect();
   }

   @Test
   public void testMulticastOperationsOnAnycastAddress() throws Exception {
      testRoutingSemantics(RoutingType.MULTICAST.toString(), getQueuePrefix() + getQueueName());
   }

   @Test
   public void testAnycastOperationsOnMulticastAddress() throws Exception {
      testRoutingSemantics(RoutingType.ANYCAST.toString(), getTopicPrefix() + getTopicName());
   }

   public void testRoutingSemantics(String routingType, String destination) throws Exception {
      conn.connect(defUser, defPass);

      String uuid = UUID.randomUUID().toString();

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SUBSCRIBE)
                                   .addHeader(Stomp.Headers.Subscribe.SUBSCRIPTION_TYPE, routingType)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, destination)
                                   .addHeader(Stomp.Headers.RECEIPT_REQUESTED, uuid);

      frame = conn.sendFrame(frame);
      assertEquals(Stomp.Responses.ERROR, frame.getCommand());

      uuid = UUID.randomUUID().toString();

      frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION_TYPE, RoutingType.MULTICAST.toString())
                                   .addHeader(Stomp.Headers.Send.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader(Stomp.Headers.RECEIPT_REQUESTED, uuid);

      frame = conn.sendFrame(frame);
      assertEquals(Stomp.Responses.ERROR, frame.getCommand());
   }

   @Test
   public void testGetManagementAttributeFromStomp() throws Exception {
      server.getActiveMQServer().getAddressSettingsRepository().addMatch("#", new AddressSettings().setAutoCreateAddresses(false).setAutoCreateQueues(false));
      conn.connect(defUser, defPass);

      subscribe(conn, null);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, ActiveMQDefaultConfiguration.getDefaultManagementAddress().toString())
                                   .addHeader(Stomp.Headers.Send.REPLY_TO, getQueuePrefix() + getQueueName())
                                   .addHeader(ManagementHelper.HDR_RESOURCE_NAME.toString(), ResourceNames.QUEUE + getQueuePrefix() + getQueueName())
                                   .addHeader(ManagementHelper.HDR_ATTRIBUTE.toString(), "Address");

      conn.sendFrame(frame);

      frame = conn.receiveFrame(10000);

      IntegrationTestLogger.LOGGER.info("Received: " + frame);

      Assert.assertEquals(Boolean.TRUE.toString(), frame.getHeader(ManagementHelper.HDR_OPERATION_SUCCEEDED.toString()));
      // the address will be returned in the message body in a JSON array
      Assert.assertEquals("[\"" + getQueuePrefix() + getQueueName() + "\"]", frame.getBody());

      unsubscribe(conn, null);

      conn.disconnect();
   }

   @Test
   public void testInvokeOperationFromStomp() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, null);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, ActiveMQDefaultConfiguration.getDefaultManagementAddress().toString())
                                   .addHeader(Stomp.Headers.Send.REPLY_TO, getQueuePrefix() + getQueueName())
                                   .addHeader(ManagementHelper.HDR_RESOURCE_NAME.toString(), ResourceNames.QUEUE + getQueuePrefix() + getQueueName())
                                   .addHeader(ManagementHelper.HDR_OPERATION_NAME.toString(), "countMessages")
                                   .setBody("[\"color = 'blue'\"]");

      conn.sendFrame(frame);

      frame = conn.receiveFrame(10000);

      IntegrationTestLogger.LOGGER.info("Received: " + frame);

      Assert.assertEquals(Boolean.TRUE.toString(), frame.getHeader(ManagementHelper.HDR_OPERATION_SUCCEEDED.toString()));
      // there is no such messages => 0 returned in a JSON array
      Assert.assertEquals("[0]", frame.getBody());

      unsubscribe(conn, null);

      conn.disconnect();
   }

   @Test
   public void testAnycastMessageRoutingExclusivity() throws Exception {
      conn.connect(defUser, defPass);

      final String addressA = "addressA";
      final String queueA = "queueA";
      final String queueB = "queueB";
      final String queueC = "queueC";

      ActiveMQServer activeMQServer = server.getActiveMQServer();
      ActiveMQServerControl serverControl = server.getActiveMQServer().getActiveMQServerControl();
      serverControl.createAddress(addressA, RoutingType.ANYCAST.toString() + "," + RoutingType.MULTICAST.toString());
      serverControl.createQueue(addressA, queueA, RoutingType.ANYCAST.toString());
      serverControl.createQueue(addressA, queueB, RoutingType.ANYCAST.toString());
      serverControl.createQueue(addressA, queueC, RoutingType.MULTICAST.toString());

      send(conn, addressA, null, "Hello World!", true, RoutingType.ANYCAST);

      assertTrue(Wait.waitFor(() -> activeMQServer.locateQueue(SimpleString.toSimpleString(queueA)).getMessageCount() + activeMQServer.locateQueue(SimpleString.toSimpleString(queueB)).getMessageCount() == 1, 2000, 100));
      assertTrue(Wait.waitFor(() -> activeMQServer.locateQueue(SimpleString.toSimpleString(queueC)).getMessageCount() == 0, 2000, 100));
   }

   @Test
   public void testMulticastMessageRoutingExclusivity() throws Exception {
      conn.connect(defUser, defPass);

      final String addressA = "addressA";
      final String queueA = "queueA";
      final String queueB = "queueB";
      final String queueC = "queueC";

      ActiveMQServer activeMQServer = server.getActiveMQServer();
      ActiveMQServerControl serverControl = server.getActiveMQServer().getActiveMQServerControl();
      serverControl.createAddress(addressA, RoutingType.ANYCAST.toString() + "," + RoutingType.MULTICAST.toString());
      serverControl.createQueue(addressA, queueA, RoutingType.ANYCAST.toString());
      serverControl.createQueue(addressA, queueB, RoutingType.MULTICAST.toString());
      serverControl.createQueue(addressA, queueC, RoutingType.MULTICAST.toString());

      send(conn, addressA, null, "Hello World!", true, RoutingType.MULTICAST);

      assertTrue(Wait.waitFor(() -> activeMQServer.locateQueue(SimpleString.toSimpleString(queueA)).getMessageCount() == 0, 2000, 100));
      assertTrue(Wait.waitFor(() -> activeMQServer.locateQueue(SimpleString.toSimpleString(queueC)).getMessageCount() + activeMQServer.locateQueue(SimpleString.toSimpleString(queueB)).getMessageCount() == 2, 2000, 100));
   }

   @Test
   public void testAmbiguousMessageRouting() throws Exception {
      conn.connect(defUser, defPass);

      final String addressA = "addressA";
      final String queueA = "queueA";
      final String queueB = "queueB";
      final String queueC = "queueC";
      final String queueD = "queueD";

      ActiveMQServer activeMQServer = server.getActiveMQServer();
      ActiveMQServerControl serverControl = server.getActiveMQServer().getActiveMQServerControl();
      serverControl.createAddress(addressA, RoutingType.ANYCAST.toString() + "," + RoutingType.MULTICAST.toString());
      serverControl.createQueue(addressA, queueA, RoutingType.ANYCAST.toString());
      serverControl.createQueue(addressA, queueB, RoutingType.ANYCAST.toString());
      serverControl.createQueue(addressA, queueC, RoutingType.MULTICAST.toString());
      serverControl.createQueue(addressA, queueD, RoutingType.MULTICAST.toString());

      send(conn, addressA, null, "Hello World!", true);

      assertTrue(Wait.waitFor(() -> activeMQServer.locateQueue(SimpleString.toSimpleString(queueA)).getMessageCount() + activeMQServer.locateQueue(SimpleString.toSimpleString(queueB)).getMessageCount() == 1, 2000, 100));
      assertTrue(Wait.waitFor(() -> activeMQServer.locateQueue(SimpleString.toSimpleString(queueC)).getMessageCount() + activeMQServer.locateQueue(SimpleString.toSimpleString(queueD)).getMessageCount() == 2, 2000, 100));
   }

   @Test
   public void testAutoCreatedAnycastAddress() throws Exception {
      conn.connect(defUser, defPass);

      String queueName = UUID.randomUUID().toString();
      SimpleString simpleQueueName = SimpleString.toSimpleString(queueName);

      ActiveMQServer activeMQServer = server.getActiveMQServer();

      Assert.assertNull(activeMQServer.getAddressInfo(simpleQueueName));
      Assert.assertNull(activeMQServer.locateQueue(simpleQueueName));

      activeMQServer.getAddressSettingsRepository().addMatch(queueName, new AddressSettings()
         .setDefaultAddressRoutingType(RoutingType.ANYCAST)
         .setDefaultQueueRoutingType(RoutingType.ANYCAST)
      );

      send(conn, queueName, null, "Hello ANYCAST");

      assertTrue("Address and queue should be created now", Wait.waitFor(() -> (activeMQServer.getAddressInfo(simpleQueueName) != null) && (activeMQServer.locateQueue(simpleQueueName) != null), 2000, 200));
      assertTrue(activeMQServer.getAddressInfo(simpleQueueName).getRoutingTypes().contains(RoutingType.ANYCAST));
      assertEquals(RoutingType.ANYCAST, activeMQServer.locateQueue(simpleQueueName).getRoutingType());
   }

   @Test
   public void testAutoCreatedMulticastAddress() throws Exception {
      conn.connect(defUser, defPass);

      String queueName = UUID.randomUUID().toString();
      SimpleString simpleQueueName = SimpleString.toSimpleString(queueName);

      ActiveMQServer activeMQServer = server.getActiveMQServer();

      Assert.assertNull(activeMQServer.getAddressInfo(simpleQueueName));
      Assert.assertNull(activeMQServer.locateQueue(simpleQueueName));

      send(conn, queueName, null, "Hello MULTICAST");

      assertTrue("Address should be created now", Wait.waitFor(() -> (activeMQServer.getAddressInfo(simpleQueueName) != null), 2000, 200));
      assertTrue(activeMQServer.getAddressInfo(simpleQueueName).getRoutingTypes().contains(RoutingType.MULTICAST));
      Assert.assertNull(activeMQServer.locateQueue(simpleQueueName));
   }
}

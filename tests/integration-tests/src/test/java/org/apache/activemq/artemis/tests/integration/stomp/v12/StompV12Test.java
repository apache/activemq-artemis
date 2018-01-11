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
package org.apache.activemq.artemis.tests.integration.stomp.v12;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.stomp.Stomp;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.stomp.StompTestBase;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionV11;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionV12;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Testing Stomp version 1.2 functionalities
 */
@RunWith(Parameterized.class)
public class StompV12Test extends StompTestBase {

   private static final transient IntegrationTestLogger log = IntegrationTestLogger.LOGGER;
   public static final String CLIENT_ID = "myclientid";

   private StompClientConnectionV12 conn;

   private URI v10Uri;

   private URI v11Uri;

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][]{{"ws+v12.stomp"}, {"tcp+v12.stomp"}});
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      v10Uri = new URI(uri.toString().replace("v12", "v10"));
      v11Uri = new URI(uri.toString().replace("v12", "v11"));
      conn = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection(uri);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         boolean connected = conn != null && conn.isConnected();
         log.debug("Connection 1.2 : " + connected);
         if (connected) {
            conn.disconnect();
         }
      } finally {
         super.tearDown();
         conn.closeTransport();
      }
   }

   @Test
   public void testConnection() throws Exception {
      server.getActiveMQServer().getConfiguration().setSecurityEnabled(true);
      StompClientConnection connection = StompClientConnectionFactory.createClientConnection(v10Uri);

      connection.connect(defUser, defPass);

      Assert.assertTrue(connection.isConnected());

      Assert.assertEquals("1.0", connection.getVersion());

      connection.disconnect();

      connection = StompClientConnectionFactory.createClientConnection(uri);

      connection.connect(defUser, defPass);

      Assert.assertTrue(connection.isConnected());

      Assert.assertEquals("1.2", connection.getVersion());

      connection.disconnect();

      connection = StompClientConnectionFactory.createClientConnection(uri);

      connection.connect();

      Assert.assertFalse(connection.isConnected());

      //new way of connection
      StompClientConnectionV11 conn = (StompClientConnectionV11) StompClientConnectionFactory.createClientConnection(v11Uri);
      conn.connect1(defUser, defPass);

      Assert.assertTrue(conn.isConnected());

      conn.disconnect();
   }

   @Test
   public void testConnectionAsInSpec() throws Exception {
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(v10Uri);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.CONNECT);
      frame.addHeader(Stomp.Headers.Connect.LOGIN, this.defUser);
      frame.addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass);
      frame.addHeader(Stomp.Headers.ACCEPT_VERSION, "1.2");
      frame.addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1");

      ClientStompFrame reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.CONNECTED, reply.getCommand());
      Assert.assertEquals("1.2", reply.getHeader(Stomp.Headers.Error.VERSION));

      conn.disconnect();

      //need 1.2 client
      conn = StompClientConnectionFactory.createClientConnection(uri);

      frame = conn.createFrame(Stomp.Commands.STOMP);
      frame.addHeader(Stomp.Headers.Connect.LOGIN, this.defUser);
      frame.addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass);
      frame.addHeader(Stomp.Headers.ACCEPT_VERSION, "1.2");
      frame.addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1");

      reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.CONNECTED, reply.getCommand());
      Assert.assertEquals("1.2", reply.getHeader(Stomp.Headers.Error.VERSION));

      conn.disconnect();
   }

   @Test
   public void testNegotiation() throws Exception {
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(v10Uri);
      // case 1 accept-version absent. It is a 1.0 connect
      ClientStompFrame frame = conn.createFrame(Stomp.Commands.CONNECT);
      frame.addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1");
      frame.addHeader(Stomp.Headers.Connect.LOGIN, this.defUser);
      frame.addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass);

      ClientStompFrame reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.CONNECTED, reply.getCommand());

      //reply headers: version, session, server
      Assert.assertEquals(null, reply.getHeader(Stomp.Headers.Error.VERSION));

      conn.disconnect();

      // case 2 accept-version=1.0, result: 1.0
      conn = StompClientConnectionFactory.createClientConnection(v11Uri);
      frame = conn.createFrame(Stomp.Commands.CONNECT);
      frame.addHeader(Stomp.Headers.ACCEPT_VERSION, "1.0");
      frame.addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1");
      frame.addHeader(Stomp.Headers.Connect.LOGIN, this.defUser);
      frame.addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass);

      reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.CONNECTED, reply.getCommand());

      //reply headers: version, session, server
      Assert.assertEquals("1.0", reply.getHeader(Stomp.Headers.Error.VERSION));

      conn.disconnect();

      // case 3 accept-version=1.1, result: 1.1
      conn = StompClientConnectionFactory.createClientConnection(v11Uri);
      frame = conn.createFrame(Stomp.Commands.CONNECT);
      frame.addHeader(Stomp.Headers.ACCEPT_VERSION, "1.1");
      frame.addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1");
      frame.addHeader(Stomp.Headers.Connect.LOGIN, this.defUser);
      frame.addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass);

      reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.CONNECTED, reply.getCommand());

      //reply headers: version, session, server
      Assert.assertEquals("1.1", reply.getHeader(Stomp.Headers.Error.VERSION));

      conn.disconnect();

      // case 4 accept-version=1.0,1.1,1.3, result 1.2
      conn = StompClientConnectionFactory.createClientConnection(v11Uri);
      frame = conn.createFrame(Stomp.Commands.CONNECT);
      frame.addHeader(Stomp.Headers.ACCEPT_VERSION, "1.0,1.1,1.3");
      frame.addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1");
      frame.addHeader(Stomp.Headers.Connect.LOGIN, this.defUser);
      frame.addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass);

      reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.CONNECTED, reply.getCommand());

      //reply headers: version, session, server
      Assert.assertEquals("1.1", reply.getHeader(Stomp.Headers.Error.VERSION));

      conn.disconnect();

      // case 5 accept-version=1.3, result error
      conn = StompClientConnectionFactory.createClientConnection(v11Uri);
      frame = conn.createFrame(Stomp.Commands.CONNECT);
      frame.addHeader(Stomp.Headers.ACCEPT_VERSION, "1.3");
      frame.addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1");
      frame.addHeader(Stomp.Headers.Connect.LOGIN, this.defUser);
      frame.addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass);

      reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.ERROR, reply.getCommand());

      conn.disconnect();

      System.out.println("Got error frame " + reply);

   }

   @Test
   public void testSendAndReceive() throws Exception {
      conn.connect(defUser, defPass);

      ClientStompFrame response = send(conn, getQueuePrefix() + getQueueName(), "text/plain", "Hello World 1!");

      Assert.assertNull(response);

      send(conn, getQueuePrefix() + getQueueName(), "text/plain", "Hello World 2!", true);

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection(uri);
      newConn.connect(defUser, defPass);
      subscribe(newConn, "a-sub");

      ClientStompFrame frame = newConn.receiveFrame();

      System.out.println("received " + frame);

      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

      Assert.assertEquals("a-sub", frame.getHeader(Stomp.Headers.Message.SUBSCRIPTION));

      //'auto' ack mode doesn't require 'ack' header
      Assert.assertNull(frame.getHeader(Stomp.Headers.Subscribe.ACK_MODE));

      Assert.assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader(Stomp.Headers.Subscribe.DESTINATION));

      Assert.assertEquals("Hello World 1!", frame.getBody());

      frame = newConn.receiveFrame();

      System.out.println("received " + frame);

      //unsub
      unsubscribe(newConn, "a-sub");

      newConn.disconnect();
   }

   @Test
   public void testHeaderContentType() throws Exception {
      conn.connect(defUser, defPass);

      send(conn, getQueuePrefix() + getQueueName(), "application/xml", "Hello World 1!");

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection(v11Uri);
      newConn.connect(defUser, defPass);
      subscribe(newConn, "a-sub");

      ClientStompFrame frame = newConn.receiveFrame();

      System.out.println("received " + frame);

      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

      Assert.assertEquals("application/xml", frame.getHeader(Stomp.Headers.CONTENT_TYPE));

      //unsub
      unsubscribe(newConn, "a-sub");

      newConn.disconnect();
   }

   @Test
   public void testHeaderContentLength() throws Exception {
      conn.connect(defUser, defPass);

      String body = "Hello World 1!";
      String cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader(Stomp.Headers.CONTENT_TYPE, "application/xml")
                                   .addHeader(Stomp.Headers.CONTENT_LENGTH, cLen)
                                   .setBody(body + "extra");

      conn.sendFrame(frame);

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection(uri);
      newConn.connect(defUser, defPass);
      subscribe(newConn, "a-sub");

      frame = newConn.receiveFrame();

      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(body, frame.getBody());

      Assert.assertEquals(cLen, frame.getHeader(Stomp.Headers.CONTENT_LENGTH));

      //send again without content-length header
      frame = conn.createFrame(Stomp.Commands.SEND)
                  .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                  .addHeader(Stomp.Headers.CONTENT_TYPE, "application/xml")
                  .setBody(body + "extra");

      conn.sendFrame(frame);

      //receive again. extra should received.
      frame = newConn.receiveFrame();

      Assert.assertEquals(body + "extra", frame.getBody());

      //although sender didn't send the content-length header,
      //the server should add it anyway
      Assert.assertEquals((body + "extra").getBytes(StandardCharsets.UTF_8).length, Integer.valueOf(frame.getHeader(Stomp.Headers.CONTENT_LENGTH)).intValue());

      //unsub
      unsubscribe(newConn, "a-sub");

      newConn.disconnect();
   }

   //test that repetitive headers
   @Test
   public void testHeaderRepetitive() throws Exception {
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setAutoCreateQueues(false);
      addressSettings.setAutoCreateAddresses(false);
      server.getActiveMQServer().getAddressSettingsRepository().addMatch("#", addressSettings);

      conn.connect(defUser, defPass);

      String body = "Hello World!";
      String cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, "aNonexistentQueue")
                                   .addHeader(Stomp.Headers.CONTENT_TYPE, "application/xml")
                                   .addHeader(Stomp.Headers.CONTENT_LENGTH, cLen)
                                   .addHeader("foo", "value1")
                                   .addHeader("foo", "value2")
                                   .addHeader("foo", "value3");

      frame.setBody(body);

      conn.sendFrame(frame);

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection(uri);
      newConn.connect(defUser, defPass);
      subscribe(newConn, "a-sub", null, null, true);

      frame = newConn.receiveFrame();

      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(body, frame.getBody());

      System.out.println("received: " + frame);
      Assert.assertEquals("value1", frame.getHeader("foo"));

      //unsub
      unsubscribe(newConn, "a-sub", true);

      newConn.disconnect();

      //should get error

      body = "Hello World!";
      cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);

      frame = conn.createFrame(Stomp.Commands.SEND)
                  .addHeader(Stomp.Headers.Subscribe.DESTINATION, "aNonexistentQueue")
                  .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                  .addHeader(Stomp.Headers.CONTENT_TYPE, "application/xml")
                  .addHeader(Stomp.Headers.CONTENT_LENGTH, cLen)
                  .addHeader(Stomp.Headers.RECEIPT_REQUESTED, "1234")
                  .setBody(body);

      ClientStompFrame reply = conn.sendFrame(frame);
      // TODO this is broken because queue auto-creation is always on
      Assert.assertEquals(Stomp.Responses.ERROR, reply.getCommand());
   }

   //padding shouldn't be trimmed
   @Test
   public void testHeadersPadding() throws Exception {
      conn.connect(defUser, defPass);

      String body = "<p>Hello World!</p>";
      String cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader(Stomp.Headers.CONTENT_TYPE, "application/xml")
                                   .addHeader(Stomp.Headers.CONTENT_LENGTH, cLen)
                                   .addHeader(" header1", "value1 ")
                                   .addHeader("  header2", "value2   ")
                                   .addHeader("header3 ", " value3")
                                   .addHeader(" header4 ", " value4 ")
                                   .addHeader(" header 5  ", " value 5 ")
                                   .addHeader("header6", "\t value\t 6 \t")
                                   .setBody(body);

      conn.sendFrame(frame);

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection(uri);
      newConn.connect(defUser, defPass);
      subscribe(newConn, "a-sub");

      frame = newConn.receiveFrame();

      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals(body, frame.getBody());

      System.out.println("received: " + frame);
      Assert.assertEquals(null, frame.getHeader("header1"));
      Assert.assertEquals("value1 ", frame.getHeader(" header1"));
      Assert.assertEquals("value2   ", frame.getHeader("  header2"));
      Assert.assertEquals(" value3", frame.getHeader("header3 "));
      Assert.assertEquals(" value4 ", frame.getHeader(" header4 "));
      Assert.assertEquals(" value 5 ", frame.getHeader(" header 5  "));
      Assert.assertEquals("\t value\t 6 \t", frame.getHeader("header6"));

      //unsub
      unsubscribe(newConn, "a-sub");

      newConn.disconnect();
   }

   /**
    * Since 1.2 the CR ('\r') needs to be escaped.
    */
   @Test
   public void testHeaderEncoding() throws Exception {
      conn.connect(defUser, defPass);
      String body = "Hello World 1!";
      String cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);
      String hKey = "\\rspecial-header\\\\\\n\\c\\r\\n";
      String hVal = "\\c\\\\\\ngood\\n\\r";

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader(Stomp.Headers.CONTENT_TYPE, "application/xml")
                                   .addHeader(Stomp.Headers.CONTENT_LENGTH, cLen)
                                   .addHeader(hKey, hVal)
                                   .setBody(body);

      System.out.println("key: |" + hKey + "| val: |" + hVal + "|");

      conn.sendFrame(frame);

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection(uri);
      newConn.connect(defUser, defPass);

      subscribe(newConn, "a-sub");

      frame = newConn.receiveFrame();

      System.out.println("received " + frame);

      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

      String value = frame.getHeader("\r" + "special-header" + "\\" + "\n" + ":" + "\r\n");

      Assert.assertEquals(":" + "\\" + "\n" + "good\n\r", value);

      //unsub
      unsubscribe(newConn, "a-sub");

      newConn.disconnect();
   }

   /**
    * In 1.2, undefined escapes must cause a fatal protocol error.
    */
   @Test
   public void testHeaderUndefinedEscape() throws Exception {
      conn.connect(defUser, defPass);
      ClientStompFrame frame = conn.createFrame("SEND");

      String body = "Hello World 1!";
      String cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.addHeader("content-length", cLen);
      String hKey = "undefined-escape";
      String hVal = "is\\ttab";
      frame.addHeader(hKey, hVal);

      System.out.println("key: |" + hKey + "| val: |" + hVal + "|");

      frame.setBody(body);

      conn.sendFrame(frame);

      ClientStompFrame error = conn.receiveFrame();

      System.out.println("received " + error);

      String desc = "Should have received an ERROR for undefined escape sequence";
      Assert.assertNotNull(desc, error);
      Assert.assertEquals(desc, "ERROR", error.getCommand());

      waitDisconnect(conn);
      Assert.assertFalse("Should be disconnected in STOMP 1.2 after ERROR", conn.isConnected());
   }

   @Test
   public void testHeartBeat() throws Exception {
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(uri);
      //no heart beat at all if heat-beat absent
      ClientStompFrame frame = conn.createFrame(Stomp.Commands.CONNECT)
                                   .addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1")
                                   .addHeader(Stomp.Headers.Connect.LOGIN, this.defUser)
                                   .addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass);

      ClientStompFrame reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.CONNECTED, reply.getCommand());

      Thread.sleep(5000);

      Assert.assertEquals(0, conn.getFrameQueueSize());

      conn.disconnect();

      //no heart beat for (0,0)
      conn = StompClientConnectionFactory.createClientConnection(uri);
      frame = conn.createFrame(Stomp.Commands.CONNECT)
                  .addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1")
                  .addHeader(Stomp.Headers.Connect.LOGIN, this.defUser)
                  .addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass)
                  .addHeader(Stomp.Headers.Connect.HEART_BEAT, "0,0")
                  .addHeader(Stomp.Headers.ACCEPT_VERSION, "1.0,1.1,1.2");

      reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.CONNECTED, reply.getCommand());

      Assert.assertEquals("0,30000", reply.getHeader(Stomp.Headers.Connect.HEART_BEAT));

      Thread.sleep(5000);

      Assert.assertEquals(0, conn.getFrameQueueSize());

      conn.disconnect();

      //heart-beat (1,0), should receive a min client ping accepted by server
      conn = StompClientConnectionFactory.createClientConnection(uri);
      frame = conn.createFrame(Stomp.Commands.CONNECT)
                  .addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1")
                  .addHeader(Stomp.Headers.Connect.LOGIN, this.defUser)
                  .addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass)
                  .addHeader(Stomp.Headers.Connect.HEART_BEAT, "1,0")
                  .addHeader(Stomp.Headers.ACCEPT_VERSION, "1.0,1.2");

      reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.CONNECTED, reply.getCommand());

      Assert.assertEquals("0,500", reply.getHeader(Stomp.Headers.Connect.HEART_BEAT));

      Thread.sleep(2000);

      //now server side should be disconnected because we didn't send ping for 2 sec
      //send will fail
      try {
         send(conn, getQueuePrefix() + getQueueName(), "text/plain", "Hello World");
         Assert.fail("connection should have been destroyed by now");
      } catch (IOException e) {
         //ignore
      }

      //heart-beat (1,0), start a ping, then send a message, should be ok.
      conn = StompClientConnectionFactory.createClientConnection(uri);
      frame = conn.createFrame(Stomp.Commands.CONNECT)
                  .addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1")
                  .addHeader(Stomp.Headers.Connect.LOGIN, this.defUser)
                  .addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass)
                  .addHeader(Stomp.Headers.Connect.HEART_BEAT, "1,0")
                  .addHeader(Stomp.Headers.ACCEPT_VERSION, "1.0,1.1,1.2");

      reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.CONNECTED, reply.getCommand());

      Assert.assertEquals("0,500", reply.getHeader(Stomp.Headers.Connect.HEART_BEAT));

      conn.startPinger(500);

      Thread.sleep(2000);

      //send will be ok
      send(conn, getQueuePrefix() + getQueueName(), "text/plain", "Hello World");

      conn.stopPinger();

      conn.disconnect();
   }

   //server ping
   @Test
   public void testHeartBeat2() throws Exception {
      //heart-beat (1,1)
      ClientStompFrame frame = conn.createFrame(Stomp.Commands.CONNECT)
                                   .addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1")
                                   .addHeader(Stomp.Headers.Connect.LOGIN, this.defUser)
                                   .addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass)
                                   .addHeader(Stomp.Headers.Connect.HEART_BEAT, "1,1")
                                   .addHeader(Stomp.Headers.ACCEPT_VERSION, "1.0,1.1,1.2");

      ClientStompFrame reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.CONNECTED, reply.getCommand());
      Assert.assertEquals("500,500", reply.getHeader(Stomp.Headers.Connect.HEART_BEAT));

      conn.disconnect();

      //heart-beat (500,1000)
      conn = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection(uri);
      frame = conn.createFrame(Stomp.Commands.CONNECT)
                  .addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1")
                  .addHeader(Stomp.Headers.Connect.LOGIN, this.defUser)
                  .addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass)
                  .addHeader(Stomp.Headers.Connect.HEART_BEAT, "500,1000")
                  .addHeader(Stomp.Headers.ACCEPT_VERSION, "1.0,1.1,1.2");

      reply = conn.sendFrame(frame);

      Assert.assertEquals(Stomp.Responses.CONNECTED, reply.getCommand());

      Assert.assertEquals("1000,500", reply.getHeader(Stomp.Headers.Connect.HEART_BEAT));

      conn.startPinger(500);

      Thread.sleep(10000);

      //now check the frame size
      int size = conn.getServerPingNumber();

      System.out.println("ping received: " + size);

      Assert.assertTrue("size: " + size, size > 5);

      //now server side should be disconnected because we didn't send ping for 2 sec
      //send will be ok
      send(conn, getQueuePrefix() + getQueueName(), "text/plain", "Hello World");

      conn.disconnect();
   }

   @Test
   public void testSendWithHeartBeatsAndReceive() throws Exception {
      StompClientConnection newConn = null;
      try {
         ClientStompFrame frame = conn.createFrame(Stomp.Commands.CONNECT);
         frame.addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1")
              .addHeader(Stomp.Headers.Connect.LOGIN, this.defUser)
              .addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass)
              .addHeader(Stomp.Headers.Connect.HEART_BEAT, "500,1000")
              .addHeader(Stomp.Headers.ACCEPT_VERSION, "1.0,1.1,1.2");

         conn.sendFrame(frame);

         conn.startPinger(500);

         for (int i = 0; i < 10; i++) {
            send(conn, getQueuePrefix() + getQueueName(), "text/plain", "Hello World " + i + "!");
            Thread.sleep(500);
         }

         // subscribe
         newConn = StompClientConnectionFactory.createClientConnection(uri);
         newConn.connect(defUser, defPass);
         subscribe(newConn, "a-sub");

         int cnt = 0;

         frame = newConn.receiveFrame();

         while (frame != null) {
            cnt++;
            Thread.sleep(500);
            frame = newConn.receiveFrame(5000);
         }

         Assert.assertEquals(10, cnt);

         // unsub
         unsubscribe(newConn, "a-sub");
      } finally {
         if (newConn != null)
            newConn.disconnect();
         conn.disconnect();
      }
   }

   @Test
   public void testSendAndReceiveWithHeartBeats() throws Exception {
      conn.connect(defUser, defPass);

      for (int i = 0; i < 10; i++) {
         send(conn, getQueuePrefix() + getQueueName(), "text/plain", "Hello World " + i + "!");
         Thread.sleep(500);
      }

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection(v11Uri);
      try {
         ClientStompFrame frame = newConn.createFrame(Stomp.Commands.CONNECT)
                                         .addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1")
                                         .addHeader(Stomp.Headers.Connect.LOGIN, this.defUser)
                                         .addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass)
                                         .addHeader(Stomp.Headers.Connect.HEART_BEAT, "500,1000")
                                         .addHeader(Stomp.Headers.ACCEPT_VERSION, "1.0,1.1");

         newConn.sendFrame(frame);

         newConn.startPinger(500);

         Thread.sleep(500);

         subscribe(newConn, "a-sub");

         int cnt = 0;

         frame = newConn.receiveFrame();

         while (frame != null) {
            cnt++;
            Thread.sleep(500);
            frame = newConn.receiveFrame(5000);
         }

         Assert.assertEquals(10, cnt);

         // unsub
         unsubscribe(newConn, "a-sub");
      } finally {
         newConn.disconnect();
      }
   }

   @Test
   public void testSendWithHeartBeatsAndReceiveWithHeartBeats() throws Exception {
      StompClientConnection newConn = null;
      try {
         ClientStompFrame frame = conn.createFrame(Stomp.Commands.CONNECT)
                                      .addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1")
                                      .addHeader(Stomp.Headers.Connect.LOGIN, this.defUser)
                                      .addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass)
                                      .addHeader(Stomp.Headers.Connect.HEART_BEAT, "500,1000")
                                      .addHeader(Stomp.Headers.ACCEPT_VERSION, "1.0,1.1,1.2");

         conn.sendFrame(frame);

         conn.startPinger(500);

         for (int i = 0; i < 10; i++) {
            send(conn, getQueuePrefix() + getQueueName(), "text/plain", "Hello World " + i + "!");
            Thread.sleep(500);
         }

         // subscribe
         newConn = StompClientConnectionFactory.createClientConnection(uri);
         frame = newConn.createFrame(Stomp.Commands.CONNECT)
                        .addHeader(Stomp.Headers.Connect.HOST, "127.0.0.1")
                        .addHeader(Stomp.Headers.Connect.LOGIN, this.defUser)
                        .addHeader(Stomp.Headers.Connect.PASSCODE, this.defPass)
                        .addHeader(Stomp.Headers.Connect.HEART_BEAT, "500,1000")
                        .addHeader(Stomp.Headers.ACCEPT_VERSION, "1.0,1.1,1.2");

         newConn.sendFrame(frame);

         newConn.startPinger(500);

         Thread.sleep(500);

         subscribe(newConn, "a-sub");

         int cnt = 0;

         frame = newConn.receiveFrame();

         while (frame != null) {
            cnt++;
            Thread.sleep(500);
            frame = newConn.receiveFrame(5000);
         }
         Assert.assertEquals(10, cnt);

         // unsub
         unsubscribe(newConn, "a-sub");
      } finally {
         if (newConn != null)
            newConn.disconnect();
         conn.disconnect();
      }
   }

   @Test
   public void testNack() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client");

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      String messageID = frame.getHeader(Stomp.Headers.Message.MESSAGE_ID);

      nack(conn, messageID);

      unsubscribe(conn, "sub1");

      conn.disconnect();

      //Nack makes the message be dropped.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testNackWithWrongSubId() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client");

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      String messageID = frame.getHeader(Stomp.Headers.Subscribe.ACK_MODE);

      nack(conn, messageID + "0");

      ClientStompFrame error = conn.receiveFrame();

      Assert.assertEquals(Stomp.Responses.ERROR, error.getCommand());

      waitDisconnect(conn);
      Assert.assertFalse("Should be disconnected in STOMP 1.2 after ERROR", conn.isConnected());

      //message should be still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testNackWithWrongMessageId() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client");

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      Assert.assertNotNull(frame);

      Assert.assertNotNull(frame.getHeader(Stomp.Headers.Subscribe.ACK_MODE));

      nack(conn, "someother");

      ClientStompFrame error = conn.receiveFrame();

      System.out.println("Receiver error: " + error);

      waitDisconnect(conn);
      Assert.assertFalse("Should be disconnected in STOMP 1.2 after ERROR", conn.isConnected());

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testAck() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client");

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      String messageID = frame.getHeader(Stomp.Headers.Subscribe.ACK_MODE);

      Assert.assertNotNull(messageID);

      ack(conn, messageID);

      unsubscribe(conn, "sub1");

      conn.disconnect();

      //Nack makes the message be dropped.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testAckNoIDHeader() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client-individual");

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      String messageID = frame.getHeader(Stomp.Headers.Subscribe.ACK_MODE);

      Assert.assertNotNull(messageID);

      ClientStompFrame ackFrame = conn.createFrame(Stomp.Commands.ACK);

      conn.sendFrame(ackFrame);

      frame = conn.receiveFrame();

      Assert.assertEquals(Stomp.Responses.ERROR, frame.getCommand());

      waitDisconnect(conn);
      Assert.assertFalse("Should be disconnected in STOMP 1.2 after ERROR", conn.isConnected());

      //message still there.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testAckWithWrongMessageId() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client");

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      Assert.assertNotNull(frame);

      ack(conn, "someother");

      ClientStompFrame error = conn.receiveFrame();

      System.out.println("Receiver error: " + error);

      waitDisconnect(conn);
      Assert.assertFalse("Should be disconnected in STOMP 1.2 after ERROR", conn.isConnected());

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testErrorWithReceipt() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client");

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      String messageID = frame.getHeader(Stomp.Headers.Message.MESSAGE_ID);

      ClientStompFrame ackFrame = conn.createFrame(Stomp.Commands.ACK);
      //give it a wrong sub id
      ackFrame.addHeader(Stomp.Headers.Message.SUBSCRIPTION, "sub2");
      ackFrame.addHeader(Stomp.Headers.Message.MESSAGE_ID, messageID);
      ackFrame.addHeader(Stomp.Headers.RECEIPT_REQUESTED, "answer-me");

      ClientStompFrame error = conn.sendFrame(ackFrame);

      System.out.println("Receiver error: " + error);

      Assert.assertEquals(Stomp.Responses.ERROR, error.getCommand());

      Assert.assertEquals("answer-me", error.getHeader("receipt-id"));

      waitDisconnect(conn);
      Assert.assertFalse("Should be disconnected in STOMP 1.2 after ERROR", conn.isConnected());

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testErrorWithReceipt2() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client");

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      String messageID = frame.getHeader(Stomp.Headers.Message.MESSAGE_ID);

      ClientStompFrame ackFrame = conn.createFrame(Stomp.Commands.ACK);
      //give it a wrong sub id
      ackFrame.addHeader(Stomp.Headers.Message.SUBSCRIPTION, "sub1");
      ackFrame.addHeader(Stomp.Headers.Message.MESSAGE_ID, String.valueOf(Long.valueOf(messageID) + 1));
      ackFrame.addHeader(Stomp.Headers.RECEIPT_REQUESTED, "answer-me");

      ClientStompFrame error = conn.sendFrame(ackFrame);

      System.out.println("Receiver error: " + error);

      Assert.assertEquals(Stomp.Responses.ERROR, error.getCommand());

      Assert.assertEquals("answer-me", error.getHeader("receipt-id"));

      waitDisconnect(conn);
      Assert.assertFalse("Should be disconnected in STOMP 1.2 after ERROR", conn.isConnected());

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   protected void waitDisconnect(StompClientConnectionV12 connection) throws Exception {

      long timeout = System.currentTimeMillis() + 10000;
      while (timeout > System.currentTimeMillis() && connection.isConnected()) {
         Thread.sleep(10);
      }
   }

   @Test
   public void testAckModeClient() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client");

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++) {
         this.sendJmsMessage("client-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++) {
         frame = conn.receiveFrame();
         Assert.assertNotNull(frame);
      }

      //ack the last
      ack(conn, frame);

      unsubscribe(conn, "sub1");

      conn.disconnect();

      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testAckModeClient2() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client");

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++) {
         this.sendJmsMessage("client-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++) {
         frame = conn.receiveFrame();
         Assert.assertNotNull(frame);

         //ack the 49th
         if (i == num - 2) {
            ack(conn, frame);
         }
      }

      unsubscribe(conn, "sub1");

      conn.disconnect();

      //one can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
      message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   //when ack is missing the mode default to auto
   @Test
   public void testAckModeDefault() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", null);

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++) {
         this.sendJmsMessage("auto-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++) {
         frame = conn.receiveFrame();
         Assert.assertNotNull(frame);
      }

      unsubscribe(conn, "sub1");

      conn.disconnect();

      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testAckModeAuto() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++) {
         this.sendJmsMessage("auto-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++) {
         frame = conn.receiveFrame();
         Assert.assertNotNull(frame);
      }

      unsubscribe(conn, "sub1");

      conn.disconnect();

      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testAckModeClientIndividual() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client-individual");

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++) {
         this.sendJmsMessage("client-individual-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++) {
         frame = conn.receiveFrame();
         Assert.assertNotNull(frame);

         System.out.println(i + " == received: " + frame);
         //ack on even numbers
         if (i % 2 == 0) {
            ack(conn, frame);
         }
      }

      unsubscribe(conn, "sub1");

      conn.disconnect();

      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);

      TextMessage message = null;
      for (int i = 0; i < num / 2; i++) {
         message = (TextMessage) consumer.receive(1000);
         Assert.assertNotNull(message);
         System.out.println("Legal: " + message.getText());
      }

      message = (TextMessage) consumer.receive(1000);

      Assert.assertNull(message);
   }

   @Test
   public void testTwoSubscribers() throws Exception {
      conn.connect(defUser, defPass, CLIENT_ID);

      subscribeTopic(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO, null);

      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection(v11Uri);
      newConn.connect(defUser, defPass, "myclientid2");

      subscribeTopic(newConn, "sub2", Stomp.Headers.Subscribe.AckModeValues.AUTO, null);

      send(conn, getTopicPrefix() + getTopicName(), null, "Hello World");

      // receive message from socket
      ClientStompFrame frame = conn.receiveFrame(1000);

      System.out.println("received frame : " + frame);
      Assert.assertEquals("Hello World", frame.getBody());
      Assert.assertEquals("sub1", frame.getHeader(Stomp.Headers.Message.SUBSCRIPTION));

      frame = newConn.receiveFrame(1000);

      System.out.println("received 2 frame : " + frame);
      Assert.assertEquals("Hello World", frame.getBody());
      Assert.assertEquals("sub2", frame.getHeader(Stomp.Headers.Message.SUBSCRIPTION));

      // remove suscription
      unsubscribe(conn, "sub1", true);
      unsubscribe(newConn, "sub2", true);

      conn.disconnect();
      newConn.disconnect();
   }

   @Test
   public void testSendAndReceiveOnDifferentConnections() throws Exception {
      conn.connect(defUser, defPass);

      send(conn, getQueuePrefix() + getQueueName(), null, "Hello World");

      StompClientConnection connV12_2 = StompClientConnectionFactory.createClientConnection(v11Uri);
      connV12_2.connect(defUser, defPass);

      subscribe(connV12_2, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);

      ClientStompFrame frame = connV12_2.receiveFrame(2000);

      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals("Hello World", frame.getBody());

      conn.disconnect();
      connV12_2.disconnect();
   }

   //----------------Note: tests below are adapted from StompTest

   @Test
   public void testBeginSameTransactionTwice() throws Exception {
      conn.connect(defUser, defPass);

      beginTransaction(conn, "tx1");

      beginTransaction(conn, "tx1");

      ClientStompFrame f = conn.receiveFrame();
      Assert.assertTrue(f.getCommand().equals(Stomp.Responses.ERROR));
   }

   @Test
   public void testBodyWithUTF8() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, getName(), Stomp.Headers.Subscribe.AckModeValues.AUTO);

      String text = "A" + "\u00ea" + "\u00f1" + "\u00fc" + "C";
      System.out.println(text);
      sendJmsMessage(text);

      ClientStompFrame frame = conn.receiveFrame();
      System.out.println(frame);
      Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.MESSAGE));
      Assert.assertNotNull(frame.getHeader(Stomp.Headers.Subscribe.DESTINATION));
      Assert.assertTrue(frame.getBody().equals(text));

      conn.disconnect();
   }

   @Test
   public void testClientAckNotPartOfTransaction() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, getName(), "client");

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.MESSAGE));
      Assert.assertNotNull(frame.getHeader(Stomp.Headers.Subscribe.DESTINATION));
      Assert.assertTrue(frame.getBody().equals(getName()));
      Assert.assertNotNull(frame.getHeader(Stomp.Headers.Message.MESSAGE_ID));
      Assert.assertNotNull(frame.getHeader(Stomp.Headers.Subscribe.ACK_MODE));

      String messageID = frame.getHeader(Stomp.Headers.Subscribe.ACK_MODE);

      beginTransaction(conn, "tx1");

      ack(conn, messageID, "tx1");

      abortTransaction(conn, "tx1");

      frame = conn.receiveFrame(500);

      Assert.assertNull(frame);

      unsubscribe(conn, getName());

      conn.disconnect();
   }

   @Test
   public void testDisconnectAndError() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, getName(), "client");

      ClientStompFrame frame = conn.createFrame("DISCONNECT");
      frame.addHeader(Stomp.Headers.RECEIPT_REQUESTED, "1");

      ClientStompFrame result = conn.sendFrame(frame);

      if (result == null || (!Stomp.Responses.RECEIPT.equals(result.getCommand())) || (!"1".equals(result.getHeader("receipt-id")))) {
         Assert.fail("Disconnect failed! " + result);
      }

      final CountDownLatch latch = new CountDownLatch(1);

      Thread thr = new Thread() {
         @Override
         public void run() {
            while (latch.getCount() != 0) {
               try {
                  send(conn, getQueuePrefix() + getQueueName(), null, "Hello World");
                  Thread.sleep(500);
               } catch (InterruptedException e) {
                  //retry
               } catch (ClosedChannelException e) {
                  //ok.
                  latch.countDown();
                  break;
               } catch (IOException e) {
                  //ok.
                  latch.countDown();
                  break;
               } finally {
                  conn.destroy();
               }
            }
         }
      };

      thr.start();
      latch.await(10, TimeUnit.SECONDS);

      long count = latch.getCount();
      if (count != 0) {
         latch.countDown();
      }
      thr.join();

      Assert.assertTrue("Server failed to disconnect.", count == 0);
   }

   @Test
   public void testDurableSubscriber() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client", getName());

      ClientStompFrame frame = subscribe(conn, "sub1", "client", getName());

      Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.ERROR));

      waitDisconnect(conn);
      Assert.assertFalse("Should be disconnected in STOMP 1.2 after ERROR", conn.isConnected());
   }

   @Test
   public void testDurableSubscriberWithReconnection() throws Exception {
      conn.connect(defUser, defPass, CLIENT_ID);

      subscribeTopic(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO, getName());

      ClientStompFrame frame = conn.createFrame("DISCONNECT");
      frame.addHeader(Stomp.Headers.RECEIPT_REQUESTED, "1");

      ClientStompFrame result = conn.sendFrame(frame);

      if (result == null || (!Stomp.Responses.RECEIPT.equals(result.getCommand())) || (!"1".equals(result.getHeader("receipt-id")))) {
         Assert.fail("Disconnect failed! " + result);
      }

      // send the message when the durable subscriber is disconnected
      sendJmsMessage(getName(), topic);

      conn.destroy();
      conn = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass, CLIENT_ID);

      subscribeTopic(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO, getName());

      // we must have received the message
      frame = conn.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.MESSAGE));
      Assert.assertNotNull(frame.getHeader(Stomp.Headers.Subscribe.DESTINATION));
      Assert.assertEquals(getName(), frame.getBody());

      unsubscribe(conn, "sub1");

      conn.disconnect();
   }

   @Test
   public void testDurableUnSubscribe() throws Exception {
      conn.connect(defUser, defPass, CLIENT_ID);

      subscribeTopic(conn, null, Stomp.Headers.Subscribe.AckModeValues.AUTO, getName());

      conn.disconnect();
      conn.destroy();
      conn = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass, CLIENT_ID);

      unsubscribe(conn, getName(), null, false, true);

      long start = System.currentTimeMillis();
      SimpleString queueName = SimpleString.toSimpleString(CLIENT_ID + "." + getName());
      while (server.getActiveMQServer().locateQueue(queueName) != null && (System.currentTimeMillis() - start) < 5000) {
         Thread.sleep(100);
      }

      assertNull(server.getActiveMQServer().locateQueue(queueName));

      conn.disconnect();
   }

   @Test
   public void testJMSXGroupIdCanBeSet() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND);
      frame.addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName());
      frame.addHeader("JMSXGroupID", "TEST");
      frame.setBody("Hello World");

      conn.sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      // differ from StompConnect
      Assert.assertEquals("TEST", message.getStringProperty("JMSXGroupID"));
   }

   @Test
   public void testMessagesAreInOrder() throws Exception {
      int ctr = 10;
      String[] data = new String[ctr];

      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);

      for (int i = 0; i < ctr; ++i) {
         data[i] = getName() + i;
         sendJmsMessage(data[i]);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < ctr; ++i) {
         frame = conn.receiveFrame();
         Assert.assertTrue("Message not in order", frame.getBody().equals(data[i]));
      }

      for (int i = 0; i < ctr; ++i) {
         data[i] = getName() + ":second:" + i;
         sendJmsMessage(data[i]);
      }

      for (int i = 0; i < ctr; ++i) {
         frame = conn.receiveFrame();
         Assert.assertTrue("Message not in order", frame.getBody().equals(data[i]));
      }

      conn.disconnect();
   }

   @Test
   public void testSubscribeWithAutoAckAndSelector() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO, null, "foo = 'zzz'");

      sendJmsMessage("Ignored message", "foo", "1234");
      sendJmsMessage("Real message", "foo", "zzz");

      ClientStompFrame frame = conn.receiveFrame();

      Assert.assertTrue("Should have received the real message but got: " + frame, frame.getBody().equals("Real message"));

      conn.disconnect();
   }

   @Test
   public void testRedeliveryWithClientAck() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "subId", "client");

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.MESSAGE));

      conn.disconnect();

      // message should be received since message was not acknowledged
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertTrue(message.getJMSRedelivered());
   }

   @Test
   public void testSendManyMessages() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      int count = 1000;
      final CountDownLatch latch = new CountDownLatch(count);
      consumer.setMessageListener(new MessageListener() {
         @Override
         public void onMessage(Message arg0) {
            TextMessage m = (TextMessage) arg0;
            latch.countDown();
            try {
               System.out.println("___> latch now: " + latch.getCount() + " m: " + m.getText());
            } catch (JMSException e) {
               Assert.fail("here failed");
               e.printStackTrace();
            }
         }
      });

      for (int i = 1; i <= count; i++) {
         send(conn, getQueuePrefix() + getQueueName(), null, "Hello World");
      }

      Assert.assertTrue(latch.await(60, TimeUnit.SECONDS));

      conn.disconnect();
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
   public void testSendMessageWithContentLength() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      byte[] data = new byte[]{1, 0, 0, 4};

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                                   .setBody(new String(data, StandardCharsets.UTF_8))
                                   .addHeader(Stomp.Headers.CONTENT_LENGTH, String.valueOf(data.length));

      conn.sendFrame(frame);

      BytesMessage message = (BytesMessage) consumer.receive(10000);
      Assert.assertNotNull(message);

      Assert.assertEquals(data.length, message.getBodyLength());
      Assert.assertEquals(data[0], message.readByte());
      Assert.assertEquals(data[1], message.readByte());
      Assert.assertEquals(data[2], message.readByte());
      Assert.assertEquals(data[3], message.readByte());
   }

   @Test
   public void testSendMessageWithCustomHeadersAndSelector() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue, "foo = 'abc'");

      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
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
   public void testSendMessageWithLeadingNewLine() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                                   .setBody("Hello World");

      conn.sendWickedFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());

      // Make sure that the timestamp is valid - should
      // be very close to the current time.
      long tnow = System.currentTimeMillis();
      long tmsg = message.getJMSTimestamp();
      Assert.assertTrue(Math.abs(tnow - tmsg) < 1000);

      Assert.assertNull(consumer.receive(1000));

      conn.disconnect();
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

      conn.disconnect();
   }

   @Test
   public void testSendMessageWithStandardHeaders() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader("correlation-id", "c123")
                                   .addHeader("persistent", "true")
                                   .addHeader("priority", "3")
                                   .addHeader(Stomp.Headers.Message.TYPE, "t345")
                                   .addHeader("JMSXGroupID", "abc")
                                   .addHeader("foo", "abc")
                                   .addHeader("bar", "123")
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

      conn.disconnect();
   }

   @Test
   public void testSendMessageWithLongHeaders() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      StringBuffer buffer = new StringBuffer();
      for (int i = 0; i < 2048; i++) {
         buffer.append("a");
      }

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader("correlation-id", "c123")
                                   .addHeader("persistent", "true")
                                   .addHeader("priority", "3")
                                   .addHeader(Stomp.Headers.Message.TYPE, "t345")
                                   .addHeader("JMSXGroupID", "abc")
                                   .addHeader("foo", "abc")
                                   .addHeader("very-very-long-stomp-message-header", buffer.toString())
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
      Assert.assertEquals("very-very-long-stomp-message-header", 2048, message.getStringProperty("very-very-long-stomp-message-header").length());

      Assert.assertEquals("JMSXGroupID", "abc", message.getStringProperty("JMSXGroupID"));

      conn.disconnect();
   }

   @Test
   public void testSubscribeToTopic() throws Exception {
      conn.connect(defUser, defPass);

      subscribeTopic(conn, "sub1", null, null, true);

      sendJmsMessage(getName(), topic);

      ClientStompFrame frame = conn.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.MESSAGE));
      Assert.assertTrue(frame.getHeader(Stomp.Headers.Subscribe.DESTINATION).equals(getTopicPrefix() + getTopicName()));
      Assert.assertTrue(frame.getBody().equals(getName()));

      unsubscribe(conn, "sub1", true);

      sendJmsMessage(getName(), topic);

      frame = conn.receiveFrame(1000);
      Assert.assertNull(frame);

      conn.disconnect();
   }

   @Test
   public void testSubscribeToTopicWithNoLocal() throws Exception {
      conn.connect(defUser, defPass);

      subscribeTopic(conn, "sub1", null, null, true, true);

      // send a message on the same connection => it should not be received
      send(conn, getTopicPrefix() + getTopicName(), null, "Hello World");

      ClientStompFrame frame = conn.receiveFrame(2000);

      Assert.assertNull(frame);

      // send message on another JMS connection => it should be received
      sendJmsMessage(getName(), topic);

      frame = conn.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.MESSAGE));
      Assert.assertTrue(frame.getHeader(Stomp.Headers.Subscribe.DESTINATION).equals(getTopicPrefix() + getTopicName()));
      Assert.assertTrue(frame.getBody().equals(getName()));

      unsubscribe(conn, "sub1");

      conn.disconnect();
   }

   @Test
   public void testSubscribeWithAutoAck() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertNotNull(frame.getHeader(Stomp.Headers.Subscribe.DESTINATION));
      Assert.assertEquals(getName(), frame.getBody());

      conn.disconnect();

      // message should not be received as it was auto-acked
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testSubscribeWithAutoAckAndBytesMessage() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);

      byte[] payload = new byte[]{1, 2, 3, 4, 5};
      sendJmsMessage(payload, queue);

      ClientStompFrame frame = conn.receiveFrame();

      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

      System.out.println("Message: " + frame);

      Assert.assertEquals("5", frame.getHeader(Stomp.Headers.CONTENT_LENGTH));

      Assert.assertEquals(null, frame.getHeader(Stomp.Headers.Message.TYPE));

      Assert.assertEquals(frame.getBody(), new String(payload, StandardCharsets.UTF_8));

      conn.disconnect();
   }

   @Test
   public void testSubscribeWithClientAck() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client");

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      ack(conn, frame);

      conn.disconnect();

      // message should not be received since message was acknowledged by the client
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testSubscribeWithClientAckThenConsumingAgainWithAutoAckWithExplicitDisconnect() throws Exception {
      assertSubscribeWithClientAckThenConsumeWithAutoAck(true);
   }

   @Test
   public void testSubscribeWithClientAckThenConsumingAgainWithAutoAckWithNoDisconnectFrame() throws Exception {
      assertSubscribeWithClientAckThenConsumeWithAutoAck(false);
   }

   @Test
   public void testSubscribeWithID() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "mysubid", Stomp.Headers.Subscribe.AckModeValues.AUTO);

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      Assert.assertTrue(frame.getHeader(Stomp.Headers.Message.SUBSCRIPTION) != null);

      conn.disconnect();
   }

   @Test
   public void testSubscribeWithMessageSentWithProperties() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);

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

      ClientStompFrame frame = conn.receiveFrame();
      Assert.assertNotNull(frame);

      Assert.assertTrue(frame.getHeader("S") != null);
      Assert.assertTrue(frame.getHeader("n") != null);
      Assert.assertTrue(frame.getHeader("byte") != null);
      Assert.assertTrue(frame.getHeader("d") != null);
      Assert.assertTrue(frame.getHeader("f") != null);
      Assert.assertTrue(frame.getHeader("i") != null);
      Assert.assertTrue(frame.getHeader("l") != null);
      Assert.assertTrue(frame.getHeader("s") != null);
      Assert.assertEquals("Hello World", frame.getBody());

      conn.disconnect();
   }

   @Test
   public void testSuccessiveTransactionsWithSameID() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      // first tx
      beginTransaction(conn, "tx1");

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader(Stomp.Headers.TRANSACTION, "tx1")
                                   .setBody("Hello World");

      conn.sendFrame(frame);

      commitTransaction(conn, "tx1");

      Message message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);

      // 2nd tx with same tx ID
      beginTransaction(conn, "tx1");

      frame = conn.createFrame(Stomp.Commands.SEND)
                  .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                  .addHeader(Stomp.Headers.TRANSACTION, "tx1")
                  .setBody("Hello World");

      conn.sendFrame(frame);

      commitTransaction(conn, "tx1");

      message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);

      conn.disconnect();
   }

   @Test
   public void testTransactionCommit() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      beginTransaction(conn, "tx1");

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader(Stomp.Headers.TRANSACTION, "tx1")
                                   .addHeader(Stomp.Headers.RECEIPT_REQUESTED, "123")
                                   .setBody("Hello World");

      frame = conn.sendFrame(frame);

      Assert.assertEquals("123", frame.getHeader("receipt-id"));

      // check the message is not committed
      Assert.assertNull(consumer.receive(100));

      commitTransaction(conn, "tx1", true);

      Message message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);

      conn.disconnect();
   }

   @Test
   public void testTransactionRollback() throws Exception {
      MessageConsumer consumer = session.createConsumer(queue);

      conn.connect(defUser, defPass);

      beginTransaction(conn, "tx1");

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                                   .addHeader(Stomp.Headers.TRANSACTION, "tx1")
                                   .setBody("first message");

      conn.sendFrame(frame);

      // rollback first message
      abortTransaction(conn, "tx1");

      beginTransaction(conn, "tx1");

      frame = conn.createFrame(Stomp.Commands.SEND)
                  .addHeader(Stomp.Headers.Subscribe.DESTINATION, getQueuePrefix() + getQueueName())
                  .addHeader(Stomp.Headers.TRANSACTION, "tx1")
                  .setBody("second message");

      conn.sendFrame(frame);

      commitTransaction(conn, "tx1", true);

      // only second msg should be received since first msg was rolled back
      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("second message", message.getText());

      conn.disconnect();
   }

   @Test
   public void testUnsubscribe() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);

      // send a message to our queue
      sendJmsMessage("first message");

      // receive message from socket
      ClientStompFrame frame = conn.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.MESSAGE));

      // remove suscription
      unsubscribe(conn, "sub1", true);

      // send a message to our queue
      sendJmsMessage("second message");

      frame = conn.receiveFrame(1000);
      Assert.assertNull(frame);

      conn.disconnect();
   }

   @Test
   public void testDisconnectWithoutUnsubscribe() throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);

      // send a message to our queue
      sendJmsMessage("first message");

      // receive message from socket
      ClientStompFrame frame = conn.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.MESSAGE));

      //now disconnect without unsubscribe
      conn.disconnect();

      // send a message to our queue
      sendJmsMessage("second message");

      //reconnect
      conn = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass);

      frame = conn.receiveFrame(1000);
      Assert.assertNull("not expected: " + frame, frame);

      //subscribe again.
      subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);

      // receive message from socket
      frame = conn.receiveFrame();

      Assert.assertNotNull(frame);
      Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.MESSAGE));

      frame = conn.receiveFrame(1000);
      Assert.assertNull("not expected: " + frame, frame);

      unsubscribe(conn, "sub1", true);

      frame = conn.receiveFrame(1000);
      Assert.assertNull(frame);

      conn.disconnect();
   }

   protected void assertSubscribeWithClientAckThenConsumeWithAutoAck(boolean sendDisconnect) throws Exception {
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", "client");

      sendJmsMessage(getName());

      ClientStompFrame frame = conn.receiveFrame();

      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

      log.info("Reconnecting!");

      if (sendDisconnect) {
         conn.disconnect();
         conn = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection(uri);
      } else {
         conn.destroy();
         conn = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection(uri);
      }

      // message should be received since message was not acknowledged
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", null);

      frame = conn.receiveFrame();
      Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.MESSAGE));

      conn.disconnect();

      // now let's make sure we don't see the message again
      conn.destroy();
      conn = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass);

      subscribe(conn, "sub1", null, null, true);

      sendJmsMessage("shouldBeNextMessage");

      frame = conn.receiveFrame();
      Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.MESSAGE));
      Assert.assertEquals("shouldBeNextMessage", frame.getBody());
   }

   @Test
   public void testSendMessageToNonExistentQueueWithAutoCreation() throws Exception {
      conn.connect(defUser, defPass);

      send(conn, "NonExistentQueue" + UUID.randomUUID().toString(), null, "Hello World", true, RoutingType.ANYCAST);

      conn.disconnect();
   }

   @Test
   public void testInvalidStompCommand() throws Exception {
      try {
         conn.connect(defUser, defPass);

         ClientStompFrame frame = conn.createAnyFrame("INVALID");
         frame.setBody("Hello World");
         frame.addHeader(Stomp.Headers.RECEIPT_REQUESTED, "1234");//make the client receives this reply.

         frame = conn.sendFrame(frame);

         Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.ERROR));
      } finally {
         //because the last frame is ERROR, the connection
         //might already have closed by the server.
         //this is expected so we ignore it.
         conn.destroy();
      }
   }

   @Test
   public void testSendAndReceiveWithEscapedCharactersInSenderId() throws Exception {
      conn.connect(defUser, defPass);

      ClientStompFrame response = send(conn, getQueuePrefix() + getQueueName(), "text/plain", "Hello World 1!");

      Assert.assertNull(response);

      //subscribe
      subscribe(conn, "ID\\cMYMACHINE-50616-635482262727823605-1\\c1\\c1\\c1");

      ClientStompFrame frame = conn.receiveFrame();

      System.out.println("Received: " + frame);

      Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());
      Assert.assertEquals("ID:MYMACHINE-50616-635482262727823605-1:1:1:1", frame.getHeader(Stomp.Headers.Message.SUBSCRIPTION));
      Assert.assertNotNull(frame.getHeader(Stomp.Headers.Message.MESSAGE_ID));
      Assert.assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader(Stomp.Headers.Subscribe.DESTINATION));
      Assert.assertEquals("Hello World 1!", frame.getBody());

      //unsub
      unsubscribe(conn, "ID\\cMYMACHINE-50616-635482262727823605-1\\c1\\c1\\c1");

      conn.disconnect();
   }

   private void ack(StompClientConnection conn, ClientStompFrame frame) throws IOException, InterruptedException {
      String messageID = frame.getHeader(Stomp.Headers.Subscribe.ACK_MODE);

      ClientStompFrame ackFrame = conn.createFrame(Stomp.Commands.ACK);

      ackFrame.addHeader(Stomp.Headers.Subscribe.ID, messageID);

      ClientStompFrame response = conn.sendFrame(ackFrame);
      if (response != null) {
         throw new IOException("failed to ack " + response);
      }
   }

   // STOMP 1.2-specific ACK and NACK methods

   private void ack(StompClientConnection conn, String mid) throws IOException, InterruptedException {
      ClientStompFrame ackFrame = conn.createFrame(Stomp.Commands.ACK);
      ackFrame.addHeader(Stomp.Headers.Subscribe.ID, mid);

      conn.sendFrame(ackFrame);
   }

   private void ack(StompClientConnection conn, String mid, String txID) throws IOException, InterruptedException {
      ClientStompFrame ackFrame = conn.createFrame(Stomp.Commands.ACK);
      ackFrame.addHeader(Stomp.Headers.Subscribe.ID, mid);
      ackFrame.addHeader(Stomp.Headers.TRANSACTION, txID);

      conn.sendFrame(ackFrame);
   }

   private void nack(StompClientConnection conn, String mid) throws IOException, InterruptedException {
      ClientStompFrame ackFrame = conn.createFrame(Stomp.Commands.NACK);
      ackFrame.addHeader(Stomp.Headers.Subscribe.ID, mid);

      conn.sendFrame(ackFrame);
   }
}

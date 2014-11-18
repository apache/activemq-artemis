/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.tests.integration.stomp.v12;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.tests.integration.stomp.util.StompClientConnectionFactory;
import org.apache.activemq.tests.integration.stomp.util.StompClientConnectionV11;
import org.apache.activemq.tests.integration.stomp.util.StompClientConnectionV12;
import org.apache.activemq.tests.integration.stomp.v11.StompV11TestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing Stomp version 1.2 functionalities
 */
public class StompV12Test extends StompV11TestBase
{
   private static final transient IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private StompClientConnectionV12 connV12;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      connV12 = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      try
      {
         log.debug("Connection 1.2 : " + connV12.isConnected());
         if (connV12 != null && connV12.isConnected())
         {
            connV12.disconnect();
         }
      }
      finally
      {
         super.tearDown();
      }
   }

   @Test
   public void testConnection() throws Exception
   {
      server.getActiveMQServer().getConfiguration().setSecurityEnabled(true);
      StompClientConnection connection = StompClientConnectionFactory.createClientConnection("1.0", hostname, port);

      connection.connect(defUser, defPass);

      assertTrue(connection.isConnected());

      assertEquals("1.0", connection.getVersion());

      connection.disconnect();

      connection = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);

      connection.connect(defUser, defPass);

      assertTrue(connection.isConnected());

      assertEquals("1.2", connection.getVersion());

      connection.disconnect();

      connection = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);

      connection.connect();

      assertFalse(connection.isConnected());

      //new way of connection
      StompClientConnectionV11 conn = (StompClientConnectionV11) StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      conn.connect1(defUser, defPass);

      assertTrue(conn.isConnected());

      conn.disconnect();
   }

   @Test
   public void testConnectionAsInSpec() throws Exception
   {
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection("1.0", hostname, port);

      ClientStompFrame frame = conn.createFrame("CONNECT");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("accept-version", "1.2");
      frame.addHeader("host", "127.0.0.1");

      ClientStompFrame reply = conn.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());
      assertEquals("1.2", reply.getHeader("version"));

      conn.disconnect();

      //need 1.2 client
      conn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);

      frame = conn.createFrame("STOMP");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("accept-version", "1.2");
      frame.addHeader("host", "127.0.0.1");

      reply = conn.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());
      assertEquals("1.2", reply.getHeader("version"));

      conn.disconnect();
   }

   @Test
   public void testNegotiation() throws Exception
   {
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection("1.0", hostname, port);
      // case 1 accept-version absent. It is a 1.0 connect
      ClientStompFrame frame = conn.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);

      ClientStompFrame reply = conn.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      //reply headers: version, session, server
      assertEquals(null, reply.getHeader("version"));

      conn.disconnect();

      // case 2 accept-version=1.0, result: 1.0
      conn = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = conn.createFrame("CONNECT");
      frame.addHeader("accept-version", "1.0");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);

      reply = conn.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      //reply headers: version, session, server
      assertEquals("1.0", reply.getHeader("version"));

      conn.disconnect();

      // case 3 accept-version=1.1, result: 1.1
      conn = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = conn.createFrame("CONNECT");
      frame.addHeader("accept-version", "1.1");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);

      reply = conn.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      //reply headers: version, session, server
      assertEquals("1.1", reply.getHeader("version"));

      conn.disconnect();

      // case 4 accept-version=1.0,1.1,1.3, result 1.2
      conn = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = conn.createFrame("CONNECT");
      frame.addHeader("accept-version", "1.0,1.1,1.3");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);

      reply = conn.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      //reply headers: version, session, server
      assertEquals("1.1", reply.getHeader("version"));

      conn.disconnect();

      // case 5 accept-version=1.3, result error
      conn = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = conn.createFrame("CONNECT");
      frame.addHeader("accept-version", "1.3");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);

      reply = conn.sendFrame(frame);

      assertEquals("ERROR", reply.getCommand());

      System.out.println("Got error frame " + reply);

   }

   @Test
   public void testSendAndReceive() throws Exception
   {
      connV12.connect(defUser, defPass);
      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World 1!");

      ClientStompFrame response = connV12.sendFrame(frame);

      assertNull(response);

      frame.addHeader("receipt", "1234");
      frame.setBody("Hello World 2!");

      response = connV12.sendFrame(frame);

      assertNotNull(response);

      assertEquals("RECEIPT", response.getCommand());

      assertEquals("1234", response.getHeader("receipt-id"));

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      newConn.connect(defUser, defPass);

      ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      newConn.sendFrame(subFrame);

      frame = newConn.receiveFrame();

      System.out.println("received " + frame);

      assertEquals("MESSAGE", frame.getCommand());

      assertEquals("a-sub", frame.getHeader("subscription"));

      //'auto' ack mode doesn't require 'ack' header
      assertNull(frame.getHeader("ack"));

      assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader("destination"));

      assertEquals("Hello World 1!", frame.getBody());

      frame = newConn.receiveFrame();

      System.out.println("received " + frame);

      //unsub
      ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      newConn.sendFrame(unsubFrame);

      newConn.disconnect();
   }

   @Test
   public void testHeaderContentType() throws Exception
   {
      connV12.connect(defUser, defPass);
      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "application/xml");
      frame.setBody("Hello World 1!");

      connV12.sendFrame(frame);

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      newConn.connect(defUser, defPass);

      ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      newConn.sendFrame(subFrame);

      frame = newConn.receiveFrame();

      System.out.println("received " + frame);

      assertEquals("MESSAGE", frame.getCommand());

      assertEquals("application/xml", frame.getHeader("content-type"));

      //unsub
      ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");

      newConn.disconnect();
   }

   @Test
   public void testHeaderContentLength() throws Exception
   {
      connV12.connect(defUser, defPass);
      ClientStompFrame frame = connV12.createFrame("SEND");

      String body = "Hello World 1!";
      String cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "application/xml");
      frame.addHeader("content-length", cLen);
      frame.setBody(body + "extra");

      connV12.sendFrame(frame);

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      newConn.connect(defUser, defPass);

      ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      newConn.sendFrame(subFrame);

      frame = newConn.receiveFrame();

      assertEquals("MESSAGE", frame.getCommand());
      assertEquals(body, frame.getBody());

      assertEquals(cLen, frame.getHeader("content-length"));

      //send again without content-length header
      frame = connV12.createFrame("SEND");

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "application/xml");
      frame.setBody(body + "extra");

      connV12.sendFrame(frame);

      //receive again. extra should received.
      frame = newConn.receiveFrame();

      assertEquals(body + "extra", frame.getBody());

      //although sender didn't send the content-length header,
      //the server should add it anyway
      assertEquals((body + "extra").getBytes(StandardCharsets.UTF_8).length, Integer.valueOf(frame.getHeader("content-length")).intValue());

      //unsub
      ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");

      newConn.disconnect();
   }

   //test that repetitive headers
   @Test
   public void testHeaderRepetitive() throws Exception
   {
      connV12.connect(defUser, defPass);
      ClientStompFrame frame = connV12.createFrame("SEND");

      String body = "Hello World!";
      String cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("destination", "aNonexistentQueue");
      frame.addHeader("content-type", "application/xml");
      frame.addHeader("content-length", cLen);
      frame.addHeader("foo", "value1");
      frame.addHeader("foo", "value2");
      frame.addHeader("foo", "value3");

      frame.setBody(body);

      connV12.sendFrame(frame);

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      newConn.connect(defUser, defPass);

      ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      newConn.sendFrame(subFrame);

      frame = newConn.receiveFrame();

      assertEquals("MESSAGE", frame.getCommand());
      assertEquals(body, frame.getBody());

      System.out.println("received: " + frame);
      assertEquals("value1", frame.getHeader("foo"));

      //unsub
      ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");

      newConn.disconnect();

      //should get error
      frame = connV12.createFrame("SEND");

      body = "Hello World!";
      cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);

      frame.addHeader("destination", "aNonexistentQueue");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "application/xml");
      frame.addHeader("content-length", cLen);
      frame.addHeader("receipt", "1234");

      frame.setBody(body);

      ClientStompFrame reply = connV12.sendFrame(frame);
      assertEquals("ERROR", reply.getCommand());
   }

   //padding shouldn't be trimmed
   @Test
   public void testHeadersPadding() throws Exception
   {
      connV12.connect(defUser, defPass);
      ClientStompFrame frame = connV12.createFrame("SEND");

      String body = "<p>Hello World!</p>";
      String cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "application/xml");
      frame.addHeader("content-length", cLen);
      frame.addHeader(" header1", "value1 ");
      frame.addHeader("  header2", "value2   ");
      frame.addHeader("header3 ", " value3");
      frame.addHeader(" header4 ", " value4 ");
      frame.addHeader(" header 5  ", " value 5 ");
      frame.addHeader("header6", "\t value\t 6 \t");

      frame.setBody(body);

      connV12.sendFrame(frame);

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      newConn.connect(defUser, defPass);

      ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      newConn.sendFrame(subFrame);

      frame = newConn.receiveFrame();

      assertEquals("MESSAGE", frame.getCommand());
      assertEquals(body, frame.getBody());

      System.out.println("received: " + frame);
      assertEquals(null, frame.getHeader("header1"));
      assertEquals("value1 ", frame.getHeader(" header1"));
      assertEquals("value2   ", frame.getHeader("  header2"));
      assertEquals(" value3", frame.getHeader("header3 "));
      assertEquals(" value4 ", frame.getHeader(" header4 "));
      assertEquals(" value 5 ", frame.getHeader(" header 5  "));
      assertEquals("\t value\t 6 \t", frame.getHeader("header6"));

      //unsub
      ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");

      newConn.disconnect();
   }

   /**
    * Since 1.2 the CR ('\r') needs to be escaped.
    */
   @Test
   public void testHeaderEncoding() throws Exception
   {
      connV12.connect(defUser, defPass);
      ClientStompFrame frame = connV12.createFrame("SEND");

      String body = "Hello World 1!";
      String cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "application/xml");
      frame.addHeader("content-length", cLen);
      String hKey = "\\rspecial-header\\\\\\n\\c\\r\\n";
      String hVal = "\\c\\\\\\ngood\\n\\r";
      frame.addHeader(hKey, hVal);

      System.out.println("key: |" + hKey + "| val: |" + hVal + "|");

      frame.setBody(body);

      connV12.sendFrame(frame);

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      newConn.connect(defUser, defPass);

      ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      newConn.sendFrame(subFrame);

      frame = newConn.receiveFrame();

      System.out.println("received " + frame);

      assertEquals("MESSAGE", frame.getCommand());

      String value = frame.getHeader("\r" + "special-header" + "\\" + "\n" + ":" + "\r\n");

      assertEquals(":" + "\\" + "\n" + "good\n\r", value);

      //unsub
      ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");

      newConn.disconnect();
   }

   @Test
   public void testHeartBeat() throws Exception
   {
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      //no heart beat at all if heat-beat absent
      ClientStompFrame frame = conn.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);

      ClientStompFrame reply = conn.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      Thread.sleep(5000);

      assertEquals(0, conn.getFrameQueueSize());

      conn.disconnect();

      //no heart beat for (0,0)
      conn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      frame = conn.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "0,0");
      frame.addHeader("accept-version", "1.0,1.1,1.2");

      reply = conn.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      assertEquals("0,0", reply.getHeader("heart-beat"));

      Thread.sleep(5000);

      assertEquals(0, conn.getFrameQueueSize());

      conn.disconnect();

      //heart-beat (1,0), should receive a min client ping accepted by server
      conn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      frame = conn.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "1,0");
      frame.addHeader("accept-version", "1.0,1.2");

      reply = conn.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      assertEquals("0,500", reply.getHeader("heart-beat"));

      Thread.sleep(2000);

      //now server side should be disconnected because we didn't send ping for 2 sec
      frame = conn.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World");

      //send will fail
      try
      {
         conn.sendFrame(frame);
         fail("connection should have been destroyed by now");
      }
      catch (IOException e)
      {
         //ignore
      }

      //heart-beat (1,0), start a ping, then send a message, should be ok.
      conn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      frame = conn.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "1,0");
      frame.addHeader("accept-version", "1.0,1.1,1.2");

      reply = conn.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      assertEquals("0,500", reply.getHeader("heart-beat"));

      conn.startPinger(500);

      Thread.sleep(2000);

      frame = conn.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World");

      //send will be ok
      conn.sendFrame(frame);

      conn.stopPinger();

      conn.disconnect();

   }

   //server ping
   @Test
   public void testHeartBeat2() throws Exception
   {
      //heart-beat (1,1)
      ClientStompFrame frame = connV12.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "1,1");
      frame.addHeader("accept-version", "1.0,1.1,1.2");

      ClientStompFrame reply = connV12.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());
      assertEquals("500,500", reply.getHeader("heart-beat"));

      connV12.disconnect();

      //heart-beat (500,1000)
      connV12 = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      frame = connV12.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "500,1000");
      frame.addHeader("accept-version", "1.0,1.1,1.2");

      reply = connV12.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      assertEquals("1000,500", reply.getHeader("heart-beat"));

      connV12.startPinger(500);

      Thread.sleep(10000);

      //now check the frame size
      int size = connV12.getServerPingNumber();

      System.out.println("ping received: " + size);

      assertTrue("size: " + size, size > 5);

      //now server side should be disconnected because we didn't send ping for 2 sec
      frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World");

      //send will be ok
      connV12.sendFrame(frame);

      connV12.disconnect();
   }

   @Test
   public void testSendWithHeartBeatsAndReceive() throws Exception
   {
      StompClientConnection newConn = null;
      try
      {
         ClientStompFrame frame = connV12.createFrame("CONNECT");
         frame.addHeader("host", "127.0.0.1");
         frame.addHeader("login", this.defUser);
         frame.addHeader("passcode", this.defPass);
         frame.addHeader("heart-beat", "500,1000");
         frame.addHeader("accept-version", "1.0,1.1,1.2");

         connV12.sendFrame(frame);

         connV12.startPinger(500);

         frame = connV12.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("content-type", "text/plain");

         for (int i = 0; i < 10; i++)
         {
            frame.setBody("Hello World " + i + "!");
            connV12.sendFrame(frame);
            Thread.sleep(500);
         }

         // subscribe
         newConn = StompClientConnectionFactory.createClientConnection("1.2",
                                                                       hostname, port);
         newConn.connect(defUser, defPass);

         ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "auto");

         newConn.sendFrame(subFrame);

         int cnt = 0;

         frame = newConn.receiveFrame();

         while (frame != null)
         {
            cnt++;
            Thread.sleep(500);
            frame = newConn.receiveFrame(5000);
         }

         assertEquals(10, cnt);

         // unsub
         ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
         unsubFrame.addHeader("id", "a-sub");
         newConn.sendFrame(unsubFrame);
      }
      finally
      {
         if (newConn != null)
            newConn.disconnect();
         connV12.disconnect();
      }
   }

   @Test
   public void testSendAndReceiveWithHeartBeats() throws Exception
   {
      connV12.connect(defUser, defPass);
      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");

      for (int i = 0; i < 10; i++)
      {
         frame.setBody("Hello World " + i + "!");
         connV12.sendFrame(frame);
         Thread.sleep(500);
      }

      //subscribe
      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      try
      {
         frame = newConn.createFrame("CONNECT");
         frame.addHeader("host", "127.0.0.1");
         frame.addHeader("login", this.defUser);
         frame.addHeader("passcode", this.defPass);
         frame.addHeader("heart-beat", "500,1000");
         frame.addHeader("accept-version", "1.0,1.1");

         newConn.sendFrame(frame);

         newConn.startPinger(500);

         Thread.sleep(500);

         ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "auto");

         newConn.sendFrame(subFrame);

         int cnt = 0;

         frame = newConn.receiveFrame();

         while (frame != null)
         {
            cnt++;
            Thread.sleep(500);
            frame = newConn.receiveFrame(5000);
         }

         assertEquals(10, cnt);

         // unsub
         ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
         unsubFrame.addHeader("id", "a-sub");
         newConn.sendFrame(unsubFrame);
      }
      finally
      {
         newConn.disconnect();
      }
   }

   @Test
   public void testSendWithHeartBeatsAndReceiveWithHeartBeats() throws Exception
   {
      StompClientConnection newConn = null;
      try
      {
         ClientStompFrame frame = connV12.createFrame("CONNECT");
         frame.addHeader("host", "127.0.0.1");
         frame.addHeader("login", this.defUser);
         frame.addHeader("passcode", this.defPass);
         frame.addHeader("heart-beat", "500,1000");
         frame.addHeader("accept-version", "1.0,1.1,1.2");

         connV12.sendFrame(frame);

         connV12.startPinger(500);

         frame = connV12.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("content-type", "text/plain");

         for (int i = 0; i < 10; i++)
         {
            frame.setBody("Hello World " + i + "!");
            connV12.sendFrame(frame);
            Thread.sleep(500);
         }

         // subscribe
         newConn = StompClientConnectionFactory.createClientConnection("1.2",
                                                                       hostname, port);
         frame = newConn.createFrame("CONNECT");
         frame.addHeader("host", "127.0.0.1");
         frame.addHeader("login", this.defUser);
         frame.addHeader("passcode", this.defPass);
         frame.addHeader("heart-beat", "500,1000");
         frame.addHeader("accept-version", "1.0,1.1,1.2");

         newConn.sendFrame(frame);

         newConn.startPinger(500);

         Thread.sleep(500);

         ClientStompFrame subFrame = newConn.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "auto");

         newConn.sendFrame(subFrame);

         int cnt = 0;

         frame = newConn.receiveFrame();

         while (frame != null)
         {
            cnt++;
            Thread.sleep(500);
            frame = newConn.receiveFrame(5000);
         }
         assertEquals(10, cnt);

         // unsub
         ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
         unsubFrame.addHeader("id", "a-sub");
         newConn.sendFrame(unsubFrame);
      }
      finally
      {
         if (newConn != null)
            newConn.disconnect();
         connV12.disconnect();
      }
   }

   @Test
   public void testNack() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      String messageID = frame.getHeader("message-id");

      nack(connV12, messageID);

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //Nack makes the message be dropped.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testNackWithWrongSubId() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      String messageID = frame.getHeader("ack");

      nack(connV12, messageID + "0");

      ClientStompFrame error = connV12.receiveFrame();

      assertEquals("ERROR", error.getCommand());

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //message should be still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testNackWithWrongMessageId() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      assertNotNull(frame);

      assertNotNull(frame.getHeader("ack"));

      nack(connV12, "someother");

      ClientStompFrame error = connV12.receiveFrame();

      System.out.println("Receiver error: " + error);

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testAck() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      String messageID = frame.getHeader("ack");

      assertNotNull(messageID);

      ack(connV12, messageID, null);

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //Nack makes the message be dropped.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testAckNoIDHeader() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", "client-individual");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      String messageID = frame.getHeader("ack");

      assertNotNull(messageID);

      ClientStompFrame ackFrame = connV12.createFrame("ACK");

      connV12.sendFrame(ackFrame);

      frame = connV12.receiveFrame();

      assertEquals("ERROR", frame.getCommand());

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //message still there.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      assertNotNull(message);
   }

   @Test
   public void testAckWithWrongMessageId() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      assertNotNull(frame);

      ack(connV12, "someother", null);

      ClientStompFrame error = connV12.receiveFrame();

      System.out.println("Receiver error: " + error);

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testErrorWithReceipt() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      String messageID = frame.getHeader("message-id");


      ClientStompFrame ackFrame = connV12.createFrame("ACK");
      //give it a wrong sub id
      ackFrame.addHeader("subscription", "sub2");
      ackFrame.addHeader("message-id", messageID);
      ackFrame.addHeader("receipt", "answer-me");

      ClientStompFrame error = connV12.sendFrame(ackFrame);

      System.out.println("Receiver error: " + error);

      assertEquals("ERROR", error.getCommand());

      assertEquals("answer-me", error.getHeader("receipt-id"));

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testErrorWithReceipt2() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      String messageID = frame.getHeader("message-id");


      ClientStompFrame ackFrame = connV12.createFrame("ACK");
      //give it a wrong sub id
      ackFrame.addHeader("subscription", "sub1");
      ackFrame.addHeader("message-id", String.valueOf(Long.valueOf(messageID) + 1));
      ackFrame.addHeader("receipt", "answer-me");

      ClientStompFrame error = connV12.sendFrame(ackFrame);

      System.out.println("Receiver error: " + error);

      assertEquals("ERROR", error.getCommand());

      assertEquals("answer-me", error.getHeader("receipt-id"));

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testAckModeClient() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", "client");

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("client-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++)
      {
         frame = connV12.receiveFrame();
         assertNotNull(frame);
      }

      //ack the last
      ack(connV12, frame);

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testAckModeClient2() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", "client");

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("client-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++)
      {
         frame = connV12.receiveFrame();
         assertNotNull(frame);

         //ack the 49th
         if (i == num - 2)
         {
            ack(connV12, frame);
         }
      }

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //one can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
      message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   //when ack is missing the mode default to auto
   @Test
   public void testAckModeDefault() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", null);

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("auto-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++)
      {
         frame = connV12.receiveFrame();
         assertNotNull(frame);
      }

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testAckModeAuto() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", "auto");

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("auto-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++)
      {
         frame = connV12.receiveFrame();
         assertNotNull(frame);
      }

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testAckModeClientIndividual() throws Exception
   {
      connV12.connect(defUser, defPass);

      subscribe(connV12, "sub1", "client-individual");

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("client-individual-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++)
      {
         frame = connV12.receiveFrame();
         assertNotNull(frame);

         System.out.println(i + " == received: " + frame);
         //ack on even numbers
         if (i % 2 == 0)
         {
            ack(connV12, frame);
         }
      }

      unsubscribe(connV12, "sub1");

      connV12.disconnect();

      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);

      TextMessage message = null;
      for (int i = 0; i < num / 2; i++)
      {
         message = (TextMessage) consumer.receive(1000);
         Assert.assertNotNull(message);
         System.out.println("Legal: " + message.getText());
      }

      message = (TextMessage) consumer.receive(1000);

      Assert.assertNull(message);
   }

   @Test
   public void testTwoSubscribers() throws Exception
   {
      connV12.connect(defUser, defPass, "myclientid");

      this.subscribeTopic(connV12, "sub1", "auto", null);

      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      newConn.connect(defUser, defPass, "myclientid2");

      this.subscribeTopic(newConn, "sub2", "auto", null);

      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getTopicPrefix() + getTopicName());

      frame.setBody("Hello World");

      connV12.sendFrame(frame);

      // receive message from socket
      frame = connV12.receiveFrame(1000);

      System.out.println("received frame : " + frame);
      assertEquals("Hello World", frame.getBody());
      assertEquals("sub1", frame.getHeader("subscription"));

      frame = newConn.receiveFrame(1000);

      System.out.println("received 2 frame : " + frame);
      assertEquals("Hello World", frame.getBody());
      assertEquals("sub2", frame.getHeader("subscription"));

      // remove suscription
      this.unsubscribe(connV12, "sub1", true);
      this.unsubscribe(newConn, "sub2", true);

      connV12.disconnect();
      newConn.disconnect();
   }

   @Test
   public void testSendAndReceiveOnDifferentConnections() throws Exception
   {
      connV12.connect(defUser, defPass);

      ClientStompFrame sendFrame = connV12.createFrame("SEND");
      sendFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      sendFrame.setBody("Hello World");

      connV12.sendFrame(sendFrame);

      StompClientConnection connV12_2 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      connV12_2.connect(defUser, defPass);

      this.subscribe(connV12_2, "sub1", "auto");

      ClientStompFrame frame = connV12_2.receiveFrame(2000);

      assertEquals("MESSAGE", frame.getCommand());
      assertEquals("Hello World", frame.getBody());

      connV12.disconnect();
      connV12_2.disconnect();
   }

   //----------------Note: tests below are adapted from StompTest

   @Test
   public void testBeginSameTransactionTwice() throws Exception
   {
      connV12.connect(defUser, defPass);

      beginTransaction(connV12, "tx1");

      beginTransaction(connV12, "tx1");

      ClientStompFrame f = connV12.receiveFrame();
      Assert.assertTrue(f.getCommand().equals("ERROR"));
   }

   @Test
   public void testBodyWithUTF8() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, getName(), "auto");

      String text = "A" + "\u00ea" + "\u00f1" + "\u00fc" + "C";
      System.out.println(text);
      sendMessage(text);

      ClientStompFrame frame = connV12.receiveFrame();
      System.out.println(frame);
      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertNotNull(frame.getHeader("destination"));
      Assert.assertTrue(frame.getBody().equals(text));

      connV12.disconnect();
   }

   @Test
   public void testClientAckNotPartOfTransaction() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, getName(), "client");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertNotNull(frame.getHeader("destination"));
      Assert.assertTrue(frame.getBody().equals(getName()));
      Assert.assertNotNull(frame.getHeader("message-id"));
      Assert.assertNotNull(frame.getHeader("ack"));

      String messageID = frame.getHeader("ack");

      beginTransaction(connV12, "tx1");

      ack(connV12, messageID, "tx1");

      abortTransaction(connV12, "tx1");

      frame = connV12.receiveFrame(500);

      assertNull(frame);

      this.unsubscribe(connV12, getName());

      connV12.disconnect();
   }

   @Test
   public void testDisconnectAndError() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, getName(), "client");

      ClientStompFrame frame = connV12.createFrame("DISCONNECT");
      frame.addHeader("receipt", "1");

      ClientStompFrame result = connV12.sendFrame(frame);

      if (result == null || (!"RECEIPT".equals(result.getCommand())) || (!"1".equals(result.getHeader("receipt-id"))))
      {
         fail("Disconnect failed! " + result);
      }

      final CountDownLatch latch = new CountDownLatch(1);

      Thread thr = new Thread()
      {
         public void run()
         {
            ClientStompFrame sendFrame = connV12.createFrame("SEND");
            sendFrame.addHeader("destination", getQueuePrefix() + getQueueName());
            sendFrame.setBody("Hello World");
            while (latch.getCount() != 0)
            {
               try
               {
                  connV12.sendFrame(sendFrame);
                  Thread.sleep(500);
               }
               catch (InterruptedException e)
               {
                  //retry
               }
               catch (ClosedChannelException e)
               {
                  //ok.
                  latch.countDown();
                  break;
               }
               catch (IOException e)
               {
                  //ok.
                  latch.countDown();
                  break;
               }
               finally
               {
                  connV12.destroy();
               }
            }
         }
      };

      thr.start();
      latch.await(10, TimeUnit.SECONDS);

      long count = latch.getCount();
      if (count != 0)
      {
         latch.countDown();
      }
      thr.join();

      assertTrue("Server failed to disconnect.", count == 0);
   }

   @Test
   public void testDurableSubscriber() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "sub1", "client", getName());

      this.subscribe(connV12, "sub1", "client", getName());

      ClientStompFrame frame = connV12.receiveFrame();
      Assert.assertTrue(frame.getCommand().equals("ERROR"));

      connV12.disconnect();
   }

   @Test
   public void testDurableSubscriberWithReconnection() throws Exception
   {
      connV12.connect(defUser, defPass, "myclientid");

      this.subscribeTopic(connV12, "sub1", "auto", getName());

      ClientStompFrame frame = connV12.createFrame("DISCONNECT");
      frame.addHeader("receipt", "1");

      ClientStompFrame result = connV12.sendFrame(frame);

      if (result == null || (!"RECEIPT".equals(result.getCommand())) || (!"1".equals(result.getHeader("receipt-id"))))
      {
         fail("Disconnect failed! " + result);
      }

      // send the message when the durable subscriber is disconnected
      sendMessage(getName(), topic);

      connV12.destroy();
      connV12 = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      connV12.connect(defUser, defPass, "myclientid");

      this.subscribeTopic(connV12, "sub1", "auto", getName());

      // we must have received the message
      frame = connV12.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertNotNull(frame.getHeader("destination"));
      Assert.assertEquals(getName(), frame.getBody());

      this.unsubscribe(connV12, "sub1");

      connV12.disconnect();
   }

   @Test
   public void testJMSXGroupIdCanBeSet() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV12.connect(defUser, defPass);

      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("JMSXGroupID", "TEST");
      frame.setBody("Hello World");

      connV12.sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      // differ from StompConnect
      Assert.assertEquals("TEST", message.getStringProperty("JMSXGroupID"));
   }

   @Test
   public void testMessagesAreInOrder() throws Exception
   {
      int ctr = 10;
      String[] data = new String[ctr];

      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "sub1", "auto");

      for (int i = 0; i < ctr; ++i)
      {
         data[i] = getName() + i;
         sendMessage(data[i]);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < ctr; ++i)
      {
         frame = connV12.receiveFrame();
         Assert.assertTrue("Message not in order", frame.getBody().equals(data[i]));
      }

      for (int i = 0; i < ctr; ++i)
      {
         data[i] = getName() + ":second:" + i;
         sendMessage(data[i]);
      }

      for (int i = 0; i < ctr; ++i)
      {
         frame = connV12.receiveFrame();
         Assert.assertTrue("Message not in order", frame.getBody().equals(data[i]));
      }

      connV12.disconnect();
   }

   @Test
   public void testSubscribeWithAutoAckAndSelector() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "sub1", "auto", null, "foo = 'zzz'");

      sendMessage("Ignored message", "foo", "1234");
      sendMessage("Real message", "foo", "zzz");

      ClientStompFrame frame = connV12.receiveFrame();

      Assert.assertTrue("Should have received the real message but got: " + frame, frame.getBody().equals("Real message"));

      connV12.disconnect();
   }

   @Test
   public void testRedeliveryWithClientAck() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "subId", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      assertTrue(frame.getCommand().equals("MESSAGE"));

      connV12.disconnect();

      // message should be received since message was not acknowledged
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertTrue(message.getJMSRedelivered());
   }

   @Test
   public void testSendManyMessages() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV12.connect(defUser, defPass);

      int count = 1000;
      final CountDownLatch latch = new CountDownLatch(count);
      consumer.setMessageListener(new MessageListener()
      {
         public void onMessage(Message arg0)
         {
            TextMessage m = (TextMessage) arg0;
            latch.countDown();
            try
            {
               System.out.println("___> latch now: " + latch.getCount() + " m: " + m.getText());
            }
            catch (JMSException e)
            {
               fail("here failed");
               e.printStackTrace();
            }
         }
      });

      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");

      for (int i = 1; i <= count; i++)
      {
         connV12.sendFrame(frame);
      }

      assertTrue(latch.await(60, TimeUnit.SECONDS));

      connV12.disconnect();
   }

   @Test
   public void testSendMessage() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV12.connect(defUser, defPass);

      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");

      connV12.sendFrame(frame);

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
   public void testSendMessageWithContentLength() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV12.connect(defUser, defPass);

      byte[] data = new byte[]{1, 0, 0, 4};

      ClientStompFrame frame = connV12.createFrame("SEND");

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody(new String(data, StandardCharsets.UTF_8));

      frame.addHeader("content-length", String.valueOf(data.length));

      connV12.sendFrame(frame);

      BytesMessage message = (BytesMessage) consumer.receive(10000);
      Assert.assertNotNull(message);

      assertEquals(data.length, message.getBodyLength());
      assertEquals(data[0], message.readByte());
      assertEquals(data[1], message.readByte());
      assertEquals(data[2], message.readByte());
      assertEquals(data[3], message.readByte());
   }

   @Test
   public void testSendMessageWithCustomHeadersAndSelector() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue, "foo = 'abc'");

      connV12.connect(defUser, defPass);

      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("foo", "abc");
      frame.addHeader("bar", "123");

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");

      connV12.sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("bar", "123", message.getStringProperty("bar"));
   }

   @Test
   public void testSendMessageWithLeadingNewLine() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV12.connect(defUser, defPass);

      ClientStompFrame frame = connV12.createFrame("SEND");

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");

      connV12.sendWickedFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());

      // Make sure that the timestamp is valid - should
      // be very close to the current time.
      long tnow = System.currentTimeMillis();
      long tmsg = message.getJMSTimestamp();
      Assert.assertTrue(Math.abs(tnow - tmsg) < 1000);

      assertNull(consumer.receive(1000));

      connV12.disconnect();
   }

   @Test
   public void testSendMessageWithReceipt() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV12.connect(defUser, defPass);

      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("receipt", "1234");
      frame.setBody("Hello World");

      frame = connV12.sendFrame(frame);

      assertTrue(frame.getCommand().equals("RECEIPT"));
      assertEquals("1234", frame.getHeader("receipt-id"));

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());

      // Make sure that the timestamp is valid - should
      // be very close to the current time.
      long tnow = System.currentTimeMillis();
      long tmsg = message.getJMSTimestamp();
      Assert.assertTrue(Math.abs(tnow - tmsg) < 1000);

      connV12.disconnect();
   }

   @Test
   public void testSendMessageWithStandardHeaders() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV12.connect(defUser, defPass);

      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("correlation-id", "c123");
      frame.addHeader("persistent", "true");
      frame.addHeader("priority", "3");
      frame.addHeader("type", "t345");
      frame.addHeader("JMSXGroupID", "abc");
      frame.addHeader("foo", "abc");
      frame.addHeader("bar", "123");

      frame.setBody("Hello World");

      frame = connV12.sendFrame(frame);

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

      connV12.disconnect();
   }

   @Test
   public void testSendMessageWithLongHeaders() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV12.connect(defUser, defPass);

      StringBuffer buffer = new StringBuffer();
      for (int i = 0; i < 2048; i++)
      {
         buffer.append("a");
      }

      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("correlation-id", "c123");
      frame.addHeader("persistent", "true");
      frame.addHeader("priority", "3");
      frame.addHeader("type", "t345");
      frame.addHeader("JMSXGroupID", "abc");
      frame.addHeader("foo", "abc");
      frame.addHeader("very-very-long-stomp-message-header", buffer.toString());

      frame.setBody("Hello World");

      frame = connV12.sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("JMSCorrelationID", "c123", message.getJMSCorrelationID());
      Assert.assertEquals("getJMSType", "t345", message.getJMSType());
      Assert.assertEquals("getJMSPriority", 3, message.getJMSPriority());
      Assert.assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("very-very-long-stomp-message-header", 2048, message.getStringProperty("very-very-long-stomp-message-header").length());

      Assert.assertEquals("JMSXGroupID", "abc", message.getStringProperty("JMSXGroupID"));

      connV12.disconnect();
   }

   @Test
   public void testSubscribeToTopic() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribeTopic(connV12, "sub1", null, null, true);

      sendMessage(getName(), topic);

      ClientStompFrame frame = connV12.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertTrue(frame.getHeader("destination").equals(getTopicPrefix() + getTopicName()));
      Assert.assertTrue(frame.getBody().equals(getName()));

      this.unsubscribe(connV12, "sub1", true);

      sendMessage(getName(), topic);

      frame = connV12.receiveFrame(1000);
      assertNull(frame);

      connV12.disconnect();
   }

   @Test
   public void testSubscribeToTopicWithNoLocal() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribeTopic(connV12, "sub1", null, null, true, true);

      // send a message on the same connection => it should not be received
      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getTopicPrefix() + getTopicName());

      frame.setBody("Hello World");

      connV12.sendFrame(frame);

      frame = connV12.receiveFrame(2000);

      assertNull(frame);

      // send message on another JMS connection => it should be received
      sendMessage(getName(), topic);

      frame = connV12.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertTrue(frame.getHeader("destination").equals(getTopicPrefix() + getTopicName()));
      Assert.assertTrue(frame.getBody().equals(getName()));

      this.unsubscribe(connV12, "sub1");

      connV12.disconnect();
   }

   @Test
   public void testSubscribeWithAutoAck() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "sub1", "auto");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      Assert.assertEquals("MESSAGE", frame.getCommand());
      Assert.assertNotNull(frame.getHeader("destination"));
      Assert.assertEquals(getName(), frame.getBody());

      connV12.disconnect();

      // message should not be received as it was auto-acked
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testSubscribeWithAutoAckAndBytesMessage() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "sub1", "auto");

      byte[] payload = new byte[]{1, 2, 3, 4, 5};
      sendMessage(payload, queue);

      ClientStompFrame frame = connV12.receiveFrame();

      assertEquals("MESSAGE", frame.getCommand());

      System.out.println("Message: " + frame);

      assertEquals("5", frame.getHeader("content-length"));

      assertEquals(null, frame.getHeader("type"));

      assertEquals(frame.getBody(), new String(payload, StandardCharsets.UTF_8));

      connV12.disconnect();
   }

   @Test
   public void testSubscribeWithClientAck() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      ack(connV12, frame);

      connV12.disconnect();

      // message should not be received since message was acknowledged by the client
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testSubscribeWithClientAckThenConsumingAgainWithAutoAckWithExplicitDisconnect() throws Exception
   {
      assertSubscribeWithClientAckThenConsumeWithAutoAck(true);
   }

   @Test
   public void testSubscribeWithClientAckThenConsumingAgainWithAutoAckWithNoDisconnectFrame() throws Exception
   {
      assertSubscribeWithClientAckThenConsumeWithAutoAck(false);
   }

   @Test
   public void testSubscribeWithID() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "mysubid", "auto");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      Assert.assertTrue(frame.getHeader("subscription") != null);

      connV12.disconnect();
   }

   @Test
   public void testSubscribeWithMessageSentWithProperties() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "sub1", "auto");

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

      ClientStompFrame frame = connV12.receiveFrame();
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

      connV12.disconnect();
   }

   @Test
   public void testSuccessiveTransactionsWithSameID() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV12.connect(defUser, defPass);

      // first tx
      this.beginTransaction(connV12, "tx1");

      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("transaction", "tx1");
      frame.setBody("Hello World");

      connV12.sendFrame(frame);

      this.commitTransaction(connV12, "tx1");

      Message message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);

      // 2nd tx with same tx ID
      this.beginTransaction(connV12, "tx1");

      frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("transaction", "tx1");

      frame.setBody("Hello World");

      connV12.sendFrame(frame);

      this.commitTransaction(connV12, "tx1");

      message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);

      connV12.disconnect();
   }

   @Test
   public void testTransactionCommit() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV12.connect(defUser, defPass);

      this.beginTransaction(connV12, "tx1");

      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("transaction", "tx1");
      frame.addHeader("receipt", "123");
      frame.setBody("Hello World");

      frame = connV12.sendFrame(frame);

      assertEquals("123", frame.getHeader("receipt-id"));

      // check the message is not committed
      assertNull(consumer.receive(100));

      this.commitTransaction(connV12, "tx1", true);

      Message message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);

      connV12.disconnect();
   }

   @Test
   public void testTransactionRollback() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV12.connect(defUser, defPass);

      this.beginTransaction(connV12, "tx1");

      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("transaction", "tx1");

      frame.setBody("first message");

      connV12.sendFrame(frame);

      // rollback first message
      this.abortTransaction(connV12, "tx1");

      this.beginTransaction(connV12, "tx1");

      frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("transaction", "tx1");

      frame.setBody("second message");

      connV12.sendFrame(frame);

      this.commitTransaction(connV12, "tx1", true);

      // only second msg should be received since first msg was rolled back
      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("second message", message.getText());

      connV12.disconnect();
   }

   @Test
   public void testUnsubscribe() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "sub1", "auto");

      // send a message to our queue
      sendMessage("first message");

      // receive message from socket
      ClientStompFrame frame = connV12.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));

      // remove suscription
      this.unsubscribe(connV12, "sub1", true);

      // send a message to our queue
      sendMessage("second message");

      frame = connV12.receiveFrame(1000);
      assertNull(frame);

      connV12.disconnect();
   }

   @Test
   public void testDisconnectWithoutUnsubscribe() throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "sub1", "auto");

      // send a message to our queue
      sendMessage("first message");

      // receive message from socket
      ClientStompFrame frame = connV12.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));

      //now disconnect without unsubscribe
      connV12.disconnect();

      // send a message to our queue
      sendMessage("second message");

      //reconnect
      connV12 = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      connV12.connect(defUser, defPass);

      frame = connV12.receiveFrame(1000);
      assertNull("not expected: " + frame, frame);

      //subscribe again.
      this.subscribe(connV12, "sub1", "auto");

      // receive message from socket
      frame = connV12.receiveFrame();

      assertNotNull(frame);
      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));

      frame = connV12.receiveFrame(1000);
      assertNull("not expected: " + frame, frame);

      this.unsubscribe(connV12, "sub1", true);

      frame = connV12.receiveFrame(1000);
      assertNull(frame);

      connV12.disconnect();
   }

   //-----------------private help methods

   private void abortTransaction(StompClientConnection conn, String txID) throws IOException, InterruptedException
   {
      ClientStompFrame abortFrame = conn.createFrame("ABORT");
      abortFrame.addHeader("transaction", txID);

      conn.sendFrame(abortFrame);
   }

   private void beginTransaction(StompClientConnection conn, String txID) throws IOException, InterruptedException
   {
      ClientStompFrame beginFrame = conn.createFrame("BEGIN");
      beginFrame.addHeader("transaction", txID);

      conn.sendFrame(beginFrame);
   }

   private void commitTransaction(StompClientConnection conn, String txID) throws IOException, InterruptedException
   {
      commitTransaction(conn, txID, false);
   }

   private void commitTransaction(StompClientConnection conn, String txID, boolean receipt) throws IOException, InterruptedException
   {
      ClientStompFrame beginFrame = conn.createFrame("COMMIT");
      beginFrame.addHeader("transaction", txID);
      if (receipt)
      {
         beginFrame.addHeader("receipt", "1234");
      }
      ClientStompFrame resp = conn.sendFrame(beginFrame);
      if (receipt)
      {
         assertEquals("1234", resp.getHeader("receipt-id"));
      }
   }

   private void ack(StompClientConnection conn, ClientStompFrame frame) throws IOException, InterruptedException
   {
      String messageID = frame.getHeader("ack");

      ClientStompFrame ackFrame = conn.createFrame("ACK");

      ackFrame.addHeader("id", messageID);

      ClientStompFrame response = conn.sendFrame(ackFrame);
      if (response != null)
      {
         throw new IOException("failed to ack " + response);
      }
   }

   private void ack(StompClientConnection conn, String mid, String txID) throws IOException, InterruptedException
   {
      ClientStompFrame ackFrame = conn.createFrame("ACK");
      ackFrame.addHeader("id", mid);
      if (txID != null)
      {
         ackFrame.addHeader("transaction", txID);
      }

      conn.sendFrame(ackFrame);
   }

   private void nack(StompClientConnection conn, String mid) throws IOException, InterruptedException
   {
      ClientStompFrame ackFrame = conn.createFrame("NACK");
      ackFrame.addHeader("id", mid);

      conn.sendFrame(ackFrame);
   }

   private void subscribe(StompClientConnection conn, String subId, String ack) throws IOException, InterruptedException
   {
      subscribe(conn, subId, ack, null, null);
   }

   private void subscribe(StompClientConnection conn, String subId,
                          String ack, String durableId) throws IOException, InterruptedException
   {
      subscribe(conn, subId, ack, durableId, null);
   }

   private void subscribe(StompClientConnection conn, String subId,
                          String ack, String durableId, boolean receipt) throws IOException, InterruptedException
   {
      subscribe(conn, subId, ack, durableId, null, receipt);
   }

   private void subscribe(StompClientConnection conn, String subId, String ack,
                          String durableId, String selector) throws IOException,
      InterruptedException
   {
      subscribe(conn, subId, ack, durableId, selector, false);
   }

   private void subscribe(StompClientConnection conn, String subId,
                          String ack, String durableId, String selector, boolean receipt) throws IOException, InterruptedException
   {
      ClientStompFrame subFrame = conn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", subId);
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      if (ack != null)
      {
         subFrame.addHeader("ack", ack);
      }
      if (durableId != null)
      {
         subFrame.addHeader("durable-subscriber-name", durableId);
      }
      if (selector != null)
      {
         subFrame.addHeader("selector", selector);
      }
      if (receipt)
      {
         subFrame.addHeader("receipt", "1234");
      }

      subFrame = conn.sendFrame(subFrame);

      if (receipt)
      {
         assertEquals("1234", subFrame.getHeader("receipt-id"));
      }
   }

   private void subscribeTopic(StompClientConnection conn, String subId,
                               String ack, String durableId) throws IOException, InterruptedException
   {
      subscribeTopic(conn, subId, ack, durableId, false);
   }

   private void subscribeTopic(StompClientConnection conn, String subId,
                               String ack, String durableId, boolean receipt) throws IOException, InterruptedException
   {
      subscribeTopic(conn, subId, ack, durableId, receipt, false);
   }

   private void subscribeTopic(StompClientConnection conn, String subId,
                               String ack, String durableId, boolean receipt, boolean noLocal) throws IOException, InterruptedException
   {
      ClientStompFrame subFrame = conn.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", subId);
      subFrame.addHeader("destination", getTopicPrefix() + getTopicName());
      if (ack != null)
      {
         subFrame.addHeader("ack", ack);
      }
      if (durableId != null)
      {
         subFrame.addHeader("durable-subscriber-name", durableId);
      }
      if (receipt)
      {
         subFrame.addHeader("receipt", "1234");
      }
      if (noLocal)
      {
         subFrame.addHeader("no-local", "true");
      }

      ClientStompFrame frame = conn.sendFrame(subFrame);

      if (receipt)
      {
         assertTrue(frame.getHeader("receipt-id").equals("1234"));
      }
   }

   private void unsubscribe(StompClientConnection conn, String subId) throws IOException, InterruptedException
   {
      ClientStompFrame subFrame = conn.createFrame("UNSUBSCRIBE");
      subFrame.addHeader("id", subId);

      conn.sendFrame(subFrame);
   }

   private void unsubscribe(StompClientConnection conn, String subId,
                            boolean receipt) throws IOException, InterruptedException
   {
      ClientStompFrame subFrame = conn.createFrame("UNSUBSCRIBE");
      subFrame.addHeader("id", subId);

      if (receipt)
      {
         subFrame.addHeader("receipt", "4321");
      }

      ClientStompFrame f = conn.sendFrame(subFrame);

      if (receipt)
      {
         System.out.println("response: " + f);
         assertEquals("RECEIPT", f.getCommand());
         assertEquals("4321", f.getHeader("receipt-id"));
      }
   }

   protected void assertSubscribeWithClientAckThenConsumeWithAutoAck(boolean sendDisconnect) throws Exception
   {
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV12.receiveFrame();

      Assert.assertEquals("MESSAGE", frame.getCommand());

      log.info("Reconnecting!");

      if (sendDisconnect)
      {
         connV12.disconnect();
         connV12 = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      }
      else
      {
         connV12.destroy();
         connV12 = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      }

      // message should be received since message was not acknowledged
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "sub1", null);

      frame = connV12.receiveFrame();
      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));

      connV12.disconnect();

      // now let's make sure we don't see the message again
      connV12.destroy();
      connV12 = (StompClientConnectionV12) StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      connV12.connect(defUser, defPass);

      this.subscribe(connV12, "sub1", null, null, true);

      sendMessage("shouldBeNextMessage");

      frame = connV12.receiveFrame();
      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertEquals("shouldBeNextMessage", frame.getBody());
   }

   @Test
   public void testSendMessageToNonExistentJmsQueue() throws Exception
   {
      connV12.connect(defUser, defPass);

      ClientStompFrame frame = connV12.createFrame("SEND");
      String guid = UUID.randomUUID().toString();
      frame.addHeader("destination", "jms.queue.NonExistentQueue" + guid);
      frame.addHeader("receipt", "1234");
      frame.setBody("Hello World");

      frame = connV12.sendFrame(frame);

      assertTrue(frame.getCommand().equals("ERROR"));
      assertEquals("1234", frame.getHeader("receipt-id"));
      System.out.println("message: " + frame.getHeader("message"));

      connV12.disconnect();
   }

   @Test
   public void testInvalidStompCommand() throws Exception
   {
      try
      {
         connV12.connect(defUser, defPass);

         ClientStompFrame frame = connV12.createAnyFrame("INVALID");
         frame.setBody("Hello World");
         frame.addHeader("receipt", "1234");//make the client receives this reply.

         frame = connV12.sendFrame(frame);

         assertTrue(frame.getCommand().equals("ERROR"));
      }
      finally
      {
         //because the last frame is ERROR, the connection
         //might already have closed by the server.
         //this is expected so we ignore it.
         connV12.destroy();
      }
   }

   @Test
   public void testSendAndReceiveWithEscapedCharactersInSenderId() throws Exception
   {
      connV12.connect(defUser, defPass);
      ClientStompFrame frame = connV12.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World 1!");

      ClientStompFrame response = connV12.sendFrame(frame);
      assertNull(response);

      //subscribe
      ClientStompFrame subFrame = connV12.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "ID\\cMYMACHINE-50616-635482262727823605-1\\c1\\c1\\c1");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      connV12.sendFrame(subFrame);

      frame = connV12.receiveFrame();

      System.out.println("Received: " + frame);

      assertEquals("MESSAGE", frame.getCommand());
      assertEquals("ID:MYMACHINE-50616-635482262727823605-1:1:1:1", frame.getHeader("subscription"));
      assertNotNull(frame.getHeader("message-id"));
      assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader("destination"));
      assertEquals("Hello World 1!", frame.getBody());

      //unsub
      ClientStompFrame unsubFrame = connV12.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "ID\\cMYMACHINE-50616-635482262727823605-1\\c1\\c1\\c1");
      connV12.sendFrame(unsubFrame);

      connV12.disconnect();
   }
}






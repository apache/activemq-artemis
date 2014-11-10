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
package org.hornetq.tests.integration.stomp.v11;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
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

import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.integration.stomp.util.ClientStompFrame;
import org.hornetq.tests.integration.stomp.util.StompClientConnection;
import org.hornetq.tests.integration.stomp.util.StompClientConnectionFactory;
import org.hornetq.tests.integration.stomp.util.StompClientConnectionV11;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/*
 *
 */
public class StompV11Test extends StompV11TestBase
{
   private static final transient IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private StompClientConnection connV11;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      try
      {
         log.debug("Connection 11 : " + connV11.isConnected());
         if (connV11 != null && connV11.isConnected())
         {
            connV11.disconnect();
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
      server.getHornetQServer().getConfiguration().setSecurityEnabled(true);
      StompClientConnection connection = StompClientConnectionFactory.createClientConnection("1.0", hostname, port);

      connection.connect(defUser, defPass);

      assertTrue(connection.isConnected());

      assertEquals("1.0", connection.getVersion());

      connection.disconnect();

      connection = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);

      connection.connect(defUser, defPass);

      assertTrue(connection.isConnected());

      assertEquals("1.1", connection.getVersion());

      connection.disconnect();

      connection = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);

      connection.connect();

      assertFalse(connection.isConnected());

      //new way of connection
      StompClientConnectionV11 conn = (StompClientConnectionV11) StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      conn.connect1(defUser, defPass);

      assertTrue(conn.isConnected());

      conn.disconnect();

      //invalid user
      conn = (StompClientConnectionV11) StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      ClientStompFrame frame = conn.connect("invaliduser", defPass);
      assertFalse(conn.isConnected());
      assertTrue("ERROR".equals(frame.getCommand()));
      assertTrue(frame.getBody().contains("The login account is not valid."));
   }

   @Test
   public void testNegotiation() throws Exception
   {
      // case 1 accept-version absent. It is a 1.0 connect
      ClientStompFrame frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);

      ClientStompFrame reply = connV11.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      //reply headers: version, session, server
      assertEquals(null, reply.getHeader("version"));

      connV11.disconnect();

      // case 2 accept-version=1.0, result: 1.0
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("accept-version", "1.0");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);

      reply = connV11.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      //reply headers: version, session, server
      assertEquals("1.0", reply.getHeader("version"));

      connV11.disconnect();

      // case 3 accept-version=1.1, result: 1.1
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("accept-version", "1.1");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);

      reply = connV11.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      //reply headers: version, session, server
      assertEquals("1.1", reply.getHeader("version"));

      connV11.disconnect();

      // case 4 accept-version=1.0,1.1,1.2, result 1.1
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("accept-version", "1.0,1.1,1.3");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);

      reply = connV11.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      //reply headers: version, session, server
      assertEquals("1.1", reply.getHeader("version"));

      connV11.disconnect();

      // case 5 accept-version=1.2, result error
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("accept-version", "1.3");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);

      reply = connV11.sendFrame(frame);

      assertEquals("ERROR", reply.getCommand());

      System.out.println("Got error frame " + reply);

   }

   @Test
   public void testSendAndReceive() throws Exception
   {
      connV11.connect(defUser, defPass);
      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World 1!");

      ClientStompFrame response = connV11.sendFrame(frame);

      assertNull(response);

      frame.addHeader("receipt", "1234");
      frame.setBody("Hello World 2!");

      response = connV11.sendFrame(frame);

      assertNotNull(response);

      assertEquals("RECEIPT", response.getCommand());

      assertEquals("1234", response.getHeader("receipt-id"));

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

      assertEquals("a-sub", frame.getHeader("subscription"));

      assertNotNull(frame.getHeader("message-id"));

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
      connV11.connect(defUser, defPass);
      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "application/xml");
      frame.setBody("Hello World 1!");

      connV11.sendFrame(frame);

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
      connV11.connect(defUser, defPass);
      ClientStompFrame frame = connV11.createFrame("SEND");

      String body = "Hello World 1!";
      String cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "application/xml");
      frame.addHeader("content-length", cLen);
      frame.setBody(body + "extra");

      connV11.sendFrame(frame);

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

      assertEquals(cLen, frame.getHeader("content-length"));

      //unsub
      ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");

      newConn.disconnect();
   }

   @Test
   public void testHeaderEncoding() throws Exception
   {
      connV11.connect(defUser, defPass);
      ClientStompFrame frame = connV11.createFrame("SEND");

      String body = "Hello World 1!";
      String cLen = String.valueOf(body.getBytes(StandardCharsets.UTF_8).length);

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "application/xml");
      frame.addHeader("content-length", cLen);
      String hKey = "special-header\\\\\\n\\c";
      String hVal = "\\c\\\\\\ngood";
      frame.addHeader(hKey, hVal);

      System.out.println("key: |" + hKey + "| val: |" + hVal + "|");

      frame.setBody(body);

      connV11.sendFrame(frame);

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

      String value = frame.getHeader("special-header" + "\\" + "\n" + ":");

      assertEquals(":" + "\\" + "\n" + "good", value);

      //unsub
      ClientStompFrame unsubFrame = newConn.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");

      newConn.disconnect();
   }

   @Test
   public void testHeartBeat() throws Exception
   {
      //no heart beat at all if heat-beat absent
      ClientStompFrame frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);

      ClientStompFrame reply = connV11.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      Thread.sleep(5000);

      assertEquals(0, connV11.getFrameQueueSize());

      connV11.disconnect();

      //no heart beat for (0,0)
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "0,0");
      frame.addHeader("accept-version", "1.0,1.1");

      reply = connV11.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      assertEquals("0,0", reply.getHeader("heart-beat"));

      Thread.sleep(5000);

      assertEquals(0, connV11.getFrameQueueSize());

      connV11.disconnect();

      //heart-beat (1,0), should receive a min client ping accepted by server
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "1,0");
      frame.addHeader("accept-version", "1.0,1.1");

      reply = connV11.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      assertEquals("0,500", reply.getHeader("heart-beat"));

      Thread.sleep(2000);

      //now server side should be disconnected because we didn't send ping for 2 sec
      frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World");

      //send will fail
      try
      {
         connV11.sendFrame(frame);
         fail("connection should have been destroyed by now");
      }
      catch (IOException e)
      {
         //ignore
      }

      //heart-beat (1,0), start a ping, then send a message, should be ok.
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "1,0");
      frame.addHeader("accept-version", "1.0,1.1");

      reply = connV11.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      assertEquals("0,500", reply.getHeader("heart-beat"));

      System.out.println("========== start pinger!");

      connV11.startPinger(500);

      Thread.sleep(2000);

      //now server side should be disconnected because we didn't send ping for 2 sec
      frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World");

      //send will be ok
      connV11.sendFrame(frame);

      connV11.stopPinger();

      connV11.disconnect();

   }

   //server ping
   @Test
   public void testHeartBeat2() throws Exception
   {
      //heart-beat (1,1)
      ClientStompFrame frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "1,1");
      frame.addHeader("accept-version", "1.0,1.1");

      ClientStompFrame reply = connV11.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());
      assertEquals("500,500", reply.getHeader("heart-beat"));

      connV11.disconnect();

      //heart-beat (500,1000)
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      frame = connV11.createFrame("CONNECT");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      frame.addHeader("heart-beat", "500,1000");
      frame.addHeader("accept-version", "1.0,1.1");

      reply = connV11.sendFrame(frame);

      assertEquals("CONNECTED", reply.getCommand());

      assertEquals("1000,500", reply.getHeader("heart-beat"));

      System.out.println("========== start pinger!");

      connV11.startPinger(500);

      Thread.sleep(10000);

      //now check the frame size
      int size = connV11.getServerPingNumber();

      System.out.println("ping received: " + size);

      assertTrue(size > 5);

      //now server side should be disconnected because we didn't send ping for 2 sec
      frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World");

      //send will be ok
      connV11.sendFrame(frame);

      connV11.disconnect();
   }

   @Test
   public void testSendWithHeartBeatsAndReceive() throws Exception
   {
      StompClientConnection newConn = null;
      try
      {
         ClientStompFrame frame = connV11.createFrame("CONNECT");
         frame.addHeader("host", "127.0.0.1");
         frame.addHeader("login", this.defUser);
         frame.addHeader("passcode", this.defPass);
         frame.addHeader("heart-beat", "500,1000");
         frame.addHeader("accept-version", "1.0,1.1");

         connV11.sendFrame(frame);

         connV11.startPinger(500);

         frame = connV11.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("content-type", "text/plain");

         for (int i = 0; i < 10; i++)
         {
            frame.setBody("Hello World " + i + "!");
            connV11.sendFrame(frame);
            Thread.sleep(500);
         }

         // subscribe
         newConn = StompClientConnectionFactory.createClientConnection("1.1",
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
         connV11.disconnect();
      }
   }

   @Test
   public void testSendAndReceiveWithHeartBeats() throws Exception
   {
      connV11.connect(defUser, defPass);
      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");

      for (int i = 0; i < 10; i++)
      {
         frame.setBody("Hello World " + i + "!");
         connV11.sendFrame(frame);
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
         ClientStompFrame frame = connV11.createFrame("CONNECT");
         frame.addHeader("host", "127.0.0.1");
         frame.addHeader("login", this.defUser);
         frame.addHeader("passcode", this.defPass);
         frame.addHeader("heart-beat", "500,1000");
         frame.addHeader("accept-version", "1.0,1.1");

         connV11.sendFrame(frame);

         connV11.startPinger(500);

         frame = connV11.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("content-type", "text/plain");

         for (int i = 0; i < 10; i++)
         {
            frame.setBody("Hello World " + i + "!");
            connV11.sendFrame(frame);
            Thread.sleep(500);
         }

         // subscribe
         newConn = StompClientConnectionFactory.createClientConnection("1.1",
                                                                       hostname, port);
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
         if (newConn != null)
            newConn.disconnect();
         connV11.disconnect();
      }
   }

   @Test
   public void testNack() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");

      nack(connV11, "sub1", messageID);

      unsubscribe(connV11, "sub1");

      connV11.disconnect();

      //Nack makes the message be dropped.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testNackWithWrongSubId() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");

      nack(connV11, "sub2", messageID);

      ClientStompFrame error = connV11.receiveFrame();

      System.out.println("Receiver error: " + error);

      unsubscribe(connV11, "sub1");

      connV11.disconnect();

      //message should be still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testNackWithWrongMessageId() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      frame.getHeader("message-id");

      nack(connV11, "sub2", "someother");

      ClientStompFrame error = connV11.receiveFrame();

      System.out.println("Receiver error: " + error);

      unsubscribe(connV11, "sub1");

      connV11.disconnect();

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }


   @Test
   public void testAck() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");


      ack(connV11, "sub1", messageID, null);

      unsubscribe(connV11, "sub1");

      connV11.disconnect();

      //Nack makes the message be dropped.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testAckWithWrongSubId() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");


      ack(connV11, "sub2", messageID, null);

      ClientStompFrame error = connV11.receiveFrame();

      System.out.println("Receiver error: " + error);

      unsubscribe(connV11, "sub1");

      connV11.disconnect();

      //message should be still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testAckWithWrongMessageId() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      frame.getHeader("message-id");

      ack(connV11, "sub2", "someother", null);

      ClientStompFrame error = connV11.receiveFrame();

      System.out.println("Receiver error: " + error);

      unsubscribe(connV11, "sub1");

      connV11.disconnect();

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testErrorWithReceipt() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");


      ClientStompFrame ackFrame = connV11.createFrame("ACK");
      //give it a wrong sub id
      ackFrame.addHeader("subscription", "sub2");
      ackFrame.addHeader("message-id", messageID);
      ackFrame.addHeader("receipt", "answer-me");

      ClientStompFrame error = connV11.sendFrame(ackFrame);

      System.out.println("Receiver error: " + error);

      assertEquals("ERROR", error.getCommand());

      assertEquals("answer-me", error.getHeader("receipt-id"));

      unsubscribe(connV11, "sub1");

      connV11.disconnect();

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testErrorWithReceipt2() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      String messageID = frame.getHeader("message-id");


      ClientStompFrame ackFrame = connV11.createFrame("ACK");
      //give it a wrong sub id
      ackFrame.addHeader("subscription", "sub1");
      ackFrame.addHeader("message-id", String.valueOf(Long.valueOf(messageID) + 1));
      ackFrame.addHeader("receipt", "answer-me");

      ClientStompFrame error = connV11.sendFrame(ackFrame);

      System.out.println("Receiver error: " + error);

      assertEquals("ERROR", error.getCommand());

      assertEquals("answer-me", error.getHeader("receipt-id"));

      unsubscribe(connV11, "sub1");

      connV11.disconnect();

      //message should still there
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testAckModeClient() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("client-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++)
      {
         frame = connV11.receiveFrame();
         assertNotNull(frame);
      }

      //ack the last
      this.ack(connV11, "sub1", frame);

      unsubscribe(connV11, "sub1");

      connV11.disconnect();

      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testAckModeClient2() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client");

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("client-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++)
      {
         frame = connV11.receiveFrame();
         assertNotNull(frame);

         //ack the 49th
         if (i == num - 2)
         {
            this.ack(connV11, "sub1", frame);
         }
      }

      unsubscribe(connV11, "sub1");

      connV11.disconnect();

      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
      message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testAckModeAuto() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "auto");

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("auto-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++)
      {
         frame = connV11.receiveFrame();
         assertNotNull(frame);
      }

      unsubscribe(connV11, "sub1");

      connV11.disconnect();

      //no messages can be received.
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testAckModeClientIndividual() throws Exception
   {
      connV11.connect(defUser, defPass);

      subscribe(connV11, "sub1", "client-individual");

      int num = 50;
      //send a bunch of messages
      for (int i = 0; i < num; i++)
      {
         this.sendMessage("client-individual-ack" + i);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < num; i++)
      {
         frame = connV11.receiveFrame();
         assertNotNull(frame);

         System.out.println(i + " == received: " + frame);
         //ack on even numbers
         if (i % 2 == 0)
         {
            this.ack(connV11, "sub1", frame);
         }
      }

      unsubscribe(connV11, "sub1");

      connV11.disconnect();

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
      connV11.connect(defUser, defPass, "myclientid");

      this.subscribeTopic(connV11, "sub1", "auto", null);

      StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      newConn.connect(defUser, defPass, "myclientid2");

      this.subscribeTopic(newConn, "sub2", "auto", null);

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getTopicPrefix() + getTopicName());

      frame.setBody("Hello World");

      connV11.sendFrame(frame);

      // receive message from socket
      frame = connV11.receiveFrame(1000);

      System.out.println("received frame : " + frame);
      assertEquals("Hello World", frame.getBody());
      assertEquals("sub1", frame.getHeader("subscription"));

      frame = newConn.receiveFrame(1000);

      System.out.println("received 2 frame : " + frame);
      assertEquals("Hello World", frame.getBody());
      assertEquals("sub2", frame.getHeader("subscription"));

      // remove suscription
      this.unsubscribe(connV11, "sub1", true);
      this.unsubscribe(newConn, "sub2", true);

      connV11.disconnect();
      newConn.disconnect();
   }

   @Test
   public void testSendAndReceiveOnDifferentConnections() throws Exception
   {
      connV11.connect(defUser, defPass);

      ClientStompFrame sendFrame = connV11.createFrame("SEND");
      sendFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      sendFrame.setBody("Hello World");

      connV11.sendFrame(sendFrame);

      StompClientConnection connV11_2 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      connV11_2.connect(defUser, defPass);

      this.subscribe(connV11_2, "sub1", "auto");

      ClientStompFrame frame = connV11_2.receiveFrame(2000);

      assertEquals("MESSAGE", frame.getCommand());
      assertEquals("Hello World", frame.getBody());

      connV11.disconnect();
      connV11_2.disconnect();
   }

   //----------------Note: tests below are adapted from StompTest

   @Test
   public void testBeginSameTransactionTwice() throws Exception
   {
      connV11.connect(defUser, defPass);

      beginTransaction(connV11, "tx1");

      beginTransaction(connV11, "tx1");

      ClientStompFrame f = connV11.receiveFrame();
      Assert.assertTrue(f.getCommand().equals("ERROR"));
   }

   @Test
   public void testBodyWithUTF8() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, getName(), "auto");

      String text = "A" + "\u00ea" + "\u00f1" + "\u00fc" + "C";
      System.out.println(text);
      sendMessage(text);

      ClientStompFrame frame = connV11.receiveFrame();
      System.out.println(frame);
      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertNotNull(frame.getHeader("destination"));
      Assert.assertTrue(frame.getBody().equals(text));

      connV11.disconnect();
   }

   @Test
   public void testClientAckNotPartOfTransaction() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, getName(), "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertNotNull(frame.getHeader("destination"));
      Assert.assertTrue(frame.getBody().equals(getName()));
      Assert.assertNotNull(frame.getHeader("message-id"));

      String messageID = frame.getHeader("message-id");

      beginTransaction(connV11, "tx1");

      this.ack(connV11, getName(), messageID, "tx1");

      abortTransaction(connV11, "tx1");

      frame = connV11.receiveFrame(500);

      assertNull(frame);

      this.unsubscribe(connV11, getName());

      connV11.disconnect();
   }

   @Test
   public void testDisconnectAndError() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, getName(), "client");

      ClientStompFrame frame = connV11.createFrame("DISCONNECT");
      frame.addHeader("receipt", "1");

      ClientStompFrame result = connV11.sendFrame(frame);

      if (result == null || (!"RECEIPT".equals(result.getCommand())) || (!"1".equals(result.getHeader("receipt-id"))))
      {
         fail("Disconnect failed! " + result);
      }

      final CountDownLatch latch = new CountDownLatch(1);

      Thread thr = new Thread()
      {
         public void run()
         {
            ClientStompFrame sendFrame = connV11.createFrame("SEND");
            sendFrame.addHeader("destination", getQueuePrefix() + getQueueName());
            sendFrame.setBody("Hello World");
            while (latch.getCount() != 0)
            {
               try
               {
                  connV11.sendFrame(sendFrame);
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
                  connV11.destroy();
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
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "sub1", "client", getName());

      this.subscribe(connV11, "sub1", "client", getName());

      ClientStompFrame frame = connV11.receiveFrame();
      Assert.assertTrue(frame.getCommand().equals("ERROR"));

      connV11.disconnect();
   }

   @Test
   public void testDurableSubscriberWithReconnection() throws Exception
   {
      connV11.connect(defUser, defPass, "myclientid");

      this.subscribeTopic(connV11, "sub1", "auto", getName());

      ClientStompFrame frame = connV11.createFrame("DISCONNECT");
      frame.addHeader("receipt", "1");

      ClientStompFrame result = connV11.sendFrame(frame);

      if (result == null || (!"RECEIPT".equals(result.getCommand())) || (!"1".equals(result.getHeader("receipt-id"))))
      {
         fail("Disconnect failed! " + result);
      }

      // send the message when the durable subscriber is disconnected
      sendMessage(getName(), topic);

      connV11.destroy();
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      connV11.connect(defUser, defPass, "myclientid");

      this.subscribeTopic(connV11, "sub1", "auto", getName());

      // we must have received the message
      frame = connV11.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertNotNull(frame.getHeader("destination"));
      Assert.assertEquals(getName(), frame.getBody());

      this.unsubscribe(connV11, "sub1");

      connV11.disconnect();
   }

   @Test
   public void testJMSXGroupIdCanBeSet() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("JMSXGroupID", "TEST");
      frame.setBody("Hello World");

      connV11.sendFrame(frame);

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

      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "sub1", "auto");

      for (int i = 0; i < ctr; ++i)
      {
         data[i] = getName() + i;
         sendMessage(data[i]);
      }

      ClientStompFrame frame = null;

      for (int i = 0; i < ctr; ++i)
      {
         frame = connV11.receiveFrame();
         Assert.assertTrue("Message not in order", frame.getBody().equals(data[i]));
      }

      for (int i = 0; i < ctr; ++i)
      {
         data[i] = getName() + ":second:" + i;
         sendMessage(data[i]);
      }

      for (int i = 0; i < ctr; ++i)
      {
         frame = connV11.receiveFrame();
         Assert.assertTrue("Message not in order", frame.getBody().equals(data[i]));
      }

      connV11.disconnect();
   }

   @Test
   public void testSubscribeWithAutoAckAndSelector() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "sub1", "auto", null, "foo = 'zzz'");

      sendMessage("Ignored message", "foo", "1234");
      sendMessage("Real message", "foo", "zzz");

      ClientStompFrame frame = connV11.receiveFrame();

      Assert.assertTrue("Should have received the real message but got: " + frame, frame.getBody().equals("Real message"));

      connV11.disconnect();
   }

   @Test
   public void testRedeliveryWithClientAck() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "subId", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      assertTrue(frame.getCommand().equals("MESSAGE"));

      connV11.disconnect();

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

      connV11.connect(defUser, defPass);

      int count = 1000;
      final CountDownLatch latch = new CountDownLatch(count);
      consumer.setMessageListener(new MessageListener()
      {
         public void onMessage(Message arg0)
         {
            latch.countDown();
         }
      });

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");

      for (int i = 1; i <= count; i++)
      {
         connV11.sendFrame(frame);
      }

      assertTrue(latch.await(60, TimeUnit.SECONDS));

      connV11.disconnect();
   }

   @Test
   public void testSendMessage() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");

      connV11.sendFrame(frame);

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

      connV11.connect(defUser, defPass);

      byte[] data = new byte[]{1, 0, 0, 4};

      ClientStompFrame frame = connV11.createFrame("SEND");

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody(new String(data, StandardCharsets.UTF_8));

      frame.addHeader("content-length", String.valueOf(data.length));

      connV11.sendFrame(frame);

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

      connV11.connect(defUser, defPass);

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("foo", "abc");
      frame.addHeader("bar", "123");

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");

      connV11.sendFrame(frame);

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

      connV11.connect(defUser, defPass);

      ClientStompFrame frame = connV11.createFrame("SEND");

      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");

      connV11.sendWickedFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());

      // Make sure that the timestamp is valid - should
      // be very close to the current time.
      long tnow = System.currentTimeMillis();
      long tmsg = message.getJMSTimestamp();
      Assert.assertTrue(Math.abs(tnow - tmsg) < 1000);

      assertNull(consumer.receive(1000));

      connV11.disconnect();
   }

   @Test
   public void testSendMessageWithReceipt() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("receipt", "1234");
      frame.setBody("Hello World");

      frame = connV11.sendFrame(frame);

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

      connV11.disconnect();
   }

   @Test
   public void testSendMessageWithStandardHeaders() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("correlation-id", "c123");
      frame.addHeader("persistent", "true");
      frame.addHeader("priority", "3");
      frame.addHeader("type", "t345");
      frame.addHeader("JMSXGroupID", "abc");
      frame.addHeader("foo", "abc");
      frame.addHeader("bar", "123");

      frame.setBody("Hello World");

      frame = connV11.sendFrame(frame);

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

      connV11.disconnect();
   }

   @Test
   public void testSendMessageWithLongHeaders() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);

      StringBuffer buffer = new StringBuffer();
      for (int i = 0; i < 2048; i++)
      {
         buffer.append("a");
      }

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("correlation-id", "c123");
      frame.addHeader("persistent", "true");
      frame.addHeader("priority", "3");
      frame.addHeader("type", "t345");
      frame.addHeader("JMSXGroupID", "abc");
      frame.addHeader("foo", "abc");
      frame.addHeader("longHeader", buffer.toString());

      frame.setBody("Hello World");

      frame = connV11.sendFrame(frame);

      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("Hello World", message.getText());
      Assert.assertEquals("JMSCorrelationID", "c123", message.getJMSCorrelationID());
      Assert.assertEquals("getJMSType", "t345", message.getJMSType());
      Assert.assertEquals("getJMSPriority", 3, message.getJMSPriority());
      Assert.assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
      Assert.assertEquals("foo", "abc", message.getStringProperty("foo"));
      Assert.assertEquals("longHeader", 2048, message.getStringProperty("longHeader").length());

      Assert.assertEquals("JMSXGroupID", "abc", message.getStringProperty("JMSXGroupID"));

      connV11.disconnect();
   }

   @Test
   public void testSubscribeToTopic() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribeTopic(connV11, "sub1", null, null, true);

      sendMessage(getName(), topic);

      ClientStompFrame frame = connV11.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertTrue(frame.getHeader("destination").equals(getTopicPrefix() + getTopicName()));
      Assert.assertTrue(frame.getBody().equals(getName()));

      this.unsubscribe(connV11, "sub1", true);

      sendMessage(getName(), topic);

      frame = connV11.receiveFrame(1000);
      assertNull(frame);

      connV11.disconnect();
   }

   @Test
   public void testSubscribeToTopicWithNoLocal() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribeTopic(connV11, "sub1", null, null, true, true);

      // send a message on the same connection => it should not be received
      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getTopicPrefix() + getTopicName());

      frame.setBody("Hello World");

      connV11.sendFrame(frame);

      frame = connV11.receiveFrame(2000);

      assertNull(frame);

      // send message on another JMS connection => it should be received
      sendMessage(getName(), topic);

      frame = connV11.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertTrue(frame.getHeader("destination").equals(getTopicPrefix() + getTopicName()));
      Assert.assertTrue(frame.getBody().equals(getName()));

      this.unsubscribe(connV11, "sub1");

      connV11.disconnect();
   }

   @Test
   public void testSubscribeWithAutoAck() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "sub1", "auto");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      Assert.assertEquals("MESSAGE", frame.getCommand());
      Assert.assertNotNull(frame.getHeader("destination"));
      Assert.assertEquals(getName(), frame.getBody());

      connV11.disconnect();

      // message should not be received as it was auto-acked
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNull(message);
   }

   @Test
   public void testSubscribeWithAutoAckAndBytesMessage() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "sub1", "auto");

      byte[] payload = new byte[]{1, 2, 3, 4, 5};
      sendMessage(payload, queue);

      ClientStompFrame frame = connV11.receiveFrame();

      assertEquals("MESSAGE", frame.getCommand());

      System.out.println("Message: " + frame);

      assertEquals("5", frame.getHeader("content-length"));

      assertEquals(null, frame.getHeader("type"));

      assertEquals(frame.getBody(), new String(payload, StandardCharsets.UTF_8));

      connV11.disconnect();
   }

   @Test
   public void testSubscribeWithClientAck() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      this.ack(connV11, "sub1", frame);

      connV11.disconnect();

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
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "mysubid", "auto");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      Assert.assertTrue(frame.getHeader("subscription") != null);

      connV11.disconnect();
   }

   @Test
   public void testSubscribeWithMessageSentWithProperties() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "sub1", "auto");

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

      ClientStompFrame frame = connV11.receiveFrame();
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

      connV11.disconnect();
   }

   @Test
   public void testSuccessiveTransactionsWithSameID() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);

      // first tx
      this.beginTransaction(connV11, "tx1");

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("transaction", "tx1");
      frame.setBody("Hello World");

      connV11.sendFrame(frame);

      this.commitTransaction(connV11, "tx1");

      Message message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);

      // 2nd tx with same tx ID
      this.beginTransaction(connV11, "tx1");

      frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("transaction", "tx1");

      frame.setBody("Hello World");

      connV11.sendFrame(frame);

      this.commitTransaction(connV11, "tx1");

      message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);

      connV11.disconnect();
   }

   @Test
   public void testTransactionCommit() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);

      this.beginTransaction(connV11, "tx1");

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("transaction", "tx1");
      frame.addHeader("receipt", "123");
      frame.setBody("Hello World");

      frame = connV11.sendFrame(frame);

      assertEquals("123", frame.getHeader("receipt-id"));

      // check the message is not committed
      assertNull(consumer.receive(100));

      this.commitTransaction(connV11, "tx1", true);

      Message message = consumer.receive(1000);
      Assert.assertNotNull("Should have received a message", message);

      connV11.disconnect();
   }

   @Test
   public void testTransactionRollback() throws Exception
   {
      MessageConsumer consumer = session.createConsumer(queue);

      connV11.connect(defUser, defPass);

      this.beginTransaction(connV11, "tx1");

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("transaction", "tx1");

      frame.setBody("first message");

      connV11.sendFrame(frame);

      // rollback first message
      this.abortTransaction(connV11, "tx1");

      this.beginTransaction(connV11, "tx1");

      frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("transaction", "tx1");

      frame.setBody("second message");

      connV11.sendFrame(frame);

      this.commitTransaction(connV11, "tx1", true);

      // only second msg should be received since first msg was rolled back
      TextMessage message = (TextMessage) consumer.receive(1000);
      Assert.assertNotNull(message);
      Assert.assertEquals("second message", message.getText());

      connV11.disconnect();
   }

   @Test
   public void testUnsubscribe() throws Exception
   {
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "sub1", "auto");

      // send a message to our queue
      sendMessage("first message");

      // receive message from socket
      ClientStompFrame frame = connV11.receiveFrame();

      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));

      // remove suscription
      this.unsubscribe(connV11, "sub1", true);

      // send a message to our queue
      sendMessage("second message");

      frame = connV11.receiveFrame(1000);
      assertNull(frame);

      connV11.disconnect();
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

   private void ack(StompClientConnection conn, String subId,
                    ClientStompFrame frame) throws IOException, InterruptedException
   {
      String messageID = frame.getHeader("message-id");

      ClientStompFrame ackFrame = conn.createFrame("ACK");

      ackFrame.addHeader("subscription", subId);
      ackFrame.addHeader("message-id", messageID);

      ClientStompFrame response = conn.sendFrame(ackFrame);
      if (response != null)
      {
         throw new IOException("failed to ack " + response);
      }
   }

   private void ack(StompClientConnection conn, String subId, String mid, String txID) throws IOException, InterruptedException
   {
      ClientStompFrame ackFrame = conn.createFrame("ACK");
      ackFrame.addHeader("subscription", subId);
      ackFrame.addHeader("message-id", mid);
      if (txID != null)
      {
         ackFrame.addHeader("transaction", txID);
      }

      conn.sendFrame(ackFrame);
   }

   private void nack(StompClientConnection conn, String subId, String mid) throws IOException, InterruptedException
   {
      ClientStompFrame ackFrame = conn.createFrame("NACK");
      ackFrame.addHeader("subscription", subId);
      ackFrame.addHeader("message-id", mid);

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
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "sub1", "client");

      sendMessage(getName());

      ClientStompFrame frame = connV11.receiveFrame();

      Assert.assertEquals("MESSAGE", frame.getCommand());

      log.info("Reconnecting!");

      if (sendDisconnect)
      {
         connV11.disconnect();
         connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      }
      else
      {
         connV11.destroy();
         connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      }

      // message should be received since message was not acknowledged
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "sub1", null);

      frame = connV11.receiveFrame();
      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));

      connV11.disconnect();

      // now let's make sure we don't see the message again
      connV11.destroy();
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      connV11.connect(defUser, defPass);

      this.subscribe(connV11, "sub1", null, null, true);

      sendMessage("shouldBeNextMessage");

      frame = connV11.receiveFrame();
      Assert.assertTrue(frame.getCommand().equals("MESSAGE"));
      Assert.assertEquals("shouldBeNextMessage", frame.getBody());
   }

   @Test
   public void testSendMessageToNonExistentJmsQueue() throws Exception
   {
      connV11.connect(defUser, defPass);

      ClientStompFrame frame = connV11.createFrame("SEND");
      String guid = UUID.randomUUID().toString();
      frame.addHeader("destination", "jms.queue.NonExistentQueue" + guid);
      frame.addHeader("receipt", "1234");
      frame.setBody("Hello World");

      frame = connV11.sendFrame(frame);

      assertTrue(frame.getCommand().equals("ERROR"));
      assertEquals("1234", frame.getHeader("receipt-id"));
      System.out.println("message: " + frame.getHeader("message"));

      connV11.disconnect();
   }

   @Test
   public void testSendAndReceiveWithEscapedCharactersInSenderId() throws Exception
   {
      connV11.connect(defUser, defPass);
      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-type", "text/plain");
      frame.setBody("Hello World 1!");

      ClientStompFrame response = connV11.sendFrame(frame);
      assertNull(response);

      //subscribe
      ClientStompFrame subFrame = connV11.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "ID\\cMYMACHINE-50616-635482262727823605-1\\c1\\c1\\c1");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      connV11.sendFrame(subFrame);

      frame = connV11.receiveFrame();

      System.out.println("Received: " + frame);

      assertEquals("MESSAGE", frame.getCommand());
      assertEquals("ID:MYMACHINE-50616-635482262727823605-1:1:1:1", frame.getHeader("subscription"));
      assertNotNull(frame.getHeader("message-id"));
      assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader("destination"));
      assertEquals("Hello World 1!", frame.getBody());

      //unsub
      ClientStompFrame unsubFrame = connV11.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "ID\\cMYMACHINE-50616-635482262727823605-1\\c1\\c1\\c1");
      connV11.sendFrame(unsubFrame);

      connV11.disconnect();
   }
}






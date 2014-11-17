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
package org.apache.activemq6.tests.integration.stomp;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.QueueBrowser;
import javax.jms.TextMessage;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.core.client.HornetQClient;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.protocol.stomp.Stomp;
import org.apache.activemq6.core.protocol.stomp.StompProtocolManagerFactory;
import org.apache.activemq6.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq6.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq6.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.core.server.HornetQServers;
import org.apache.activemq6.jms.server.JMSServerManager;
import org.apache.activemq6.jms.server.config.JMSConfiguration;
import org.apache.activemq6.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq6.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq6.jms.server.config.impl.TopicConfigurationImpl;
import org.apache.activemq6.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq6.tests.integration.largemessage.LargeMessageTestBase;
import org.apache.activemq6.tests.integration.largemessage.LargeMessageTestBase.TestLargeMessageInputStream;
import org.apache.activemq6.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq6.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq6.tests.integration.stomp.util.StompClientConnectionFactory;
import org.apache.activemq6.tests.unit.util.InVMNamingContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExtraStompTest extends StompTestBase
{
   @Override
   @Before
   public void setUp() throws Exception
   {
      autoCreateServer = false;
      super.setUp();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
   }


   @Test
   public void testConnectionTTL() throws Exception
   {
      try
      {
         server = createServerWithTTL("2000");
         server.start();

         setUpAfterServer();

         String connect_frame = "CONNECT\n" + "login: brianm\n"
            + "passcode: wombats\n" + "request-id: 1\n" + "\n" + Stomp.NULL;
         sendFrame(connect_frame);

         String f = receiveFrame(10000);
         Assert.assertTrue(f.startsWith("CONNECTED"));
         Assert.assertTrue(f.indexOf("response-id:1") >= 0);

         String frame = "SEND\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n\n" + "Hello World 1" + Stomp.NULL;
         sendFrame(frame);

         assertChannelClosed();

         MessageConsumer consumer = session.createConsumer(queue);

         TextMessage message = (TextMessage) consumer.receiveNoWait();
         Assert.assertNotNull(message);

         message = (TextMessage) consumer.receiveNoWait();
         Assert.assertNull(message);
      }
      finally
      {
         cleanUp();
         server.stop();
      }
   }

   @Test
   public void testEnableMessageID() throws Exception
   {
      enableMessageIDTest(true);
   }

   @Test
   public void testDisableMessageID() throws Exception
   {
      enableMessageIDTest(false);
   }

   @Test
   public void testDefaultEnableMessageID() throws Exception
   {
      enableMessageIDTest(null);
   }

   //stomp sender -> large -> stomp receiver
   @Test
   public void testSendReceiveLargePersistentMessages() throws Exception
   {
      try
      {
         server = createPersistentServerWithStompMinLargeSize(2048);
         server.start();

         setUpAfterServer();

         String frame = "CONNECT\n" + "login: brianm\n"
            + "passcode: wombats\n\n" + Stomp.NULL;
         sendFrame(frame);
         frame = receiveFrame(10000);

         Assert.assertTrue(frame.startsWith("CONNECTED"));
         int count = 10;
         int szBody = 1024 * 1024;
         char[] contents = new char[szBody];
         for (int i = 0; i < szBody; i++)
         {
            contents[i] = 'A';
         }
         String body = new String(contents);

         frame = "SEND\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n"
            + "persistent:true\n"
            + "\n\n" + body + Stomp.NULL;

         for (int i = 0; i < count; i++)
         {
            sendFrame(frame);
         }

         frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:auto\n\nfff" + Stomp.NULL;
         sendFrame(frame);

         for (int i = 0; i < count; i++)
         {
            frame = receiveFrame(60000);
            Assert.assertNotNull(frame);
            System.out.println("part of frame: " + frame.substring(0, 200));
            Assert.assertTrue(frame.startsWith("MESSAGE"));
            Assert.assertTrue(frame.indexOf("destination:") > 0);
            int index = frame.indexOf("AAAA");
            assertEquals(szBody, (frame.length() - index));
         }

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

         frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
         sendFrame(frame);
      }
      catch (Exception ex)
      {
         ex.printStackTrace();
         throw ex;
      }
      finally
      {
         cleanUp();
         server.stop();
      }
   }

   //core sender -> large -> stomp receiver
   @Test
   public void testReceiveLargePersistentMessagesFromCore() throws Exception
   {
      try
      {
         server = createPersistentServerWithStompMinLargeSize(2048);
         server.start();

         setUpAfterServer();

         int msgSize = 3 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
         char[] contents = new char[msgSize];
         for (int i = 0; i < msgSize; i++)
         {
            contents[i] = 'B';
         }
         String msg = new String(contents);

         int count = 10;
         for (int i = 0; i < count; i++)
         {
            this.sendMessage(msg);
         }

         String frame = "CONNECT\n" + "login: brianm\n"
            + "passcode: wombats\n\n" + Stomp.NULL;
         sendFrame(frame);
         frame = receiveFrame(10000);

         Assert.assertTrue(frame.startsWith("CONNECTED"));

         frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:auto\n\nfff" + Stomp.NULL;
         sendFrame(frame);

         for (int i = 0; i < count; i++)
         {
            frame = receiveFrame(60000);
            Assert.assertNotNull(frame);
            System.out.println("part of frame: " + frame.substring(0, 250));
            Assert.assertTrue(frame.startsWith("MESSAGE"));
            Assert.assertTrue(frame.indexOf("destination:") > 0);
            int index = frame.indexOf("BBBB");
            assertEquals(msgSize, (frame.length() - index));
         }

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

         frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
         sendFrame(frame);
      }
      catch (Exception ex)
      {
         ex.printStackTrace();
         throw ex;
      }
      finally
      {
         cleanUp();
         server.stop();
      }
   }

   //stomp v12 sender -> large -> stomp v12 receiver
   @Test
   public void testSendReceiveLargePersistentMessagesV12() throws Exception
   {
      try
      {
         server = createPersistentServerWithStompMinLargeSize(2048);
         server.start();

         setUpAfterServer();

         StompClientConnection connV12 = StompClientConnectionFactory.createClientConnection("1.2", "localhost", port);
         connV12.connect(defUser, defPass);

         int count = 10;
         int szBody = 1024 * 1024;
         char[] contents = new char[szBody];
         for (int i = 0; i < szBody; i++)
         {
            contents[i] = 'A';
         }
         String body = new String(contents);

         ClientStompFrame frame = connV12.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("persistent", "true");
         frame.setBody(body);

         for (int i = 0; i < count; i++)
         {
            connV12.sendFrame(frame);
         }

         ClientStompFrame subFrame = connV12.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "auto");

         connV12.sendFrame(subFrame);

         for (int i = 0; i < count; i++)
         {
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
      catch (Exception ex)
      {
         ex.printStackTrace();
         throw ex;
      }
      finally
      {
         cleanUp();
         server.stop();
      }
   }

   //core sender -> large -> stomp v12 receiver
   @Test
   public void testReceiveLargePersistentMessagesFromCoreV12() throws Exception
   {
      try
      {
         server = createPersistentServerWithStompMinLargeSize(2048);
         server.start();

         setUpAfterServer();

         int msgSize = 3 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
         char[] contents = new char[msgSize];
         for (int i = 0; i < msgSize; i++)
         {
            contents[i] = 'B';
         }
         String msg = new String(contents);

         int count = 10;
         for (int i = 0; i < count; i++)
         {
            this.sendMessage(msg);
         }

         StompClientConnection connV12 = StompClientConnectionFactory.createClientConnection("1.2", "localhost", port);
         connV12.connect(defUser, defPass);

         ClientStompFrame subFrame = connV12.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "auto");

         connV12.sendFrame(subFrame);

         for (int i = 0; i < count; i++)
         {
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
      catch (Exception ex)
      {
         ex.printStackTrace();
         throw ex;
      }
      finally
      {
         cleanUp();
         server.stop();
      }
   }

   //core sender -> large (compressed regular) -> stomp v10 receiver
   @Test
   public void testReceiveLargeCompressedToRegularPersistentMessagesFromCore() throws Exception
   {
      try
      {
         server = createPersistentServerWithStompMinLargeSize(2048);
         server.start();

         setUpAfterServer(true);

         TestLargeMessageInputStream input = new TestLargeMessageInputStream(HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, true);
         LargeMessageTestBase.adjustLargeCompression(true, input, HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         char[] contents = input.toArray();
         String msg = new String(contents);

         String leadingPart = msg.substring(0, 100);

         int count = 10;
         for (int i = 0; i < count; i++)
         {
            this.sendMessage(msg);
         }

         String frame = "CONNECT\n" + "login: brianm\n"
            + "passcode: wombats\n\n" + Stomp.NULL;
         sendFrame(frame);
         frame = receiveFrame(10000);

         Assert.assertTrue(frame.startsWith("CONNECTED"));

         frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:auto\n\nfff" + Stomp.NULL;
         sendFrame(frame);

         for (int i = 0; i < count; i++)
         {
            frame = receiveFrame(60000);
            Assert.assertNotNull(frame);
            System.out.println("part of frame: " + frame.substring(0, 250));
            Assert.assertTrue(frame.startsWith("MESSAGE"));
            Assert.assertTrue(frame.indexOf("destination:") > 0);
            int index = frame.indexOf(leadingPart);
            assertEquals(msg.length(), (frame.length() - index));
         }

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

         frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
         sendFrame(frame);
      }
      catch (Exception ex)
      {
         ex.printStackTrace();
         throw ex;
      }
      finally
      {
         cleanUp();
         server.stop();
      }
   }

   //core sender -> large (compressed regular) -> stomp v12 receiver
   @Test
   public void testReceiveLargeCompressedToRegularPersistentMessagesFromCoreV12() throws Exception
   {
      try
      {
         server = createPersistentServerWithStompMinLargeSize(2048);
         server.start();

         setUpAfterServer(true);

         TestLargeMessageInputStream input = new TestLargeMessageInputStream(HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, true);
         LargeMessageTestBase.adjustLargeCompression(true, input, HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         char[] contents = input.toArray();
         String msg = new String(contents);

         int count = 10;
         for (int i = 0; i < count; i++)
         {
            this.sendMessage(msg);
         }

         StompClientConnection connV12 = StompClientConnectionFactory.createClientConnection("1.2", "localhost", port);
         connV12.connect(defUser, defPass);

         ClientStompFrame subFrame = connV12.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "auto");

         connV12.sendFrame(subFrame);

         for (int i = 0; i < count; i++)
         {
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
      catch (Exception ex)
      {
         ex.printStackTrace();
         throw ex;
      }
      finally
      {
         cleanUp();
         server.stop();
      }
   }

   //core sender -> large (compressed large) -> stomp v12 receiver
   @Test
   public void testReceiveLargeCompressedToLargePersistentMessagesFromCoreV12() throws Exception
   {
      try
      {
         server = createPersistentServerWithStompMinLargeSize(2048);
         server.start();

         setUpAfterServer(true);

         TestLargeMessageInputStream input = new TestLargeMessageInputStream(HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, true);
         input.setSize(10 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);
         LargeMessageTestBase.adjustLargeCompression(false, input, 10 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         char[] contents = input.toArray();
         String msg = new String(contents);

         int count = 10;
         for (int i = 0; i < count; i++)
         {
            this.sendMessage(msg);
         }

         StompClientConnection connV12 = StompClientConnectionFactory.createClientConnection("1.2", "localhost", port);
         connV12.connect(defUser, defPass);

         ClientStompFrame subFrame = connV12.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "auto");

         connV12.sendFrame(subFrame);

         for (int i = 0; i < count; i++)
         {
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
      catch (Exception ex)
      {
         ex.printStackTrace();
         throw ex;
      }
      finally
      {
         cleanUp();
         server.stop();
      }
   }

   //core sender -> large (compressed large) -> stomp v10 receiver
   @Test
   public void testReceiveLargeCompressedToLargePersistentMessagesFromCore() throws Exception
   {
      try
      {
         server = createPersistentServerWithStompMinLargeSize(2048);
         server.start();

         setUpAfterServer(true);

         TestLargeMessageInputStream input = new TestLargeMessageInputStream(HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, true);
         input.setSize(10 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);
         LargeMessageTestBase.adjustLargeCompression(false, input, 10 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         char[] contents = input.toArray();
         String msg = new String(contents);

         String leadingPart = msg.substring(0, 100);

         int count = 10;
         for (int i = 0; i < count; i++)
         {
            this.sendMessage(msg);
         }

         String frame = "CONNECT\n" + "login: brianm\n"
            + "passcode: wombats\n\n" + Stomp.NULL;
         sendFrame(frame);
         frame = receiveFrame(10000);

         Assert.assertTrue(frame.startsWith("CONNECTED"));

         frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:auto\n\nfff" + Stomp.NULL;
         sendFrame(frame);

         for (int i = 0; i < count; i++)
         {
            frame = receiveFrame(60000);
            Assert.assertNotNull(frame);
            System.out.println("part of frame: " + frame.substring(0, 250));
            Assert.assertTrue(frame.startsWith("MESSAGE"));
            Assert.assertTrue(frame.indexOf("destination:") > 0);
            int index = frame.indexOf(leadingPart);
            assertEquals(msg.length(), (frame.length() - index));
         }

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

         frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
         sendFrame(frame);
      }
      catch (Exception ex)
      {
         ex.printStackTrace();
         throw ex;
      }
      finally
      {
         cleanUp();
         server.stop();
      }
   }

   protected JMSServerManager createPersistentServerWithStompMinLargeSize(int sz) throws Exception
   {
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, StompProtocolManagerFactory.STOMP_PROTOCOL_NAME);
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT);
      params.put(TransportConstants.STOMP_CONSUMERS_CREDIT, "-1");
      params.put(TransportConstants.STOMP_MIN_LARGE_MESSAGE_SIZE, sz);
      TransportConfiguration stompTransport = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);

      Configuration config = createBasicConfig()
         .setPersistenceEnabled(true)
         .addAcceptorConfiguration(stompTransport)
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

      HornetQServer hornetQServer = HornetQServers.newHornetQServer(config, defUser, defPass);

      JMSConfiguration jmsConfig = new JMSConfigurationImpl();
      jmsConfig.getQueueConfigurations().add(new JMSQueueConfigurationImpl()
                                                .setName(getQueueName())
                                                .setBindings(getQueueName()));
      jmsConfig.getTopicConfigurations().add(new TopicConfigurationImpl()
                                                .setName(getTopicName())
                                                .setBindings(getTopicName()));
      server = new JMSServerManagerImpl(hornetQServer, jmsConfig);
      server.setContext(new InVMNamingContext());
      return server;
   }

   private void enableMessageIDTest(Boolean enable) throws Exception
   {
      try
      {
         server = createServerWithExtraStompOptions(null, enable);
         server.start();

         setUpAfterServer();

         String connect_frame = "CONNECT\n" + "login: brianm\n"
            + "passcode: wombats\n" + "request-id: 1\n" + "\n" + Stomp.NULL;
         sendFrame(connect_frame);

         String f = receiveFrame(10000);
         Assert.assertTrue(f.startsWith("CONNECTED"));
         Assert.assertTrue(f.indexOf("response-id:1") >= 0);

         String frame = "SEND\n" + "destination:" + getQueuePrefix()
            + getQueueName() + "\n\n" + "Hello World 1" + Stomp.NULL;
         sendFrame(frame);

         frame = "SEND\n" + "destination:" + getQueuePrefix() + getQueueName()
            + "\n\n" + "Hello World 2" + Stomp.NULL;

         sendFrame(frame);

         QueueBrowser browser = session.createBrowser(queue);

         Enumeration enu = browser.getEnumeration();

         while (enu.hasMoreElements())
         {
            Message msg = (Message) enu.nextElement();
            String msgId = msg.getStringProperty("hqMessageId");
            if (enable != null && enable.booleanValue())
            {
               assertNotNull(msgId);
               assertTrue(msgId.indexOf("STOMP") == 0);
            }
            else
            {
               assertNull(msgId);
            }
         }

         browser.close();

         MessageConsumer consumer = session.createConsumer(queue);

         TextMessage message = (TextMessage) consumer.receive(1000);
         Assert.assertNotNull(message);

         message = (TextMessage) consumer.receive(1000);
         Assert.assertNotNull(message);

         message = (TextMessage) consumer.receive(2000);
         Assert.assertNull(message);
      }
      finally
      {
         cleanUp();
         server.stop();
      }
   }

   protected JMSServerManager createServerWithTTL(String ttl) throws Exception
   {
      return createServerWithExtraStompOptions(ttl, null);
   }

   protected JMSServerManager createServerWithExtraStompOptions(String ttl, Boolean enableMessageID) throws Exception
   {

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, StompProtocolManagerFactory.STOMP_PROTOCOL_NAME);
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT);
      if (ttl != null)
      {
         params.put(TransportConstants.CONNECTION_TTL, ttl);
      }
      if (enableMessageID != null)
      {
         params.put(TransportConstants.STOMP_ENABLE_MESSAGE_ID, enableMessageID);
      }
      params.put(TransportConstants.STOMP_CONSUMERS_CREDIT, "-1");
      TransportConfiguration stompTransport = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);

      Configuration config = createBasicConfig()
         .setPersistenceEnabled(false)
         .addAcceptorConfiguration(stompTransport)
         .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY));

      HornetQServer hornetQServer = addServer(HornetQServers.newHornetQServer(config, defUser, defPass));

      JMSConfiguration jmsConfig = new JMSConfigurationImpl();
      jmsConfig.getQueueConfigurations().add(new JMSQueueConfigurationImpl()
                                                .setName(getQueueName())
                                                .setDurable(false)
                                                .setBindings(getQueueName()));
      jmsConfig.getTopicConfigurations().add(new TopicConfigurationImpl()
                                                .setName(getTopicName())
                                                .setBindings(getTopicName()));
      server = new JMSServerManagerImpl(hornetQServer, jmsConfig);
      server.setContext(new InVMNamingContext());
      return server;
   }

}

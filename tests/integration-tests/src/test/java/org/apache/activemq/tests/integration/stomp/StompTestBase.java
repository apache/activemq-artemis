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
package org.apache.activemq.tests.integration.stomp;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.IOException;
import java.net.Socket;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.protocol.stomp.StompProtocolManagerFactory;
import org.apache.activemq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.activemq.jms.server.JMSServerManager;
import org.apache.activemq.jms.server.config.JMSConfiguration;
import org.apache.activemq.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.jms.server.config.impl.TopicConfigurationImpl;
import org.apache.activemq.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.tests.unit.util.InVMNamingContext;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Before;

public abstract class StompTestBase extends UnitTestCase
{
   protected final int port = 61613;

   private ConnectionFactory connectionFactory;

   protected Connection connection;

   protected Session session;

   protected Queue queue;

   protected Topic topic;

   protected JMSServerManager server;

   protected String defUser = "brianm";

   protected String defPass = "wombats";

   protected boolean autoCreateServer = true;

   private Bootstrap bootstrap;

   private Channel channel;

   private BlockingQueue priorityQueue;

   private EventLoopGroup group;


   // Implementation methods
   // -------------------------------------------------------------------------
   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      priorityQueue = new ArrayBlockingQueue(1000);
      if (autoCreateServer)
      {
         server = createServer();
         server.start();
         connectionFactory = createConnectionFactory();
         createBootstrap();

         connection = connectionFactory.createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         queue = session.createQueue(getQueueName());
         topic = session.createTopic(getTopicName());
         connection.start();
      }
   }

   private void createBootstrap()
   {
      group = new NioEventLoopGroup();
      bootstrap = new Bootstrap();
      bootstrap.group(group)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.TCP_NODELAY, true)
         .handler(new ChannelInitializer<SocketChannel>()
         {
            @Override
            public void initChannel(SocketChannel ch) throws Exception
            {
               addChannelHandlers(ch);
            }
         });

      // Start the client.
      try
      {
         channel = bootstrap.connect("localhost", port).sync().channel();
         handshake();
      }
      catch (InterruptedException e)
      {
         e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }

   }

   protected void handshake() throws InterruptedException
   {
   }


   protected void addChannelHandlers(SocketChannel ch) throws URISyntaxException
   {
      ch.pipeline().addLast("decoder", new StringDecoder(StandardCharsets.UTF_8));
      ch.pipeline().addLast("encoder", new StringEncoder(StandardCharsets.UTF_8));
      ch.pipeline().addLast(new StompClientHandler());
   }

   protected void setUpAfterServer() throws Exception
   {
      setUpAfterServer(false);
   }

   protected void setUpAfterServer(boolean jmsCompressLarge) throws Exception
   {
      connectionFactory = createConnectionFactory();
      ActiveMQConnectionFactory activeMQConnectionFactory = (ActiveMQConnectionFactory) connectionFactory;

      activeMQConnectionFactory.setCompressLargeMessage(jmsCompressLarge);
      createBootstrap();

      connection = connectionFactory.createConnection();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queue = session.createQueue(getQueueName());
      topic = session.createTopic(getTopicName());
      connection.start();

   }

   /**
    * @return
    * @throws Exception
    */
   protected JMSServerManager createServer() throws Exception
   {
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, StompProtocolManagerFactory.STOMP_PROTOCOL_NAME);
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT);
      params.put(TransportConstants.STOMP_CONSUMERS_CREDIT, "-1");
      TransportConfiguration stompTransport = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);

      Configuration config = createBasicConfig()
         .setPersistenceEnabled(false)
         .addAcceptorConfiguration(stompTransport)
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

      ActiveMQServer activeMQServer = ActiveMQServers.newActiveMQServer(config, defUser, defPass);

      JMSConfiguration jmsConfig = new JMSConfigurationImpl();
      jmsConfig.getQueueConfigurations().add(new JMSQueueConfigurationImpl()
                                                .setName(getQueueName())
                                                .setDurable(false)
                                                .setBindings(getQueueName()));
      jmsConfig.getTopicConfigurations().add(new TopicConfigurationImpl()
                                                .setName(getTopicName())
                                                .setBindings(getTopicName()));
      server = new JMSServerManagerImpl(activeMQServer, jmsConfig);
      server.setContext(new InVMNamingContext());
      return server;
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (autoCreateServer)
      {
         connection.close();

         if (group != null)
         {
            channel.close();
            group.shutdown();
         }
         server.stop();
      }
      super.tearDown();
   }

   protected void cleanUp() throws Exception
   {
      connection.close();
      if (group != null)
      {
         group.shutdown();
      }
   }

   protected void reconnect() throws Exception
   {
      reconnect(0);
   }

   protected void reconnect(long sleep) throws Exception
   {
      group.shutdown();

      if (sleep > 0)
      {
         Thread.sleep(sleep);
      }

      createBootstrap();
   }

   protected ConnectionFactory createConnectionFactory()
   {
      return new ActiveMQJMSConnectionFactory(false, new TransportConfiguration(InVMConnectorFactory.class.getName()));
   }

   protected Socket createSocket() throws IOException
   {
      return new Socket("localhost", port);
   }

   protected String getQueueName()
   {
      return "test";
   }

   protected String getQueuePrefix()
   {
      return "jms.queue.";
   }

   protected String getTopicName()
   {
      return "testtopic";
   }

   protected String getTopicPrefix()
   {
      return "jms.topic.";
   }


   protected void assertChannelClosed() throws InterruptedException
   {
      boolean closed = channel.closeFuture().await(5000);
      assertTrue("channel not closed", closed);
   }

   public void sendFrame(String data) throws Exception
   {
      channel.writeAndFlush(data);
   }

   public void sendFrame(byte[] data) throws Exception
   {
      ByteBuf buffer = Unpooled.buffer(data.length);
      buffer.writeBytes(data);
      channel.writeAndFlush(buffer);
   }

   public String receiveFrame(long timeOut) throws Exception
   {
      String msg = (String) priorityQueue.poll(timeOut, TimeUnit.MILLISECONDS);
      return msg;
   }

   public void sendMessage(String msg) throws Exception
   {
      sendMessage(msg, queue);
   }

   public void sendMessage(String msg, Destination destination) throws Exception
   {
      MessageProducer producer = session.createProducer(destination);
      TextMessage message = session.createTextMessage(msg);
      producer.send(message);
   }

   public void sendMessage(byte[] data, Destination destination) throws Exception
   {
      sendMessage(data, "foo", "xyz", destination);
   }

   public void sendMessage(String msg, String propertyName, String propertyValue) throws Exception
   {
      sendMessage(msg.getBytes(StandardCharsets.UTF_8), propertyName, propertyValue, queue);
   }

   public void sendMessage(byte[] data, String propertyName, String propertyValue, Destination destination) throws Exception
   {
      MessageProducer producer = session.createProducer(destination);
      BytesMessage message = session.createBytesMessage();
      message.setStringProperty(propertyName, propertyValue);
      message.writeBytes(data);
      producer.send(message);
   }

   protected void waitForReceipt() throws Exception
   {
      String frame = receiveFrame(50000);
      assertNotNull(frame);
      assertTrue(frame.indexOf("RECEIPT") > -1);
   }

   protected void waitForFrameToTakeEffect() throws InterruptedException
   {
      // bit of a dirty hack :)
      // another option would be to force some kind of receipt to be returned
      // from the frame
      Thread.sleep(500);
   }

   class StompClientHandler extends SimpleChannelInboundHandler<String>
   {
      StringBuffer currentMessage = new StringBuffer("");

      @Override
      protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception
      {
         currentMessage.append(msg);
         String fullMessage = currentMessage.toString();
         if (fullMessage.contains("\0\n"))
         {
            int messageEnd = fullMessage.indexOf("\0\n");
            String actualMessage = fullMessage.substring(0, messageEnd);
            fullMessage = fullMessage.substring(messageEnd + 2);
            currentMessage = new StringBuffer("");
            priorityQueue.add(actualMessage);
            if (fullMessage.length() > 0)
            {
               channelRead(ctx, fullMessage);
            }
         }
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
      {
         cause.printStackTrace();
         ctx.close();
      }
   }


}

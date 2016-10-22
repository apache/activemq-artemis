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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.protocol.stomp.StompProtocolManagerFactory;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.TopicConfigurationImpl;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Before;

public abstract class StompTestBase extends ActiveMQTestBase {

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

   private List<Bootstrap> bootstraps = new ArrayList<>();

   //   private Channel channel;

   private List<BlockingQueue<String>> priorityQueues = new ArrayList<>();

   private List<EventLoopGroup> groups = new ArrayList<>();

   private List<Channel> channels = new ArrayList<>();

   // Implementation methods
   // -------------------------------------------------------------------------
   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      if (autoCreateServer) {
         server = createServer();
         addServer(server.getActiveMQServer());
         server.start();
         connectionFactory = createConnectionFactory();
         createBootstrap();

         if (isSecurityEnabled()) {
            connection = connectionFactory.createConnection("brianm", "wombats");
         } else {
            connection = connectionFactory.createConnection();
         }
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         queue = session.createQueue(getQueueName());
         topic = session.createTopic(getTopicName());
         connection.start();
      }
   }

   private void createBootstrap() {
      createBootstrap(0, port);
   }

   protected void createBootstrap(int port) {
      createBootstrap(0, port);
   }

   protected void createBootstrap(final int index, int port) {
      priorityQueues.add(index, new ArrayBlockingQueue<String>(1000));
      groups.add(index, new NioEventLoopGroup());
      bootstraps.add(index, new Bootstrap());
      bootstraps.get(index).group(groups.get(index)).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true).handler(new ChannelInitializer<SocketChannel>() {
         @Override
         public void initChannel(SocketChannel ch) throws Exception {
            addChannelHandlers(index, ch);
         }
      });

      // Start the client.
      try {
         channels.add(index, bootstraps.get(index).connect("localhost", port).sync().channel());
         handshake();
      } catch (InterruptedException e) {
         e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }

   }

   protected void handshake() throws InterruptedException {
   }

   protected void addChannelHandlers(int index, SocketChannel ch) throws URISyntaxException {
      ch.pipeline().addLast("decoder", new StringDecoder(StandardCharsets.UTF_8));
      ch.pipeline().addLast("encoder", new StringEncoder(StandardCharsets.UTF_8));
      ch.pipeline().addLast(new StompClientHandler(index));
   }

   protected void setUpAfterServer() throws Exception {
      setUpAfterServer(false);
   }

   protected void setUpAfterServer(boolean jmsCompressLarge) throws Exception {
      connectionFactory = createConnectionFactory();
      ActiveMQConnectionFactory activeMQConnectionFactory = (ActiveMQConnectionFactory) connectionFactory;

      activeMQConnectionFactory.setCompressLargeMessage(jmsCompressLarge);
      createBootstrap();

      connection = connectionFactory.createConnection();
      connection.start();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queue = session.createQueue(getQueueName());
      topic = session.createTopic(getTopicName());

   }

   /**
    * @return
    * @throws Exception
    */
   protected JMSServerManager createServer() throws Exception {
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, StompProtocolManagerFactory.STOMP_PROTOCOL_NAME);
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT);
      params.put(TransportConstants.STOMP_CONSUMERS_CREDIT, "-1");
      TransportConfiguration stompTransport = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);
      TransportConfiguration allTransport = new TransportConfiguration(NettyAcceptorFactory.class.getName());

      Configuration config = createBasicConfig().setSecurityEnabled(isSecurityEnabled()).setPersistenceEnabled(true).addAcceptorConfiguration(stompTransport).addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      config.addAcceptorConfiguration(allTransport);

      ActiveMQServer activeMQServer = addServer(ActiveMQServers.newActiveMQServer(config, defUser, defPass));

      if (isSecurityEnabled()) {
         ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) activeMQServer.getSecurityManager();

         final String role = "testRole";
         securityManager.getConfiguration().addRole(defUser, role);
         config.getSecurityRoles().put("#", new HashSet<Role>() {
            {
               add(new Role(role, true, true, true, true, true, true, true, true, true));
            }
         });
      }

      JMSConfiguration jmsConfig = new JMSConfigurationImpl();
      jmsConfig.getQueueConfigurations().add(new JMSQueueConfigurationImpl().setName(getQueueName()).setDurable(false).setBindings(getQueueName()));
      jmsConfig.getTopicConfigurations().add(new TopicConfigurationImpl().setName(getTopicName()).setBindings(getTopicName()));
      server = new JMSServerManagerImpl(activeMQServer, jmsConfig);
      server.setRegistry(new JndiBindingRegistry(new InVMNamingContext()));
      return server;
   }

   @Override
   @After
   public void tearDown() throws Exception {
      if (autoCreateServer) {
         connection.close();

         for (EventLoopGroup group : groups) {
            if (group != null) {
               for (Channel channel : channels) {
                  channel.close();
               }
               group.shutdownGracefully(0, 5000, TimeUnit.MILLISECONDS);
            }
         }
      }
      super.tearDown();
   }

   protected void cleanUp() throws Exception {
      connection.close();
      if (groups.get(0) != null) {
         groups.get(0).shutdown();
      }
   }

   protected void reconnect() throws Exception {
      reconnect(0);
   }

   protected void reconnect(long sleep) throws Exception {
      groups.get(0).shutdown();

      if (sleep > 0) {
         Thread.sleep(sleep);
      }

      createBootstrap();
   }

   protected ConnectionFactory createConnectionFactory() {
      return new ActiveMQJMSConnectionFactory(false, new TransportConfiguration(InVMConnectorFactory.class.getName()));
   }

   protected Socket createSocket() throws IOException {
      return new Socket("localhost", port);
   }

   protected String getQueueName() {
      return "test";
   }

   protected String getQueuePrefix() {
      return "";
   }

   protected String getTopicName() {
      return "testtopic";
   }

   protected String getTopicPrefix() {
      return "";
   }

   protected void assertChannelClosed() throws InterruptedException {
      assertChannelClosed(0);
   }

   protected void assertChannelClosed(int index) throws InterruptedException {
      boolean closed = channels.get(index).closeFuture().await(5000);
      assertTrue("channel not closed", closed);
   }

   public void sendFrame(String data) throws Exception {
      IntegrationTestLogger.LOGGER.info("Sending: " + data);
      sendFrame(0, data);
   }

   public void sendFrame(int index, String data) throws Exception {
      channels.get(index).writeAndFlush(data);
   }

   public void sendFrame(byte[] data) throws Exception {
      sendFrame(0, data);
   }

   public void sendFrame(int index, byte[] data) throws Exception {
      ByteBuf buffer = Unpooled.buffer(data.length);
      buffer.writeBytes(data);
      channels.get(index).writeAndFlush(buffer);
   }

   public String receiveFrame(long timeOut) throws Exception {
      return receiveFrame(0, timeOut);
   }

   public String receiveFrame(int index, long timeOut) throws Exception {
      String msg = priorityQueues.get(index).poll(timeOut, TimeUnit.MILLISECONDS);
      return msg;
   }

   public void sendMessage(String msg) throws Exception {
      sendMessage(msg, queue);
   }

   public void sendMessage(String msg, Destination destination) throws Exception {
      MessageProducer producer = session.createProducer(destination);
      TextMessage message = session.createTextMessage(msg);
      producer.send(message);
   }

   public void sendMessage(byte[] data, Destination destination) throws Exception {
      sendMessage(data, "foo", "xyz", destination);
   }

   public void sendMessage(String msg, String propertyName, String propertyValue) throws Exception {
      sendMessage(msg.getBytes(StandardCharsets.UTF_8), propertyName, propertyValue, queue);
   }

   public void sendMessage(byte[] data,
                           String propertyName,
                           String propertyValue,
                           Destination destination) throws Exception {
      MessageProducer producer = session.createProducer(destination);
      BytesMessage message = session.createBytesMessage();
      message.setStringProperty(propertyName, propertyValue);
      message.writeBytes(data);
      producer.send(message);
   }

   protected void waitForReceipt() throws Exception {
      String frame = receiveFrame(50000);
      assertNotNull(frame);
      assertTrue(frame.indexOf("RECEIPT") > -1);
   }

   protected void waitForFrameToTakeEffect() throws InterruptedException {
      // bit of a dirty hack :)
      // another option would be to force some kind of receipt to be returned
      // from the frame
      Thread.sleep(500);
   }

   public boolean isSecurityEnabled() {
      return false;
   }

   class StompClientHandler extends SimpleChannelInboundHandler<String> {

      int index = 0;

      StompClientHandler(int index) {
         this.index = index;
      }

      StringBuffer currentMessage = new StringBuffer("");

      @Override
      protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
         currentMessage.append(msg);
         String fullMessage = currentMessage.toString();
         if (fullMessage.contains("\0\n")) {
            int messageEnd = fullMessage.indexOf("\0\n");
            String actualMessage = fullMessage.substring(0, messageEnd);
            fullMessage = fullMessage.substring(messageEnd + 2);
            currentMessage = new StringBuffer("");
            BlockingQueue queue = priorityQueues.get(index);
            if (queue == null) {
               queue = new ArrayBlockingQueue(1000);
               priorityQueues.add(index, queue);
            }
            queue.add(actualMessage);
            if (fullMessage.length() > 0) {
               channelRead(ctx, fullMessage);
            }
         }
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
         cause.printStackTrace();
         ctx.close();
      }
   }

}

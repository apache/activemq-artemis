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
package org.hornetq.tests.integration.transports.netty;

import java.util.HashMap;
import java.util.Map;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.HornetQNotConnectedException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.netty.NettyAcceptor;
import org.hornetq.core.remoting.impl.netty.PartialPooledByteBufAllocator;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.netty.handler.codec.http.HttpHeaders.Names.UPGRADE;
import static io.netty.handler.codec.http.HttpResponseStatus.SWITCHING_PROTOCOLS;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.hornetq.core.remoting.impl.netty.NettyConnector.MAGIC_NUMBER;
import static org.hornetq.core.remoting.impl.netty.NettyConnector.SEC_HORNETQ_REMOTING_ACCEPT;
import static org.hornetq.core.remoting.impl.netty.NettyConnector.SEC_HORNETQ_REMOTING_KEY;
import static org.hornetq.core.remoting.impl.netty.NettyConnector.createExpectedResponse;
import static org.hornetq.tests.util.RandomUtil.randomString;

/**
 * Test that Netty Connector can connect to a Web Server and upgrade from a HTTP request to its remoting protocol.
 *
 * @author <a href="http://jmesnil.net/">Jeff Mesnil</a> (c) 2013 Red Hat inc.
 */
public class NettyConnectorWithHTTPUpgradeTest extends UnitTestCase
{

   private static final SimpleString QUEUE = new SimpleString("NettyConnectorWithHTTPUpgradeTest");

   private static final int HTTP_PORT = 8080;

   private Configuration conf;
   private HornetQServer server;
   private ServerLocator locator;
   private String acceptorName;

   private NioEventLoopGroup bossGroup;
   private NioEventLoopGroup workerGroup;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      HashMap<String, Object> httpParams = new HashMap<String, Object>();
      // This prop controls the usage of HTTP Get + Upgrade from Netty connector
      httpParams.put(TransportConstants.HTTP_UPGRADE_ENABLED_PROP_NAME, true);
      httpParams.put(TransportConstants.PORT_PROP_NAME, HTTP_PORT);
      acceptorName = randomString();
      HashMap<String, Object> emptyParams = new HashMap<>();

      conf = createDefaultConfig()
         .setSecurityEnabled(false)
         .addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, httpParams, acceptorName))
         .addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, emptyParams, randomString()));

      server = addServer(HornetQServers.newHornetQServer(conf, false));

      server.start();
      locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NETTY_CONNECTOR_FACTORY, httpParams));
      addServerLocator(locator);

      // THe web server owns the HTTP port, not HornetQ.
      startWebServer(HTTP_PORT);
   }

   @After
   public void tearDown() throws Exception
   {
      stopWebServer();
      super.tearDown();
   }

   @Test
   public void sendAndReceiveOverHTTPPort() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte) 1);
         message.getBodyBuffer().writeString("sendAndReceiveOverHTTPPort");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertNotNull(message2);
         assertEquals("sendAndReceiveOverHTTPPort", message2.getBodyBuffer().readString());

         message2.acknowledge();
      }

      session.close();
   }

   @Test
   public void HTTPUpgradeConnectorUsingNormalAcceptor() throws Exception
   {
      HashMap<String, Object> params = new HashMap<>();

      // create a new locator that points an HTTP-upgrade connector to the normal acceptor
      params.put(TransportConstants.HTTP_UPGRADE_ENABLED_PROP_NAME, true);

      long start = System.currentTimeMillis();
      locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NETTY_CONNECTOR_FACTORY, params));

      Exception e = null;

      try
      {
         createSessionFactory(locator);

         // we shouldn't ever get here
         fail();
      }
      catch (Exception x)
      {
         e = x;
      }

      // make sure we failed *before* the HTTP hand-shake timeout elapsed (which is hard-coded to 30 seconds, see org.hornetq.core.remoting.impl.netty.NettyConnector.HttpUpgradeHandler.awaitHandshake())
      assertTrue((System.currentTimeMillis() - start) < 30000);
      assertNotNull(e);
      assertTrue(e instanceof HornetQNotConnectedException);
      assertTrue(((HornetQException) e).getType() == HornetQExceptionType.NOT_CONNECTED);
   }

   private void startWebServer(int port) throws InterruptedException
   {
      bossGroup = new NioEventLoopGroup();
      workerGroup = new NioEventLoopGroup();
      ServerBootstrap b = new ServerBootstrap();
      b.childOption(ChannelOption.ALLOCATOR, PartialPooledByteBufAllocator.INSTANCE);
      b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .childHandler(new ChannelInitializer<SocketChannel>()
         {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception
            {
               // create a HTTP server
               ChannelPipeline p = ch.pipeline();
               p.addLast("decoder", new HttpRequestDecoder());
               p.addLast("encoder", new HttpResponseEncoder());
               p.addLast("http-upgrade-handler", new SimpleChannelInboundHandler<Object>()
               {
                  // handle HTTP GET + Upgrade with a handshake specific to HornetQ remoting.
                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception
                  {
                     if (msg instanceof HttpRequest)
                     {
                        HttpRequest request = (HttpRequest) msg;

                        for (Map.Entry<String, String> entry : request.headers())
                        {
                           System.out.println(entry);
                        }
                        String upgrade = request.headers().get(UPGRADE);
                        String secretKey = request.headers().get(SEC_HORNETQ_REMOTING_KEY);

                        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, SWITCHING_PROTOCOLS);
                        response.headers().set(UPGRADE, upgrade);
                        response.headers().set(SEC_HORNETQ_REMOTING_ACCEPT, createExpectedResponse(MAGIC_NUMBER, secretKey));
                        ctx.writeAndFlush(response);

                        // when the handshake is successful, the HTTP handlers are removed
                        ctx.pipeline().remove("decoder");
                        ctx.pipeline().remove("encoder");
                        ctx.pipeline().remove(this);

                        System.out.println("HTTP handshake sent, transferring channel");
                        // transfer the control of the channel to the Netty Acceptor
                        NettyAcceptor acceptor = (NettyAcceptor) server.getRemotingService().getAcceptor(acceptorName);
                        acceptor.transfer(ctx.channel());
                        // at this point, the HTTP upgrade process is over and the netty acceptor behaves like regular ones.
                     }
                  }
               });
            }

            @Override
            public void channelReadComplete(ChannelHandlerContext ctx) throws Exception
            {
               ctx.flush();
            }
         });
      b.bind(port).sync();
   }

   private void stopWebServer()
   {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
   }
}

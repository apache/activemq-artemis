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
package org.apache.activemq.cli.test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.CharsetUtil;
import org.apache.activemq.artemis.component.WebServerComponent;
import org.apache.activemq.artemis.core.remoting.impl.ssl.SSLSupport;
import org.apache.activemq.artemis.dto.WebServerDTO;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WebServerComponentTest extends Assert {

   static final String URL = System.getProperty("url", "http://localhost:8161/WebServerComponentTest.txt");
   static final String SECURE_URL = System.getProperty("url", "https://localhost:8448/WebServerComponentTest.txt");
   private Bootstrap bootstrap;
   private EventLoopGroup group;

   @Before
   public void setupNetty() throws URISyntaxException {
      // Configure the client.
      group = new NioEventLoopGroup();
      bootstrap = new Bootstrap();
   }

   @Test
   public void simpleServer() throws Exception {
      WebServerDTO webServerDTO = new WebServerDTO();
      webServerDTO.bind = "http://localhost:8161";
      webServerDTO.path = "webapps";
      WebServerComponent webServerComponent = new WebServerComponent();
      Assert.assertFalse(webServerComponent.isStarted());
      webServerComponent.configure(webServerDTO, "./src/test/resources/", "./src/test/resources/");
      webServerComponent.start();
      // Make the connection attempt.
      CountDownLatch latch = new CountDownLatch(1);
      final ClientHandler clientHandler = new ClientHandler(latch);
      bootstrap.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer() {
         @Override
         protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast(new HttpClientCodec());
            ch.pipeline().addLast(clientHandler);
         }
      });
      Channel ch = bootstrap.connect("localhost", 8161).sync().channel();

      URI uri = new URI(URL);
      // Prepare the HTTP request.
      HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getRawPath());
      request.headers().set(HttpHeaders.Names.HOST, "localhost");

      // Send the HTTP request.
      ch.writeAndFlush(request);
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      assertEquals(clientHandler.body, "12345");
      // Wait for the server to close the connection.
      ch.close();
      Assert.assertTrue(webServerComponent.isStarted());
      webServerComponent.stop();
      Assert.assertFalse(webServerComponent.isStarted());
   }

   @Test
   public void simpleSecureServer() throws Exception {
      WebServerDTO webServerDTO = new WebServerDTO();
      webServerDTO.bind = "https://localhost:8448";
      webServerDTO.path = "webapps";
      webServerDTO.keyStorePath = "./src/test/resources/server.keystore";
      webServerDTO.keyStorePassword = "password";

      WebServerComponent webServerComponent = new WebServerComponent();
      Assert.assertFalse(webServerComponent.isStarted());
      webServerComponent.configure(webServerDTO, "./src/test/resources/", "./src/test/resources/");
      webServerComponent.start();
      // Make the connection attempt.
      String keyStoreProvider = "JKS";

      SSLContext context = SSLSupport.createContext(keyStoreProvider, webServerDTO.keyStorePath, webServerDTO.keyStorePassword, keyStoreProvider, webServerDTO.keyStorePath, webServerDTO.keyStorePassword);

      SSLEngine engine = context.createSSLEngine();
      engine.setUseClientMode(true);
      engine.setWantClientAuth(true);
      final SslHandler sslHandler = new SslHandler(engine);

      CountDownLatch latch = new CountDownLatch(1);
      final ClientHandler clientHandler = new ClientHandler(latch);
      bootstrap.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer() {
         @Override
         protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast(sslHandler);
            ch.pipeline().addLast(new HttpClientCodec());
            ch.pipeline().addLast(clientHandler);
         }
      });
      Channel ch = bootstrap.connect("localhost", 8448).sync().channel();

      URI uri = new URI(SECURE_URL);
      // Prepare the HTTP request.
      HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getRawPath());
      request.headers().set(HttpHeaders.Names.HOST, "localhost");

      // Send the HTTP request.
      ch.writeAndFlush(request);
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      assertEquals(clientHandler.body, "12345");
      // Wait for the server to close the connection.
      ch.close();
      Assert.assertTrue(webServerComponent.isStarted());
      webServerComponent.stop();
      Assert.assertFalse(webServerComponent.isStarted());
   }

   @Test
   public void simpleSecureServerWithClientAuth() throws Exception {
      WebServerDTO webServerDTO = new WebServerDTO();
      webServerDTO.bind = "https://localhost:8448";
      webServerDTO.path = "webapps";
      webServerDTO.keyStorePath = "./src/test/resources/server.keystore";
      webServerDTO.keyStorePassword = "password";
      webServerDTO.clientAuth = true;
      webServerDTO.trustStorePath = "./src/test/resources/server.keystore";
      webServerDTO.trustStorePassword = "password";

      WebServerComponent webServerComponent = new WebServerComponent();
      Assert.assertFalse(webServerComponent.isStarted());
      webServerComponent.configure(webServerDTO, "./src/test/resources/", "./src/test/resources/");
      webServerComponent.start();
      // Make the connection attempt.
      String keyStoreProvider = "JKS";

      SSLContext context = SSLSupport.createContext(keyStoreProvider, webServerDTO.keyStorePath, webServerDTO.keyStorePassword, keyStoreProvider, webServerDTO.trustStorePath, webServerDTO.trustStorePassword);

      SSLEngine engine = context.createSSLEngine();
      engine.setUseClientMode(true);
      engine.setWantClientAuth(true);
      final SslHandler sslHandler = new SslHandler(engine);

      CountDownLatch latch = new CountDownLatch(1);
      final ClientHandler clientHandler = new ClientHandler(latch);
      bootstrap.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer() {
         @Override
         protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast(sslHandler);
            ch.pipeline().addLast(new HttpClientCodec());
            ch.pipeline().addLast(clientHandler);
         }
      });
      Channel ch = bootstrap.connect("localhost", 8448).sync().channel();

      URI uri = new URI(SECURE_URL);
      // Prepare the HTTP request.
      HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getRawPath());
      request.headers().set(HttpHeaders.Names.HOST, "localhost");

      // Send the HTTP request.
      ch.writeAndFlush(request);
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      assertEquals(clientHandler.body, "12345");
      // Wait for the server to close the connection.
      ch.close();
      Assert.assertTrue(webServerComponent.isStarted());
      webServerComponent.stop();
      Assert.assertFalse(webServerComponent.isStarted());
   }

   class ClientHandler extends SimpleChannelInboundHandler<HttpObject> {

      private CountDownLatch latch;
      private String body;

      ClientHandler(CountDownLatch latch) {
         this.latch = latch;
      }

      @Override
      public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
         if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;
            body = content.content().toString(CharsetUtil.UTF_8);
            latch.countDown();
         }
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
         cause.printStackTrace();
         ctx.close();
      }
   }
}

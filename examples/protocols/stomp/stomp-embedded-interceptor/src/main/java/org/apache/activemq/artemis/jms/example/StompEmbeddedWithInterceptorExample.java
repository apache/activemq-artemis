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
package org.apache.activemq.artemis.jms.example;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.protocol.stomp.StompConnection;
import org.apache.activemq.artemis.core.protocol.stomp.StompFrame;
import org.apache.activemq.artemis.core.protocol.stomp.StompFrameInterceptor;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.HashSet;

/**
 * This example demonstrates how to run an ActiveMQ Artemis embedded with JMS
 */
public class StompEmbeddedWithInterceptorExample {

   public static class MyStompInterceptor
      implements StompFrameInterceptor {

      public boolean intercept(StompFrame frame, RemotingConnection remotingConnection)
         throws ActiveMQException {

         StompConnection connection = (StompConnection) remotingConnection;

         System.out.println("Intercepted frame " + frame + " on connection " + connection);

         return false;
      }
   }

   private static final String END_OF_FRAME = "\u0000";

   public static void main(final String[] args) throws Exception {
      EmbeddedJMS jmsServer = new EmbeddedJMS(); 
      String brokerConfigPath = StompEmbeddedWithInterceptorExample.class.getResource("/broker.xml").toString();
      jmsServer.setConfigResourcePath(brokerConfigPath);

      jmsServer.start();
      jmsServer.getActiveMQServer().getRemotingService().addIncomingInterceptor(new MyStompInterceptor());

      System.out.println("Started Embedded JMS Server");

      // Step 1. Create a TCP socket to connect to the Stomp port
      Socket socket = new Socket("localhost", 61616);

      // Step 2. Send a CONNECT frame to connect to the server
      String connectFrame = "CONNECT\n" +
         "accept-version:1.2\n" +
         "host:localhost\n" +
         "login:guest\n" +
         "passcode:guest\n" +
         "request-id:1\n" +
         "\n" +
         END_OF_FRAME;
      sendFrame(socket, connectFrame);

      // Step 3. Send a SEND frame (a Stomp message) to the
      // jms.queue.exampleQueue address with a text body
      String text = "Hello World from Stomp 1.2 !";
      String message = "SEND\n" +
         "destination:jms.queue.exampleQueue\n" +
         "\n" +
         text +
         END_OF_FRAME;
      sendFrame(socket, message);
      System.out.println("Sent Stomp message: " + text);

      // Step 4. Send a DISCONNECT frame to disconnect from the server
      String disconnectFrame = "DISCONNECT\n" +
         "\n" +
         END_OF_FRAME;
      sendFrame(socket, disconnectFrame);

      // Step 5. Slose the TCP socket
      socket.close();

      Thread.sleep(1000);
      jmsServer.stop();
   }

   private static void sendFrame(Socket socket, String data) throws Exception {
      byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
      OutputStream outputStream = socket.getOutputStream();
      for (int i = 0; i < bytes.length; i++) {
         outputStream.write(bytes[i]);
      }
      outputStream.flush();
   }
}

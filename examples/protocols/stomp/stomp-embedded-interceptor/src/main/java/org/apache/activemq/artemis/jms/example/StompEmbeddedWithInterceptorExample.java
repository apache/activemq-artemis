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

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * This example demonstrates how to run an ActiveMQ Artemis embedded with JMS
 */
public class StompEmbeddedWithInterceptorExample {

   private static final String END_OF_FRAME = "\u0000";

   public static void main(final String[] args) throws Exception {
      // Step 1. Create a TCP socket to connect to the Stomp port
      try (Socket socket = new Socket("localhost", 61616)) {

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
            "destination:exampleQueue" +
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
      }

      // It will use a regular JMS connection to show how the injected data will appear at the final message

      try (ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
           Connection connection = factory.createConnection();
           Session session = connection.createSession()) {
         connection.start();

         MessageConsumer consumer = session.createConsumer(session.createQueue("exampleQueue"));
         Message messageReceived = consumer.receive(5000);

         String propStomp = messageReceived.getStringProperty("stompIntercepted");

         String propRegular = messageReceived.getStringProperty("regularIntercepted");

         System.out.println("propStomp is Hello!! - " + propStomp.equals("Hello"));
         System.out.println("propRegular is HelloAgain!! - " + propRegular.equals("HelloAgain"));
      }
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

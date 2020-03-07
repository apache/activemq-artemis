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
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * An example where a Stomp 1.2 client sends a message on a TCP socket
 * and consumes it from a JMS MessageConsumer.
 */
public class StompExample {

   private static final String END_OF_FRAME = "\u0000";

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      InitialContext initialContext = null;

      try {
         // Step 1. Create a TCP socket to connect to the Stomp port
         Socket socket = new Socket("localhost", 61613);

         // Step 2. Send a CONNECT frame to connect to the server
         String connectFrame = "CONNECT\n" +
            "accept-version:1.2\n" +
            "host:localhost\n" +
            "request-id:1\n" +
            "\n" +
            END_OF_FRAME;
         sendFrame(socket, connectFrame);

         String response = receiveFrame(socket);
         System.out.println("response: " + response);

         // Step 3. Send a SEND frame (a Stomp message) to the
         // jms.queue.exampleQueue address with a text body
         String text = "Hello World from Stomp 1.2 !";
         String message = "SEND\n" +
            "destination:exampleQueue\n" +
            "destination-type:ANYCAST\n" +
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

         // We will now consume from JMS the message sent with Stomp.

         // Step 6. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 7. Perform a lookup on the queue and the connection factory
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 8.Create a JMS Connection, Session and a MessageConsumer on the queue
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(queue);

         // Step 9. Start the Connection
         connection.start();

         // Step 10. Receive the message
         TextMessage messageReceived = (TextMessage) consumer.receive(5000);
         System.out.println("Received JMS message: " + messageReceived.getText());
      } finally {
         // Step 11. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
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

   private static String receiveFrame(Socket socket) throws Exception {
      InputStream inputStream = socket.getInputStream();
      byte[] buffer = new byte[1024];
      int size = inputStream.read(buffer);

      byte[] data = new byte[size];
      System.arraycopy(buffer, 0, data, 0, size);

      String frame = new String(data, StandardCharsets.UTF_8);
      return frame;
   }
}

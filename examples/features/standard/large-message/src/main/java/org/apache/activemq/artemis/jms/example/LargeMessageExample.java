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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.activemq.artemis.util.ServerUtil;

/**
 * This example demonstrates the ability of ActiveMQ Artemis to send and consume a very large message, much
 * bigger than can fit in RAM.
 */
public class LargeMessageExample {

   /**
    * The message we will send is size 2GiB, even though we are only running in 50MB of RAM on both
    * client and server.
    * <p>
    * This may take some considerable time to create, send and consume - if it takes too long or you
    * don't have enough disk space just reduce the file size here
    */
   private static final long FILE_SIZE = 2L * 1024 * 1024 * 1024; // 2 GiB message

   public static void main(final String[] args) throws Exception {
      Process server = null;
      Connection connection = null;
      InitialContext initialContext = null;
      File inputFile = null;
      File outputFile = null;
      boolean deleteFiles = Boolean.parseBoolean(args[1]);

      try {
         server = ServerUtil.startServer(args[0], LargeMessageExample.class.getSimpleName(), 0, 5000);

         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory. This ConnectionFactory has a special attribute set on
         // it.
         // Messages with more than 10K are considered large
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4. Create the JMS objects
         connection = cf.createConnection();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(queue);

         // Step 5. Create a huge file - this will form the body of the message we will send.

         System.out.println("Creating a file to send of size " + FILE_SIZE +
                               " bytes. This may take a little while... " +
                               "If this is too big for your disk you can easily change the FILE_SIZE in the example.");

         inputFile = new File("huge_message_to_send.dat");

         createFile(inputFile, FILE_SIZE);

         System.out.println("File created.");

         // Step 6. Create a BytesMessage
         BytesMessage message = session.createBytesMessage();

         // Step 7. We set the InputStream on the message. When sending the message will read the InputStream
         // until it gets EOF. In this case we point the InputStream at a file on disk, and it will suck up the entire
         // file, however we could use any InputStream not just a FileInputStream.
         FileInputStream fileInputStream = new FileInputStream(inputFile);
         BufferedInputStream bufferedInput = new BufferedInputStream(fileInputStream);

         message.setObjectProperty("JMS_AMQ_InputStream", bufferedInput);

         System.out.println("Sending the huge message.");

         // Step 9. Send the Message
         producer.send(message);

         System.out.println("Large Message sent");

         System.out.println("Stopping server.");

         // Step 10. To demonstrate that that we're not simply streaming the message from sending to consumer, we stop
         // the server and restart it before consuming the message. This demonstrates that the large message gets
         // persisted, like a
         // normal persistent message, on the server. If you look at ./build/data/largeMessages you will see the
         // largeMessage stored on disk the server

         connection.close();

         initialContext.close();

         ServerUtil.killServer(server);

         server = ServerUtil.startServer(args[0], "LargeMessageExample", 0, 5000);

         System.out.println("Server restarted.");

         // Step 11. Now the server is restarted we can recreate the JMS Objects, and start the new connection

         initialContext = new InitialContext();

         queue = (Queue) initialContext.lookup("queue/exampleQueue");

         cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         connection = cf.createConnection();

         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer messageConsumer = session.createConsumer(queue);

         connection.start();

         System.out.println("Receiving message.");

         // Step 12. Receive the message. When we receive the large message we initially just receive the message with
         // an empty body.
         BytesMessage messageReceived = (BytesMessage) messageConsumer.receive(120000);

         System.out.println("Received message with: " + messageReceived.getLongProperty("_AMQ_LARGE_SIZE") +
                               " bytes. Now streaming to file on disk.");

         // Step 13. We set an OutputStream on the message. This causes the message body to be written to the
         // OutputStream until there are no more bytes to be written.
         // You don't have to use a FileOutputStream, you can use any OutputStream.
         // You may choose to use the regular BytesMessage or
         // StreamMessage interface but this method is much faster for large messages.

         outputFile = new File("huge_message_received.dat");

         try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) {
            BufferedOutputStream bufferedOutput = new BufferedOutputStream(fileOutputStream);

            // Step 14. This will save the stream and wait until the entire message is written before continuing.
            messageReceived.setObjectProperty("JMS_AMQ_SaveStream", bufferedOutput);
         }

         System.out.println("File streamed to disk. Size of received file on disk is " + outputFile.length());
      } finally {
         // Step 12. Be sure to close our resources!
         if (initialContext != null) {
            initialContext.close();
         }

         if (connection != null) {
            connection.close();
         }

         if (inputFile != null && deleteFiles) {
            inputFile.delete();
         }

         if (outputFile != null && deleteFiles) {
            outputFile.delete();
         }

         ServerUtil.killServer(server);
      }
   }

   private static void createFile(final File file, final long fileSize) throws IOException {
      FileOutputStream fileOut = new FileOutputStream(file);
      try (BufferedOutputStream buffOut = new BufferedOutputStream(fileOut)) {
         byte[] outBuffer = new byte[1024 * 1024];
         for (long i = 0; i < fileSize; i += outBuffer.length) {
            buffOut.write(outBuffer);
         }
      }
   }

}

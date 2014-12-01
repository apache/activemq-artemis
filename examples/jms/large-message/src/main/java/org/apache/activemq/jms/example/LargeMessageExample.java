/**
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
package org.apache.activemq.jms.example;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;

import org.apache.activemq.common.example.ActiveMQExample;

/**
 * This example demonstrates the ability of ActiveMQ to send and consume a very large message, much
 * bigger than can fit in RAM.
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class LargeMessageExample extends ActiveMQExample
{
   public static void main(final String[] args)
   {
      new LargeMessageExample().run(args);
   }

   /**
    * The message we will send is size 10GiB, even though we are only running in 50MB of RAM on both
    * client and server.
    * <p>
    * This may take some considerable time to create, send and consume - if it takes too long or you
    * don't have enough disk space just reduce the file size here
    */
   private static final long FILE_SIZE = 2L * 1024 * 1024 * 1024; // 10 GiB message

   @Override
   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;

      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory. This ConnectionFactory has a special attribute set on
         // it.
         // Messages with more than 10K are considered large
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. Create the JMS objects
         connection = cf.createConnection();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(queue);

         // Step 5. Create a huge file - this will form the body of the message we will send.

         System.out.println("Creating a file to send of size " + FILE_SIZE +
                            " bytes. This may take a little while... " +
                            "If this is too big for your disk you can easily change the FILE_SIZE in the example.");

         File fileInput = new File("huge_message_to_send.dat");

         createFile(fileInput, FILE_SIZE);

         System.out.println("File created.");

         // Step 6. Create a BytesMessage
         BytesMessage message = session.createBytesMessage();

         // Step 7. We set the InputStream on the message. When sending the message will read the InputStream
         // until it gets EOF. In this case we point the InputStream at a file on disk, and it will suck up the entire
         // file, however we could use any InputStream not just a FileInputStream.
         FileInputStream fileInputStream = new FileInputStream(fileInput);
         BufferedInputStream bufferedInput = new BufferedInputStream(fileInputStream);

         message.setObjectProperty("JMS_HQ_InputStream", bufferedInput);

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

         killServer(0);

         // Give the server a little time to shutdown properly
         Thread.sleep(5000);

         reStartServer(0, 60000);

         System.out.println("Server restarted.");

         // Step 11. Now the server is restarted we can recreate the JMS Objects, and start the new connection

         initialContext = getContext(0);

         queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         connection = cf.createConnection();

         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer messageConsumer = session.createConsumer(queue);

         connection.start();

         System.out.println("Receiving message.");

         // Step 12. Receive the message. When we receive the large message we initially just receive the message with
         // an empty body.
         BytesMessage messageReceived = (BytesMessage)messageConsumer.receive(120000);

         System.out.println("Received message with: " + messageReceived.getLongProperty("_HQ_LARGE_SIZE") +
                            " bytes. Now streaming to file on disk.");

         // Step 13. We set an OutputStream on the message. This causes the message body to be written to the
         // OutputStream until there are no more bytes to be written.
         // You don't have to use a FileOutputStream, you can use any OutputStream.
         // You may choose to use the regular BytesMessage or
         // StreamMessage interface but this method is much faster for large messages.

         File outputFile = new File("huge_message_received.dat");

         FileOutputStream fileOutputStream = new FileOutputStream(outputFile);

         BufferedOutputStream bufferedOutput = new BufferedOutputStream(fileOutputStream);

         // Step 14. This will save the stream and wait until the entire message is written before continuing.
         messageReceived.setObjectProperty("JMS_HQ_SaveStream", bufferedOutput);

         fileOutputStream.close();

         System.out.println("File streamed to disk. Size of received file on disk is " + outputFile.length());

         return true;
      }
      finally
      {
         // Step 12. Be sure to close our resources!
         if (initialContext != null)
         {
            initialContext.close();
         }

         if (connection != null)
         {
            connection.close();
         }
      }
   }

   /**
    * @param file
    * @param fileSize
    * @throws FileNotFoundException
    * @throws IOException
    */
   private void createFile(final File file, final long fileSize) throws IOException
   {
      FileOutputStream fileOut = new FileOutputStream(file);
      BufferedOutputStream buffOut = new BufferedOutputStream(fileOut);
      byte[] outBuffer = new byte[1024 * 1024];
      for (long i = 0; i < fileSize; i += outBuffer.length)
      {
         buffOut.write(outBuffer);
      }
      buffOut.close();
   }

}

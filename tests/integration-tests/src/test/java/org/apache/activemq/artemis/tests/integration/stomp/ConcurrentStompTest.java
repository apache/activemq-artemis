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
package org.apache.activemq.tests.integration.stomp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.core.protocol.stomp.Stomp;
import org.junit.Assert;
import org.junit.Test;

public class ConcurrentStompTest extends StompTestBase
{
   private Socket stompSocket_2;

   private ByteArrayOutputStream inputBuffer_2;

   /**
    * Send messages on 1 socket and receives them concurrently on another socket.
    */
   @Test
   public void testSendManyMessages() throws Exception
   {
      try
      {
         String connect = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;

         sendFrame(connect);
         String connected = receiveFrame(10000);
         Assert.assertTrue(connected.startsWith("CONNECTED"));

         stompSocket_2 = createSocket();
         inputBuffer_2 = new ByteArrayOutputStream();

         sendFrame(stompSocket_2, connect);
         connected = receiveFrame(stompSocket_2, inputBuffer_2, 10000);
         Assert.assertTrue(connected.startsWith("CONNECTED"));

         final int count = 1000;
         final CountDownLatch latch = new CountDownLatch(count);

         String subscribe =
            "SUBSCRIBE\n" +
               "destination:" + getQueuePrefix() + getQueueName() + "\n" +
               "ack:auto\n\n" +
               Stomp.NULL;
         sendFrame(stompSocket_2, subscribe);
         Thread.sleep(2000);

         new Thread()
         {
            @Override
            public void run()
            {
               int i = 0;
               while (true)
               {
                  try
                  {
                     String frame = receiveFrame(stompSocket_2, inputBuffer_2, 10000);
                     Assert.assertTrue(frame.startsWith("MESSAGE"));
                     Assert.assertTrue(frame.indexOf("destination:") > 0);
                     System.out.println("<<< " + i++);
                     latch.countDown();
                  }
                  catch (Exception e)
                  {
                     break;
                  }
               }
            }
         }.start();

         String send = "SEND\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n";
         for (int i = 1; i <= count; i++)
         {
            // Thread.sleep(1);
            System.out.println(">>> " + i);
            sendFrame(send + "count:" + i + "\n\n" + Stomp.NULL);
         }

         assertTrue(latch.await(60, TimeUnit.SECONDS));

      }
      finally
      {
         stompSocket_2.close();
         inputBuffer_2.close();
      }


   }

   // Implementation methods
   // -------------------------------------------------------------------------
   public void sendFrame(Socket socket, String data) throws Exception
   {
      byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
      OutputStream outputStream = socket.getOutputStream();
      for (byte b : bytes)
      {
         outputStream.write(b);
      }
      outputStream.flush();
   }

   public String receiveFrame(Socket socket, ByteArrayOutputStream input, long timeOut) throws Exception
   {
      socket.setSoTimeout((int) timeOut);
      InputStream is = socket.getInputStream();
      int c = 0;
      for (;;)
      {
         c = is.read();
         if (c < 0)
         {
            throw new IOException("socket closed.");
         }
         else if (c == 0)
         {
            c = is.read();
            if (c != '\n')
            {
               byte[] ba = input.toByteArray();
               System.out.println(new String(ba, StandardCharsets.UTF_8));
            }
            Assert.assertEquals("Expecting stomp frame to terminate with \0\n", c, '\n');
            byte[] ba = input.toByteArray();
            input.reset();
            return new String(ba, StandardCharsets.UTF_8);
         }
         else
         {
            input.write(c);
         }
      }
   }

}

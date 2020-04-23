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

package org.apache.activemq.artemis.tests.integration.amqp.largemessages;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.junit.Assert;
import org.junit.Test;

public class InterruptedAMQPLargeMessage extends AmqpClientTestSupport {

   private static final int NUMBER_OF_THREADS = 10;
   private static final int MINIMAL_SEND = 2;

   private static final int MESSAGE_SIZE = 1024 * 300;

   private static final String smallFrameAcceptor = new String("tcp://localhost:" + (AMQP_PORT + 8));

   @Override
   protected void addAdditionalAcceptors(ActiveMQServer server) throws Exception {
      server.getConfiguration().addAcceptorConfiguration("flow", smallFrameAcceptor + "?protocols=AMQP;useEpoll=false;maxFrameSize=" + 512 + ";amqpMinLargeMessageSize=" + 10000);
   }

   public static void main(String[] arg) {
      // have everybody aligned on sending before we start
      CyclicBarrier startFlag = new CyclicBarrier(NUMBER_OF_THREADS);

      CountDownLatch minimalKill = new CountDownLatch(MINIMAL_SEND * NUMBER_OF_THREADS);
      Runnable runnable = () -> {

         try {
            AmqpClient client = createLocalClient();
            AmqpConnection connection = client.createConnection();
            connection.setMaxFrameSize(2 * 1024);
            connection.connect();
            AmqpSession session = connection.createSession();

            AmqpSender sender = session.createSender(arg[0]);
            startFlag.await();
            for (int m = 0; m < 1000; m++) {
               AmqpMessage message = new AmqpMessage();
               message.setDurable(true);
               byte[] bytes = new byte[MESSAGE_SIZE];
               for (int i = 0; i < bytes.length; i++) {
                  bytes[i] = (byte) 'z';
               }

               message.setBytes(bytes);
               sender.send(message);
               minimalKill.countDown();
            }
            connection.close();
         } catch (Exception e) {
            e.printStackTrace();
         }
      };


      for (int t = 0; t < NUMBER_OF_THREADS; t++) {
         Thread thread = new Thread(runnable);
         thread.start();
      }

      try {
         minimalKill.await();
      } catch (Exception e) {
         e.printStackTrace();
      }
      System.exit(-1);
   }

   private static AmqpClient createLocalClient() throws URISyntaxException {
      return new AmqpClient(new URI(smallFrameAcceptor), null, null);
   }

   @Test
   public void testInterruptedLargeMessage() throws Exception {
      Process p = SpawnedVMSupport.spawnVM(InterruptedAMQPLargeMessage.class.getName(), getQueueName());
      p.waitFor();

      Queue serverQueue = server.locateQueue(getQueueName());

      Assert.assertTrue(serverQueue.getMessageCount() >= MINIMAL_SEND * NUMBER_OF_THREADS);

      LinkedListIterator<MessageReference> browserIterator = serverQueue.browserIterator();

      while (browserIterator.hasNext()) {
         MessageReference ref = browserIterator.next();
         Message message = ref.getMessage();

         Assert.assertNotNull(message);
         Assert.assertTrue(message instanceof LargeServerMessage);

         Assert.assertFalse(((LargeServerMessage)message).hasPendingRecord());
      }
      browserIterator.close();

      instanceLog.debug("There are " + serverQueue.getMessageCount() + " on the queue");
      int messageCount = (int)serverQueue.getMessageCount();

      AmqpClient client = createLocalClient();
      AmqpConnection connection = addConnection(client.createConnection());
      connection.setMaxFrameSize(2 * 1024);
      connection.connect();
      AmqpSession session = connection.createSession();
      AmqpReceiver receiver = session.createReceiver(getQueueName());

      int received = 0;
      receiver.flow((int) (messageCount + 10));
      for (int m = 0; m < messageCount; m++) {
         receiver.flow(1);
         AmqpMessage message = receiver.receive(10, TimeUnit.SECONDS);
         Assert.assertNotNull(message);
         message.accept(true);
         received++;

         instanceLog.debug("Received " + received);
         Data data = (Data)message.getWrappedMessage().getBody();
         byte[] byteArray = data.getValue().getArray();

         Assert.assertEquals(MESSAGE_SIZE, byteArray.length);
         for (int i = 0; i < byteArray.length; i++) {
            Assert.assertEquals((byte)'z', byteArray[i]);
         }
      }


      Assert.assertNull(receiver.receiveNoWait());

      validateNoFilesOnLargeDir();
   }

}

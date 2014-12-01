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
package org.proton.plug.test.minimalserver;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.AMQPSessionContext;
import org.proton.plug.context.ProtonPlugSender;
import org.proton.plug.context.server.ProtonServerSessionContext;
import org.proton.plug.SASLResult;
import org.proton.plug.util.ProtonServerMessage;

/**
 * @author Clebert Suconic
 */

public class MinimalSessionSPI implements AMQPSessionCallback
{

   private SASLResult result;
   ProtonServerSessionContext session;

   @Override
   public void init(AMQPSessionContext session, SASLResult result)
   {
      this.session = (ProtonServerSessionContext) session;
      this.result = result;
   }

   @Override
   public void start()
   {
   }

   static AtomicInteger tempQueueGenerator = new AtomicInteger(0);

   public String tempQueueName()
   {
      return "TempQueueName" + tempQueueGenerator.incrementAndGet();
   }

   @Override
   public Object createSender(ProtonPlugSender plugSender, String queue, String filer, boolean browserOnly)
   {
      Consumer consumer = new Consumer(DumbServer.getQueue(queue));
      return consumer;
   }

   @Override
   public void startSender(Object brokerConsumer)
   {
      ((Consumer) brokerConsumer).start();
   }

   @Override
   public void createTemporaryQueue(String queueName)
   {

   }

   @Override
   public void onFlowConsumer(Object consumer, int credits)
   {
   }

   @Override
   public boolean queueQuery(String queueName)
   {
      return true;
   }

   @Override
   public void closeSender(Object brokerConsumer)
   {
      ((Consumer) brokerConsumer).close();
   }

   @Override
   public ProtonJMessage encodeMessage(Object message, int deliveryCount)
   {
      // We are storing internally as EncodedMessage on this minimalserver server
      return (ProtonServerMessage) message;
   }

   @Override
   public Binary getCurrentTXID()
   {
      return new Binary(new byte[]{1});
   }

   @Override
   public void commitCurrentTX()
   {
   }

   @Override
   public void rollbackCurrentTX()
   {
   }

   @Override
   public void close()
   {

   }

   @Override
   public void ack(Object brokerConsumer, Object message)
   {

   }

   @Override
   public void cancel(Object brokerConsumer, Object message, boolean updateCounts)
   {

   }

   @Override
   public void resumeDelivery(Object consumer)
   {
      System.out.println("Resume delivery!!!");
      ((Consumer) consumer).start();
   }

   @Override
   public void serverSend(Receiver receiver, Delivery delivery, String address, int messageFormat, ByteBuf buffer)
   {
      ProtonServerMessage serverMessage = new ProtonServerMessage();
      serverMessage.decode(buffer.nioBuffer());

      BlockingDeque<Object> queue = DumbServer.getQueue(address);
      queue.add(serverMessage);
   }


   class Consumer
   {
      final BlockingDeque<Object> queue;

      Consumer(BlockingDeque<Object> queue)
      {
         this.queue = queue;
      }

      boolean running = false;
      volatile Thread thread;

      public void close()
      {
         System.out.println("Closing!!!");
         running = false;
         if (thread != null)
         {
            try
            {
               thread.join();
            }
            catch (Throwable ignored)
            {
            }
         }

         thread = null;
      }

      public synchronized void start()
      {
         running = true;
         if (thread == null)
         {
            System.out.println("Start!!!");
            thread = new Thread()
            {
               public void run()
               {
                  try
                  {
                     while (running)
                     {
                        Object msg = queue.poll(1, TimeUnit.SECONDS);

                        if (msg != null)
                        {
                           session.serverDelivery(msg, Consumer.this, 1);
                        }
                     }
                  }
                  catch (Exception e)
                  {
                     e.printStackTrace();
                  }
               }
            };
            thread.start();
         }
      }

   }
}

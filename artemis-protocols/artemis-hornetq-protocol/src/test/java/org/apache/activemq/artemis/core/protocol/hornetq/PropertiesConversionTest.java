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

package org.apache.activemq.artemis.core.protocol.hornetq;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.MessagePacket;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class PropertiesConversionTest {

   class FakeMessagePacket extends MessagePacket {

      FakeMessagePacket(ICoreMessage message) {
         super(PacketImpl.SESS_SEND, message);
      }

      @Override
      public int expectedEncodeSize() {
         return 0;
      }

      @Override
      public void release() {

      }
   }

   @Test
   public void testParallelConversions() throws Throwable {
      CoreMessage coreMessage = new CoreMessage(1, 1024);
      for (int i = 0; i < 10; i++) {
         coreMessage.putBooleanProperty(SimpleString.toSimpleString("key1"), true);
      }
      coreMessage.putStringProperty(new SimpleString("_HQ_ORIG_ADDRESS"), SimpleString.toSimpleString("hqOne"));
      coreMessage.putStringProperty(new SimpleString("_AMQ_ORIG_QUEUE"), SimpleString.toSimpleString("amqOne"));

      int threads = 1000;
      int conversions = 100;
      Thread[] t = new Thread[threads];
      CyclicBarrier barrier = new CyclicBarrier(threads);
      HQPropertiesConversionInterceptor hq = new HQPropertiesConversionInterceptor(true);
      HQPropertiesConversionInterceptor amq = new HQPropertiesConversionInterceptor(true);
      AtomicInteger errors = new AtomicInteger(0);
      AtomicInteger counts = new AtomicInteger(0);

      for (int i = 0; i < threads; i++) {
         t[i] = new Thread() {

            @Override
            public void run() {
               try {
                  for (int i = 0; i < conversions; i++) {
                     counts.incrementAndGet();
                     FakeMessagePacket packetSend = new FakeMessagePacket(coreMessage);
                     if (i == 0) {
                        barrier.await();
                     }
                     hq.intercept(packetSend, null);
                     FakeMessagePacket packetRec = new FakeMessagePacket(coreMessage);
                     amq.intercept(packetRec, null);

                     // heads or tails here, I need part of the messages with a big header, part of the messages with a small header
                     boolean heads = RandomUtil.randomBoolean();

                     // this is playing with a scenario where the horentq interceptor will change the size of the message
                     if (heads) {
                        packetRec.getMessage().putStringProperty("propChanges", "looooooooooooooooooooong property text");
                     } else {
                        packetRec.getMessage().putStringProperty("propChanges", "short one");
                     }

                     ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(packetRec.getMessage().getEncodeSize() + 4);
                     packetRec.getMessage().sendBuffer_1X(buf);
                     buf.release();
                     if (i > conversions / 2) {
                        // I only validate half of the messages
                        // to give it a chance of Races and Exceptions
                        // that could happen from reusing the same message on these conversions
                        Assert.assertNotSame(packetRec.getMessage(), coreMessage);
                        Assert.assertNotSame(packetSend.getMessage(), coreMessage);
                     }
                  }
               } catch (Throwable e) {
                  errors.incrementAndGet();
                  e.printStackTrace();
               }
            }
         };
         t[i].start();
      }

      for (Thread thread : t) {
         thread.join();
      }

      Assert.assertEquals(threads * conversions, counts.get());

      Assert.assertEquals(0, errors.get());
   }

}

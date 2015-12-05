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
package org.apache.activemq.artemis.tests.integration.transports.netty;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.activemq.artemis.core.remoting.impl.netty.ActiveMQFrameDecoder2;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;

public class ActiveMQFrameDecoder2Test extends ActiveMQTestBase {

   private static final int MSG_CNT = 10000;

   private static final int MSG_LEN = 1000;

   private static final int FRAGMENT_MAX_LEN = 1500;

   private static final Random rand = new Random();

   @Test
   public void testOrdinaryFragmentation() throws Exception {
      final EmbeddedChannel decoder = new EmbeddedChannel(new ActiveMQFrameDecoder2());
      final byte[] data = new byte[ActiveMQFrameDecoder2Test.MSG_LEN];
      ActiveMQFrameDecoder2Test.rand.nextBytes(data);

      ByteBuf src = Unpooled.buffer(ActiveMQFrameDecoder2Test.MSG_CNT * (ActiveMQFrameDecoder2Test.MSG_LEN + 4));
      while (src.writerIndex() < src.capacity()) {
         src.writeInt(ActiveMQFrameDecoder2Test.MSG_LEN);
         src.writeBytes(data);
      }

      List<ByteBuf> packets = new ArrayList<>();
      while (src.isReadable()) {
         int length = Math.min(ActiveMQFrameDecoder2Test.rand.nextInt(ActiveMQFrameDecoder2Test.FRAGMENT_MAX_LEN), src.readableBytes());
         packets.add(src.readBytes(length));
      }

      int cnt = 0;
      for (ByteBuf p : packets) {
         decoder.writeInbound(p);
         for (;;) {
            ByteBuf frame = (ByteBuf) decoder.readInbound();
            if (frame == null) {
               break;
            }
            Assert.assertEquals(4, frame.readerIndex());
            Assert.assertEquals(ActiveMQFrameDecoder2Test.MSG_LEN, frame.readableBytes());
            Assert.assertEquals(Unpooled.wrappedBuffer(data), frame);
            cnt++;
            frame.release();
         }
      }
      Assert.assertEquals(ActiveMQFrameDecoder2Test.MSG_CNT, cnt);
   }

   @Test
   public void testExtremeFragmentation() throws Exception {
      final EmbeddedChannel decoder = new EmbeddedChannel(new ActiveMQFrameDecoder2());

      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[]{0}));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[]{0}));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[]{0}));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[]{4}));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[]{5}));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[]{6}));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[]{7}));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[]{8}));

      ByteBuf frame = (ByteBuf) decoder.readInbound();
      Assert.assertEquals(4, frame.readerIndex());
      Assert.assertEquals(4, frame.readableBytes());
      Assert.assertEquals(5, frame.getByte(4));
      Assert.assertEquals(6, frame.getByte(5));
      Assert.assertEquals(7, frame.getByte(6));
      Assert.assertEquals(8, frame.getByte(7));
      frame.release();
   }
}

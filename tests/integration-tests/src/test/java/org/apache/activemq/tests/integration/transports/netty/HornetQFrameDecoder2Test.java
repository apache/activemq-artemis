/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.tests.integration.transports.netty;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.activemq.core.remoting.impl.netty.HornetQFrameDecoder2;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A HornetQFrameDecoder2Test
 *
 * @author <a href="tlee@redhat.com">Trustin Lee</a>
 * @version $Revision$, $Date$
 */
public class HornetQFrameDecoder2Test extends UnitTestCase
{
   private static final int MSG_CNT = 10000;

   private static final int MSG_LEN = 1000;

   private static final int FRAGMENT_MAX_LEN = 1500;

   private static final Random rand = new Random();

   @Before
   public void setUp() throws Exception
   {
      super.setUp();
   }

   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   @Test
   public void testOrdinaryFragmentation() throws Exception
   {
      final EmbeddedChannel decoder = new EmbeddedChannel(new HornetQFrameDecoder2());
      final byte[] data = new byte[HornetQFrameDecoder2Test.MSG_LEN];
      HornetQFrameDecoder2Test.rand.nextBytes(data);

      ByteBuf src = Unpooled.buffer(HornetQFrameDecoder2Test.MSG_CNT * (HornetQFrameDecoder2Test.MSG_LEN + 4));
      while (src.writerIndex() < src.capacity())
      {
         src.writeInt(HornetQFrameDecoder2Test.MSG_LEN);
         src.writeBytes(data);
      }

      List<ByteBuf> packets = new ArrayList<ByteBuf>();
      while (src.isReadable())
      {
         int length = Math.min(HornetQFrameDecoder2Test.rand.nextInt(HornetQFrameDecoder2Test.FRAGMENT_MAX_LEN),
                               src.readableBytes());
         packets.add(src.readBytes(length));
      }

      int cnt = 0;
      for (ByteBuf p : packets)
      {
         decoder.writeInbound(p);
         for (;;)
         {
            ByteBuf frame = (ByteBuf) decoder.readInbound();
            if (frame == null)
            {
               break;
            }
            Assert.assertEquals(4, frame.readerIndex());
            Assert.assertEquals(HornetQFrameDecoder2Test.MSG_LEN, frame.readableBytes());
            Assert.assertEquals(Unpooled.wrappedBuffer(data), frame);
            cnt++;
            frame.release();
         }
      }
      Assert.assertEquals(HornetQFrameDecoder2Test.MSG_CNT, cnt);
   }

   @Test
   public void testExtremeFragmentation() throws Exception
   {
      final EmbeddedChannel decoder = new EmbeddedChannel(new HornetQFrameDecoder2());

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

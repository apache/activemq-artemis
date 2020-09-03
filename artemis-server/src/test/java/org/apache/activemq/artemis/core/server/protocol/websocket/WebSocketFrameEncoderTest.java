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
package org.apache.activemq.artemis.core.server.protocol.websocket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.nio.charset.StandardCharsets;
import java.util.List;

import io.netty.buffer.ByteBufUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

/**
 * WebSocketContinuationFrameEncoderTest
 */
@RunWith(MockitoJUnitRunner.class)
public class WebSocketFrameEncoderTest {

   private int maxFramePayloadLength = 100;
   private WebSocketFrameEncoder spy;

   @Mock
   private ChannelHandlerContext ctx;
   @Mock
   private ChannelPromise promise;

   @Before
   public void setUp() throws Exception {
      spy = spy(new WebSocketFrameEncoder(maxFramePayloadLength));
   }

   @Test
   public void testWriteNonByteBuf() throws Exception {
      Object msg = "Not a ByteBuf";

      spy.write(ctx, msg, promise); //test

      verify(spy).write(ctx, msg, promise);
      verify(ctx).write(msg, promise);
      verifyNoMoreInteractions(spy, ctx);
      verifyZeroInteractions(promise);
   }

   @Test
   public void testWriteReleaseBuffer() throws Exception {
      String content = "Buffer should be released";

      int utf8Bytes = ByteBufUtil.utf8Bytes(content);
      ByteBuf msg = Unpooled.directBuffer(utf8Bytes);
      ByteBufUtil.reserveAndWriteUtf8(msg, content, utf8Bytes);

      ArgumentCaptor<WebSocketFrame> frameCaptor = ArgumentCaptor.forClass(WebSocketFrame.class);

      spy.write(ctx, msg, promise); //test

      assertEquals(0, msg.refCnt());
      assertEquals(0, msg.readableBytes());
      verify(ctx).writeAndFlush(frameCaptor.capture(), eq(promise));
      WebSocketFrame frame = frameCaptor.getValue();
      assertTrue(frame instanceof BinaryWebSocketFrame);
      assertTrue(frame.isFinalFragment());
      assertEquals(content, frame.content().toString(StandardCharsets.UTF_8));
   }


   @Test
   public void testWriteSingleFrame() throws Exception {
      String content = "Content MSG length less than max frame payload length: " + maxFramePayloadLength;
      ByteBuf msg = Unpooled.copiedBuffer(content, StandardCharsets.UTF_8);
      ArgumentCaptor<WebSocketFrame> frameCaptor = ArgumentCaptor.forClass(WebSocketFrame.class);

      spy.write(ctx, msg, promise); //test

      assertEquals(0, msg.readableBytes());
      verify(ctx).writeAndFlush(frameCaptor.capture(), eq(promise));
      WebSocketFrame frame = frameCaptor.getValue();
      assertTrue(frame instanceof BinaryWebSocketFrame);
      assertTrue(frame.isFinalFragment());
      assertEquals(content, frame.content().toString(StandardCharsets.UTF_8));
   }

   @Test
   public void testWriteContinuationFrames() throws Exception {
      String contentPart = "Content MSG Length @ ";
      StringBuilder contentBuilder = new StringBuilder(3 * maxFramePayloadLength);

      while (contentBuilder.length() < 2 * maxFramePayloadLength) {
         contentBuilder.append(contentPart);
         contentBuilder.append(contentBuilder.length());
         contentBuilder.append('\n');
      }

      String content = contentBuilder.toString();
      int length = content.length();
      assertTrue(length > 2 * maxFramePayloadLength); //at least 3 frames of data
      ByteBuf msg = Unpooled.copiedBuffer(content, StandardCharsets.UTF_8);
      ArgumentCaptor<WebSocketFrame> frameCaptor = ArgumentCaptor.forClass(WebSocketFrame.class);

      spy.write(ctx, msg, promise); //test

      assertEquals(0, msg.readableBytes());
      verify(spy).write(ctx, msg, promise);
      verify(ctx, times(3)).writeAndFlush(frameCaptor.capture(), eq(promise));
      List<WebSocketFrame> frames = frameCaptor.getAllValues();
      assertEquals(3, frames.size());

      int offset = 0;
      WebSocketFrame first = frames.get(0);
      assertTrue(first instanceof BinaryWebSocketFrame);
      assertFalse(first.isFinalFragment());
      assertEquals(content.substring(offset, offset + maxFramePayloadLength),
                   first.content().toString(StandardCharsets.UTF_8));

      offset += maxFramePayloadLength;
      WebSocketFrame second = frames.get(1);
      assertTrue(second instanceof ContinuationWebSocketFrame);
      assertFalse(second.isFinalFragment());
      assertEquals(content.substring(offset, offset + maxFramePayloadLength),
                   second.content().toString(StandardCharsets.UTF_8));

      offset += maxFramePayloadLength;
      WebSocketFrame last = frames.get(2);
      assertTrue(last instanceof ContinuationWebSocketFrame);
      assertTrue(last.isFinalFragment());
      assertEquals(content.substring(offset), last.content().toString(StandardCharsets.UTF_8));

      verifyNoMoreInteractions(spy, ctx);
      verifyZeroInteractions(promise);
   }
}

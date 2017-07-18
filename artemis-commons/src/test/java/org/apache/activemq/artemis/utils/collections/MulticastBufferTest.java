/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.utils.collections;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import io.netty.util.internal.PlatformDependent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MulticastBufferTest {

   private MulticastBuffer.Reader sampleReader;
   private MulticastBuffer.Writer sampleWriter;

   @Before
   public void init() {
      final int messageSize = Integer.BYTES;
      final int requiredSize = MulticastBuffer.requiredSize(messageSize);
      final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(requiredSize);
      byteBuffer.order(ByteOrder.nativeOrder());
      this.sampleWriter = MulticastBuffer.writer(byteBuffer, messageSize);
      this.sampleReader = MulticastBuffer.reader(byteBuffer, messageSize);
   }

   private static void publish(MulticastBuffer.Writer sampleWriter, int messageId) {
      final long version = sampleWriter.claim();
      try {
         messageId(sampleWriter.buffer(), messageId);
      } finally {
         sampleWriter.commit(version);
      }
   }

   private static void messageId(ByteBuffer buffer, int messageId) {
      buffer.putInt(0, messageId);
   }

   private static int messageId(ByteBuffer buffer) {
      return buffer.getInt(0);
   }

   @Test
   public void noLostWhenEmpty() {
      Assert.assertFalse("can't read if not published", this.sampleReader.hasNext());
      Assert.assertEquals("can't loss when no message is present", 0, this.sampleReader.lost());
   }

   @Test
   public void countLost() {
      Assert.assertFalse("can't read if not published", this.sampleReader.hasNext());
      Assert.assertEquals("can't loss when no message is present", 0, this.sampleReader.lost());
      final int writtenMessages = 10_000;
      for (int i = 0; i < writtenMessages; i++) {
         publish(sampleWriter, 0);
      }
      Assert.assertTrue("the is at least 1 message to be read", this.sampleReader.hasNext());
      Assert.assertEquals("the overwritten messages are lost", writtenMessages - 1, this.sampleReader.lost());
      Assert.assertTrue("the last message can be read", this.sampleReader.validateMessage());
      Assert.assertEquals("the reader is keeping up", writtenMessages - 1, this.sampleReader.lost());
      publish(sampleWriter, 0);
      Assert.assertTrue("a new message is arrived", this.sampleReader.hasNext());
      Assert.assertEquals("the reader is keeping up", writtenMessages - 1, this.sampleReader.lost());
      publish(sampleWriter, 0);
      Assert.assertEquals("the reader is keeping up", writtenMessages - 1, this.sampleReader.lost());
      Assert.assertFalse("the last perceived message is being overwritten", this.sampleReader.validateMessage());
      Assert.assertEquals("the reader has lost a another message", writtenMessages, this.sampleReader.lost());
   }

   @Test
   public void orderedReadWithNoOverwrites() {
      Assert.assertFalse("can't read if not published", this.sampleReader.hasNext());
      for (int i = 0; i < 10_000; i++) {
         final int writtenMessageId = i + 1;
         publish(sampleWriter, writtenMessageId);
         Assert.assertTrue("there is something to read", this.sampleReader.hasNext());
         final int readMessageId = messageId(this.sampleReader.next());
         Assert.assertEquals("can't read something not written", writtenMessageId, readMessageId);
      }
      Assert.assertFalse("can't read if not published", this.sampleReader.hasNext());
   }

   @Test
   public void orderedReadWithOverwrites() {
      Assert.assertFalse("can't read if not published", this.sampleReader.hasNext());
      int lastWrittenMessage = 0;
      for (int i = 0; i < 10_000; i++) {
         final int writtenMessageId = i + 1;
         publish(sampleWriter, writtenMessageId);
         lastWrittenMessage = writtenMessageId;
      }
      Assert.assertTrue("the writer has written some value", this.sampleReader.hasNext());
      final int readMessageId = messageId(this.sampleReader.next());
      Assert.assertEquals("can't read something not written", lastWrittenMessage, readMessageId);
      Assert.assertFalse("can't read if not published", this.sampleReader.hasNext());
   }

   @Test
   public void monotonicVersionChangesOnWrites() {
      final int messages = 10_000;
      Assert.assertFalse("can't read if not published", this.sampleReader.hasNext());
      for (int i = 0; i < messages; i++) {
         publish(sampleWriter, 1);
      }
      Assert.assertEquals("the reader has no messages yet", 0, this.sampleReader.version());
      Assert.assertTrue("the writer has written some value", this.sampleReader.hasNext());
      Assert.assertEquals("the reader has read the " + messages + "th message", messages, this.sampleReader.version());
   }

   @Test
   public void invalidMessageWhenOverwritten() {
      Assert.assertFalse("can't read if not published", this.sampleReader.hasNext());
      int messageId = 0;
      publish(sampleWriter, ++messageId);
      final long version = sampleWriter.claim();
      Assert.assertTrue("claim doesn't invalidate the readable message", this.sampleReader.hasNext());
      Assert.assertFalse("claim invalidates the validity of the readable message", this.sampleReader.validateMessage());
      try {
         messageId(sampleWriter.buffer(), ++messageId);
      } finally {
         sampleWriter.commit(version);
      }
      Assert.assertTrue("the writer has written some value", this.sampleReader.hasNext());
      final int readMessageId = messageId(this.sampleReader.next());
      Assert.assertEquals("can't read something not written", messageId, readMessageId);
      Assert.assertFalse("can't read if not published", this.sampleReader.hasNext());
   }

   @After
   public void freeByteBuffers() {
      PlatformDependent.freeDirectBuffer(this.sampleWriter.buffer());
   }

}

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

package org.apache.activemq.artemis.protocol.amqp.broker;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

public class AmqpReadableBuffer implements ReadableBuffer {
   private ByteBuffer buffer;

   public AmqpReadableBuffer(ByteBuffer buffer) {
      this.buffer = buffer;
   }

   public ByteBuffer getBuffer() {
      return this.buffer;
   }

   @Override
   public int capacity() {
      return this.buffer.capacity();
   }

   @Override
   public boolean hasArray() {
      return this.buffer.hasArray();
   }

   @Override
   public byte[] array() {
      if (this.buffer.hasArray()) {
         return this.buffer.array();
      } else {
         byte[] bytes = new byte[buffer.remaining()];
         buffer.get(bytes);
         return bytes;
      }
   }

   public void freeDirectBuffer() {
      // releasing direct buffer created from mmap
      PlatformDependent.freeDirectBuffer(buffer);
   }

   @Override
   public int arrayOffset() {
      return this.buffer.arrayOffset();
   }

   @Override
   public ReadableBuffer reclaimRead() {
      return this;
   }

   @Override
   public byte get() {
      return this.buffer.get();
   }

   @Override
   public byte get(int index) {
      return this.buffer.get(index);
   }

   @Override
   public int getInt() {
      return this.buffer.getInt();
   }

   @Override
   public long getLong() {
      return this.buffer.getLong();
   }

   @Override
   public short getShort() {
      return this.buffer.getShort();
   }

   @Override
   public float getFloat() {
      return this.buffer.getFloat();
   }

   @Override
   public double getDouble() {
      return this.buffer.getDouble();
   }

   @Override
   public ReadableBuffer get(byte[] target, int offset, int length) {
      this.buffer.get(target, offset, length);
      return this;
   }

   @Override
   public ReadableBuffer get(byte[] target) {
      this.buffer.get(target);
      return this;
   }

   @Override
   public ReadableBuffer get(WritableBuffer target) {
      int start = target.position();
      if (this.buffer.hasArray()) {
         target.put(this.buffer.array(), this.buffer.arrayOffset() + this.buffer.position(), this.buffer.remaining());
      } else {
         target.put(this.buffer);
      }

      int written = target.position() - start;
      this.buffer.position(this.buffer.position() + written);
      return this;
   }

   @Override
   public ReadableBuffer slice() {
      return new AmqpReadableBuffer(this.buffer.slice());
   }

   @Override
   public ReadableBuffer flip() {
      this.buffer.flip();
      return this;
   }

   @Override
   public ReadableBuffer limit(int limit) {
      this.buffer.limit(limit);
      return this;
   }

   @Override
   public int limit() {
      return this.buffer.limit();
   }

   @Override
   public ReadableBuffer position(int position) {
      this.buffer.position(position);
      return this;
   }

   @Override
   public int position() {
      return this.buffer.position();
   }

   @Override
   public ReadableBuffer mark() {
      this.buffer.mark();
      return this;
   }

   @Override
   public ReadableBuffer reset() {
      this.buffer.reset();
      return this;
   }

   @Override
   public ReadableBuffer rewind() {
      this.buffer.rewind();
      return this;
   }

   @Override
   public ReadableBuffer clear() {
      this.buffer.clear();
      return this;
   }

   @Override
   public int remaining() {
      return this.buffer.remaining();
   }

   @Override
   public boolean hasRemaining() {
      return this.buffer.hasRemaining();
   }

   @Override
   public ReadableBuffer duplicate() {
      return new AmqpReadableBuffer(this.buffer.duplicate());
   }

   @Override
   public ByteBuffer byteBuffer() {
      return this.buffer;
   }

   @Override
   public String readUTF8() throws CharacterCodingException {
      ByteBuf wrappedNetty = Unpooled.wrappedBuffer(buffer);
      return wrappedNetty.toString(StandardCharsets.UTF_8);
   }

   @Override
   public String readString(CharsetDecoder decoder) throws CharacterCodingException {
      ByteBuf wrappedNetty = Unpooled.wrappedBuffer(buffer);
      return wrappedNetty.toString(StandardCharsets.UTF_8);
   }
}

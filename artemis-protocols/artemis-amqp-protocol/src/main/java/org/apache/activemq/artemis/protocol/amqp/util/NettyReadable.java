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

package org.apache.activemq.artemis.protocol.amqp.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import org.apache.qpid.proton.codec.ReadableBuffer;

public class NettyReadable implements ReadableBuffer {

   private static final Charset Charset_UTF8 = Charset.forName("UTF-8");

   private final ByteBuf buffer;

   public NettyReadable(ByteBuf buffer) {
      this.buffer = buffer;
   }

   @Override
   public void put(ReadableBuffer other) {
      buffer.writeBytes(other.byteBuffer());
   }

   @Override
   public byte get() {
      return buffer.readByte();
   }

   @Override
   public int getInt() {
      return buffer.readInt();
   }

   @Override
   public long getLong() {
      return buffer.readLong();
   }

   @Override
   public short getShort() {
      return buffer.readShort();
   }

   @Override
   public float getFloat() {
      return buffer.readFloat();
   }

   @Override
   public double getDouble() {
      return buffer.readDouble();
   }

   @Override
   public ReadableBuffer get(byte[] data, int offset, int length) {
      buffer.readBytes(data, offset, length);
      return this;
   }

   @Override
   public ReadableBuffer get(byte[] data) {
      buffer.readBytes(data);
      return this;
   }

   @Override
   public ReadableBuffer position(int position) {
      buffer.readerIndex(position);
      return this;
   }

   @Override
   public ReadableBuffer slice() {
      return new NettyReadable(buffer.slice());
   }

   @Override
   public ReadableBuffer flip() {
      return new NettyReadable(buffer.duplicate().setIndex(0, buffer.readerIndex()));
   }

   @Override
   public ReadableBuffer limit(int limit) {
      buffer.writerIndex(limit);
      return this;
   }

   @Override
   public int limit() {
      return buffer.writerIndex();
   }

   @Override
   public int remaining() {
      return buffer.readableBytes();
   }

   @Override
   public int position() {
      return buffer.readerIndex();
   }

   @Override
   public boolean hasRemaining() {
      return buffer.readableBytes() > 0;
   }

   @Override
   public ReadableBuffer duplicate() {
      return new NettyReadable(buffer.duplicate());
   }

   @Override
   public ByteBuffer byteBuffer() {
      return buffer.nioBuffer(0, buffer.writerIndex());
   }

   @Override
   public String readUTF8() {
      return buffer.toString(Charset_UTF8);
   }
}

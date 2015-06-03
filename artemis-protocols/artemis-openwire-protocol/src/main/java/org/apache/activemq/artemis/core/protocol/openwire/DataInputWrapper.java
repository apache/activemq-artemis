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
package org.apache.activemq.artemis.core.protocol.openwire;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.utils.UTF8Util;
import org.apache.activemq.artemis.utils.UTF8Util.StringUtilBuffer;

public class DataInputWrapper implements DataInput
{
   private static final int DEFAULT_CAPACITY = 1024 * 1024;
   private static final NotEnoughBytesException exception = new NotEnoughBytesException();
   private ByteBuffer internalBuffer;

   public DataInputWrapper()
   {
      this(DEFAULT_CAPACITY);
   }

   public DataInputWrapper(int capacity)
   {
      this.internalBuffer = ByteBuffer.allocateDirect(capacity);
      this.internalBuffer.mark();
      this.internalBuffer.limit(0);
   }

   public void receiveData(byte[] data)
   {
      int newSize = data.length;
      int freeSpace = internalBuffer.capacity() - internalBuffer.limit();
      if (freeSpace < newSize)
      {
         internalBuffer.reset();
         internalBuffer.compact();
         if (internalBuffer.remaining() < newSize)
         {
            //need to enlarge
         }
         //make sure mark is at zero and position is at effective limit
         int pos = internalBuffer.position();
         internalBuffer.position(0);
         internalBuffer.mark();
         internalBuffer.position(pos);
      }
      else
      {
         internalBuffer.position(internalBuffer.limit());
         internalBuffer.limit(internalBuffer.capacity());
      }
      internalBuffer.put(data);
      internalBuffer.limit(internalBuffer.position());
      internalBuffer.reset();
   }

   public void receiveData(ActiveMQBuffer buffer)
   {
      int newSize = buffer.readableBytes();
      byte[] newData = new byte[newSize];
      buffer.readBytes(newData);
      this.receiveData(newData);
   }

   //invoke after each successful unmarshall
   public void mark()
   {
      this.internalBuffer.mark();
   }

   @Override
   public void readFully(byte[] b) throws IOException
   {
      readFully(b, 0, b.length);
   }

   private void checkSize(int n) throws NotEnoughBytesException
   {
      if (internalBuffer.remaining() < n)
      {
         throw exception;
      }
   }

   @Override
   public void readFully(byte[] b, int off, int len) throws IOException
   {
      checkSize(len);
      internalBuffer.get(b, off, len);
   }

   @Override
   public int skipBytes(int n) throws IOException
   {
      checkSize(n);
      int pos = internalBuffer.position();
      internalBuffer.position(pos + n);
      return n;
   }

   @Override
   public boolean readBoolean() throws IOException
   {
      checkSize(1);
      byte b = internalBuffer.get();
      return b != 0;
   }

   @Override
   public byte readByte() throws IOException
   {
      checkSize(1);
      return this.internalBuffer.get();
   }

   @Override
   public int readUnsignedByte() throws IOException
   {
      checkSize(1);
      return 0xFF & this.internalBuffer.get();
   }

   @Override
   public short readShort() throws IOException
   {
      checkSize(2);
      return this.internalBuffer.getShort();
   }

   @Override
   public int readUnsignedShort() throws IOException
   {
      checkSize(2);
      return 0xFFFF & this.internalBuffer.getShort();
   }

   @Override
   public char readChar() throws IOException
   {
      checkSize(2);
      return this.internalBuffer.getChar();
   }

   @Override
   public int readInt() throws IOException
   {
      checkSize(4);
      return this.internalBuffer.getInt();
   }

   @Override
   public long readLong() throws IOException
   {
      checkSize(8);
      return this.internalBuffer.getLong();
   }

   @Override
   public float readFloat() throws IOException
   {
      checkSize(4);
      return this.internalBuffer.getFloat();
   }

   @Override
   public double readDouble() throws IOException
   {
      checkSize(8);
      return this.internalBuffer.getDouble();
   }

   @Override
   public String readLine() throws IOException
   {
      StringBuilder sb = new StringBuilder("");
      char c = this.readChar();
      while (c != '\n')
      {
         sb.append(c);
         c = this.readChar();
      }
      return sb.toString();
   }

   @Override
   public String readUTF() throws IOException
   {
      StringUtilBuffer buffer = UTF8Util.getThreadLocalBuffer();

      final int size = this.readUnsignedShort();

      if (size > buffer.byteBuffer.length)
      {
         buffer.resizeByteBuffer(size);
      }

      if (size > buffer.charBuffer.length)
      {
         buffer.resizeCharBuffer(size);
      }

      int count = 0;
      int byte1, byte2, byte3;
      int charCount = 0;

      this.readFully(buffer.byteBuffer, 0, size);

      while (count < size)
      {
         byte1 = buffer.byteBuffer[count++];

         if (byte1 > 0 && byte1 <= 0x7F)
         {
            buffer.charBuffer[charCount++] = (char)byte1;
         }
         else
         {
            int c = byte1 & 0xff;
            switch (c >> 4)
            {
               case 0xc:
               case 0xd:
                  byte2 = buffer.byteBuffer[count++];
                  buffer.charBuffer[charCount++] = (char)((c & 0x1F) << 6 | byte2 & 0x3F);
                  break;
               case 0xe:
                  byte2 = buffer.byteBuffer[count++];
                  byte3 = buffer.byteBuffer[count++];
                  buffer.charBuffer[charCount++] = (char)((c & 0x0F) << 12 | (byte2 & 0x3F) << 6 | (byte3 & 0x3F) << 0);
                  break;
               default:
                  throw new InternalError("unhandled utf8 byte " + c);
            }
         }
      }

      return new String(buffer.charBuffer, 0, charCount);
   }

   public boolean readable()
   {
      return this.internalBuffer.hasRemaining();
   }

}

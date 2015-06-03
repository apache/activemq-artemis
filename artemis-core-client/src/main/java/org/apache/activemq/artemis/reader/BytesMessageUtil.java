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
package org.apache.activemq.artemis.reader;

import org.apache.activemq.artemis.api.core.Message;

public class BytesMessageUtil extends MessageUtil
{

   public static boolean bytesReadBoolean(Message message)
   {
      return getBodyBuffer(message).readBoolean();
   }

   public static byte bytesReadByte(Message message)
   {
      return getBodyBuffer(message).readByte();
   }

   public static int bytesReadUnsignedByte(Message message)
   {
      return getBodyBuffer(message).readUnsignedByte();
   }

   public static short bytesReadShort(Message message)
   {
      return getBodyBuffer(message).readShort();
   }

   public static int bytesReadUnsignedShort(Message message)
   {
      return getBodyBuffer(message).readUnsignedShort();
   }

   public static char bytesReadChar(Message message)
   {
      return (char)getBodyBuffer(message).readShort();
   }

   public static int bytesReadInt(Message message)
   {
      return getBodyBuffer(message).readInt();
   }

   public static long bytesReadLong(Message message)
   {
      return getBodyBuffer(message).readLong();
   }

   public static float bytesReadFloat(Message message)
   {
      return Float.intBitsToFloat(getBodyBuffer(message).readInt());
   }

   public static double bytesReadDouble(Message message)
   {
      return Double.longBitsToDouble(getBodyBuffer(message).readLong());
   }

   public static String bytesReadUTF(Message message)
   {
      return getBodyBuffer(message).readUTF();
   }



   public static int bytesReadBytes(Message message, final byte[] value)
   {
      return bytesReadBytes(message, value, value.length);
   }

   public static int bytesReadBytes(Message message, final byte[] value, final int length)
   {
      if (!getBodyBuffer(message).readable())
      {
         return -1;
      }

      int read = Math.min(length, getBodyBuffer(message).readableBytes());

      if (read != 0)
      {
         getBodyBuffer(message).readBytes(value, 0, read);
      }

      return read;

   }


   public static void bytesWriteBoolean(Message message, boolean value)
   {
      getBodyBuffer(message).writeBoolean(value);
   }



   public static void bytesWriteByte(Message message, byte value)
   {
      getBodyBuffer(message).writeByte(value);
   }



   public static void bytesWriteShort(Message message, short value)
   {
      getBodyBuffer(message).writeShort(value);
   }


   public static void bytesWriteChar(Message message, char value)
   {
      getBodyBuffer(message).writeShort((short)value);
   }

   public static void bytesWriteInt(Message message, int value)
   {
      getBodyBuffer(message).writeInt(value);
   }

   public static void bytesWriteLong(Message message, long value)
   {
      getBodyBuffer(message).writeLong(value);
   }

   public static void bytesWriteFloat(Message message, float value)
   {
      getBodyBuffer(message).writeInt(Float.floatToIntBits(value));
   }

   public static void bytesWriteDouble(Message message, double value)
   {
      getBodyBuffer(message).writeLong(Double.doubleToLongBits(value));
   }

   public static void bytesWriteUTF(Message message, String value)
   {
      getBodyBuffer(message).writeUTF(value);
   }

   public static void bytesWriteBytes(Message message, byte[] value)
   {
      getBodyBuffer(message).writeBytes(value);
   }

   public static void bytesWriteBytes(Message message, final byte[] value, final int offset, final int length)
   {
      getBodyBuffer(message).writeBytes(value, offset, length);
   }


   /**
    * Returns true if it could send the Object to any known format
    * @param message
    * @param value
    * @return
    */
   public static boolean bytesWriteObject(Message message, Object value)
   {
      if (value == null)
      {
         throw new NullPointerException("Attempt to write a null value");
      }
      if (value instanceof String)
      {
         bytesWriteUTF(message, (String) value);
      }
      else if (value instanceof Boolean)
      {
         bytesWriteBoolean(message, (Boolean) value);
      }
      else if (value instanceof Character)
      {
         bytesWriteChar(message, (Character) value);
      }
      else if (value instanceof Byte)
      {
         bytesWriteByte(message, (Byte) value);
      }
      else if (value instanceof Short)
      {
         bytesWriteShort(message, (Short) value);
      }
      else if (value instanceof Integer)
      {
         bytesWriteInt(message, (Integer) value);
      }
      else if (value instanceof Long)
      {
         bytesWriteLong(message, (Long) value);
      }
      else if (value instanceof Float)
      {
         bytesWriteFloat(message, (Float) value);
      }
      else if (value instanceof Double)
      {
         bytesWriteDouble(message, (Double) value);
      }
      else if (value instanceof byte[])
      {
         bytesWriteBytes(message, (byte[]) value);
      }
      else
      {
         return false;
      }


      return true;
   }

   public static void bytesMessageReset(Message message)
   {
      getBodyBuffer(message).resetReaderIndex();
   }

}

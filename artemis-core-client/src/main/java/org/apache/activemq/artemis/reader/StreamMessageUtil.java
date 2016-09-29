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

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.utils.DataConstants;

public class StreamMessageUtil extends MessageUtil {

   /**
    * Method to read boolean values out of the Stream protocol existent on JMS Stream Messages
    * Throws IllegalStateException if the type was invalid
    *
    * @param buff
    * @return
    */
   public static boolean streamReadBoolean(ActiveMQBuffer buff) {
      byte type = buff.readByte();

      switch (type) {
         case DataConstants.BOOLEAN:
            return buff.readBoolean();
         case DataConstants.STRING:
            String s = buff.readNullableString();
            return Boolean.valueOf(s);
         default:
            throw new IllegalStateException("Invalid conversion, type byte was " + type);
      }

   }

   public static byte streamReadByte(ActiveMQBuffer buff) {
      int index = buff.readerIndex();
      try {
         byte type = buff.readByte();
         switch (type) {
            case DataConstants.BYTE:
               return buff.readByte();
            case DataConstants.STRING:
               String s = buff.readNullableString();
               return Byte.parseByte(s);
            default:
               throw new IllegalStateException("Invalid conversion");
         }
      } catch (NumberFormatException e) {
         buff.readerIndex(index);
         throw e;
      }

   }

   public static short streamReadShort(ActiveMQBuffer buff) {
      byte type = buff.readByte();
      switch (type) {
         case DataConstants.BYTE:
            return buff.readByte();
         case DataConstants.SHORT:
            return buff.readShort();
         case DataConstants.STRING:
            String s = buff.readNullableString();
            return Short.parseShort(s);
         default:
            throw new IllegalStateException("Invalid conversion");
      }
   }

   public static char streamReadChar(ActiveMQBuffer buff) {
      byte type = buff.readByte();
      switch (type) {
         case DataConstants.CHAR:
            return (char) buff.readShort();
         case DataConstants.STRING:
            String str = buff.readNullableString();
            if (str == null) {
               throw new NullPointerException("Invalid conversion");
            } else {
               throw new IllegalStateException("Invalid conversion");
            }
         default:
            throw new IllegalStateException("Invalid conversion");
      }

   }

   public static int streamReadInteger(ActiveMQBuffer buff) {
      byte type = buff.readByte();
      switch (type) {
         case DataConstants.BYTE:
            return buff.readByte();
         case DataConstants.SHORT:
            return buff.readShort();
         case DataConstants.INT:
            return buff.readInt();
         case DataConstants.STRING:
            String s = buff.readNullableString();
            return Integer.parseInt(s);
         default:
            throw new IllegalStateException("Invalid conversion");
      }
   }

   public static long streamReadLong(ActiveMQBuffer buff) {
      byte type = buff.readByte();
      switch (type) {
         case DataConstants.BYTE:
            return buff.readByte();
         case DataConstants.SHORT:
            return buff.readShort();
         case DataConstants.INT:
            return buff.readInt();
         case DataConstants.LONG:
            return buff.readLong();
         case DataConstants.STRING:
            String s = buff.readNullableString();
            return Long.parseLong(s);
         default:
            throw new IllegalStateException("Invalid conversion");
      }
   }

   public static float streamReadFloat(ActiveMQBuffer buff) {
      byte type = buff.readByte();
      switch (type) {
         case DataConstants.FLOAT:
            return Float.intBitsToFloat(buff.readInt());
         case DataConstants.STRING:
            String s = buff.readNullableString();
            return Float.parseFloat(s);
         default:
            throw new IllegalStateException("Invalid conversion");
      }
   }

   public static double streamReadDouble(ActiveMQBuffer buff) {
      byte type = buff.readByte();
      switch (type) {
         case DataConstants.FLOAT:
            return Float.intBitsToFloat(buff.readInt());
         case DataConstants.DOUBLE:
            return Double.longBitsToDouble(buff.readLong());
         case DataConstants.STRING:
            String s = buff.readNullableString();
            return Double.parseDouble(s);
         default:
            throw new IllegalStateException("Invalid conversion: " + type);
      }
   }

   public static String streamReadString(ActiveMQBuffer buff) {
      byte type = buff.readByte();
      switch (type) {
         case DataConstants.BOOLEAN:
            return String.valueOf(buff.readBoolean());
         case DataConstants.BYTE:
            return String.valueOf(buff.readByte());
         case DataConstants.SHORT:
            return String.valueOf(buff.readShort());
         case DataConstants.CHAR:
            return String.valueOf((char) buff.readShort());
         case DataConstants.INT:
            return String.valueOf(buff.readInt());
         case DataConstants.LONG:
            return String.valueOf(buff.readLong());
         case DataConstants.FLOAT:
            return String.valueOf(Float.intBitsToFloat(buff.readInt()));
         case DataConstants.DOUBLE:
            return String.valueOf(Double.longBitsToDouble(buff.readLong()));
         case DataConstants.STRING:
            return buff.readNullableString();
         default:
            throw new IllegalStateException("Invalid conversion");
      }
   }

   /**
    * Utility for reading bytes out of streaming.
    * It will return remainingBytes, bytesRead
    *
    * @param remainingBytes remaining Bytes from previous read. Send it to 0 if it was the first call for the message
    * @param buff
    * @return a pair of remaining bytes and bytes read
    */
   public static Pair<Integer, Integer> streamReadBytes(ActiveMQBuffer buff, int remainingBytes, byte[] value) {
      if (remainingBytes == -1) {
         return new Pair<>(0, -1);
      } else if (remainingBytes == 0) {
         byte type = buff.readByte();
         if (type != DataConstants.BYTES) {
            throw new IllegalStateException("Invalid conversion");
         }
         remainingBytes = buff.readInt();
      }
      int read = Math.min(value.length, remainingBytes);
      buff.readBytes(value, 0, read);
      remainingBytes -= read;
      if (remainingBytes == 0) {
         remainingBytes = -1;
      }
      return new Pair<>(remainingBytes, read);

   }

   public static Object streamReadObject(ActiveMQBuffer buff) {
      byte type = buff.readByte();
      switch (type) {
         case DataConstants.BOOLEAN:
            return buff.readBoolean();
         case DataConstants.BYTE:
            return buff.readByte();
         case DataConstants.SHORT:
            return buff.readShort();
         case DataConstants.CHAR:
            return (char) buff.readShort();
         case DataConstants.INT:
            return buff.readInt();
         case DataConstants.LONG:
            return buff.readLong();
         case DataConstants.FLOAT:
            return Float.intBitsToFloat(buff.readInt());
         case DataConstants.DOUBLE:
            return Double.longBitsToDouble(buff.readLong());
         case DataConstants.STRING:
            return buff.readNullableString();
         case DataConstants.BYTES:
            int bufferLen = buff.readInt();
            byte[] bytes = new byte[bufferLen];
            buff.readBytes(bytes);
            return bytes;
         default:
            throw new IllegalStateException("Invalid conversion");
      }

   }

}

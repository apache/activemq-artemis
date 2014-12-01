/**
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
package org.apache.activemq.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;

/**
 * @author Clebert Suconic
 */

public class ByteUtil
{

   private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

   public static String maxString(String value, int size)
   {
      if (value.length() < size)
      {
         return value;
      }
      else
      {
         return value.substring(0, size / 2) + " ... " + value.substring(value.length() - size / 2);
      }
   }
   public static String bytesToHex(byte[] bytes, int groupSize)
   {
      if (bytes == null)
      {
         return "null";
      }
      else
      {
         char[] hexChars = new char[bytes.length * 2 + numberOfGroups(bytes, groupSize)];
         int outPos = 0;
         for (int j = 0; j < bytes.length; j++)
         {
            if (j > 0 && j % groupSize == 0)
            {
               hexChars[outPos++] = ' ';
            }
            int v = bytes[j] & 0xFF;
            hexChars[outPos++] = hexArray[v >>> 4];
            hexChars[outPos++] = hexArray[v & 0x0F];
         }
         return new String(hexChars);
      }
   }

   private static int numberOfGroups(byte[] bytes, int groupSize)
   {
      int groups = bytes.length / groupSize;

      if (bytes.length % groupSize == 0)
      {
         groups--;
      }

      return groups;
   }

   public static byte[] longToBytes(long x)
   {
      ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.heapBuffer(8, 8);
      buffer.writeLong(x);
      return buffer.array();
   }

}

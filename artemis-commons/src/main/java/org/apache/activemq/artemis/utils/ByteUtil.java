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
package org.apache.activemq.artemis.utils;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.logs.ActiveMQUtilBundle;
import org.jboss.logging.Logger;

public class ByteUtil {

   public static final String NON_ASCII_STRING = "@@@@@";

   private static final char[] hexArray = "0123456789ABCDEF".toCharArray();
   private static final String prefix = "^\\s*(\\d+)\\s*";
   private static final String suffix = "(b)?\\s*$";
   private static final Pattern ONE = Pattern.compile(prefix + suffix, Pattern.CASE_INSENSITIVE);
   private static final Pattern KILO = Pattern.compile(prefix + "k" + suffix, Pattern.CASE_INSENSITIVE);
   private static final Pattern MEGA = Pattern.compile(prefix + "m" + suffix, Pattern.CASE_INSENSITIVE);
   private static final Pattern GIGA = Pattern.compile(prefix + "g" + suffix, Pattern.CASE_INSENSITIVE);

   public static void debugFrame(Logger logger, String message, ByteBuf byteIn) {
      if (logger.isTraceEnabled()) {
         int location = byteIn.readerIndex();
         // debugging
         byte[] frame = new byte[byteIn.writerIndex()];
         byteIn.readBytes(frame);

         try {
            logger.trace(message + "\n" + ByteUtil.formatGroup(ByteUtil.bytesToHex(frame), 8, 16));
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
         }

         byteIn.readerIndex(location);
      }
   }

   public static String formatGroup(String str, int groupSize, int lineBreak) {
      StringBuffer buffer = new StringBuffer();

      int line = 1;
      buffer.append("/*  1 */ \"");
      for (int i = 0; i < str.length(); i += groupSize) {
         buffer.append(str.substring(i, i + Math.min(str.length() - i, groupSize)));

         if ((i + groupSize) % lineBreak == 0) {
            buffer.append("\" +\n/* ");
            line++;
            if (line < 10) {
               buffer.append(" ");
            }
            buffer.append(Integer.toString(line) + " */ \"");
         } else if ((i + groupSize) % groupSize == 0 && str.length() - i > groupSize) {
            buffer.append("\" + \"");
         }
      }

      buffer.append("\";");

      return buffer.toString();

   }

   public static String maxString(String value, int size) {
      if (value.length() < size) {
         return value;
      } else {
         return value.substring(0, size / 2) + " ... " + value.substring(value.length() - size / 2);
      }
   }

   public static String bytesToHex(byte[] bytes) {
      char[] hexChars = new char[bytes.length * 2];
      for (int j = 0; j < bytes.length; j++) {
         int v = bytes[j] & 0xFF;
         hexChars[j * 2] = hexArray[v >>> 4];
         hexChars[j * 2 + 1] = hexArray[v & 0x0F];
      }
      return new String(hexChars);
   }

   public static String bytesToHex(byte[] bytes, int groupSize) {
      if (bytes == null) {
         return "NULL";
      }

      if (bytes.length == 0) {
         return "[]";
      }

      char[] hexChars = new char[bytes.length * 2 + numberOfGroups(bytes, groupSize)];
      int outPos = 0;
      for (int j = 0; j < bytes.length; j++) {
         if (j > 0 && j % groupSize == 0) {
            hexChars[outPos++] = ' ';
         }
         int v = bytes[j] & 0xFF;
         hexChars[outPos++] = hexArray[v >>> 4];
         hexChars[outPos++] = hexArray[v & 0x0F];
      }
      return new String(hexChars);
   }

   public static String toSimpleString(byte[] bytes) {
      SimpleString simpleString = new SimpleString(bytes);
      String value = simpleString.toString();

      for (char c : value.toCharArray()) {
         if (c < ' ' || c > 127) {
            return NON_ASCII_STRING;
         }
      }

      return value;
   }

   private static int numberOfGroups(byte[] bytes, int groupSize) {
      int groups = bytes.length / groupSize;

      if (bytes.length % groupSize == 0) {
         groups--;
      }

      return groups;
   }

   public static byte[] longToBytes(long x) {
      ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.heapBuffer(8, 8);
      buffer.writeLong(x);
      return buffer.array();
   }

   public static byte[] hexToBytes(String hexStr) {
      byte[] bytes = new byte[hexStr.length() / 2];
      for (int i = 0; i < bytes.length; i++) {
         bytes[i] = (byte) Integer.parseInt(hexStr.substring(2 * i, 2 * i + 2), 16);
      }
      return bytes;
   }

   public static String readLine(ActiveMQBuffer buffer) {
      StringBuilder sb = new StringBuilder("");
      char c = buffer.readChar();
      while (c != '\n') {
         sb.append(c);
         c = buffer.readChar();
      }
      return sb.toString();
   }

   public static byte[] getActiveArray(ByteBuffer buffer) {
      byte[] ret = new byte[buffer.remaining()];
      if (buffer.hasArray()) {
         byte[] array = buffer.array();
         System.arraycopy(array, buffer.arrayOffset() + buffer.position(), ret, 0, ret.length);
      } else {
         buffer.slice().get(ret);
      }
      return ret;
   }

   public static long convertTextBytes(final String text) {
      try {
         Matcher m = ONE.matcher(text);
         if (m.matches()) {
            return Long.valueOf(Long.parseLong(m.group(1)));
         }

         m = KILO.matcher(text);
         if (m.matches()) {
            return Long.valueOf(Long.parseLong(m.group(1)) * 1024);
         }

         m = MEGA.matcher(text);
         if (m.matches()) {
            return Long.valueOf(Long.parseLong(m.group(1)) * 1024 * 1024);
         }

         m = GIGA.matcher(text);
         if (m.matches()) {
            return Long.valueOf(Long.parseLong(m.group(1)) * 1024 * 1024 * 1024);
         }

         return Long.parseLong(text);
      } catch (NumberFormatException e) {
         throw ActiveMQUtilBundle.BUNDLE.failedToParseLong(text);
      }
   }
}

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
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;

import io.netty.util.internal.PlatformDependent;
import org.jboss.logging.Logger;
import org.junit.Assert;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

public class ByteUtilTest {

   private static Logger log = Logger.getLogger(ByteUtilTest.class);

   @Test
   public void testBytesToString() {
      byte[] byteArray = new byte[]{0, 1, 2, 3};

      testEquals("0001 0203", ByteUtil.bytesToHex(byteArray, 2));
      testEquals("00 01 02 03", ByteUtil.bytesToHex(byteArray, 1));
      testEquals("000102 03", ByteUtil.bytesToHex(byteArray, 3));
   }

   @Test
   public void testNonASCII() {
      assertEquals("aA", ByteUtil.toSimpleString(new byte[]{97, 0, 65, 0}));
      assertEquals(ByteUtil.NON_ASCII_STRING, ByteUtil.toSimpleString(new byte[]{0, 97, 0, 65}));
      log.debug(ByteUtil.toSimpleString(new byte[]{0, 97, 0, 65}));
   }

   @Test
   public void testMaxString() {
      byte[] byteArray = new byte[20 * 1024];
      log.debug(ByteUtil.maxString(ByteUtil.bytesToHex(byteArray, 2), 150));
   }

   void testEquals(String string1, String string2) {
      if (!string1.equals(string2)) {
         Assert.fail("String are not the same:=" + string1 + "!=" + string2);
      }
   }

   @Test
   public void testTextBytesToLongBytes() {
      long[] factor = new long[] {1, 5, 10};
      String[] type = new String[]{"", "b", "k", "m", "g"};
      long[] size = new long[]{1, 1, 1024, 1024 * 1024, 1024 * 1024 * 1024};

      for (int i = 0; i < 3; i++) {
         for (int j = 0; j < type.length; j++) {
            assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + type[j]));
            assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + " " + type[j]));
            assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + type[j].toUpperCase()));
            assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + " " + type[j].toUpperCase()));
            if (j >= 2) {
               assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + type[j] + "b"));
               assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + " " + type[j] + "b"));
               assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + type[j].toUpperCase() + "B"));
               assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + " " + type[j].toUpperCase() + "B"));
            }
         }
      }
   }

   @Test
   public void testTextBytesToLongBytesNegative() {
      try {
         ByteUtil.convertTextBytes("x");
         fail();
      } catch (Exception e) {
         assertTrue(e instanceof IllegalArgumentException);
      }
   }

   private static byte[] duplicateRemaining(ByteBuffer buffer, int offset, int bytes) {
      final int end = offset + bytes;
      final int expectedRemaining = buffer.capacity() - end;
      //it is handling the case of <0 just to allow from to > capacity
      if (expectedRemaining <= 0) {
         return null;
      }
      final byte[] remaining = new byte[expectedRemaining];
      final ByteBuffer duplicate = buffer.duplicate();
      duplicate.clear().position(end);
      duplicate.get(remaining, 0, expectedRemaining);
      return remaining;
   }

   private static byte[] duplicateBefore(ByteBuffer buffer, int offset) {
      if (offset <= 0) {
         return null;
      }
      final int size = Math.min(buffer.capacity(), offset);
      final byte[] remaining = new byte[size];
      final ByteBuffer duplicate = buffer.duplicate();
      duplicate.clear();
      duplicate.get(remaining, 0, size);
      return remaining;
   }

   private static void shouldZeroesByteBuffer(ByteBuffer buffer, int offset, int bytes) {
      final byte[] originalBefore = duplicateBefore(buffer, offset);
      final byte[] originalRemaining = duplicateRemaining(buffer, offset, bytes);
      final int position = buffer.position();
      final int limit = buffer.limit();
      ByteUtil.zeros(buffer, offset, bytes);
      Assert.assertEquals(position, buffer.position());
      Assert.assertEquals(limit, buffer.limit());
      final byte[] zeros = new byte[bytes];
      final byte[] content = new byte[bytes];
      final ByteBuffer duplicate = buffer.duplicate();
      duplicate.clear().position(offset);
      duplicate.get(content, 0, bytes);
      Assert.assertArrayEquals(zeros, content);
      if (originalRemaining != null) {
         final byte[] remaining = new byte[duplicate.remaining()];
         //duplicate position has been moved of bytes
         duplicate.get(remaining);
         Assert.assertArrayEquals(originalRemaining, remaining);
      }
      if (originalBefore != null) {
         final byte[] before = new byte[offset];
         //duplicate position has been moved of bytes: need to reset it
         duplicate.position(0);
         duplicate.get(before);
         Assert.assertArrayEquals(originalBefore, before);
      }
   }

   private ByteBuffer fill(ByteBuffer buffer, int offset, int length, byte value) {
      for (int i = 0; i < length; i++) {
         buffer.put(offset + i, value);
      }
      return buffer;
   }

   @Test
   public void shouldZeroesDirectByteBuffer() {
      final byte one = (byte) 1;
      final int capacity = 64;
      final int bytes = 32;
      final int offset = 1;
      final ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
      try {
         fill(buffer, 0, capacity, one);
         shouldZeroesByteBuffer(buffer, offset, bytes);
      } finally {
         if (PlatformDependent.hasUnsafe()) {
            PlatformDependent.freeDirectBuffer(buffer);
         }
      }
   }

   @Test
   public void shouldZeroesLimitedDirectByteBuffer() {
      final byte one = (byte) 1;
      final int capacity = 64;
      final int bytes = 32;
      final int offset = 1;
      final ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
      try {
         fill(buffer, 0, capacity, one);
         buffer.limit(0);
         shouldZeroesByteBuffer(buffer, offset, bytes);
      } finally {
         if (PlatformDependent.hasUnsafe()) {
            PlatformDependent.freeDirectBuffer(buffer);
         }
      }
   }

   @Test
   public void shouldZeroesHeapByteBuffer() {
      final byte one = (byte) 1;
      final int capacity = 64;
      final int bytes = 32;
      final int offset = 1;
      final ByteBuffer buffer = ByteBuffer.allocate(capacity);
      fill(buffer, 0, capacity, one);
      shouldZeroesByteBuffer(buffer, offset, bytes);
   }

   @Test
   public void shouldZeroesLimitedHeapByteBuffer() {
      final byte one = (byte) 1;
      final int capacity = 64;
      final int bytes = 32;
      final int offset = 1;
      final ByteBuffer buffer = ByteBuffer.allocate(capacity);
      fill(buffer, 0, capacity, one);
      buffer.limit(0);
      shouldZeroesByteBuffer(buffer, offset, bytes);
   }

   @Test(expected = ReadOnlyBufferException.class)
   public void shouldFailWithReadOnlyHeapByteBuffer() {
      final byte one = (byte) 1;
      final int capacity = 64;
      final int bytes = 32;
      final int offset = 1;
      ByteBuffer buffer = ByteBuffer.allocate(capacity);
      fill(buffer, 0, capacity, one);
      buffer = buffer.asReadOnlyBuffer();
      shouldZeroesByteBuffer(buffer, offset, bytes);
   }

   @Test(expected = IndexOutOfBoundsException.class)
   public void shouldFailIfOffsetIsGreaterOrEqualHeapByteBufferCapacity() {
      final byte one = (byte) 1;
      final int capacity = 64;
      final int bytes = 0;
      final int offset = 64;
      ByteBuffer buffer = ByteBuffer.allocate(capacity);
      fill(buffer, 0, capacity, one);
      try {
         shouldZeroesByteBuffer(buffer, offset, bytes);
      } catch (IndexOutOfBoundsException expectedEx) {
         //verify that the buffer hasn't changed
         final byte[] originalContent = duplicateRemaining(buffer, 0, 0);
         final byte[] expectedContent = new byte[capacity];
         Arrays.fill(expectedContent, one);
         Assert.assertArrayEquals(expectedContent, originalContent);
         throw expectedEx;
      }
   }

   @Test(expected = IllegalArgumentException.class)
   public void shouldFailIfOffsetIsNegative() {
      final byte one = (byte) 1;
      final int capacity = 64;
      final int bytes = 1;
      final int offset = -1;
      ByteBuffer buffer = ByteBuffer.allocate(capacity);
      fill(buffer, 0, capacity, one);
      try {
         shouldZeroesByteBuffer(buffer, offset, bytes);
      } catch (IndexOutOfBoundsException expectedEx) {
         //verify that the buffer hasn't changed
         final byte[] originalContent = duplicateRemaining(buffer, 0, 0);
         final byte[] expectedContent = new byte[capacity];
         Arrays.fill(expectedContent, one);
         Assert.assertArrayEquals(expectedContent, originalContent);
         throw expectedEx;
      }
   }

   @Test(expected = IllegalArgumentException.class)
   public void shouldFailIfBytesIsNegative() {
      final byte one = (byte) 1;
      final int capacity = 64;
      final int bytes = -1;
      final int offset = 0;
      ByteBuffer buffer = ByteBuffer.allocate(capacity);
      fill(buffer, 0, capacity, one);
      try {
         shouldZeroesByteBuffer(buffer, offset, bytes);
      } catch (IndexOutOfBoundsException expectedEx) {
         //verify that the buffer hasn't changed
         final byte[] originalContent = duplicateRemaining(buffer, 0, 0);
         final byte[] expectedContent = new byte[capacity];
         Arrays.fill(expectedContent, one);
         Assert.assertArrayEquals(expectedContent, originalContent);
         throw expectedEx;
      }
   }

   @Test(expected = IndexOutOfBoundsException.class)
   public void shouldFailIfExceedingHeapByteBufferCapacity() {
      final byte one = (byte) 1;
      final int capacity = 64;
      final int bytes = 65;
      final int offset = 1;
      ByteBuffer buffer = ByteBuffer.allocate(capacity);
      fill(buffer, 0, capacity, one);
      try {
         shouldZeroesByteBuffer(buffer, offset, bytes);
      } catch (IndexOutOfBoundsException expectedEx) {
         //verify that the buffer hasn't changed
         final byte[] originalContent = duplicateRemaining(buffer, 0, 0);
         final byte[] expectedContent = new byte[capacity];
         Arrays.fill(expectedContent, one);
         Assert.assertArrayEquals(expectedContent, originalContent);
         throw expectedEx;
      }
   }



   @Test
   public void testIntToByte() {
      for (int i = 0; i < 1000; i++) {
         int randomInt = RandomUtil.randomInt();
         byte[] expected = ByteBuffer.allocate(4).putInt(randomInt).array();

         byte[] actual = ByteUtil.intToBytes(randomInt);
         assertArrayEquals(expected, actual);

         assertEquals(randomInt, ByteUtil.bytesToInt(expected));
         assertEquals(randomInt, ByteUtil.bytesToInt(actual));
      }
   }

   @Test
   public void testLongToBytes() {
      ByteBuffer buffer = ByteBuffer.allocate(8);
      long randomLong = RandomUtil.randomLong();
      buffer.putLong(randomLong);
      byte[] longArrayAssert = buffer.array();

      byte[] convertedArray = ByteUtil.longToBytes(randomLong);

      assertArrayEquals(longArrayAssert, convertedArray);
   }

   @Test
   public void testDoubleLongToBytes() {
      long randomLong1 = RandomUtil.randomLong();
      long randomLong2 = RandomUtil.randomLong();
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.putLong(randomLong1);
      buffer.putLong(randomLong2);
      byte[] assertContent = buffer.array();

      byte[] convertedContent = ByteUtil.doubleLongToBytes(randomLong1, randomLong2);

      assertArrayEquals(assertContent, convertedContent);
   }


}

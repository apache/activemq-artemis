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
package org.apache.activemq.artemis.core.io.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.util.internal.PlatformDependent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class ThreadLocalByteBufferPoolTest {

   //testing using heap buffers to avoid killing the test suite
   private static final boolean isDirect = false;
   private final ByteBufferPool pool = ByteBufferPool.threadLocal(isDirect);
   private final boolean zeroed;

   public ThreadLocalByteBufferPoolTest(boolean zeroed) {
      this.zeroed = zeroed;
   }

   @Parameterized.Parameters(name = "zeroed={0}")
   public static Collection<Object[]> getParams() {
      return Arrays.asList(new Object[][]{{Boolean.TRUE}, {Boolean.FALSE}});
   }

   private static void assertZeroed(ByteBuffer buffer) {
      ByteBuffer bb = buffer.slice();
      final byte[] content = new byte[bb.remaining()];
      bb.get(content);
      final byte[] zeroed = new byte[content.length];
      Arrays.fill(zeroed, (byte) 0);
      Assert.assertArrayEquals(zeroed, content);
   }

   @Test
   public void shouldBorrowOnlyBuffersOfTheCorrectType() {
      Assert.assertEquals(isDirect, pool.borrow(0, zeroed).isDirect());
   }

   @Test
   public void shouldBorrowZeroedBuffer() {
      final int size = 32;
      final ByteBuffer buffer = pool.borrow(size, zeroed);
      Assert.assertEquals(0, buffer.position());
      Assert.assertEquals(size, buffer.limit());
      if (zeroed) {
         assertZeroed(buffer);
      }
   }

   @Test
   public void shouldBorrowTheSameBuffer() {
      final int size = 32;
      final ByteBuffer buffer = pool.borrow(size, zeroed);
      buffer.put(0, (byte) 1);
      buffer.position(1);
      buffer.limit(2);
      pool.release(buffer);
      final int newSize = size - 1;
      final ByteBuffer sameBuffer = pool.borrow(newSize, zeroed);
      Assert.assertSame(buffer, sameBuffer);
      Assert.assertEquals(0, sameBuffer.position());
      Assert.assertEquals(newSize, sameBuffer.limit());
      if (zeroed) {
         assertZeroed(sameBuffer);
      }
   }

   @Test
   public void shouldBorrowNewBufferIfExceedPooledCapacity() {
      final int size = 32;
      final ByteBuffer buffer = pool.borrow(size, zeroed);
      pool.release(buffer);
      final int newSize = buffer.capacity() + 1;
      final ByteBuffer differentBuffer = pool.borrow(newSize, zeroed);
      Assert.assertNotSame(buffer, differentBuffer);
   }

   @Test
   public void shouldPoolTheBiggestBuffer() {
      final int size = 32;
      final ByteBuffer small = pool.borrow(size, zeroed);
      final ByteBuffer big = pool.borrow(small.capacity() + 1, zeroed);
      pool.release(small);
      big.limit(0);
      pool.release(big);
      Assert.assertSame(big, pool.borrow(big.capacity(), zeroed));
   }

   @Test
   public void shouldNotPoolTheSmallestBuffer() {
      final int size = 32;
      final ByteBuffer small = pool.borrow(size, zeroed);
      final ByteBuffer big = pool.borrow(small.capacity() + 1, zeroed);
      big.limit(0);
      pool.release(big);
      pool.release(small);
      Assert.assertSame(big, pool.borrow(big.capacity(), zeroed));
   }

   @Test
   public void shouldNotPoolBufferOfDifferentType() {
      final int size = 32;
      final ByteBuffer buffer = isDirect ? ByteBuffer.allocate(size) : ByteBuffer.allocateDirect(size);
      try {
         pool.release(buffer);
         Assert.assertNotSame(buffer, pool.borrow(size, zeroed));
      } catch (Throwable t) {
         if (PlatformDependent.hasUnsafe()) {
            if (buffer.isDirect()) {
               PlatformDependent.freeDirectBuffer(buffer);
            }
         }
      }
   }

   @Test
   public void shouldNotPoolReadOnlyBuffer() {
      final int size = 32;
      final ByteBuffer borrow = pool.borrow(size, zeroed);
      final ByteBuffer readOnlyBuffer = borrow.asReadOnlyBuffer();
      pool.release(readOnlyBuffer);
      Assert.assertNotSame(readOnlyBuffer, pool.borrow(size, zeroed));
   }

   @Test(expected = NullPointerException.class)
   public void shouldFailPoolingNullBuffer() {
      pool.release(null);
   }

   @Test(expected = NullPointerException.class)
   public void shouldFailPoolingNullBufferIfNotEmpty() {
      final int size = 32;
      pool.release(pool.borrow(size, zeroed));
      pool.release(null);
   }

   @Test
   public void shouldBorrowOnlyThreadLocalBuffers() throws ExecutionException, InterruptedException {
      final int size = 32;
      final ByteBuffer buffer = pool.borrow(size, zeroed);
      pool.release(buffer);
      final ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
         Assert.assertNotSame(buffer, executor.submit(() -> pool.borrow(size, zeroed)).get());
      } finally {
         executor.shutdown();
      }
   }

   @Test
   public void shouldResetReusedBufferLimitBeforeZeroing() throws Exception {
      doResetReusedBufferLimitBeforeZeroingTestImpl(true);
   }

   @Test
   public void shouldResetReusedBufferLimitBeforeZeroingWithoutArray() throws Exception {
      doResetReusedBufferLimitBeforeZeroingTestImpl(false);
   }

   private void doResetReusedBufferLimitBeforeZeroingTestImpl(boolean withArray) {
      // Testing zero'ing and non-direct behaviour, ignore others.
      assumeTrue(zeroed);
      assumeFalse(isDirect);

      final int size = 32;
      final ByteBuffer buffer = pool.borrow(size, true);

      assertEquals("Unexpected buffer limit", size, buffer.limit());
      assertFalse(buffer.isDirect());

      // Put a non-zero value at the first byte, updating the position
      buffer.put((byte) 4);
      // Put a non-zero value at the last byte, not updating the position
      buffer.put(size - 1, (byte) 5);

      assertEquals("Unexpected buffer value at index 0", (byte) 4, buffer.get(0));
      assertEquals("Unexpected buffer value at index " + (size - 1), (byte) 5, buffer.get(size - 1));
      assertEquals("Unexpected buffer position", 1, buffer.position());

      // Set the buffer limit to half its current size, making it less than we will
      // ask for the next time we borrow, ensuring it then needs to be zeroed
      // beyond this reduced limit.
      buffer.limit(size / 2);

      ByteBuffer spy = null;
      if (withArray) {
         pool.release(buffer);
      } else {
         // Fake out this being a non-direct buffer that does not have an array.
         spy = Mockito.spy(buffer);
         Mockito.doReturn(false).when(spy).hasArray();

         assertEquals("Unexpected buffer limit", size / 2, spy.limit());

         pool.release(spy);
      }

      // Borrow what should be the same underlying buffer again, ask for it to be
      // zeroed; pool will need to handle the limit and position
      final ByteBuffer buffer2 = pool.borrow(size, true);

      if (withArray) {
         assertSame(buffer, buffer2);
      } else {
         assertSame(spy, buffer2);
      }

      // Verify position + limit, and content is zeroed
      assertEquals("Unexpected buffer limit", size, buffer2.limit());
      assertEquals("Unexpected buffer position", 0, buffer2.position());
      assertZeroed(buffer2);
   }
}

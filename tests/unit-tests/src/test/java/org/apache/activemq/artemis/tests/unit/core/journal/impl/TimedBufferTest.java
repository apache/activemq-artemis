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
package org.apache.activemq.artemis.tests.unit.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.buffer.TimedBuffer;
import org.apache.activemq.artemis.core.io.buffer.TimedBufferObserver;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.junit.Assert;
import org.junit.Test;

public class TimedBufferTest extends ActiveMQTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final int ONE_SECOND_IN_NANOS = 1000000000; // in nanoseconds

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   IOCallback dummyCallback = new IOCallback() {

      @Override
      public void done() {
      }

      @Override
      public void onError(final int errorCode, final String errorMessage) {
      }
   };

   @Test
   public void testFillBuffer() {
      final ArrayList<ByteBuffer> buffers = new ArrayList<>();
      final AtomicInteger flushTimes = new AtomicInteger(0);
      class TestObserver implements TimedBufferObserver {

         @Override
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOCallback> callbacks) {
            buffers.add(buffer);
            flushTimes.incrementAndGet();
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         @Override
         public ByteBuffer newBuffer(final int minSize, final int maxSize) {
            return ByteBuffer.allocate(maxSize);
         }

         @Override
         public int getRemainingBytes() {
            return 1024 * 1024;
         }
      }

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND_IN_NANOS, false);

      timedBuffer.start();

      try {

         timedBuffer.setObserver(new TestObserver());

         int x = 0;
         for (int i = 0; i < 10; i++) {
            byte[] bytes = new byte[10];
            for (int j = 0; j < 10; j++) {
               bytes[j] = ActiveMQTestBase.getSamplebyte(x++);
            }

            ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(bytes);

            timedBuffer.checkSize(10);
            timedBuffer.addBytes(buff, false, dummyCallback);
         }

         timedBuffer.checkSize(1);

         Assert.assertEquals(1, flushTimes.get());

         ByteBuffer flushedBuffer = buffers.get(0);

         Assert.assertEquals(100, flushedBuffer.limit());

         Assert.assertEquals(100, flushedBuffer.capacity());

         flushedBuffer.rewind();

         for (int i = 0; i < 100; i++) {
            Assert.assertEquals(ActiveMQTestBase.getSamplebyte(i), flushedBuffer.get());
         }
      } finally {
         timedBuffer.stop();
      }

   }
   @Test
   public void testTimeOnTimedBuffer() throws Exception {
      final ReusableLatch latchFlushed = new ReusableLatch(0);
      final AtomicInteger flushes = new AtomicInteger(0);
      class TestObserver implements TimedBufferObserver {

         @Override
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOCallback> callbacks) {
            for (IOCallback callback : callbacks) {
               callback.done();
            }
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         @Override
         public ByteBuffer newBuffer(final int minSize, final int maxSize) {
            return ByteBuffer.allocate(maxSize);
         }

         @Override
         public int getRemainingBytes() {
            return 1024 * 1024;
         }
      }

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND_IN_NANOS / 2, false);

      timedBuffer.start();

      TestObserver observer = new TestObserver();
      timedBuffer.setObserver(observer);


      int x = 0;

      byte[] bytes = new byte[10];
      for (int j = 0; j < 10; j++) {
         bytes[j] = ActiveMQTestBase.getSamplebyte(x++);
      }

      ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(bytes);

      IOCallback callback = new IOCallback() {
         @Override
         public void done() {
            System.out.println("done");
            latchFlushed.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      };


      try {
         latchFlushed.setCount(2);

         // simulating a low load period
         timedBuffer.addBytes(buff, true, callback);
         Thread.sleep(1000);
         timedBuffer.addBytes(buff, true, callback);
         Assert.assertTrue(latchFlushed.await(5, TimeUnit.SECONDS));
         latchFlushed.setCount(5);


         flushes.set(0);

         // Sending like crazy... still some wait (1 millisecond) between each send..
         long time = System.currentTimeMillis();
         for (int i = 0; i < 5; i++) {
            timedBuffer.addBytes(buff, true, callback);
            Thread.sleep(1);
         }
         Assert.assertTrue(latchFlushed.await(5, TimeUnit.SECONDS));

         // The purpose of the timed buffer is to batch writes up to a millisecond.. or up to the size of the buffer.
         Assert.assertTrue("Timed Buffer is not batching accordingly, it was expected to take at least 500 seconds batching multiple writes while it took " + (System.currentTimeMillis() - time) + " milliseconds", System.currentTimeMillis() - time >= 500);

         // it should be in fact only writing once..
         // i will set for 3 just in case there's a GC or anything else happening on the test
         Assert.assertTrue("Too many writes were called", flushes.get() <= 3);
      } finally {
         timedBuffer.stop();
      }



   }

   @Test
   public void testTimingAndFlush() throws Exception {
      final ArrayList<ByteBuffer> buffers = new ArrayList<>();
      final AtomicInteger flushTimes = new AtomicInteger(0);
      class TestObserver implements TimedBufferObserver {

         @Override
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOCallback> callbacks) {
            buffers.add(buffer);
            flushTimes.incrementAndGet();
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         @Override
         public ByteBuffer newBuffer(final int minSize, final int maxSize) {
            return ByteBuffer.allocate(maxSize);
         }

         @Override
         public int getRemainingBytes() {
            return 1024 * 1024;
         }
      }

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND_IN_NANOS / 10, false);

      timedBuffer.start();

      try {

         timedBuffer.setObserver(new TestObserver());

         int x = 0;

         byte[] bytes = new byte[10];
         for (int j = 0; j < 10; j++) {
            bytes[j] = ActiveMQTestBase.getSamplebyte(x++);
         }

         ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(bytes);

         timedBuffer.checkSize(10);
         timedBuffer.addBytes(buff, false, dummyCallback);

         Thread.sleep(200);

         Assert.assertEquals(0, flushTimes.get());

         bytes = new byte[10];
         for (int j = 0; j < 10; j++) {
            bytes[j] = ActiveMQTestBase.getSamplebyte(x++);
         }

         buff = ActiveMQBuffers.wrappedBuffer(bytes);

         timedBuffer.checkSize(10);
         timedBuffer.addBytes(buff, true, dummyCallback);

         Thread.sleep(500);

         Assert.assertEquals(1, flushTimes.get());

         ByteBuffer flushedBuffer = buffers.get(0);

         Assert.assertEquals(20, flushedBuffer.limit());

         Assert.assertEquals(20, flushedBuffer.capacity());

         flushedBuffer.rewind();

         for (int i = 0; i < 20; i++) {
            Assert.assertEquals(ActiveMQTestBase.getSamplebyte(i), flushedBuffer.get());
         }
      } finally {
         timedBuffer.stop();
      }

   }
}

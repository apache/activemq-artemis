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
package org.apache.activemq.artemis.tests.unit.core.journal.impl;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.tests.util.UnitTestCase;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.apache.activemq.artemis.core.journal.IOAsyncTask;
import org.apache.activemq.artemis.core.journal.impl.TimedBuffer;
import org.apache.activemq.artemis.core.journal.impl.TimedBufferObserver;

public class TimedBufferTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final int ONE_SECOND_IN_NANOS = 1000000000; // in nanoseconds

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   IOAsyncTask dummyCallback = new IOAsyncTask()
   {

      public void done()
      {
      }

      public void onError(final int errorCode, final String errorMessage)
      {
      }
   };


   @Test
   public void testFillBuffer()
   {
      final ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
      final AtomicInteger flushTimes = new AtomicInteger(0);
      class TestObserver implements TimedBufferObserver
      {
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOAsyncTask> callbacks)
         {
            buffers.add(buffer);
            flushTimes.incrementAndGet();
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         public ByteBuffer newBuffer(final int minSize, final int maxSize)
         {
            return ByteBuffer.allocate(maxSize);
         }

         public int getRemainingBytes()
         {
            return 1024 * 1024;
         }
      }

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND_IN_NANOS, false);

      timedBuffer.start();

      try
      {

         timedBuffer.setObserver(new TestObserver());

         int x = 0;
         for (int i = 0; i < 10; i++)
         {
            byte[] bytes = new byte[10];
            for (int j = 0; j < 10; j++)
            {
               bytes[j] = UnitTestCase.getSamplebyte(x++);
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

         for (int i = 0; i < 100; i++)
         {
            Assert.assertEquals(UnitTestCase.getSamplebyte(i), flushedBuffer.get());
         }
      }
      finally
      {
         timedBuffer.stop();
      }

   }

   @Test
   public void testTimingAndFlush() throws Exception
   {
      final ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
      final AtomicInteger flushTimes = new AtomicInteger(0);
      class TestObserver implements TimedBufferObserver
      {
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOAsyncTask> callbacks)
         {
            buffers.add(buffer);
            flushTimes.incrementAndGet();
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         public ByteBuffer newBuffer(final int minSize, final int maxSize)
         {
            return ByteBuffer.allocate(maxSize);
         }

         public int getRemainingBytes()
         {
            return 1024 * 1024;
         }
      }

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND_IN_NANOS / 10, false);

      timedBuffer.start();

      try
      {

         timedBuffer.setObserver(new TestObserver());

         int x = 0;

         byte[] bytes = new byte[10];
         for (int j = 0; j < 10; j++)
         {
            bytes[j] = UnitTestCase.getSamplebyte(x++);
         }

         ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(bytes);

         timedBuffer.checkSize(10);
         timedBuffer.addBytes(buff, false, dummyCallback);

         Thread.sleep(200);

         Assert.assertEquals(0, flushTimes.get());

         bytes = new byte[10];
         for (int j = 0; j < 10; j++)
         {
            bytes[j] = UnitTestCase.getSamplebyte(x++);
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

         for (int i = 0; i < 20; i++)
         {
            Assert.assertEquals(UnitTestCase.getSamplebyte(i), flushedBuffer.get());
         }
      }
      finally
      {
         timedBuffer.stop();
      }

   }

   /**
    * This test will verify if the system will switch to spin case the system can't perform sleeps timely
    * due to proper kernel installations
    * @throws Exception
    */
   @Test
   public void testVerifySwitchToSpin() throws Exception
   {
      class TestObserver implements TimedBufferObserver
      {
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOAsyncTask> callbacks)
         {
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         public ByteBuffer newBuffer(final int minSize, final int maxSize)
         {
            return ByteBuffer.allocate(maxSize);
         }

         public int getRemainingBytes()
         {
            return 1024 * 1024;
         }
      }

      final CountDownLatch sleptLatch = new CountDownLatch(1);

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND_IN_NANOS / 1000, false)
      {

         @Override
         protected void stopSpin()
         {
            // keeps spinning forever
         }

         @Override
         protected void sleep(int sleepMillis, int sleepNanos) throws InterruptedException
         {
            Thread.sleep(10);
         }

         @Override
         public synchronized void setUseSleep(boolean param)
         {
            super.setUseSleep(param);
            sleptLatch.countDown();
         }

      };

      timedBuffer.start();

      try
      {

         timedBuffer.setObserver(new TestObserver());

         int x = 0;

         byte[] bytes = new byte[10];
         for (int j = 0; j < 10; j++)
         {
            bytes[j] = UnitTestCase.getSamplebyte(x++);
         }

         ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(bytes);

         timedBuffer.checkSize(10);
         timedBuffer.addBytes(buff, true, dummyCallback);

         sleptLatch.await(10, TimeUnit.SECONDS);

         assertFalse(timedBuffer.isUseSleep());
      }
      finally
      {
         timedBuffer.stop();
      }

   }


   /**
    * This test will verify if the system will switch to spin case the system can't perform sleeps timely
    * due to proper kernel installations
    * @throws Exception
    */
   @Test
   public void testStillSleeps() throws Exception
   {
      class TestObserver implements TimedBufferObserver
      {
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOAsyncTask> callbacks)
         {
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         public ByteBuffer newBuffer(final int minSize, final int maxSize)
         {
            return ByteBuffer.allocate(maxSize);
         }

         public int getRemainingBytes()
         {
            return 1024 * 1024;
         }
      }

      final CountDownLatch sleptLatch = new CountDownLatch(TimedBuffer.MAX_CHECKS_ON_SLEEP);

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND_IN_NANOS / 1000, false)
      {

         @Override
         protected void stopSpin()
         {
            // keeps spinning forever
         }

         @Override
         protected void sleep(int sleepMillis, int sleepNanos) throws InterruptedException
         {
            sleptLatch.countDown();
            // no sleep
         }

         @Override
         public synchronized void setUseSleep(boolean param)
         {
            super.setUseSleep(param);
            sleptLatch.countDown();
         }

      };

      timedBuffer.start();

      try
      {

         timedBuffer.setObserver(new TestObserver());

         int x = 0;

         byte[] bytes = new byte[10];
         for (int j = 0; j < 10; j++)
         {
            bytes[j] = UnitTestCase.getSamplebyte(x++);
         }

         ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(bytes);

         timedBuffer.checkSize(10);
         timedBuffer.addBytes(buff, true, dummyCallback);

         // waits all the sleeps to be done
         sleptLatch.await(10, TimeUnit.SECONDS);

         // keeps waiting a bit longer
         Thread.sleep(100);

         assertTrue(timedBuffer.isUseSleep());
      }
      finally
      {
         timedBuffer.stop();
      }
   }
}

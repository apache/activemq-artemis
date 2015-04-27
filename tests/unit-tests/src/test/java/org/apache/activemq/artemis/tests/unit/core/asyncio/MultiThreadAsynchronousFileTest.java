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
package org.apache.activemq.artemis.tests.unit.core.asyncio;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.tests.util.UnitTestCase;
import org.apache.activemq.artemis.core.asyncio.AIOCallback;
import org.apache.activemq.artemis.core.asyncio.impl.AsynchronousFileImpl;
import org.apache.activemq.artemis.core.journal.impl.AIOSequentialFileFactory;
import org.apache.activemq.artemis.tests.unit.UnitTestLogger;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * you need to define -Djava.library.path=${project-root}/native/src/.libs when calling the JVM
 * If you are running this test in eclipse you should do:
 * I - Run->Open Run Dialog
 * II - Find the class on the list (you will find it if you already tried running this testcase before)
 * III - Add -Djava.library.path=<your project place>/native/src/.libs
 */
public class MultiThreadAsynchronousFileTest extends AIOTestBase
{
   @BeforeClass
   public static void hasAIO()
   {
      org.junit.Assume.assumeTrue("Test case needs AIO to run", AIOSequentialFileFactory.isSupported());
   }

   AtomicInteger position = new AtomicInteger(0);

   static final int SIZE = 1024;

   static final int NUMBER_OF_THREADS = 10;

   static final int NUMBER_OF_LINES = 1000;

   ExecutorService executor;

   ExecutorService pollerExecutor;

   private static void debug(final String msg)
   {
      UnitTestLogger.LOGGER.debug(msg);
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      pollerExecutor = Executors.newCachedThreadPool(new ActiveMQThreadFactory("ActiveMQ-AIO-poller-pool" + System.identityHashCode(this),
                                                                              false, this.getClass().getClassLoader()));
      executor = Executors.newSingleThreadExecutor();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      executor.shutdown();
      pollerExecutor.shutdown();
      super.tearDown();
   }

   @Test
   public void testMultipleASynchronousWrites() throws Throwable
   {
      executeTest(false);
   }

   @Test
   public void testMultipleSynchronousWrites() throws Throwable
   {
      executeTest(true);
   }

   private void executeTest(final boolean sync) throws Throwable
   {
      MultiThreadAsynchronousFileTest.debug(sync ? "Sync test:" : "Async test");
      AsynchronousFileImpl jlibAIO = new AsynchronousFileImpl(executor, pollerExecutor);
      jlibAIO.open(fileName, 21000);
      try
      {
         MultiThreadAsynchronousFileTest.debug("Preallocating file");

         jlibAIO.fill(0L,
                      MultiThreadAsynchronousFileTest.NUMBER_OF_THREADS,
                      MultiThreadAsynchronousFileTest.SIZE * MultiThreadAsynchronousFileTest.NUMBER_OF_LINES,
                      (byte) 0);
         MultiThreadAsynchronousFileTest.debug("Done Preallocating file");

         CountDownLatch latchStart = new CountDownLatch(MultiThreadAsynchronousFileTest.NUMBER_OF_THREADS + 1);

         ArrayList<ThreadProducer> list = new ArrayList<ThreadProducer>(MultiThreadAsynchronousFileTest.NUMBER_OF_THREADS);
         for (int i = 0; i < MultiThreadAsynchronousFileTest.NUMBER_OF_THREADS; i++)
         {
            ThreadProducer producer = new ThreadProducer("Thread " + i, latchStart, jlibAIO, sync);
            list.add(producer);
            producer.start();
         }

         latchStart.countDown();
         UnitTestCase.waitForLatch(latchStart);

         long startTime = System.currentTimeMillis();

         for (ThreadProducer producer : list)
         {
            producer.join();
            if (producer.failed != null)
            {
               throw producer.failed;
            }
         }
         long endTime = System.currentTimeMillis();

         MultiThreadAsynchronousFileTest.debug((sync ? "Sync result:" : "Async result:") + " Records/Second = " +
                                                  MultiThreadAsynchronousFileTest.NUMBER_OF_THREADS *
                                                     MultiThreadAsynchronousFileTest.NUMBER_OF_LINES *
                                                     1000 /
                                                     (endTime - startTime) +
                                                  " total time = " +
                                                  (endTime - startTime) +
                                                  " total number of records = " +
                                                  MultiThreadAsynchronousFileTest.NUMBER_OF_THREADS *
                                                     MultiThreadAsynchronousFileTest.NUMBER_OF_LINES);
      }
      finally
      {
         jlibAIO.close();
      }

   }

   private int getNewPosition()
   {
      return position.addAndGet(1);
   }

   class ThreadProducer extends Thread
   {
      Throwable failed = null;

      CountDownLatch latchStart;

      boolean sync;

      AsynchronousFileImpl libaio;

      public ThreadProducer(final String name,
                            final CountDownLatch latchStart,
                            final AsynchronousFileImpl libaio,
                            final boolean sync)
      {
         super(name);
         this.latchStart = latchStart;
         this.libaio = libaio;
         this.sync = sync;
      }

      @Override
      public void run()
      {
         super.run();

         ByteBuffer buffer = null;

         synchronized (MultiThreadAsynchronousFileTest.class)
         {
            buffer = AsynchronousFileImpl.newBuffer(MultiThreadAsynchronousFileTest.SIZE);
         }

         try
         {

            // I'm always reusing the same buffer, as I don't want any noise from
            // malloc on the measurement
            // Encoding buffer
            MultiThreadAsynchronousFileTest.addString("Thread name=" + Thread.currentThread().getName() + ";" + "\n",
                                                      buffer);
            for (int local = buffer.position(); local < buffer.capacity() - 1; local++)
            {
               buffer.put((byte) ' ');
            }
            buffer.put((byte) '\n');

            latchStart.countDown();
            waitForLatch(latchStart);

            CountDownLatch latchFinishThread = null;

            if (!sync)
            {
               latchFinishThread = new CountDownLatch(MultiThreadAsynchronousFileTest.NUMBER_OF_LINES);
            }

            LinkedList<CountDownCallback> list = new LinkedList<CountDownCallback>();

            for (int i = 0; i < MultiThreadAsynchronousFileTest.NUMBER_OF_LINES; i++)
            {

               if (sync)
               {
                  latchFinishThread = new CountDownLatch(1);
               }
               CountDownCallback callback = new CountDownCallback(latchFinishThread, null, null, 0);
               if (!sync)
               {
                  list.add(callback);
               }
               addData(libaio, buffer, callback);
               if (sync)
               {
                  waitForLatch(latchFinishThread);
                  assertTrue(callback.doneCalled);
                  assertFalse(callback.errorCalled != 0);
               }
            }
            if (!sync)
            {
               waitForLatch(latchFinishThread);
            }

            for (CountDownCallback callback : list)
            {
               assertTrue(callback.doneCalled);
               assertFalse(callback.errorCalled != 0);
            }

            for (CountDownCallback callback : list)
            {
               assertTrue(callback.doneCalled);
               assertFalse(callback.errorCalled != 0);
            }

         }
         catch (Throwable e)
         {
            e.printStackTrace();
            failed = e;
         }
         finally
         {
            synchronized (MultiThreadAsynchronousFileTest.class)
            {
               AsynchronousFileImpl.destroyBuffer(buffer);
            }
         }

      }
   }

   private static void addString(final String str, final ByteBuffer buffer)
   {
      byte[] bytes = str.getBytes();
      buffer.put(bytes);
   }

   private void addData(final AsynchronousFileImpl aio, final ByteBuffer buffer, final AIOCallback callback) throws Exception
   {
      executor.execute(new WriteRunnable(aio, buffer, callback));
   }

   private class WriteRunnable implements Runnable
   {

      AsynchronousFileImpl aio;

      ByteBuffer buffer;

      AIOCallback callback;

      public WriteRunnable(final AsynchronousFileImpl aio, final ByteBuffer buffer, final AIOCallback callback)
      {
         this.aio = aio;
         this.buffer = buffer;
         this.callback = callback;
      }

      public void run()
      {
         try
         {
            aio.write(getNewPosition() * MultiThreadAsynchronousFileTest.SIZE,
                      MultiThreadAsynchronousFileTest.SIZE,
                      buffer,
                      callback);

         }
         catch (Exception e)
         {
            callback.onError(ActiveMQExceptionType.GENERIC_EXCEPTION.getCode(), e.toString());
            e.printStackTrace();
         }
      }

   }

}

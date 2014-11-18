/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.tests.unit.core.asyncio;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.core.asyncio.AIOCallback;
import org.apache.activemq.core.asyncio.BufferCallback;
import org.apache.activemq.core.asyncio.impl.AsynchronousFileImpl;
import org.apache.activemq.core.journal.impl.AIOSequentialFileFactory;
import org.apache.activemq.tests.unit.UnitTestLogger;
import org.apache.activemq.tests.util.UnitTestCase;
import org.apache.activemq.utils.ActiveMQThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * you need to define -Djava.library.path=${project-root}/native/src/.libs when calling the JVM
 * If you are running this test in eclipse you should do:
 * I - Run->Open Run Dialog
 * II - Find the class on the list (you will find it if you already tried running this testcase before)
 * III - Add -Djava.library.path=<your project place>/native/src/.libs
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>.
 */
public class AsynchronousFileTest extends AIOTestBase
{

   @BeforeClass
   public static void hasAIO()
   {
      org.junit.Assume.assumeTrue("Test case needs AIO to run", AIOSequentialFileFactory.isSupported());
   }

   private static CharsetEncoder UTF_8_ENCODER = StandardCharsets.UTF_8.newEncoder();

   byte[] commonBuffer = null;

   ExecutorService executor;

   ExecutorService pollerExecutor;

   private AsynchronousFileImpl controller;
   private ByteBuffer buffer;

   private final BufferCallback bufferCallbackDestroy = new BufferCallback()
   {

      public void bufferDone(final ByteBuffer buffer1)
      {
         AsynchronousFileImpl.destroyBuffer(buffer1);
      }

   };

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
                                                                              false,
                                                                              this.getClass().getClassLoader()));
      executor = Executors.newSingleThreadExecutor();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      destroy(buffer);
      if (controller != null)
      {
         try
         {
            controller.close();
         }
         catch (Exception e)
         {
            // ignored
         }
      }
      executor.shutdown();
      pollerExecutor.shutdown();
      super.tearDown();
   }

   /**
    * Opening and closing a file immediately can lead to races on the native layer,
    * creating crash conditions.
    */
   @Test
   public void testOpenClose() throws Exception
   {
      controller = new AsynchronousFileImpl(executor, pollerExecutor);
      for (int i = 0; i < 100; i++)
      {
         controller.open(fileName, 10000);
         controller.close();

      }
   }

   @Test
   public void testReleaseBuffers() throws Exception
   {
      controller = new AsynchronousFileImpl(executor, pollerExecutor);
      WeakReference<ByteBuffer> bufferCheck = null;

      controller.open(fileName, 10000);
      bufferCheck = new WeakReference<ByteBuffer>(controller.getHandler());
      controller.fill(0, 10, 1024, (byte) 0);

      ByteBuffer write = AsynchronousFileImpl.newBuffer(1024);

      for (int i = 0; i < 1024; i++)
      {
         write.put(UnitTestCase.getSamplebyte(i));
      }

      final CountDownLatch latch = new CountDownLatch(1);

      controller.write(0, 1024, write, new AIOCallback()
      {

         public void onError(final int errorCode, final String errorMessage)
         {
         }

         public void done()
         {
            latch.countDown();
         }
      });

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

      WeakReference<ByteBuffer> bufferCheck2 = new WeakReference<ByteBuffer>(write);

      destroy(write);

      write = null;

      UnitTestCase.forceGC(bufferCheck2, 5000);

      Assert.assertNull(bufferCheck2.get());

      controller.close();

      controller = null;

      UnitTestCase.forceGC(bufferCheck, 5000);

      Assert.assertNull(bufferCheck.get());
   }

   @Test
   public void testFileNonExistent() throws Exception
   {
      controller = new AsynchronousFileImpl(executor, pollerExecutor);
      for (int i = 0; i < 100; i++)
      {
         try
         {
            controller.open("/non-existent/IDontExist.error", 10000);
            Assert.fail("Exception expected! The test could create a file called /non-existent/IDontExist.error when it was supposed to fail.");
         }
         catch (Exception ignored)
         {
         }
         try
         {
            controller.close();
            Assert.fail("Supposed to throw exception as the file wasn't opened");
         }
         catch (Exception ignored)
         {
         }

      }
   }

   /**
    * This test is validating if the AIO layer can open two different
    * simultaneous files without loose any callbacks. This test made the native
    * layer to crash at some point during development
    */
   @Test
   public void testTwoFiles() throws Exception
   {
      controller = new AsynchronousFileImpl(executor, pollerExecutor);
      final AsynchronousFileImpl controller2 = new AsynchronousFileImpl(executor, pollerExecutor);
      controller.open(fileName + ".1", 10000);
      controller2.open(fileName + ".2", 10000);

      int numberOfLines = 1000;
      int size = 1024;

      ArrayList<Integer> listResult1 = new ArrayList<Integer>();
      ArrayList<Integer> listResult2 = new ArrayList<Integer>();

      AtomicInteger errors = new AtomicInteger(0);


      try
      {
         CountDownLatch latchDone = new CountDownLatch(numberOfLines);
         CountDownLatch latchDone2 = new CountDownLatch(numberOfLines);

         buffer = AsynchronousFileImpl.newBuffer(size);
         encodeBufer(buffer);

         preAlloc(controller, numberOfLines * size);
         preAlloc(controller2, numberOfLines * size);

         ArrayList<CountDownCallback> list = new ArrayList<CountDownCallback>();
         ArrayList<CountDownCallback> list2 = new ArrayList<CountDownCallback>();

         for (int i = 0; i < numberOfLines; i++)
         {
            list.add(new CountDownCallback(latchDone, errors, listResult1, i));
            list2.add(new CountDownCallback(latchDone2, errors, listResult2, i));
         }

         int counter = 0;

         Iterator<CountDownCallback> iter2 = list2.iterator();

         for (CountDownCallback cb1 : list)
         {
            CountDownCallback cb2 = iter2.next();

            controller.write(counter * size, size, buffer, cb1);
            controller2.write(counter * size, size, buffer, cb2);
            ++counter;

         }

         UnitTestCase.waitForLatch(latchDone);
         UnitTestCase.waitForLatch(latchDone2);

         CountDownCallback.checkResults(numberOfLines, listResult1);
         CountDownCallback.checkResults(numberOfLines, listResult2);

         for (CountDownCallback callback : list)
         {
            Assert.assertEquals(1, callback.timesDoneCalled.get());
            Assert.assertTrue(callback.doneCalled);
         }

         for (CountDownCallback callback : list2)
         {
            Assert.assertEquals(1, callback.timesDoneCalled.get());
            Assert.assertTrue(callback.doneCalled);
         }

         Assert.assertEquals(0, errors.get());

         controller.close();
      }
      finally
      {
         try
         {
            controller2.close();
         }
         catch (Exception ignored)
         {
         }
      }
   }

   @Test
   public void testAddBeyongSimultaneousLimit() throws Exception
   {
      asyncData(3000, 1024, 10);
   }

   @Test
   public void testAddAsyncData() throws Exception
   {
      asyncData(10000, 1024, 30000);
   }

   private static final class LocalCallback implements AIOCallback
   {
      private final CountDownLatch latch = new CountDownLatch(1);

      volatile boolean error;

      public void done()
      {
         latch.countDown();
      }

      public void onError(final int errorCode, final String errorMessage)
      {
         error = true;
         latch.countDown();
      }
   }

   @Test
   public void testReleaseNullBuffer() throws Exception
   {
      boolean failed = false;
      try
      {
         AsynchronousFileImpl.destroyBuffer(null);
      }
      catch (Exception expected)
      {
         failed = true;
      }

      assertTrue("Exception expected", failed);
   }

   @Test
   public void testInvalidReads() throws Exception
   {
      controller = new AsynchronousFileImpl(executor, pollerExecutor);

      final int SIZE = 512;

      controller.open(fileName, 10);
      controller.close();

      controller = new AsynchronousFileImpl(executor, pollerExecutor);

      controller.open(fileName, 10);

      controller.fill(0, 1, 512, (byte) 'j');

      buffer = AsynchronousFileImpl.newBuffer(SIZE);

      buffer.clear();

      for (int i = 0; i < SIZE; i++)
      {
         buffer.put((byte) (i % 100));
      }

      LocalCallback callbackLocal = new LocalCallback();

      controller.write(0, 512, buffer, callbackLocal);

      waitForLatch(callbackLocal.latch);

      {
         ByteBuffer newBuffer = AsynchronousFileImpl.newBuffer(512);

         try
         {
            callbackLocal = new LocalCallback();

            controller.read(0, 50, newBuffer, callbackLocal);

            waitForLatch(callbackLocal.latch);

            Assert.assertTrue(callbackLocal.error);

         }
         finally
         {
            // We have to destroy the native buffer manually as it was created with a malloc like C function
            destroy(newBuffer);
            newBuffer = null;
         }
      }
      callbackLocal = new LocalCallback();

      byte[] bytes = new byte[512];

      {
         try
         {
            ByteBuffer newBuffer = ByteBuffer.wrap(bytes);

            controller.read(0, 512, newBuffer, callbackLocal);

            Assert.fail("An exception was supposed to be thrown");
         }
         catch (ActiveMQException ignored)
         {
            System.out.println(ignored);
         }
      }

      {
         final ByteBuffer newBuffer = AsynchronousFileImpl.newBuffer(512);
         try
         {
            callbackLocal = new LocalCallback();
            controller.read(0, 512, newBuffer, callbackLocal);
            waitForLatch(callbackLocal.latch);
            Assert.assertFalse(callbackLocal.error);

            newBuffer.rewind();

            byte[] bytesRead = new byte[SIZE];

            newBuffer.get(bytesRead);

            for (int i = 0; i < SIZE; i++)
            {
               Assert.assertEquals((byte) (i % 100), bytesRead[i]);
            }
         }
         finally
         {
            destroy(newBuffer);
         }
      }
   }

   private static void destroy(ByteBuffer buffer0)
   {
      if (buffer0 != null)
      {
         AsynchronousFileImpl.destroyBuffer(buffer0);
      }
   }

   @Test
   public void testBufferCallbackUniqueBuffers() throws Exception
   {
      controller = new AsynchronousFileImpl(executor, pollerExecutor);
      final int NUMBER_LINES = 1000;
      final int SIZE = 512;

      controller.open(fileName, 1000);

      controller.fill(0, 1, NUMBER_LINES * SIZE, (byte) 'j');

      final ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

      BufferCallback bufferCallback = new BufferCallback()
      {
         public void bufferDone(final ByteBuffer buffer)
         {
            buffers.add(buffer);
         }
      };

      controller.setBufferCallback(bufferCallback);

      CountDownLatch latch = new CountDownLatch(NUMBER_LINES);
      ArrayList<Integer> result = new ArrayList<Integer>();
      for (int i = 0; i < NUMBER_LINES; i++)
      {
         ByteBuffer buffer1 = AsynchronousFileImpl.newBuffer(SIZE);
         buffer1.rewind();
         for (int j = 0; j < SIZE; j++)
         {
            buffer1.put((byte) (j % Byte.MAX_VALUE));
         }
         CountDownCallback aio = new CountDownCallback(latch, null, result, i);
         controller.write(i * SIZE, SIZE, buffer1, aio);
      }

      // The buffer callback is only called after the complete callback was
      // called.
      // Because of that a race could happen on the assertions to
      // buffers.size what would invalidate the test
      // We close the file and that would guarantee the buffer callback was
      // called for all the elements
      controller.close();

      CountDownCallback.checkResults(NUMBER_LINES, result);

      // Make sure all the buffers are unique
      ByteBuffer lineOne = null;
      for (ByteBuffer bufferTmp : buffers)
      {
         if (lineOne == null)
         {
            lineOne = bufferTmp;
         }
         else
         {
            Assert.assertTrue(lineOne != bufferTmp);
         }
      }

      for (ByteBuffer bufferTmp : buffers)
      {
         destroy(bufferTmp);
      }

      buffers.clear();
   }

   @Test
   public void testBufferCallbackAwaysSameBuffer() throws Exception
   {

      controller = new AsynchronousFileImpl(executor, pollerExecutor);

      final int NUMBER_LINES = 1000;
      final int SIZE = 512;

      controller.open(fileName, 1000);

      controller.fill(0, 1, NUMBER_LINES * SIZE, (byte) 'j');

      final ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

      BufferCallback bufferCallback = new BufferCallback()
      {
         public void bufferDone(final ByteBuffer buffer)
         {
            buffers.add(buffer);
         }
      };

      controller.setBufferCallback(bufferCallback);

      CountDownLatch latch = new CountDownLatch(NUMBER_LINES);

      buffer = AsynchronousFileImpl.newBuffer(SIZE);
      buffer.rewind();
      for (int j = 0; j < SIZE; j++)
      {
         buffer.put((byte) (j % Byte.MAX_VALUE));
      }

      ArrayList<Integer> result = new ArrayList<Integer>();

      for (int i = 0; i < NUMBER_LINES; i++)
      {
         CountDownCallback aio = new CountDownCallback(latch, null, result, i);
         controller.write(i * SIZE, SIZE, buffer, aio);
      }

      // The buffer callback is only called after the complete callback was
      // called.
      // Because of that a race could happen on the assertions to
      // buffers.size what would invalidate the test
      // We close the file and that would guarantee the buffer callback was
      // called for all the elements
      controller.close();

      CountDownCallback.checkResults(NUMBER_LINES, result);

      Assert.assertEquals(NUMBER_LINES, buffers.size());

      // Make sure all the buffers are unique
      ByteBuffer lineOne = null;
      for (ByteBuffer bufferTmp : buffers)
      {
         if (lineOne == null)
         {
            lineOne = bufferTmp;
         }
         else
         {
            Assert.assertTrue(lineOne == bufferTmp);
         }
      }

      buffers.clear();
   }

   @Test
   public void testRead() throws Exception
   {
      controller = new AsynchronousFileImpl(executor, pollerExecutor);
      controller.setBufferCallback(bufferCallbackDestroy);

      final int NUMBER_LINES = 1000;
      final int SIZE = 1024;

      controller.open(fileName, 1000);

      controller.fill(0, 1, NUMBER_LINES * SIZE, (byte) 'j');

      {
         CountDownLatch latch = new CountDownLatch(NUMBER_LINES);
         ArrayList<Integer> result = new ArrayList<Integer>();

         AtomicInteger errors = new AtomicInteger(0);

         for (int i = 0; i < NUMBER_LINES; i++)
         {
            if (i % 100 == 0)
            {
               System.out.println("Wrote " + i + " lines");
            }
            final ByteBuffer buffer0 = AsynchronousFileImpl.newBuffer(SIZE);
            for (int j = 0; j < SIZE; j++)
            {
               buffer0.put(UnitTestCase.getSamplebyte(j));
            }

            CountDownCallback aio = new CountDownCallback(latch, errors, result, i);
            controller.write(i * SIZE, SIZE, buffer0, aio);
         }

         waitForLatch(latch);

         Assert.assertEquals(0, errors.get());

         CountDownCallback.checkResults(NUMBER_LINES, result);
      }

      // If you call close you're supposed to wait events to finish before
      // closing it
      controller.close();
      controller.setBufferCallback(null);

      controller.open(fileName, 10);

      buffer = AsynchronousFileImpl.newBuffer(SIZE);

      for (int i = 0; i < NUMBER_LINES; i++)
      {
         if (i % 100 == 0)
         {
            System.out.println("Read " + i + " lines");
         }
         AsynchronousFileImpl.clearBuffer(buffer);

         CountDownLatch latch = new CountDownLatch(1);
         AtomicInteger errors = new AtomicInteger(0);
         CountDownCallback aio = new CountDownCallback(latch, errors, null, 0);

         controller.read(i * SIZE, SIZE, buffer, aio);

         waitForLatch(latch);
         Assert.assertEquals(0, errors.get());
         Assert.assertTrue(aio.doneCalled);

         byte[] bytesRead = new byte[SIZE];
         buffer.get(bytesRead);

         for (int count = 0; count < SIZE; count++)
         {
            Assert.assertEquals("byte position " + count + " differs on line " + i + " position = " + count,
                                UnitTestCase.getSamplebyte(count),
                                bytesRead[count]);
         }
      }
   }

   /**
    * This test will call file.close() when there are still callbacks being processed.
    * This could cause a crash or callbacks missing and this test is validating both situations.
    * The file is also read after being written to validate its correctness
    */
   @Test
   public void testConcurrentClose() throws Exception
   {
      controller = new AsynchronousFileImpl(executor, pollerExecutor);
      final int NUMBER_LINES = 1000;
      CountDownLatch readLatch = new CountDownLatch(NUMBER_LINES);
      final int SIZE = 1024;

      controller.open(fileName, 10000);

      controller.fill(0, 1, NUMBER_LINES * SIZE, (byte) 'j');

      controller.setBufferCallback(bufferCallbackDestroy);

      for (int i = 0; i < NUMBER_LINES; i++)
      {
         ByteBuffer buffer = AsynchronousFileImpl.newBuffer(SIZE);

         buffer.clear();
         addString("Str value " + i + "\n", buffer);
         for (int j = buffer.position(); j < buffer.capacity() - 1; j++)
         {
            buffer.put((byte) ' ');
         }
         buffer.put((byte) '\n');

         CountDownCallback aio = new CountDownCallback(readLatch, null, null, 0);
         controller.write(i * SIZE, SIZE, buffer, aio);
      }

      // If you call close you're supposed to wait events to finish before
      // closing it
      controller.close();

      controller.setBufferCallback(null);

      Assert.assertEquals(0, readLatch.getCount());
      waitForLatch(readLatch);
      controller.open(fileName, 10);

      ByteBuffer newBuffer = AsynchronousFileImpl.newBuffer(SIZE);

      ByteBuffer buffer = AsynchronousFileImpl.newBuffer(SIZE);

      for (int i = 0; i < NUMBER_LINES; i++)
      {
         newBuffer.clear();
         addString("Str value " + i + "\n", newBuffer);
         for (int j = newBuffer.position(); j < newBuffer.capacity() - 1; j++)
         {
            newBuffer.put((byte) ' ');
         }
         newBuffer.put((byte) '\n');

         CountDownLatch latch = new CountDownLatch(1);
         CountDownCallback aio = new CountDownCallback(latch, null, null, 0);
         controller.read(i * SIZE, SIZE, buffer, aio);
         waitForLatch(latch);
         Assert.assertEquals(0, aio.errorCalled);
         Assert.assertTrue(aio.doneCalled);

         byte[] bytesRead = new byte[SIZE];
         byte[] bytesCompare = new byte[SIZE];

         newBuffer.rewind();
         newBuffer.get(bytesCompare);
         buffer.rewind();
         buffer.get(bytesRead);

         for (int count = 0; count < SIZE; count++)
         {
            Assert.assertEquals("byte position " + count + " differs on line " + i,
                                bytesCompare[count],
                                bytesRead[count]);
         }

         Assert.assertTrue(buffer.equals(newBuffer));
      }

      destroy(newBuffer);
   }

   private void asyncData(final int numberOfLines, final int size, final int aioLimit) throws Exception
   {
      controller = new AsynchronousFileImpl(executor, pollerExecutor);
      controller.open(fileName, aioLimit);


      CountDownLatch latchDone = new CountDownLatch(numberOfLines);

      buffer = AsynchronousFileImpl.newBuffer(size);
      encodeBufer(buffer);

      preAlloc(controller, numberOfLines * size);

      ArrayList<CountDownCallback> list = new ArrayList<CountDownCallback>();

      ArrayList<Integer> result = new ArrayList<Integer>();

      for (int i = 0; i < numberOfLines; i++)
      {
         list.add(new CountDownCallback(latchDone, null, result, i));
      }

      long valueInitial = System.currentTimeMillis();

      long lastTime = System.currentTimeMillis();
      int counter = 0;
      for (CountDownCallback tmp : list)
      {
         controller.write(counter * size, size, buffer, tmp);
         if (++counter % 20000 == 0)
         {
            AsynchronousFileTest.debug(20000 * 1000 / (System.currentTimeMillis() - lastTime) + " rec/sec (Async)");
            lastTime = System.currentTimeMillis();
         }

      }

      UnitTestCase.waitForLatch(latchDone);

      long timeTotal = System.currentTimeMillis() - valueInitial;

      CountDownCallback.checkResults(numberOfLines, result);

      AsynchronousFileTest.debug("After completions time = " + timeTotal +
                                    " for " +
                                    numberOfLines +
                                    " registers " +
                                    " size each line = " +
                                    size +
                                    ", Records/Sec=" +
                                    numberOfLines *
                                       1000 /
                                       timeTotal +
                                    " (Assynchronous)");

      for (CountDownCallback tmp : list)
      {
         Assert.assertEquals(1, tmp.timesDoneCalled.get());
         Assert.assertTrue(tmp.doneCalled);
         Assert.assertEquals(0, tmp.errorCalled);
      }

      controller.close();
   }

   @Test
   public void testDirectSynchronous() throws Exception
   {

      final int NUMBER_LINES = 3000;
      final int SIZE = 1024;

      controller = new AsynchronousFileImpl(executor, pollerExecutor);
      controller.open(fileName, 2000);

      buffer = AsynchronousFileImpl.newBuffer(SIZE);
      encodeBufer(buffer);

      preAlloc(controller, NUMBER_LINES * SIZE);

      long startTime = System.currentTimeMillis();

      for (int i = 0; i < NUMBER_LINES; i++)
      {
         CountDownLatch latchDone = new CountDownLatch(1);
         CountDownCallback aioBlock = new CountDownCallback(latchDone, null, null, 0);
         controller.write(i * 512, 512, buffer, aioBlock);
         UnitTestCase.waitForLatch(latchDone);
         Assert.assertTrue(aioBlock.doneCalled);
         Assert.assertEquals(0, aioBlock.errorCalled);
      }

      long timeTotal = System.currentTimeMillis() - startTime;
      AsynchronousFileTest.debug("time = " + timeTotal +
                                    " for " +
                                    NUMBER_LINES +
                                    " registers " +
                                    " size each line = " +
                                    SIZE +
                                    " Records/Sec=" +
                                    NUMBER_LINES *
                                       1000 /
                                       timeTotal +
                                    " Synchronous");

      controller.close();
   }


   @Test
   public void testInternalWrite() throws Exception
   {
      controller = new AsynchronousFileImpl(executor, pollerExecutor);
      controller.open(fileName, 2000);

      final int SIZE = 10 * 512;

      buffer = AsynchronousFileImpl.newBuffer(SIZE);

      for (int i = 0; i < SIZE; i++)
      {
         buffer.put(getSamplebyte(i));
      }

      controller.writeInternal(0, SIZE, buffer);

      InputStream fileInput = new BufferedInputStream(new FileInputStream(new File(fileName)));

      for (int i = 0; i < SIZE; i++)
      {
         assertEquals(getSamplebyte(i), fileInput.read());
      }

      assertEquals(-1, fileInput.read());

   }


   @Test
   public void testInvalidWrite() throws Exception
   {
      controller = new AsynchronousFileImpl(executor, pollerExecutor);
      controller.open(fileName, 2000);

      final int SIZE = 512;

      buffer = AsynchronousFileImpl.newBuffer(SIZE);
      encodeBufer(buffer);

      preAlloc(controller, 10 * 512);

      CountDownLatch latchDone = new CountDownLatch(1);

      CountDownCallback aioBlock = new CountDownCallback(latchDone, null, null, 0);
      controller.write(11, 512, buffer, aioBlock);

      UnitTestCase.waitForLatch(latchDone);

      Assert.assertTrue(aioBlock.errorCalled != 0);
      Assert.assertFalse(aioBlock.doneCalled);
   }

   @Test
   public void testInvalidAlloc() throws Exception
   {
      try
      {
         @SuppressWarnings("unused")
         ByteBuffer buffer = AsynchronousFileImpl.newBuffer(300);
         Assert.fail("Exception expected");
      }
      catch (Exception ignored)
      {
      }

   }

   // This is in particular testing for http://bugs.sun.com/view_bug.do?bug_id=6791815
   @Test
   public void testAllocations() throws Exception
   {
      final AtomicInteger errors = new AtomicInteger(0);

      Thread[] ts = new Thread[100];

      final CountDownLatch align = new CountDownLatch(ts.length);
      final CountDownLatch start = new CountDownLatch(1);

      for (int i = 0; i < ts.length; i++)
      {
         ts[i] = new Thread()
         {
            @Override
            public void run()
            {
               try
               {
                  align.countDown();
                  start.await();
                  for (int j = 0; j < 1000; j++)
                  {
                     ByteBuffer buffer = AsynchronousFileImpl.newBuffer(512);
                     AsynchronousFileTest.destroy(buffer);
                  }
               }
               catch (Throwable e)
               {
                  e.printStackTrace();
                  errors.incrementAndGet();
               }
            }
         };
         ts[i].start();
      }

      align.await();
      start.countDown();

      for (Thread t : ts)
      {
         t.join();
      }

      Assert.assertEquals(0, errors.get());
   }

   @Test
   public void testSize() throws Exception
   {
      controller = new AsynchronousFileImpl(executor, pollerExecutor);

      final int NUMBER_LINES = 10;
      final int SIZE = 1024;

      controller.open(fileName, 1);

      controller.fill(0, 1, NUMBER_LINES * SIZE, (byte) 'j');

      Assert.assertEquals(NUMBER_LINES * SIZE, controller.size());
   }

   private static void addString(final String str, final ByteBuffer buffer)
   {
      CharBuffer charBuffer = CharBuffer.wrap(str);
      AsynchronousFileTest.UTF_8_ENCODER.encode(charBuffer, buffer, true);
   }
}

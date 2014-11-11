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
package org.apache.activemq6.tests.unit.core.asyncio;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.core.asyncio.AIOCallback;
import org.apache.activemq6.core.asyncio.impl.AsynchronousFileImpl;
import org.apache.activemq6.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * The base class for AIO Tests
 * @author Clebert Suconic
 *
 */
public abstract class AIOTestBase extends UnitTestCase
{
   // The AIO Test must use a local filesystem. Sometimes $HOME is on a NFS on
   // most enterprise systems

   protected String fileName;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      fileName = getTestDir() + "/fileUsedOnNativeTests.log";

      Assert.assertTrue(String.format("libAIO is not loaded on %s %s %s", System.getProperty("os.name"),
                                      System.getProperty("os.arch"), System.getProperty("os.version")),
                        AsynchronousFileImpl.isLoaded());
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      Assert.assertEquals(0, AsynchronousFileImpl.getTotalMaxIO());

      super.tearDown();
   }

   protected void encodeBufer(final ByteBuffer buffer)
   {
      buffer.clear();
      int size = buffer.limit();
      for (int i = 0; i < size - 1; i++)
      {
         buffer.put((byte)('a' + i % 20));
      }

      buffer.put((byte)'\n');

   }

   protected void preAlloc(final AsynchronousFileImpl controller, final long size) throws HornetQException
   {
      controller.fill(0L, 1, size, (byte)0);
   }

   protected static class CountDownCallback implements AIOCallback
   {
      private final CountDownLatch latch;

      private final List<Integer> outputList;

      private final int order;

      private final AtomicInteger errors;

      public CountDownCallback(final CountDownLatch latch,
                               final AtomicInteger errors,
                               final List<Integer> outputList,
                               final int order)
      {
         this.latch = latch;

         this.outputList = outputList;

         this.order = order;

         this.errors = errors;
      }

      volatile boolean doneCalled = false;

      volatile int errorCalled = 0;

      final AtomicInteger timesDoneCalled = new AtomicInteger(0);

      public void done()
      {
         if (outputList != null)
         {
            outputList.add(order);
         }
         doneCalled = true;
         timesDoneCalled.incrementAndGet();
         if (latch != null)
         {
            latch.countDown();
         }
      }

      public void onError(final int errorCode, final String errorMessage)
      {
         errorCalled++;
         if (outputList != null)
         {
            outputList.add(order);
         }
         if (errors != null)
         {
            errors.incrementAndGet();
         }
         if (latch != null)
         {
            // even thought an error happened, we need to inform the latch,
            // or the test won't finish
            latch.countDown();
         }
      }

      public static void checkResults(final int numberOfElements, final ArrayList<Integer> result)
      {
         Assert.assertEquals(numberOfElements, result.size());
         int i = 0;
         for (Integer resultI : result)
         {
            Assert.assertEquals(i++, resultI.intValue());
         }
      }
   }

}

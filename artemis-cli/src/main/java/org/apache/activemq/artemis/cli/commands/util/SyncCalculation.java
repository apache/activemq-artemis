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

package org.apache.activemq.artemis.cli.commands.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.utils.ReusableLatch;

/**
 * It will perform a simple test to evaluate how many syncs a disk can make per second
 * * *
 */
public class SyncCalculation
{
   /**
    * It will perform a write test of blockSize * bocks, sinc on each write, for N tries.
    * It will return the lowest spent time from the tries.
    */
   public static long syncTest(File datafolder, int blockSize, int blocks, int tries, boolean verbose, boolean aio) throws Exception
   {
      SequentialFileFactory factory = newFactory(datafolder, aio);
      SequentialFile file = factory.createSequentialFile("test.tmp");

      try
      {
         file.delete();
         file.open();

         file.fill(blockSize * blocks);

         long[] result = new long[tries];

         byte[] block = new byte[blockSize];

         for (int i = 0; i < block.length; i++)
         {
            block[i] = (byte) 't';
         }

         ByteBuffer bufferBlock = factory.newBuffer(blockSize);
         bufferBlock.put(block);
         bufferBlock.position(0);

         final ReusableLatch latch = new ReusableLatch(0);

         IOCallback callback = new IOCallback()
         {
            @Override
            public void done()
            {
               latch.countDown();
            }

            @Override
            public void onError(int errorCode, String errorMessage)
            {

            }
         };

         DecimalFormat dcformat = new DecimalFormat("###.##");
         for (int ntry = 0; ntry < tries; ntry++)
         {

            if (verbose)
            {
               System.out.println("**************************************************");
               System.out.println(ntry + " of " + tries + " calculation");
            }
            file.position(0);
            long start = System.currentTimeMillis();
            for (int i = 0; i < blocks; i++)
            {
               bufferBlock.position(0);
               latch.countUp();
               file.writeDirect(bufferBlock, true, callback);
               if (!latch.await(5, TimeUnit.SECONDS))
               {
                  throw new IOException("Callback wasn't called");
               }
            }
            long end = System.currentTimeMillis();

            result[ntry] = (end - start);

            if (verbose)
            {
               double writesPerMillisecond = (double)blocks / (double) result[ntry];
               System.out.println("Time = " + result[ntry]);
               System.out.println("Writes / millisecond = " + dcformat.format(writesPerMillisecond));
               System.out.println("bufferTimeout = " + toNanos(result[ntry], blocks));
               System.out.println("**************************************************");
            }
         }

         factory.releaseDirectBuffer(bufferBlock);

         long totalTime = Long.MAX_VALUE;
         for (int i = 0; i < tries; i++)
         {
            if (result[i] < totalTime)
            {
               totalTime = result[i];
            }
         }

         return totalTime;
      }
      finally
      {
         try
         {
            file.close();
         }
         catch (Exception e)
         {
         }
         try
         {
            file.delete();
         }
         catch (Exception e)
         {
         }
         try
         {
            factory.stop();
         }
         catch (Exception e)
         {
         }
      }
   }


   public static long toNanos(long time, long blocks)
   {

      double blocksPerMillisecond = (double) blocks / (double) (time);

      long nanoSeconds = TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS);

      long timeWait = (long) (nanoSeconds / blocksPerMillisecond);

      return timeWait;
   }

   private static SequentialFileFactory newFactory(File datafolder, boolean aio)
   {
      if (aio && LibaioContext.isLoaded())
      {
         SequentialFileFactory factory = new AIOSequentialFileFactory(datafolder, 1);
         factory.start();
         ((AIOSequentialFileFactory) factory).disableBufferReuse();

         return factory;
      }
      else
      {
         SequentialFileFactory factory = new NIOSequentialFileFactory(datafolder, 1);
         factory.start();
         return factory;
      }
   }
}

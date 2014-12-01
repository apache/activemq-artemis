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
package org.apache.activemq.core.journal.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.apache.activemq.core.journal.IOAsyncTask;
import org.apache.activemq.core.journal.SequentialFile;
import org.apache.activemq.core.journal.SequentialFileFactory;
import org.apache.activemq.journal.ActiveMQJournalLogger;

/**
 * A SyncSpeedTest
 *
 * This class just provides some diagnostics on how fast your disk can sync
 * Useful when determining performance issues
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> fox
 *
 *
 */
public class SyncSpeedTest
{
   public static void main(final String[] args)
   {
      try
      {
         new SyncSpeedTest().testScaleAIO();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   protected SequentialFileFactory fileFactory;

   public boolean AIO = true;

   protected void setupFactory()
   {
      if (AIO)
      {
         fileFactory = new AIOSequentialFileFactory(".", 0, 0, false, null);
      }
      else
      {
         fileFactory = new NIOSequentialFileFactory(".", false, 0, 0, false, null);
      }
   }

   protected SequentialFile createSequentialFile(final String fileName)
   {
      if (AIO)
      {
         return new AIOSequentialFile(fileFactory,
                                      0,
                                      0,
                                      ".",
                                      fileName,
                                      100000,
                                      null,
                                      null,
                                      Executors.newSingleThreadExecutor());
      }
      else
      {
         return new NIOSequentialFile(fileFactory, new File(fileName), 1000, null);
      }
   }

   public void run2() throws Exception
   {
      setupFactory();

      int recordSize = 128 * 1024;

      while (true)
      {
         System.out.println("** record size is " + recordSize);

         int warmup = 500;

         int its = 500;

         int fileSize = (its + warmup) * recordSize;

         SequentialFile file = createSequentialFile("sync-speed-test.dat");

         if (file.exists())
         {
            file.delete();
         }

         file.open();

         file.fill(0, fileSize, (byte)'X');

         if (!AIO)
         {
            file.sync();
         }

         ByteBuffer bb1 = generateBuffer(recordSize, (byte)'h');

         long start = 0;

         for (int i = 0; i < its + warmup; i++)
         {
            if (i == warmup)
            {
               start = System.currentTimeMillis();
            }

            bb1.rewind();

            file.writeDirect(bb1, true);
         }

         long end = System.currentTimeMillis();

         double rate = 1000 * (double)its / (end - start);

         double throughput = recordSize * rate;

         System.out.println("Rate of " + rate + " syncs per sec");
         System.out.println("Throughput " + throughput + " bytes per sec");
         System.out.println("*************");

         recordSize *= 2;
      }
   }

   public void run() throws Exception
   {
      int recordSize = 256;

      while (true)
      {
         System.out.println("** record size is " + recordSize);

         int warmup = 500;

         int its = 500;

         int fileSize = (its + warmup) * recordSize;

         File file = new File("sync-speed-test.dat");

         if (file.exists())
         {
            if (!file.delete())
            {
               ActiveMQJournalLogger.LOGGER.errorDeletingFile(file);
            }
         }

         boolean created = file.createNewFile();
         if (!created)
            throw new IOException("could not create file " + file);

         RandomAccessFile rfile = new RandomAccessFile(file, "rw");

         FileChannel channel = rfile.getChannel();

         ByteBuffer bb = generateBuffer(fileSize, (byte)'x');

         write(bb, channel, fileSize);

         channel.force(true);

         channel.position(0);

         ByteBuffer bb1 = generateBuffer(recordSize, (byte)'h');

         long start = 0;

         for (int i = 0; i < its + warmup; i++)
         {
            if (i == warmup)
            {
               start = System.currentTimeMillis();
            }

            bb1.flip();
            channel.write(bb1);
            channel.force(false);
         }

         long end = System.currentTimeMillis();

         double rate = 1000 * (double)its / (end - start);

         double throughput = recordSize * rate;

         System.out.println("Rate of " + rate + " syncs per sec");
         System.out.println("Throughput " + throughput + " bytes per sec");

         recordSize *= 2;
      }
   }

   public void testScaleAIO() throws Exception
   {
      setupFactory();

      final int recordSize = 1024;

      System.out.println("** record size is " + recordSize);

      final int its = 10;

      for (int numThreads = 1; numThreads <= 10; numThreads++)
      {

         int fileSize = its * recordSize * numThreads;

         final SequentialFile file = createSequentialFile("sync-speed-test.dat");

         if (file.exists())
         {
            file.delete();
         }

         file.open();

         file.fill(0, fileSize, (byte)'X');

         if (!AIO)
         {
            file.sync();
         }

         final CountDownLatch latch = new CountDownLatch(its * numThreads);

         class MyIOAsyncTask implements IOAsyncTask
         {
            public void done()
            {
               latch.countDown();
            }

            public void onError(final int errorCode, final String errorMessage)
            {

            }
         }

         final MyIOAsyncTask task = new MyIOAsyncTask();

         class MyRunner implements Runnable
         {
            private final ByteBuffer bb1;

            MyRunner()
            {
               bb1 = generateBuffer(recordSize, (byte)'h');
            }

            public void run()
            {
               for (int i = 0; i < its; i++)
               {
                  bb1.rewind();

                  file.writeDirect(bb1, true, task);
                  // try
                  // {
                  // file.writeDirect(bb1, true);
                  // }
                  // catch (Exception e)
                  // {
                  // e.printStackTrace();
                  // }
               }
            }
         }

         Set<Thread> threads = new HashSet<Thread>();

         for (int i = 0; i < numThreads; i++)
         {
            MyRunner runner = new MyRunner();

            Thread t = new Thread(runner);

            threads.add(t);
         }

         long start = System.currentTimeMillis();

         for (Thread t : threads)
         {
            ActiveMQJournalLogger.LOGGER.startingThread();
            t.start();
         }

         for (Thread t : threads)
         {
            t.join();
         }

         latch.await();

         long end = System.currentTimeMillis();

         double rate = 1000 * (double)its * numThreads / (end - start);

         double throughput = recordSize * rate;

         System.out.println("For " + numThreads + " threads:");
         System.out.println("Rate of " + rate + " records per sec");
         System.out.println("Throughput " + throughput + " bytes per sec");
         System.out.println("*************");
      }
   }

   private void write(final ByteBuffer buffer, final FileChannel channel, final int size) throws Exception
   {
      buffer.flip();

      channel.write(buffer);
   }

   private ByteBuffer generateBuffer(final int size, final byte ch)
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(size);

      for (int i = 0; i < size; i++)
      {
         bb.put(ch);
      }

      return bb;
   }
}
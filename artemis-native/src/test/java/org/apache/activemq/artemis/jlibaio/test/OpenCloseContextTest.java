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

package org.apache.activemq.artemis.jlibaio.test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.jlibaio.LibaioFile;
import org.apache.activemq.artemis.jlibaio.SubmitInfo;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OpenCloseContextTest {

   @BeforeClass
   public static void testAIO() {
      Assume.assumeTrue(LibaioContext.isLoaded());
   }

   @Rule
   public TemporaryFolder folder;

   public OpenCloseContextTest() {
      folder = new TemporaryFolder(new File("./target"));
   }

   @Test
   public void testRepeatOpenCloseContext() throws Exception {
      ByteBuffer buffer = LibaioContext.newAlignedBuffer(512, 512);
      for (int i = 0; i < 512; i++)
         buffer.put((byte) 'x');

      for (int i = 0; i < 10; i++) {
         System.out.println("#test " + i);
         final LibaioContext control = new LibaioContext<>(5, true, true);
         Thread t = new Thread() {
            @Override
            public void run() {
               control.poll();
            }
         };
         t.start();
         LibaioFile file = control.openFile(folder.newFile(), true);
         file.fill(file.getBlockSize(),4 * 1024);
         final CountDownLatch insideMethod = new CountDownLatch(1);
         final CountDownLatch awaitInside = new CountDownLatch(1);
         file.write(0, 512, buffer, new SubmitInfo() {
            @Override
            public void onError(int errno, String message) {

            }

            @Override
            public void done() {
               insideMethod.countDown();
               try {
                  awaitInside.await();
               } catch (Throwable e) {
                  e.printStackTrace();
               }
               System.out.println("done");
            }
         });

         insideMethod.await();

         file.write(512, 512, buffer, new SubmitInfo() {
            @Override
            public void onError(int errno, String message) {
            }

            @Override
            public void done() {
            }
         });

         awaitInside.countDown();
         control.close();

         t.join();
      }

   }

   @Test
   public void testRepeatOpenCloseContext2() throws Exception {
      ByteBuffer buffer = LibaioContext.newAlignedBuffer(512, 512);
      for (int i = 0; i < 512; i++)
         buffer.put((byte) 'x');

      for (int i = 0; i < 10; i++) {
         System.out.println("#test " + i);
         final LibaioContext control = new LibaioContext<>(5, true, true);
         Thread t = new Thread() {
            @Override
            public void run() {
               control.poll();
            }
         };
         t.start();
         LibaioFile file = control.openFile(folder.newFile(), true);
         file.fill(file.getBlockSize(), 4 * 1024);
         final CountDownLatch insideMethod = new CountDownLatch(1);
         final CountDownLatch awaitInside = new CountDownLatch(1);
         file.write(0, 512, buffer, new SubmitInfo() {
            @Override
            public void onError(int errno, String message) {

            }

            @Override
            public void done() {
               insideMethod.countDown();
               try {
                  awaitInside.await(100, TimeUnit.MILLISECONDS);
               } catch (Throwable e) {
                  e.printStackTrace();
               }
               System.out.println("done");
            }
         });

         insideMethod.await();

         file.write(512, 512, buffer, new SubmitInfo() {
            @Override
            public void onError(int errno, String message) {
            }

            @Override
            public void done() {
            }
         });

         awaitInside.countDown();

         control.close();

         t.join();
      }

   }

   @Test
   public void testCloseAndStart() throws Exception {
      final LibaioContext control2 = new LibaioContext<>(5, true, true);

      final LibaioContext control = new LibaioContext<>(5, true, true);
      control.close();
      control.poll();

      control2.close();
      control2.poll();

      System.out.println("Hello!!");
   }

}

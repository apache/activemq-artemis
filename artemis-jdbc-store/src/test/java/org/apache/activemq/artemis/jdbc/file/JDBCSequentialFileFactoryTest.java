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
package org.apache.activemq.artemis.jdbc.file;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFile;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFileFactory;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JDBCSequentialFileFactoryTest {

   private static String connectionUrl = "jdbc:derby:target/data;create=true";

   private static String tableName = "FILES";

   private static String className = EmbeddedDriver.class.getCanonicalName();

   private JDBCSequentialFileFactory factory;

   @Before
   public void setup() throws Exception {
      Executor executor = Executors.newSingleThreadExecutor(ActiveMQThreadFactory.defaultThreadFactory());

      factory = new JDBCSequentialFileFactory(connectionUrl, tableName, className, executor);
      factory.start();
   }

   @After
   public void tearDown() throws SQLException {
      factory.destroy();
   }

   @Test
   public void testJDBCFileFactoryStarted() throws Exception {
      assertTrue(factory.isStarted());
   }

   @Test
   public void testCreateFiles() throws Exception {
      int noFiles = 100;
      Set<String> fileNames = new HashSet<String>();
      for (int i = 0; i < noFiles; i++) {
         String fileName = UUID.randomUUID().toString() + ".txt";
         fileNames.add(fileName);
         SequentialFile file = factory.createSequentialFile(fileName);
         // We create files on Open
         file.open();
      }

      List<String> queryFileNames = factory.listFiles("txt");
      assertTrue(queryFileNames.containsAll(fileNames));
   }

   @Test
   public void testAsyncAppendToFile() throws Exception {

      JDBCSequentialFile file = (JDBCSequentialFile) factory.createSequentialFile("test.txt");
      file.open();

      // Create buffer and fill with test data
      int bufferSize = 1024;
      ActiveMQBuffer src = ActiveMQBuffers.fixedBuffer(bufferSize);
      for (int i = 0; i < bufferSize; i++) {
         src.writeByte((byte) 1);
      }

      IOCallbackCountdown callback = new IOCallbackCountdown(1);
      file.internalWrite(src, callback);

      callback.assertEmpty(5);
      checkData(file, src);
   }

   @Test
   public void testCopyFile() throws Exception {
      JDBCSequentialFile file = (JDBCSequentialFile) factory.createSequentialFile("test.txt");
      file.open();

      // Create buffer and fill with test data
      int bufferSize = 1024;
      ActiveMQBuffer src = ActiveMQBuffers.fixedBuffer(bufferSize);
      for (int i = 0; i < bufferSize; i++) {
         src.writeByte((byte) 5);
      }

      IOCallbackCountdown callback = new IOCallbackCountdown(1);
      file.internalWrite(src, callback);

      JDBCSequentialFile copy = (JDBCSequentialFile) factory.createSequentialFile("copy.txt");
      file.copyTo(copy);

      checkData(copy, src);
      checkData(file, src);
   }

   @Test
   public void testCloneFile() throws Exception {
      JDBCSequentialFile file = (JDBCSequentialFile) factory.createSequentialFile("test.txt");
      file.open();

      // Create buffer and fill with test data
      int bufferSize = 1024;
      ActiveMQBuffer src = ActiveMQBuffers.fixedBuffer(bufferSize);
      for (int i = 0; i < bufferSize; i++) {
         src.writeByte((byte) 5);
      }

      IOCallbackCountdown callback = new IOCallbackCountdown(1);
      file.internalWrite(src, callback);

      JDBCSequentialFile copy = (JDBCSequentialFile) file.cloneFile();
   }

   private void checkData(JDBCSequentialFile file, ActiveMQBuffer expectedData) throws SQLException {
      expectedData.resetReaderIndex();

      byte[] resultingBytes = new byte[expectedData.readableBytes()];
      ByteBuffer byteBuffer = ByteBuffer.allocate(expectedData.readableBytes());

      file.read(byteBuffer, null);
      expectedData.getBytes(0, resultingBytes);

      assertArrayEquals(resultingBytes, byteBuffer.array());
   }

   private class IOCallbackCountdown implements IOCallback {

      private final CountDownLatch countDownLatch;

      public IOCallbackCountdown(int size) {
         this.countDownLatch = new CountDownLatch(size);
      }

      @Override
      public void done() {
         countDownLatch.countDown();
      }

      @Override
      public void onError(int errorCode, String errorMessage) {
         fail(errorMessage);
      }

      public void assertEmpty(int timeout) throws InterruptedException {
         countDownLatch.await(timeout, TimeUnit.SECONDS);
         assertEquals(countDownLatch.getCount(), 0);
      }
   }
}

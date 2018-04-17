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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFile;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFileFactory;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JDBCSequentialFileFactoryTest {

   @Rule
   public ThreadLeakCheckRule leakCheckRule = new ThreadLeakCheckRule();

   private static String className = EmbeddedDriver.class.getCanonicalName();

   private JDBCSequentialFileFactory factory;

   private ExecutorService executor;

   @Before
   public void setup() throws Exception {
      executor = Executors.newSingleThreadExecutor(ActiveMQThreadFactory.defaultThreadFactory());

      String connectionUrl = "jdbc:derby:target/data;create=true";
      String tableName = "FILES";
      factory = new JDBCSequentialFileFactory(connectionUrl, className, JDBCUtils.getSQLProvider(className, tableName, SQLProvider.DatabaseStoreType.PAGE), executor, new IOCriticalErrorListener() {
         @Override
         public void onIOException(Throwable code, String message, SequentialFile file) {
         }
      });
      factory.start();
   }

   @After
   public void tearDown() throws Exception {
      executor.shutdown();
      factory.destroy();
   }

   @After
   public void shutdownDerby() {
      try {
         DriverManager.getConnection("jdbc:derby:;shutdown=true");
      } catch (Exception ignored) {
      }
   }

   @Test
   public void testJDBCFileFactoryStarted() throws Exception {
      assertTrue(factory.isStarted());
   }

   @Test
   public void testReadZeroBytesOnEmptyFile() throws Exception {
      JDBCSequentialFile file = (JDBCSequentialFile) factory.createSequentialFile("test.txt");
      file.open();
      try {
         final ByteBuffer readBuffer = ByteBuffer.allocate(0);
         final int bytes = file.read(readBuffer);
         assertEquals(0, bytes);
      } finally {
         file.close();
      }
   }

   @Test
   public void testReadZeroBytesOnNotEmptyFile() throws Exception {
      final int fileLength = 8;
      JDBCSequentialFile file = (JDBCSequentialFile) factory.createSequentialFile("test.txt");
      file.open();
      try {
         file.writeDirect(ByteBuffer.allocate(fileLength), true);
         assertEquals(fileLength, file.size());
         final ByteBuffer readBuffer = ByteBuffer.allocate(0);
         final int bytes = file.read(readBuffer);
         assertEquals(0, bytes);
      } finally {
         file.close();
      }
   }

   @Test
   public void testReadOutOfBoundsOnEmptyFile() throws Exception {
      JDBCSequentialFile file = (JDBCSequentialFile) factory.createSequentialFile("test.txt");
      file.open();
      try {
         final ByteBuffer readBuffer = ByteBuffer.allocate(1);
         file.position(1);
         final int bytes = file.read(readBuffer);
         assertTrue("bytes read should be < 0", bytes < 0);
      } finally {
         file.close();
      }
   }

   @Test
   public void testReadOutOfBoundsOnNotEmptyFile() throws Exception {
      final int fileLength = 8;
      JDBCSequentialFile file = (JDBCSequentialFile) factory.createSequentialFile("test.txt");
      file.open();
      try {
         file.writeDirect(ByteBuffer.allocate(fileLength), true);
         assertEquals(fileLength, file.size());
         file.position(fileLength + 1);
         final ByteBuffer readBuffer = ByteBuffer.allocate(fileLength);
         final int bytes = file.read(readBuffer);
         assertTrue("bytes read should be < 0", bytes < 0);
      } finally {
         file.close();
      }
   }

   @Test
   public void testCreateFiles() throws Exception {
      int noFiles = 100;
      List<SequentialFile> files = new LinkedList<>();

      Set<String> fileNames = new HashSet<>();
      for (int i = 0; i < noFiles; i++) {
         String fileName = UUID.randomUUID().toString() + ".txt";
         fileNames.add(fileName);
         SequentialFile file = factory.createSequentialFile(fileName);
         // We create files on Open
         file.open();
         files.add(file);
      }

      List<String> queryFileNames = factory.listFiles("txt");
      assertTrue(queryFileNames.containsAll(fileNames));

      for (SequentialFile file: files) {
         file.close();
      }

      Assert.assertEquals(0, factory.getNumberOfOpenFiles());
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

      checkData(file, src);
      checkData(copy, src);
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

      assertEquals(bufferSize, file.size());
      JDBCSequentialFile copy = (JDBCSequentialFile) file.cloneFile();
      copy.open();

      assertEquals(bufferSize, copy.size());
      assertEquals(bufferSize, file.size());
   }

   /**
    * Using a real file system users are not required to call file.open() in order to read the file size.  The file
    * descriptor has enough information.  However, with JDBC we do require that some information is loaded in order to
    * get the underlying BLOB.  This tests ensures that file.size() returns the correct value, without the user calling
    * file.open() with JDBCSequentialFile.
    *
    * @throws Exception
    */
   @Test
   public void testGetFileSizeWorksWhenNotOpen() throws Exception {
      // Create test file with some data.
      int testFileSize = 1024;
      String fileName = "testFile.txt";
      SequentialFile file = factory.createSequentialFile(fileName);
      file.open();

      // Write some data to the file
      ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(new byte[1024]);
      file.write(buffer, true);
      file.close();

      try {
         // Create a new pointer to the test file and ensure file.size() returns the correct value.
         SequentialFile file2 = factory.createSequentialFile(fileName);
         assertEquals(testFileSize, file2.size());
      } catch (Throwable t) {
         t.printStackTrace();
      }
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

      private IOCallbackCountdown(int size) {
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

      void assertEmpty(int timeout) throws InterruptedException {
         countDownLatch.await(timeout, TimeUnit.SECONDS);
         assertEquals(countDownLatch.getCount(), 0);
      }
   }
}

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
package org.apache.activemq.tests.unit.core.journal.impl;
import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.ActiveMQBuffers;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;

import org.apache.activemq.core.asyncio.impl.AsynchronousFileImpl;
import org.apache.activemq.core.journal.SequentialFile;
import org.apache.activemq.core.journal.SequentialFileFactory;
import org.apache.activemq.tests.util.UnitTestCase;

public abstract class SequentialFileFactoryTestBase extends UnitTestCase
{
   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      factory = createFactory();

      factory.start();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      Assert.assertEquals(0, AsynchronousFileImpl.getTotalMaxIO());

      factory.stop();

      factory = null;

      UnitTestCase.forceGC();

      super.tearDown();
   }

   protected abstract SequentialFileFactory createFactory();

   protected SequentialFileFactory factory;

   @Test
   public void testCreateAndListFiles() throws Exception
   {
      List<String> expectedFiles = new ArrayList<String>();

      final int numFiles = 10;

      for (int i = 0; i < numFiles; i++)
      {
         String fileName = UUID.randomUUID().toString() + ".amq";

         expectedFiles.add(fileName);

         SequentialFile sf = factory.createSequentialFile(fileName, 1);

         sf.open();

         Assert.assertEquals(fileName, sf.getFileName());

         sf.close();
      }

      // Create a couple with a different extension - they shouldn't be picked
      // up

      SequentialFile sf1 = factory.createSequentialFile("different.file", 1);
      sf1.open();

      SequentialFile sf2 = factory.createSequentialFile("different.cheese", 1);
      sf2.open();

      List<String> fileNames = factory.listFiles("amq");

      Assert.assertEquals(expectedFiles.size(), fileNames.size());

      for (String fileName : expectedFiles)
      {
         Assert.assertTrue(fileNames.contains(fileName));
      }

      fileNames = factory.listFiles("file");

      Assert.assertEquals(1, fileNames.size());

      Assert.assertTrue(fileNames.contains("different.file"));

      fileNames = factory.listFiles("cheese");

      Assert.assertEquals(1, fileNames.size());

      Assert.assertTrue(fileNames.contains("different.cheese"));

      sf1.close();
      sf2.close();
   }

   @Test
   public void testFill() throws Exception
   {
      SequentialFile sf = factory.createSequentialFile("fill.amq", 1);

      sf.open();

      try
      {

         checkFill(sf, 0, 2048, (byte)'X');

         checkFill(sf, 512, 512, (byte)'Y');

         checkFill(sf, 0, 1, (byte)'Z');

         checkFill(sf, 512, 1, (byte)'A');

         checkFill(sf, 1024, 512 * 4, (byte)'B');
      }
      finally
      {
         sf.close();
      }
   }

   @Test
   public void testDelete() throws Exception
   {
      SequentialFile sf = factory.createSequentialFile("delete-me.amq", 1);

      sf.open();

      SequentialFile sf2 = factory.createSequentialFile("delete-me2.amq", 1);

      sf2.open();

      List<String> fileNames = factory.listFiles("amq");

      Assert.assertEquals(2, fileNames.size());

      Assert.assertTrue(fileNames.contains("delete-me.amq"));

      Assert.assertTrue(fileNames.contains("delete-me2.amq"));

      sf.delete();

      fileNames = factory.listFiles("amq");

      Assert.assertEquals(1, fileNames.size());

      Assert.assertTrue(fileNames.contains("delete-me2.amq"));

      sf2.close();

   }

   @Test
   public void testRename() throws Exception
   {
      SequentialFile sf = factory.createSequentialFile("test1.amq", 1);

      sf.open();

      List<String> fileNames = factory.listFiles("amq");

      Assert.assertEquals(1, fileNames.size());

      Assert.assertTrue(fileNames.contains("test1.amq"));

      sf.renameTo("test1.cmp");

      fileNames = factory.listFiles("cmp");

      Assert.assertEquals(1, fileNames.size());

      Assert.assertTrue(fileNames.contains("test1.cmp"));

      sf.delete();

      fileNames = factory.listFiles("amq");

      Assert.assertEquals(0, fileNames.size());

      fileNames = factory.listFiles("cmp");

      Assert.assertEquals(0, fileNames.size());

   }

   @Test
   public void testWriteandRead() throws Exception
   {
      SequentialFile sf = factory.createSequentialFile("write.amq", 1);

      sf.open();

      String s1 = "aardvark";
      byte[] bytes1 = s1.getBytes(StandardCharsets.UTF_8);
      ActiveMQBuffer bb1 = wrapBuffer(bytes1);

      String s2 = "hippopotamus";
      byte[] bytes2 = s2.getBytes(StandardCharsets.UTF_8);
      ActiveMQBuffer bb2 = wrapBuffer(bytes2);

      String s3 = "echidna";
      byte[] bytes3 = s3.getBytes(StandardCharsets.UTF_8);
      ActiveMQBuffer bb3 = wrapBuffer(bytes3);

      long initialPos = sf.position();
      sf.write(bb1, true);
      long bytesWritten = sf.position() - initialPos;

      Assert.assertEquals(calculateRecordSize(bytes1.length, sf.getAlignment()), bytesWritten);

      initialPos = sf.position();
      sf.write(bb2, true);
      bytesWritten = sf.position() - initialPos;

      Assert.assertEquals(calculateRecordSize(bytes2.length, sf.getAlignment()), bytesWritten);

      initialPos = sf.position();
      sf.write(bb3, true);
      bytesWritten = sf.position() - initialPos;

      Assert.assertEquals(calculateRecordSize(bytes3.length, sf.getAlignment()), bytesWritten);

      sf.position(0);

      ByteBuffer rb1 = factory.newBuffer(bytes1.length);
      ByteBuffer rb2 = factory.newBuffer(bytes2.length);
      ByteBuffer rb3 = factory.newBuffer(bytes3.length);

      int bytesRead = sf.read(rb1);
      Assert.assertEquals(calculateRecordSize(bytes1.length, sf.getAlignment()), bytesRead);

      for (int i = 0; i < bytes1.length; i++)
      {
         Assert.assertEquals(bytes1[i], rb1.get(i));
      }

      bytesRead = sf.read(rb2);
      Assert.assertEquals(calculateRecordSize(bytes2.length, sf.getAlignment()), bytesRead);
      for (int i = 0; i < bytes2.length; i++)
      {
         Assert.assertEquals(bytes2[i], rb2.get(i));
      }

      bytesRead = sf.read(rb3);
      Assert.assertEquals(calculateRecordSize(bytes3.length, sf.getAlignment()), bytesRead);
      for (int i = 0; i < bytes3.length; i++)
      {
         Assert.assertEquals(bytes3[i], rb3.get(i));
      }

      sf.close();

   }

   @Test
   public void testPosition() throws Exception
   {
      SequentialFile sf = factory.createSequentialFile("position.amq", 1);

      sf.open();

      try
      {

         sf.fill(0, 3 * 512, (byte)0);

         String s1 = "orange";
         byte[] bytes1 = s1.getBytes(StandardCharsets.UTF_8);

         byte[] bytes2 = s1.getBytes(StandardCharsets.UTF_8);

         String s3 = "lemon";
         byte[] bytes3 = s3.getBytes(StandardCharsets.UTF_8);

         long initialPos = sf.position();
         sf.write(wrapBuffer(bytes1), true);
         long bytesWritten = sf.position() - initialPos;

         Assert.assertEquals(calculateRecordSize(bytes1.length, sf.getAlignment()), bytesWritten);

         initialPos = sf.position();
         sf.write(wrapBuffer(bytes2), true);
         bytesWritten = sf.position() - initialPos;

         Assert.assertEquals(calculateRecordSize(bytes2.length, sf.getAlignment()), bytesWritten);

         initialPos = sf.position();
         sf.write(wrapBuffer(bytes3), true);
         bytesWritten = sf.position() - initialPos;

         Assert.assertEquals(calculateRecordSize(bytes3.length, sf.getAlignment()), bytesWritten);

         byte[] rbytes1 = new byte[bytes1.length];

         byte[] rbytes2 = new byte[bytes2.length];

         byte[] rbytes3 = new byte[bytes3.length];

         ByteBuffer rb1 = factory.newBuffer(rbytes1.length);
         ByteBuffer rb2 = factory.newBuffer(rbytes2.length);
         ByteBuffer rb3 = factory.newBuffer(rbytes3.length);

         sf.position(calculateRecordSize(bytes1.length, sf.getAlignment()) + calculateRecordSize(bytes2.length,
                                                                                                 sf.getAlignment()));

         int bytesRead = sf.read(rb3);
         Assert.assertEquals(rb3.limit(), bytesRead);
         rb3.rewind();
         rb3.get(rbytes3);
         UnitTestCase.assertEqualsByteArrays(bytes3, rbytes3);

         sf.position(rb1.limit());

         bytesRead = sf.read(rb2);
         Assert.assertEquals(rb2.limit(), bytesRead);
         rb2.get(rbytes2);
         UnitTestCase.assertEqualsByteArrays(bytes2, rbytes2);

         sf.position(0);

         bytesRead = sf.read(rb1);
         Assert.assertEquals(rb1.limit(), bytesRead);
         rb1.get(rbytes1);

         UnitTestCase.assertEqualsByteArrays(bytes1, rbytes1);

      }
      finally
      {
         try
         {
            sf.close();
         }
         catch (Exception ignored)
         {
         }
      }
   }

   @Test
   public void testOpenClose() throws Exception
   {
      SequentialFile sf = factory.createSequentialFile("openclose.amq", 1);

      sf.open();

      sf.fill(0, 512, (byte)0);

      String s1 = "cheesecake";
      byte[] bytes1 = s1.getBytes(StandardCharsets.UTF_8);

      long initialPos = sf.position();
      sf.write(wrapBuffer(bytes1), true);
      long bytesWritten = sf.position() - initialPos;

      Assert.assertEquals(calculateRecordSize(bytes1.length, sf.getAlignment()), bytesWritten);

      sf.close();

      try
      {

         sf.write(wrapBuffer(bytes1), true);

         Assert.fail("Should throw exception");
      }
      catch (Exception e)
      {
      }

      sf.open();

      sf.write(wrapBuffer(bytes1), true);

      sf.close();
   }

   // Private ---------------------------------

   private ActiveMQBuffer wrapBuffer(final byte[] bytes)
   {
      return ActiveMQBuffers.wrappedBuffer(bytes);
   }

   protected void checkFill(final SequentialFile file, final int pos, final int size, final byte fillChar) throws Exception
   {
      file.fill(pos, size, fillChar);

      file.close();

      file.open();

      file.position(pos);

      ByteBuffer bb = factory.newBuffer(size);

      int bytesRead = file.read(bb);

      Assert.assertEquals(calculateRecordSize(size, file.getAlignment()), bytesRead);

      for (int i = 0; i < size; i++)
      {
         // log.debug(" i is " + i);
         Assert.assertEquals(fillChar, bb.get(i));
      }

   }

}

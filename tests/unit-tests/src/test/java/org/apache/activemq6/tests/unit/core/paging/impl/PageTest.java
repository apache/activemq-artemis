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
package org.apache.activemq6.tests.unit.core.paging.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.core.journal.SequentialFile;
import org.apache.activemq6.core.journal.SequentialFileFactory;
import org.apache.activemq6.core.journal.impl.NIOSequentialFileFactory;
import org.apache.activemq6.core.paging.PagedMessage;
import org.apache.activemq6.core.paging.impl.Page;
import org.apache.activemq6.core.paging.impl.PagedMessageImpl;
import org.apache.activemq6.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq6.core.server.ServerMessage;
import org.apache.activemq6.core.server.impl.ServerMessageImpl;
import org.apache.activemq6.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.apache.activemq6.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class PageTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testPageWithNIO() throws Exception
   {
      recreateDirectory(getTestDir());
      testAdd(new NIOSequentialFileFactory(getTestDir()), 1000);
   }

   @Test
   public void testDamagedDataWithNIO() throws Exception
   {
      recreateDirectory(getTestDir());
      testDamagedPage(new NIOSequentialFileFactory(getTestDir()), 1000);
   }

   @Test
   public void testPageFakeWithoutCallbacks() throws Exception
   {
      testAdd(new FakeSequentialFileFactory(1, false), 10);
   }

   /**
    * Validate if everything we add is recovered
    */
   @Test
   public void testDamagedPage() throws Exception
   {
      testDamagedPage(new FakeSequentialFileFactory(1, false), 100);
   }

   /**
    * Validate if everything we add is recovered
    */
   protected void testAdd(final SequentialFileFactory factory, final int numberOfElements) throws Exception
   {

      SequentialFile file = factory.createSequentialFile("00010.page", 1);

      Page impl = new Page(new SimpleString("something"), new NullStorageManager(), factory, file, 10);

      Assert.assertEquals(10, impl.getPageId());

      impl.open();

      Assert.assertEquals(1, factory.listFiles("page").size());

      SimpleString simpleDestination = new SimpleString("Test");

      ArrayList<HornetQBuffer> buffers = addPageElements(simpleDestination, impl, numberOfElements);

      impl.sync();
      impl.close();

      file = factory.createSequentialFile("00010.page", 1);
      file.open();
      impl = new Page(new SimpleString("something"), new NullStorageManager(), factory, file, 10);

      List<PagedMessage> msgs = impl.read(new NullStorageManager());

      Assert.assertEquals(numberOfElements, msgs.size());

      Assert.assertEquals(numberOfElements, impl.getNumberOfMessages());

      for (int i = 0; i < msgs.size(); i++)
      {
         Assert.assertEquals(simpleDestination, msgs.get(i).getMessage().getAddress());

         UnitTestCase.assertEqualsByteArrays(buffers.get(i).toByteBuffer().array(), msgs.get(i)
            .getMessage()
            .getBodyBuffer()
            .toByteBuffer()
            .array());
      }

      impl.delete(null);

      Assert.assertEquals(0, factory.listFiles(".page").size());

   }

   protected void testDamagedPage(final SequentialFileFactory factory, final int numberOfElements) throws Exception
   {

      SequentialFile file = factory.createSequentialFile("00010.page", 1);

      Page impl = new Page(new SimpleString("something"), new NullStorageManager(), factory, file, 10);

      Assert.assertEquals(10, impl.getPageId());

      impl.open();

      Assert.assertEquals(1, factory.listFiles("page").size());

      SimpleString simpleDestination = new SimpleString("Test");

      ArrayList<HornetQBuffer> buffers = addPageElements(simpleDestination, impl, numberOfElements);

      impl.sync();

      long positionA = file.position();

      // Add one record that will be damaged
      addPageElements(simpleDestination, impl, 1);

      long positionB = file.position();

      // Add more 10 as they will need to be ignored
      addPageElements(simpleDestination, impl, 10);

      // Damage data... position the file on the middle between points A and B
      file.position(positionA + (positionB - positionA) / 2);

      ByteBuffer buffer = ByteBuffer.allocate((int) (positionB - file.position()));

      for (int i = 0; i < buffer.capacity(); i++)
      {
         buffer.put((byte) 'Z');
      }

      buffer.rewind();

      file.writeDirect(buffer, true);

      impl.close();

      file = factory.createSequentialFile("00010.page", 1);
      file.open();
      impl = new Page(new SimpleString("something"), new NullStorageManager(), factory, file, 10);

      List<PagedMessage> msgs = impl.read(new NullStorageManager());

      Assert.assertEquals(numberOfElements, msgs.size());

      Assert.assertEquals(numberOfElements, impl.getNumberOfMessages());

      for (int i = 0; i < msgs.size(); i++)
      {
         Assert.assertEquals(simpleDestination, msgs.get(i).getMessage().getAddress());

         UnitTestCase.assertEqualsByteArrays(buffers.get(i).toByteBuffer().array(), msgs.get(i)
            .getMessage()
            .getBodyBuffer()
            .toByteBuffer()
            .array());
      }

      impl.delete(null);

      Assert.assertEquals(0, factory.listFiles("page").size());

      Assert.assertEquals(1, factory.listFiles("invalidPage").size());

   }

   /**
    * @param simpleDestination
    * @param page
    * @param numberOfElements
    * @return
    * @throws Exception
    */
   protected ArrayList<HornetQBuffer> addPageElements(final SimpleString simpleDestination,
                                                      final Page page,
                                                      final int numberOfElements) throws Exception
   {
      ArrayList<HornetQBuffer> buffers = new ArrayList<HornetQBuffer>();

      int initialNumberOfMessages = page.getNumberOfMessages();

      for (int i = 0; i < numberOfElements; i++)
      {
         ServerMessage msg = new ServerMessageImpl(i, 100);

         for (int j = 0; j < 10; j++)
         {
            msg.getBodyBuffer().writeByte((byte) 'b');
         }

         buffers.add(msg.getBodyBuffer());

         msg.setAddress(simpleDestination);

         page.write(new PagedMessageImpl(msg, new long[0]));

         Assert.assertEquals(initialNumberOfMessages + i + 1, page.getNumberOfMessages());
      }
      return buffers;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

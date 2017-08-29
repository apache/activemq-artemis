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
package org.apache.activemq.artemis.tests.unit.core.paging.impl;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.message.impl.CoreMessagePersister;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.paging.impl.PagedMessageImpl;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessagePersister;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PageTest extends ActiveMQTestBase {
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Before
   public void registerProtocols() {
      MessagePersister.registerPersister(CoreMessagePersister.getInstance());
      MessagePersister.registerPersister(AMQPMessagePersister.getInstance());
   }

   @Test
   public void testPageWithNIO() throws Exception {
      recreateDirectory(getTestDir());
      testAdd(new NIOSequentialFileFactory(getTestDirfile(), 1), 1000);
   }

   @Test
   public void testDamagedDataWithNIO() throws Exception {
      recreateDirectory(getTestDir());
      testDamagedPage(new NIOSequentialFileFactory(getTestDirfile(), 1), 1000);
   }

   @Test
   public void testPageFakeWithoutCallbacks() throws Exception {
      testAdd(new FakeSequentialFileFactory(1, false), 10);
   }

   @Test
   public void testAddCore() throws Exception {
      testAdd(new NIOSequentialFileFactory(getTestDirfile(), 1), 1);
   }

   /**
    * Validate if everything we add is recovered
    */
   @Test
   public void testDamagedPage() throws Exception {
      testDamagedPage(new FakeSequentialFileFactory(1, false), 100);
   }

   /**
    * Validate if everything we add is recovered
    */
   protected void testAdd(final SequentialFileFactory factory, final int numberOfElements) throws Exception {

      SequentialFile file = factory.createSequentialFile("00010.page");

      Page impl = new Page(new SimpleString("something"), new NullStorageManager(), factory, file, 10);

      Assert.assertEquals(10, impl.getPageId());

      impl.open();

      Assert.assertEquals(1, factory.listFiles("page").size());

      SimpleString simpleDestination = new SimpleString("Test");

      addPageElements(simpleDestination, impl, numberOfElements);

      impl.sync();
      impl.close();

      file = factory.createSequentialFile("00010.page");
      file.open();
      impl = new Page(new SimpleString("something"), new NullStorageManager(), factory, file, 10);

      List<PagedMessage> msgs = impl.read(new NullStorageManager());

      Assert.assertEquals(numberOfElements, msgs.size());

      Assert.assertEquals(numberOfElements, impl.getNumberOfMessages());

      for (int i = 0; i < msgs.size(); i++) {
         Assert.assertEquals(simpleDestination, msgs.get(i).getMessage().getAddressSimpleString());
      }

      impl.delete(null);

      Assert.assertEquals(0, factory.listFiles(".page").size());

   }

   protected void testDamagedPage(final SequentialFileFactory factory, final int numberOfElements) throws Exception {

      SequentialFile file = factory.createSequentialFile("00010.page");

      Page impl = new Page(new SimpleString("something"), new NullStorageManager(), factory, file, 10);

      Assert.assertEquals(10, impl.getPageId());

      impl.open();

      Assert.assertEquals(1, factory.listFiles("page").size());

      SimpleString simpleDestination = new SimpleString("Test");

      addPageElements(simpleDestination, impl, numberOfElements);

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

      for (int i = 0; i < buffer.capacity(); i++) {
         buffer.put((byte) 'Z');
      }

      buffer.rewind();

      file.writeDirect(buffer, true);

      impl.close();

      file = factory.createSequentialFile("00010.page");
      file.open();
      impl = new Page(new SimpleString("something"), new NullStorageManager(), factory, file, 10);

      List<PagedMessage> msgs = impl.read(new NullStorageManager());

      Assert.assertEquals(numberOfElements, msgs.size());

      Assert.assertEquals(numberOfElements, impl.getNumberOfMessages());

      for (int i = 0; i < msgs.size(); i++) {
         Assert.assertEquals(simpleDestination, msgs.get(i).getMessage().getAddressSimpleString());
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
   protected void addPageElements(final SimpleString simpleDestination,
                                                       final Page page,
                                                       final int numberOfElements) throws Exception {

      int initialNumberOfMessages = page.getNumberOfMessages();

      for (int i = 0; i < numberOfElements; i++) {
         ICoreMessage msg = new CoreMessage().initBuffer(100);

         for (int j = 0; j < 10; j++) {
            msg.getBodyBuffer().writeByte((byte) 'b');
         }

         msg.setAddress(simpleDestination);

         page.write(new PagedMessageImpl(msg, new long[0]));

         Assert.assertEquals(initialNumberOfMessages + i + 1, page.getNumberOfMessages());
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

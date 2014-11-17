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
package org.apache.activemq6.tests.unit.core.journal.impl;

import org.junit.Test;

import java.nio.ByteBuffer;

import org.junit.Assert;

import org.apache.activemq6.core.asyncio.impl.AsynchronousFileImpl;
import org.apache.activemq6.core.journal.SequentialFileFactory;
import org.apache.activemq6.core.journal.impl.AIOSequentialFileFactory;
import org.apache.activemq6.core.journal.impl.NIOSequentialFileFactory;
import org.apache.activemq6.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.apache.activemq6.tests.util.UnitTestCase;

/**
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class CleanBufferTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testCleanOnNIO()
   {
      SequentialFileFactory factory = new NIOSequentialFileFactory("Whatever");

      testBuffer(factory);
   }

   @Test
   public void testCleanOnAIO()
   {
      if (AsynchronousFileImpl.isLoaded())
      {
         SequentialFileFactory factory = new AIOSequentialFileFactory("Whatever");

         testBuffer(factory);
      }
   }

   @Test
   public void testCleanOnFake()
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      testBuffer(factory);
   }

   private void testBuffer(final SequentialFileFactory factory)
   {
      ByteBuffer buffer = factory.newBuffer(100);

      try
      {
         for (byte b = 0; b < 100; b++)
         {
            buffer.put(b);
         }

         buffer.rewind();

         for (byte b = 0; b < 100; b++)
         {
            Assert.assertEquals(b, buffer.get());
         }

         buffer.limit(10);
         factory.clearBuffer(buffer);
         buffer.limit(100);

         buffer.rewind();

         for (byte b = 0; b < 100; b++)
         {
            if (b < 10)
            {
               Assert.assertEquals(0, buffer.get());
            }
            else
            {
               Assert.assertEquals(b, buffer.get());
            }
         }
      }
      finally
      {
         factory.releaseBuffer(buffer);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

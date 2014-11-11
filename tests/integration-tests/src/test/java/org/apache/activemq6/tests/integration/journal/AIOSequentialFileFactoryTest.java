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
package org.apache.activemq6.tests.integration.journal;
import java.nio.ByteBuffer;

import org.apache.activemq6.core.journal.SequentialFile;
import org.apache.activemq6.core.journal.SequentialFileFactory;
import org.apache.activemq6.core.journal.impl.AIOSequentialFileFactory;
import org.apache.activemq6.tests.unit.core.journal.impl.SequentialFileFactoryTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * A AIOSequentialFileFactoryTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AIOSequentialFileFactoryTest extends SequentialFileFactoryTestBase
{

   @BeforeClass
   public static void hasAIO()
   {
      org.junit.Assume.assumeTrue("Test case needs AIO to run", AIOSequentialFileFactory.isSupported());
   }

   @Override
   protected SequentialFileFactory createFactory()
   {
      return new AIOSequentialFileFactory(getTestDir());
   }

   @Test
   public void testBuffer() throws Exception
   {
      SequentialFile file = factory.createSequentialFile("filtetmp.log", 10);
      file.open();
      ByteBuffer buff = factory.newBuffer(10);
      Assert.assertEquals(512, buff.limit());
      file.close();
      factory.releaseBuffer(buff);
   }

}

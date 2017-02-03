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
package org.apache.activemq.artemis.tests.integration.journal;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.SequentialFileFactoryTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AIOSequentialFileFactoryTest extends SequentialFileFactoryTestBase {

   @BeforeClass
   public static void hasAIO() {
      org.junit.Assume.assumeTrue("Test case needs AIO to run", AIOSequentialFileFactory.isSupported());
   }

   @Override
   protected SequentialFileFactory createFactory(String folder) {
      return new AIOSequentialFileFactory(new File(folder), 10);
   }

   @Test
   public void testBuffer() throws Exception {
      SequentialFile file = factory.createSequentialFile("filtetmp.log");
      file.open();
      ByteBuffer buff = factory.newBuffer(10);
      Assert.assertEquals(factory.getAlignment(), buff.limit());
      file.close();
      factory.releaseBuffer(buff);
   }

}

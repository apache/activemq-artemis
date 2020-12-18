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
package org.apache.activemq.artemis.tests.extras.byteman;

import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.JournalImplTestBase;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

@RunWith(BMUnitRunner.class)
public class JournalImplConcurrencyTest extends JournalImplTestBase {

   @Override
   protected SequentialFileFactory getFileFactory() {
      return new FakeSequentialFileFactory();
   }


   /**
    * Tests that JournalImpl#checkKnownRecordID() doesn't hang when the executor thread fails with
    * an exception before setting the future value.
    */
   @Test(timeout = 2000)
   @BMRules(rules = {
         @BMRule(
               name = "BlockOnFinalLargeMessagePacket",
               targetClass = "java.util.concurrent.locks.ReentrantReadWriteLock",
               targetMethod = "readLock()",
               targetLocation = "EXIT",
               condition = "Thread.currentThread().getName().contains(\"ArtemisIOThread\")",
               action = "throw RuntimeException(\"Injected exception\");"
         )
   })
   public void testTryDelete() throws Exception {
      setup(10, 10 * 1024, true);
      createJournal();
      startJournal();
      load();
      addTx(1, 1, 2, 3);

      try {
         tryDelete(1);
         Assert.fail("Expected to throw exception injected by a byteman rule");
      } catch (ExecutionException e) {
         Assert.assertTrue(e.getCause() instanceof RuntimeException);
         Assert.assertEquals("Injected exception", e.getCause().getMessage());
      }

      stopJournal();
   }

}

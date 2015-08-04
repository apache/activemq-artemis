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
package org.apache.activemq.artemis.tests.stress.journal;

import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.server.JournalType;
import org.junit.BeforeClass;

public class AIOMultiThreadCompactorStressTest extends NIOMultiThreadCompactorStressTest {

   @BeforeClass
   public static void hasAIO() {
      org.junit.Assume.assumeTrue("Test case needs AIO to run", AIOSequentialFileFactory.isSupported());
   }

   /**
    * @return
    */
   @Override
   protected JournalType getJournalType() {
      return JournalType.ASYNCIO;
   }

}

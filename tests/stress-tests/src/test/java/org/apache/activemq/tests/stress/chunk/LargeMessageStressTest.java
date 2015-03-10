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
package org.apache.activemq.tests.stress.chunk;

import org.junit.Test;

import org.apache.activemq.tests.integration.largemessage.LargeMessageTestBase;

/**
 * A MessageChunkSoakTest
 */
public class LargeMessageStressTest extends LargeMessageTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testMessageChunkFilePersistenceOneHugeMessage() throws Exception
   {
      testChunks(false,
                 false,
                 false,
                 true,
                 true,
                 false,
                 false,
                 false,
                 true,
                 1,
                 200 * 1024L * 1024L + 1024L,
                 120000,
                 0,
                 10 * 1024 * 1024,
                 1024 * 1024);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

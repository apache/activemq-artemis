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
package org.apache.activemq.artemis.tests.integration.server;

import org.junit.jupiter.api.Test;

public class AIOFileLockTimeoutTest extends FileLockTimeoutTest {

   /**
    * When running this test from an IDE add this to the test command line so that the AssertionLoggerHandler works properly:
    *
    *   -Dlog4j2.configurationFile=file:<path_to_source>/tests/config/log4j2-tests-config.properties
    */
   @Test
   public void testAIOFileLockExpiration() throws Exception {
      doTest(true);
   }
}
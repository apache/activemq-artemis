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
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Test;

/**
 * This tests is placed in duplication here to validate that the libaio module is properly loaded on this
 * test module.
 *
 * This test should be placed on each one of the tests modules to make sure the library is loaded correctly.
 */
public class LibaioDependencyCheckTest extends ActiveMQTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testDependency() throws Exception {
      if (System.getProperties().get("os.name").equals("Linux")) {
         assertTrue("Libaio is not available on this platform", LibaioContext.isLoaded());
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

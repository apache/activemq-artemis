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
package org.apache.activemq.artemis.tests.unit.core.server.impl;

import java.io.File;

import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class FileLockTest extends ActiveMQTestBase {

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      File file = new File(getTestDir());
      file.mkdirs();
   }

   @Test
   public void testNIOLock() throws Exception {
      doTestLock(new FileLockNodeManager(getTestDirfile(), false), new FileLockNodeManager(getTestDirfile(), false));

   }

   public void doTestLock(final FileLockNodeManager lockManager1,
                          final FileLockNodeManager lockManager2) throws Exception {
      lockManager1.start();
      lockManager2.start();

      lockManager1.startLiveNode();

      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               lockManager2.startLiveNode();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      };

      t.start();

      assertTrue(lockManager1.isLiveLocked());
      Thread.sleep(500);
      assertFalse(lockManager2.isLiveLocked());

      lockManager1.crashLiveServer();

      t.join();

      assertFalse(lockManager1.isLiveLocked());
      assertTrue(lockManager2.isLiveLocked());

      lockManager2.crashLiveServer();

      lockManager1.stop();
      lockManager2.stop();

   }

}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.reload;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.reload.ReloadCallback;
import org.apache.activemq.artemis.core.server.reload.ReloadManagerImpl;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReloadManagerTest extends ActiveMQTestBase {

   private ScheduledExecutorService scheduledExecutorService;

   private ExecutorService executorService;

   private ReloadManagerImpl manager;

   @Before
   public void startScheduled() {
      scheduledExecutorService = new ScheduledThreadPoolExecutor(5);
      executorService = Executors.newSingleThreadExecutor();
      manager = new ReloadManagerImpl(scheduledExecutorService, executorService, 100);
   }

   @After
   public void stopScheduled() {
      manager.stop();
      scheduledExecutorService.shutdown();
      executorService.shutdown();
      scheduledExecutorService = null;
   }

   @Test
   public void testUpdate() throws Exception {

      File file = new File(getTemporaryDir(), "checkFile.tst");
      internalTest(manager, file);

   }

   @Test
   public void testUpdateWithSpace() throws Exception {
      File spaceDir = new File(getTemporaryDir(), "./with %25space");
      spaceDir.mkdirs();
      File file = new File(spaceDir, "checkFile.tst");
      internalTest(manager, file);
   }

   private void internalTest(ReloadManagerImpl manager, File file) throws IOException, InterruptedException {
      file.createNewFile();

      final ReusableLatch latch = new ReusableLatch(1);

      manager.addCallback(file.toURL(), new ReloadCallback() {
         @Override
         public void reload(URL uri) {
            latch.countDown();
         }
      });

      Assert.assertFalse(latch.await(1, TimeUnit.SECONDS));

      file.setLastModified(System.currentTimeMillis());

      Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
   }
}

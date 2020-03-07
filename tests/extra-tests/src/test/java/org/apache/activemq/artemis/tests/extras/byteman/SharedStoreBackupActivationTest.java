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

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.ScaleDownConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.apache.activemq.artemis.tests.integration.cluster.failover.FailoverTestBase;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class SharedStoreBackupActivationTest extends FailoverTestBase {

   private static Logger logger = Logger.getLogger(SharedStoreBackupActivationTest.class);

   private static volatile boolean throwException = false;
   private static CountDownLatch exceptionThrownLatch;

   public static synchronized boolean isThrowException() {
      logger.debugf("Throwing IOException during FileLockNodeManager.tryLock(): %s", throwException);
      if (exceptionThrownLatch != null) {
         exceptionThrownLatch.countDown();
      }
      return throwException;
   }

   /**
    * Waits for the backup server to call FileLockNodeManager.tryLock().
    */
   public static void awaitTryLock(boolean throwException) throws InterruptedException {
      synchronized (SharedStoreBackupActivationTest.class) {
         SharedStoreBackupActivationTest.throwException = throwException;
         exceptionThrownLatch = new CountDownLatch(1);
      }
      logger.debugf("Awaiting backup to perform FileLockNodeManager.tryLock()");
      boolean ret = exceptionThrownLatch.await(10, TimeUnit.SECONDS);
      SharedStoreBackupActivationTest.throwException = false;

      Assert.assertTrue("FileLockNodeManager.tryLock() was not called during specified timeout", ret);
      logger.debugf("Awaiting FileLockNodeManager.tryLock() done");
   }

   @Test
   @BMRules(
         rules = {@BMRule(
               name = "throw IOException during activation",
               targetClass = "org.apache.activemq.artemis.core.server.impl.FileLockNodeManager",
               targetMethod = "tryLock",
               targetLocation = "AT ENTRY",
               condition = "org.apache.activemq.artemis.tests.extras.byteman.SharedStoreBackupActivationTest.isThrowException()",
               action = "THROW new IOException(\"IO Error\");")
         })
   public void testFailOverAfterTryLockException() throws Exception {
      Assert.assertTrue(liveServer.isActive());
      Assert.assertFalse(backupServer.isActive());

      // wait for backup to try to acquire lock, once without exception (acquiring will not succeed because live is
      // still active)
      awaitTryLock(false);

      // wait for backup to try to acquire lock, this time throw an IOException
      logger.debug("Causing exception");
      awaitTryLock(true);

      // stop live server
      logger.debugf("Stopping live server");
      liveServer.stop();
      waitForServerToStop(liveServer.getServer());
      logger.debugf("Live server stopped, waiting for backup activation");
      backupServer.getServer().waitForActivation(10, TimeUnit.SECONDS);

      // backup should be activated by now
      Assert.assertFalse(liveServer.isActive());
      Assert.assertTrue("Backup server didn't activate", backupServer.isActive());
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

   @Override
   protected void createConfigs() throws Exception {
      File sharedDir = File.createTempFile("shared-dir", "");
      sharedDir.delete();
      Assert.assertTrue(sharedDir.mkdir());
      logger.debugf("Created shared store directory %s", sharedDir.getCanonicalPath());

      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);

      // nodes must use separate FileLockNodeManager instances!
      NodeManager liveNodeManager = new FileLockNodeManager(sharedDir, false, new ScheduledThreadPoolExecutor(1));
      NodeManager backupNodeManager = new FileLockNodeManager(sharedDir, false, new ScheduledThreadPoolExecutor(1));

      backupConfig = super.createDefaultConfig(false)
            .clearAcceptorConfigurations()
            .addAcceptorConfiguration(getAcceptorTransportConfiguration(false))
            .setHAPolicyConfiguration(
                  new SharedStoreSlavePolicyConfiguration()
                        .setScaleDownConfiguration(new ScaleDownConfiguration().setEnabled(false))
                        .setRestartBackup(false))
            .addConnectorConfiguration(liveConnector.getName(), liveConnector)
            .addConnectorConfiguration(backupConnector.getName(), backupConnector)
            .addClusterConfiguration(basicClusterConnectionConfig(backupConnector.getName(), liveConnector.getName()));
      backupServer = createTestableServer(backupConfig, backupNodeManager);

      liveConfig = super.createDefaultConfig(false)
            .clearAcceptorConfigurations()
            .addAcceptorConfiguration(getAcceptorTransportConfiguration(true))
            .setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration().setFailoverOnServerShutdown(true))
            .addClusterConfiguration(basicClusterConnectionConfig(liveConnector.getName()))
            .addConnectorConfiguration(liveConnector.getName(), liveConnector);
      liveServer = createTestableServer(liveConfig, liveNodeManager);
   }

}

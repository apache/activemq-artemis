/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.critical;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.junit.Assert;
import org.junit.Test;

public class ShutdownOnCriticalIOErrorMoveNextTest extends ActiveMQTestBase {

   private static final int OK = 3;

   public static void main(String[] arg) {
      ShutdownOnCriticalIOErrorMoveNextTest testInst = new ShutdownOnCriticalIOErrorMoveNextTest();
      // some methods are not static, so we need an instance
      testInst.testSimplyDownAfterErrorSpawned();
   }

   @Test
   public void testSimplyDownAfterError() throws Exception {
      Process process = SpawnedVMSupport.spawnVM(ShutdownOnCriticalIOErrorMoveNextTest.class.getName());
      runAfter(process::destroyForcibly);
      Assert.assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      Assert.assertEquals(OK, process.exitValue());
   }

   public void testSimplyDownAfterErrorSpawned() {
      try {
         deleteDirectory(new File("./target/server"));
         ActiveMQServer server = createServer("./target/server");

         server.start();

         ConnectionFactory factory = new ActiveMQConnectionFactory();
         Connection connection = factory.createConnection();

         Session session = connection.createSession();

         MessageProducer producer = session.createProducer(session.createQueue("queue"));

         try {
            for (int i = 0; i < 500; i++) {
               producer.send(session.createTextMessage("text"));
            }
         } catch (JMSException expected) {
         }

         Wait.waitFor(() -> !server.isStarted());

         Assert.assertFalse(server.isStarted());
         System.exit(OK);
      } catch (Throwable e) {
         e.printStackTrace(System.out);
         System.exit(-1);
      }

   }

   ActiveMQServer createServer(String folder) throws Exception {
      final AtomicBoolean blocked = new AtomicBoolean(false);
      Configuration conf = createConfig(folder);
      ActiveMQSecurityManager securityManager = new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), new SecurityConfiguration());

      conf.setPersistenceEnabled(true);

      ActiveMQServer server = new ActiveMQServerImpl(conf, securityManager) {

         @Override
         protected StorageManager createStorageManager() {

            JournalStorageManager storageManager = new JournalStorageManager(conf, getCriticalAnalyzer(), executorFactory, scheduledPool, ioExecutorFactory, ioCriticalErrorListener) {

               @Override
               protected Journal createMessageJournal(Configuration config,
                                                      IOCriticalErrorListener criticalErrorListener,
                                                      int fileSize) {
                  return new JournalImpl(ioExecutorFactory, fileSize, config.getJournalMinFiles(), config.getJournalPoolFiles(), config.getJournalCompactMinFiles(), config.getJournalCompactPercentage(), config.getJournalFileOpenTimeout(), journalFF, "activemq-data", "amq", journalFF.getMaxIO(), 0, criticalErrorListener, config.getJournalMaxAtticFiles()) {
                     @Override
                     protected void moveNextFile(boolean scheduleReclaim, boolean block) throws Exception {
                        super.moveNextFile(scheduleReclaim, block);
                        if (blocked.get()) {
                           throw new IllegalStateException("forcibly down");
                        }
                     }
                  };
               }

               @Override
               public void storeMessage(Message message) throws Exception {
                  super.storeMessage(message);
                  blocked.set(true);
               }
            };

            this.getCriticalAnalyzer().add(storageManager);

            return storageManager;
         }

      };

      return addServer(server);
   }

   Configuration createConfig(String folder) throws Exception {

      Configuration configuration = createDefaultConfig(true);
      configuration.setSecurityEnabled(false).setJournalMinFiles(2).setJournalFileSize(100 * 1024).setJournalType(getDefaultJournalType()).setJournalDirectory(folder + "/journal").setBindingsDirectory(folder + "/bindings").setPagingDirectory(folder + "/paging").
         setLargeMessagesDirectory(folder + "/largemessage").setJournalCompactMinFiles(0).setJournalCompactPercentage(0).setClusterPassword(CLUSTER_PASSWORD).setJournalDatasync(false);
      configuration.setSecurityEnabled(false);
      configuration.setPersistenceEnabled(true);

      return configuration;
   }

}

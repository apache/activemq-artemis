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
package org.apache.activemq.artemis.tests.extras.byteman.critical.analyzer;

import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class FileSystemSyncBlockedTest extends CriticalAnalyzerFaultInjectionTestBase {

   @BMRules(rules = {@BMRule(name = "Simulate Slow Disk Sync", targetClass = "org.apache.activemq.artemis.core.io.nio.NIOSequentialFile", targetMethod = "sync", targetLocation = "ENTRY", condition = "!flagged(\"testSlowDiskSync\")",  // Once the server shutdowns we stop applying this rule.
      action = "waitFor(\"testSlowDiskSync\")"), @BMRule(
      // We ensure that no more
      name = "Release Suspended Thread during Server Shutdown", // Releases wakes up suspended threads to allow shutdown to complete
      targetClass = "org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl", targetMethod = "stop", targetLocation = "ENTRY", action = "flag(\"testSlowDiskSync\"); signalWake(\"testSlowDiskSync\")")})
   @Test(timeout = 60000)
   public void testSlowDiskSync() throws Exception {
      testSendDurableMessage();
   }

   @Test
   public void testManyFiles() throws Exception {
      Session s = conn.createSession(true, Session.SESSION_TRANSACTED);

      Queue jmsQueue = s.createQueue(address.toString());
      MessageProducer p = s.createProducer(jmsQueue);
      p.setDeliveryMode(DeliveryMode.PERSISTENT);
      conn.start();
      for (int i = 0; i < 1000; i++) {
         p.send(s.createTextMessage("payload"));
         server.getStorageManager().getMessageJournal().forceMoveNextFile();
      }
      s.commit();

      // if you have more than 100 components, then you have a leak!
      Assert.assertTrue(server.getStorageManager().getJournalSequentialFileFactory().getCriticalAnalyzer().getNumberOfComponents() < 10);
      System.out.println("Number of components:" + server.getStorageManager().getJournalSequentialFileFactory().getCriticalAnalyzer().getNumberOfComponents());

   }
}

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

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class DeadlockedQueueTest extends CriticalAnalyzerFaultInjectionTestBase {

   @BMRules(
      rules = {
         @BMRule(
            name = "Deadlock during Queue Delivery",
            targetClass = "org.apache.activemq.artemis.core.server.impl.QueueImpl",
            targetMethod = "deliver",
            targetLocation = "AFTER SYNCHRONIZE",
            condition = "!flagged(\"testDeadlockOnQueue\")",
            action = "waitFor(\"testDeadlockOnQueue\")"),
         @BMRule(
            name = "Release Suspended Thread during Server Shutdown", // Releases wakes up suspended threads to allow shutdown to complete
            targetClass = "org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl",
            targetMethod = "stop",
            targetLocation = "ENTRY",
            action = "flag(\"testDeadlockOnQueue\"); signalWake(\"testDeadlockOnQueue\")")
      })
   @Test(timeout = 60000)
   public void testDeadlockOnQueue() throws Exception {
      testSendDurableMessage();
   }
}

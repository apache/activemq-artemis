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

package org.apache.activemq.artemis.junit;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnector;
import org.apache.activemq.artemis.utils.UnitTestWatcher;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.rules.RuleChain;

/**
 * Messaging tests are usually Thread intensive and a thread leak or a server leakage may affect future tests.
 * This Rule will prevent Threads leaking from one test into another by checking left over threads.
 * This will also clear Client Thread Pools from ActiveMQClient.
 */
public class ThreadLeakCheckRule extends org.apache.activemq.artemis.utils.ThreadLeakCheckRule {

   private static Logger log = Logger.getLogger(ThreadLeakCheckRule.class);

   private ThreadLeakCheckRule() {
   }

   /**
    * Override to tear down your specific external resource.
    */
   @Override
   protected void after() {
      ActiveMQClient.clearThreadPools();
      InVMConnector.resetThreadPool();

      try {
         if (enabled) {
            boolean failed = true;

            boolean failedOnce = false;

            long timeout = System.currentTimeMillis() + 60000;
            while (failed && timeout > System.currentTimeMillis()) {
               failed = checkThread();

               if (failed) {
                  failedOnce = true;
                  forceGC();
                  try {
                     Thread.sleep(500);
                  } catch (Throwable e) {
                  }
               }
            }

            if (failed) {
               Assert.fail("Thread leaked");
            } else if (failedOnce) {
               System.out.println("******************** Threads cleared after retries ********************");
               System.out.println();
            }

         } else {
            enabled = true;
         }
      } finally {
         // clearing just to help GC
         previousThreads = null;
      }
   }

   public static RuleChain getRule() {
      return RuleChain.outerRule(new ThreadLeakCheckRule()).around(new UnitTestWatcher());
   }
}

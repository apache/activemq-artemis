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

package org.apache.activemq.artemis.tests.util;

import java.util.Map;

import org.junit.Assert;
import org.junit.rules.ExternalResource;

/**
 * This is useful to make sure you won't have leaking threads between tests
 */
public class ThreadLeakCheckRule extends ExternalResource {

   boolean enabled = true;

   private Map<Thread, StackTraceElement[]> previousThreads;

   public void disable() {
      enabled = false;
   }

   /**
    * Override to set up your specific external resource.
    *
    * @throws if setup fails (which will disable {@code after}
    */
   @Override
   protected void before() throws Throwable {
      // do nothing

      previousThreads = Thread.getAllStackTraces();

   }

   /**
    * Override to tear down your specific external resource.
    */
   @Override
   protected void after() {
      try {
         if (enabled) {
            boolean failed = true;

            boolean failedOnce = false;

            long timeout = System.currentTimeMillis() + 60000;
            while (failed && timeout > System.currentTimeMillis()) {
               failed = checkThread();

               if (failed) {
                  failedOnce = true;
                  ActiveMQTestBase.forceGC();
                  try {
                     Thread.sleep(500);
                  }
                  catch (Throwable e) {
                  }
               }
            }

            if (failed) {
               Assert.fail("Thread leaked");
            }
            else if (failedOnce) {
               System.out.println("******************** Threads cleared after retries ********************");
               System.out.println();
            }

         }
         else {
            enabled = true;
         }
      }
      finally {
         // clearing just to help GC
         previousThreads = null;
      }

   }


   private boolean checkThread() {
      boolean failedThread = false;

      Map<Thread, StackTraceElement[]> postThreads = Thread.getAllStackTraces();

      if (postThreads != null && previousThreads != null && postThreads.size() > previousThreads.size()) {


         for (Thread aliveThread : postThreads.keySet()) {
            if (aliveThread.isAlive() && !isExpectedThread(aliveThread) && !previousThreads.containsKey(aliveThread)) {
               if (!failedThread) {
                  System.out.println("*********************************************************************************");
                  System.out.println("LEAKING THREADS");
               }
               failedThread = true;
               System.out.println("=============================================================================");
               System.out.println("Thread " + aliveThread + " is still alive with the following stackTrace:");
               StackTraceElement[] elements = postThreads.get(aliveThread);
               for (StackTraceElement el : elements) {
                  System.out.println(el);
               }
            }

         }
         if (failedThread) {
            System.out.println("*********************************************************************************");
         }
      }


      return failedThread;
   }


   /**
    * if it's an expected thread... we will just move along ignoring it
    *
    * @param thread
    * @return
    */
   private boolean isExpectedThread(Thread thread) {
      final String threadName = thread.getName();
      final ThreadGroup group = thread.getThreadGroup();
      final boolean isSystemThread = group != null && "system".equals(group.getName());
      final String javaVendor = System.getProperty("java.vendor");

      if (threadName.contains("SunPKCS11")) {
         return true;
      }
      else if (threadName.contains("Attach Listener")) {
         return true;
      }
      else if ((javaVendor.contains("IBM") || isSystemThread) && threadName.equals("process reaper")) {
         return true;
      }
      else if ((javaVendor.contains("IBM") || isSystemThread) && threadName.equals("ClassCache Reaper")) {
         return true;
      }
      else if (javaVendor.contains("IBM") && threadName.equals("MemoryPoolMXBean notification dispatcher")) {
         return true;
      }
      else if (threadName.contains("globalEventExecutor")) {
         return true;
      }
      else if (threadName.contains("threadDeathWatcher")) {
         return true;
      }
      else if (threadName.contains("netty-threads")) {
         // This is ok as we use EventLoopGroup.shutdownGracefully() which will shutdown things with a bit of delay
         // if the EventLoop's are still busy.
         return true;
      }
      else if (threadName.contains("threadDeathWatcher")) {
         //another netty thread
         return true;
      }
      else if (threadName.contains("derby")) {
         // The derby engine is initialized once, and lasts the lifetime of the VM
         return true;
      }
      else if (threadName.contains("Timer")) {
         // The timer threads in Derby and JDBC use daemon and shutdown once user threads exit.
         return true;
      }
      else if (threadName.contains("hawtdispatch")) {
         // Static workers used by MQTT client.
         return true;
      }
      else {
         for (StackTraceElement element : thread.getStackTrace()) {
            if (element.getClassName().contains("org.jboss.byteman.agent.TransformListener")) {
               return true;
            }
         }
         return false;
      }
   }


}

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
package org.apache.activemq.artemis.tests.util;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import org.apache.activemq.artemis.tests.rules.NoFilesBehind;
import org.apache.activemq.artemis.tests.rules.NoProcessFilesBehind;
import org.apache.activemq.artemis.utils.CleanupSystemPropertiesRule;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parent class with basic utilities around creating unit etc test classes.
 */
public abstract class ArtemisTestCase extends Assert {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static String testClassName = "not-yet-set";

   /** This will make sure threads are not leaking between tests */
   @ClassRule
   public static ThreadLeakCheckRule leakCheckRule = new ThreadLeakCheckRule();

   @ClassRule
   public static NoProcessFilesBehind noProcessFilesBehind = new NoProcessFilesBehind(1000);

   @ClassRule
   public static TestRule classWatcher = new TestWatcher() {
      @Override
      protected void starting(Description description) {
         testClassName = description.getClassName();
      }
   };

   /** We should not under any circumstance create data outside of ./target
    *  if you have a test failing because because of this rule for any reason,
    *  even if you use afterClass events, move the test to ./target and always cleanup after
    *  your data even under ./target.
    *  Do not try to disable this rule! Fix your test! */
   @Rule
   public NoFilesBehind noFilesBehind = new NoFilesBehind("data", "null");

   /** This will cleanup any system property changed inside tests */
   @Rule
   public CleanupSystemPropertiesRule propertiesRule = new CleanupSystemPropertiesRule();

   @Rule
   public TestName name = new TestName();

   @Rule
   public TestRule watcher = new TestWatcher() {
      @Override
      protected void starting(Description description) {
         logger.info("**** start #test {}() ***", description.getMethodName());
      }

      @Override
      protected void finished(Description description) {
         logger.info("**** end #test {}() ***", description.getMethodName());
      }
   };

   public interface TestCompletionTask {
      void run() throws Exception;
   }

   private List<TestCompletionTask> runAfter;

   /**
    * Use this method to cleanup your resources by passing a TestCleanupTask.
    *
    * Exceptions thrown from your tasks will just be logged and not passed as failures.
    *
    * @param completionTask A TestCleanupTask that will be passed, possibly from a lambda
    */
   protected void runAfter(TestCompletionTask completionTask) {
      Assert.assertNotNull(completionTask);
      runAfterEx(() -> {
         try {
            completionTask.run();
         } catch (Throwable e) {
            logger.warn("Lambda {} is throwing an exception", completionTask.toString(), e);
         }
      });
   }

   /**
    * Use this method to cleanup your resources and validating exceptional results by passing a TestCompletionTask.
    *
    * An exception thrown from a task will be thrown to JUnit. If more than one task is present, all tasks will be
    * be executed, however only the exception of the first one will then be thrown the JUnit runner. All will be
    * logged as they occur.
    *
    * @param completionTask A TestCompletionTask that will be passed, possibly from a lambda method
    */
   protected synchronized void runAfterEx(TestCompletionTask completionTask) {
      Assert.assertNotNull(completionTask);
      if (runAfter == null) {
         runAfter = new ArrayList<>();
      }
      runAfter.add(completionTask);
   }

   @After
   public synchronized void runAfter() throws Throwable {
      ArrayList<Throwable> throwables = new ArrayList<>();
      List<TestCompletionTask> localRunAfter = runAfter;
      runAfter = null;
      if (localRunAfter != null) {
         localRunAfter.forEach((r) -> {
            try {
               r.run();
            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
               throwables.add(e);
            }
         });
      }

      if (!throwables.isEmpty()) {
         throw throwables.get(0);
      }
   }

   public static void forceGC() {
      ThreadLeakCheckRule.forceGC();
   }

   public MBeanServer createMBeanServer() {
      MBeanServer mBeanServer = MBeanServerFactory.createMBeanServer();
      runAfter(() -> MBeanServerFactory.releaseMBeanServer(mBeanServer));
      return mBeanServer;
   }

   public static String getTestClassName() {
      return testClassName;
   }

}

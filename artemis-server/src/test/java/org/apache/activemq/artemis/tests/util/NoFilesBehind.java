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

import java.io.File;

import org.apache.activemq.artemis.utils.FileUtil;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * This is useful to make sure you won't have leaking threads between tests
 */
public class NoFilesBehind extends TestWatcher {

   private static Logger log = Logger.getLogger(NoFilesBehind.class);

   private final String[] filesToCheck;

   public NoFilesBehind(String... filesToCheck) {
      this.filesToCheck = filesToCheck;
   }

   private File checkFiles() {
      for (String f : filesToCheck) {
         File fileCheck = new File(f);
         if (fileCheck.exists()) {
            return fileCheck;
         }
      }

      return null;
   }

   /**
    * Override to set up your specific external resource.
    *
    * @throws if setup fails (which will disable {@code after}
    */
   @Override
   protected void starting(Description description) {
      // do nothing

      File leaked = checkFiles();

      if (leaked != null) {
         Assert.fail("A previous test left a folder around:: " + leaked.getAbsolutePath());
      }

   }


   @Override
   protected void failed(Throwable e, Description description) {
   }

   @Override
   protected void succeeded(Description description) {
   }

   /**
    * Override to tear down your specific external resource.
    */
   @Override
   protected void finished(Description description) {

      File leaked = checkFiles();

      if (leaked != null) {
         try {
            Assert.fail(leaked.getAbsolutePath() + " is being left behind");
         } finally {
            try {
               FileUtil.deleteDirectory(leaked);
            } catch (Throwable almostIgnored) {
               // nothing we can do about it.. but we will log a stack trace for debugging
               almostIgnored.printStackTrace();
            }
         }
      }
   }

}

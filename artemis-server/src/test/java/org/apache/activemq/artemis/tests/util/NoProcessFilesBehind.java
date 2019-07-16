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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.activemq.artemis.nativo.jlibaio.LibaioContext;
import org.apache.activemq.artemis.utils.Wait;
import org.jboss.logging.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * This is useful to make sure you won't have leaking threads between tests
 */
public class NoProcessFilesBehind extends TestWatcher {

   private static Logger log = Logger.getLogger(NoProcessFilesBehind.class);

   /**
    * -1 on maxVariance means no check
    */
   public NoProcessFilesBehind(int maxVariance, long maxFiles) {

      this.maxVariance = maxVariance;
      this.maxFiles = maxFiles;
   }

   long fdBefore;
   int maxVariance;
   long maxFiles;
   List<String> openFilesBefore;
   List<String> openFilesAfter;

   static OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

   public static long getOpenFD() {
      if (os instanceof UnixOperatingSystemMXBean) {
         return ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
      } else {
         return 0;
      }
   }

   @Override
   protected void starting(Description description) {
      LibaioContext.isLoaded();
      fdBefore = getOpenFD();
      openFilesBefore = getOpenFiles(true);
   }

   public static List<String> getOpenFiles(boolean filtered) {
      ArrayList<String> openFiles = new ArrayList<>();

      try {
         String outputLine;
         int processId = getProcessId();

         Process child = Runtime.getRuntime().exec("lsof -a -p " + processId + " -d ^txt,^mem,^cwd,^rtd,^DEL", new String[] {});

         try (BufferedReader processInput = new BufferedReader(new InputStreamReader(child.getInputStream()))) {
            processInput.readLine();
            while ((outputLine = processInput.readLine()) != null) {
               if (!filtered || (!outputLine.endsWith(".jar") && !outputLine.endsWith(".so") && !outputLine.contains("type=STREAM")))
               openFiles.add(outputLine);
            }
         }
      } catch (Exception ignore) {
      }

      return openFiles;
   }

   private static List<String> getDiffFiles(List<String> xOpenFiles, List<String> yOpenFiles) {
      ArrayList<String> diffOpenFiles = new ArrayList<>();
      for (String xOpenFile : xOpenFiles) {
         boolean found = false;
         Iterator<String> yOpenFilesIterator = yOpenFiles.iterator();
         String xOpenFileS = xOpenFile.replaceAll("\\s+", "\\\\s+");

         while (yOpenFilesIterator.hasNext() && !found) {
            String yOpenFileS = yOpenFilesIterator.next().replaceAll("\\s+", "\\\\s+");
            found = yOpenFileS.equals(xOpenFileS);
         }

         if (!found) {
            diffOpenFiles.add(xOpenFile);
         }
      }

      return diffOpenFiles;
   }

   private static int getProcessId() throws ReflectiveOperationException {
      java.lang.management.RuntimeMXBean runtime = java.lang.management.ManagementFactory.getRuntimeMXBean();
      java.lang.reflect.Field jvmField = runtime.getClass().getDeclaredField("jvm");
      jvmField.setAccessible(true);
      sun.management.VMManagement jvm = (sun.management.VMManagement) jvmField.get(runtime);
      java.lang.reflect.Method getProcessIdMethod = jvm.getClass().getDeclaredMethod("getProcessId");
      getProcessIdMethod.setAccessible(true);
      return (Integer) getProcessIdMethod.invoke(jvm);
   }

   public void tearDown() {
      openFilesAfter = getOpenFiles(true);
   }

   @Override
   protected void failed(Throwable e, Description description) {
   }

   @Override
   protected void succeeded(Description description) {
   }

   List<String> getVariance() {

      long fdAfter = getOpenFD();

      long variance = fdAfter - fdBefore;

      if (variance > 0) {
         List<String> currOpenFiles = getOpenFiles(true);
         List<String> diffOpenFiles = getDiffFiles(currOpenFiles, openFilesBefore);
         List<String> skippingOpenFiles = getDiffFiles(currOpenFiles, openFilesAfter);
         List<String> leavingOpenFiles = getDiffFiles(diffOpenFiles, skippingOpenFiles);

         return leavingOpenFiles;
      } else {
         return new ArrayList<>();
      }
   }

   /**
    * Override to tear down your specific external resource.
    */
   @Override
   protected void finished(Description description) {

      long fdAfter = getOpenFD();
      List<String> variance = getVariance();

      if (variance.size() > 0) {
         log.warn("test " + description.toString() + " is leaving " + variance.size() + " files open with a total number of files open = " + fdAfter);
         System.err.println("test " + description.toString() + " is leaving " + variance.size() + " files open with a total number of files open = " + fdAfter);

         for (String openFile : variance) {
            System.err.println(openFile);
         }
      }

      if (maxVariance > 0) {
         VarianceCondition varianceCondition = new VarianceCondition();
         Wait.assertTrue("The test " + description.toString() + " is leaving " + varianceCondition.getVarianceSize() + " files open, which is more than " + maxVariance + " max open", varianceCondition, 5000, 0);
      }

      Wait.assertTrue("Too many open files", () -> getOpenFD() < maxFiles, 5000, 0);

   }

   class VarianceCondition implements Wait.Condition {
      private List<String> variance = null;

      public long getVarianceSize() {
         if (variance != null) {
            return variance.size();
         } else {
            return 0;
         }
      }

      @Override
      public boolean isSatisfied() throws Exception {
         variance = getVariance();

         return variance.size() < maxVariance;
      }
   }

}

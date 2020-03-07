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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.List;

import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.activemq.artemis.nativo.jlibaio.LibaioContext;
import org.apache.activemq.artemis.utils.Wait;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * This is useful to make sure you won't have leaking threads between tests
 */
public class NoProcessFilesBehind extends TestWatcher {

   private static Logger log = Logger.getLogger(NoProcessFilesBehind.class);

   public NoProcessFilesBehind(long maxFiles) {
      this(-1, maxFiles);
   }

   /**
    * -1 on maxVariance means no check
    */
   public NoProcessFilesBehind(long variance, long maxFiles) {

      this.maxFiles = maxFiles;
      if (variance < 0) {
         maxvariance = null;
      } else {
         this.maxvariance = variance;
      }
   }

   long fdBefore;
   long maxFiles;
   Long maxvariance;

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
      if (maxvariance != null) {
         fdBefore = getOpenFD();
      }
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
   private static int getProcessId() throws ReflectiveOperationException {
      java.lang.management.RuntimeMXBean runtime = java.lang.management.ManagementFactory.getRuntimeMXBean();
      java.lang.reflect.Field jvmField = runtime.getClass().getDeclaredField("jvm");
      jvmField.setAccessible(true);
      Object jvm = jvmField.get(runtime);
      java.lang.reflect.Method getProcessIdMethod = jvm.getClass().getDeclaredMethod("getProcessId");
      getProcessIdMethod.setAccessible(true);
      return (Integer) getProcessIdMethod.invoke(jvm);
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

      Wait.waitFor(() -> getOpenFD() < maxFiles, 5000, 0);

      if (maxvariance != null) {
         long currentVariance  = getOpenFD() - fdBefore;

         if (currentVariance > 0 && currentVariance > maxvariance) {
            Assert.fail("too many files were opened files on this test::" + getOpenList());
         }

      }

      if (!Wait.waitFor(() -> getOpenFD() < maxFiles, 5000, 0)) {
         String fileList = getOpenList();
         Assert.fail("Too many files open (" + getOpenFD()  + ">" + maxFiles + "). A possible list: " + fileList);
      }

   }

   private String getOpenList() {
      List<String> openFiles = getOpenFiles(true);
      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);
      boolean first = true;
      for (String str : openFiles) {
         if (!first) printWriter.print("\n");
         first = false;
         printWriter.print(str);
      }
      return stringWriter.toString();
   }

}

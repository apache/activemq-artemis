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

package org.apache.activemq.artemis.utils;

/**
 * Utility that detects various properties specific to the current runtime
 * environment, such as JVM bitness and OS type.
 */
public final class Env {

   /** The system will change a few logs and semantics to be suitable to
    *  run a long testsuite.
    *  Like a few log entries that are only valid during a production system.
    *  or a few cases we need to know as warn on the testsuite and as log in production. */
   private static boolean testEnv = false;


   private static final String OS = System.getProperty("os.name").toLowerCase();
   private static final boolean IS_LINUX = OS.startsWith("linux");
   private static final boolean IS_64BIT = checkIs64bit();

   private Env() {

   }

   public static boolean isTestEnv() {
      return testEnv;
   }

   public static void setTestEnv(boolean testEnv) {
      Env.testEnv = testEnv;
   }

   public static boolean isLinuxOs() {
      return IS_LINUX == true;
   }

   public static boolean is64BitJvm() {
      return IS_64BIT;
   }

   private static boolean checkIs64bit() {
      //check the more used JVMs
      String systemProp;
      systemProp = System.getProperty("com.ibm.vm.bitmode");
      if (systemProp != null) {
         return "64".equals(systemProp);
      }
      systemProp = System.getProperty("sun.arch.data.model");
      if (systemProp != null) {
         return "64".equals(systemProp);
      }
      systemProp = System.getProperty("java.vm.version");
      return systemProp != null && systemProp.contains("_64");
   }
}

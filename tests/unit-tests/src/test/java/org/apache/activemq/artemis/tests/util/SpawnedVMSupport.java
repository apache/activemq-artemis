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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import org.apache.activemq.artemis.tests.unit.UnitTestLogger;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.junit.Assert;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class SpawnedVMSupport {

   private static final UnitTestLogger log = UnitTestLogger.LOGGER;

   public static Process spawnVM(final String className, final String... args) throws Exception {
      return SpawnedVMSupport.spawnVM(className, new String[0], true, args);
   }

   public static Process spawnVM(final String className,
                                 final boolean logOutput,
                                 final String... args) throws Exception {
      return SpawnedVMSupport.spawnVM(className, new String[0], logOutput, args);
   }

   public static Process spawnVM(final String className, final String[] vmargs, final String... args) throws Exception {
      return SpawnedVMSupport.spawnVM(className, vmargs, true, args);
   }

   public static Process spawnVM(final String className,
                                 final String[] vmargs,
                                 final boolean logOutput,
                                 final String... args) throws Exception {
      return SpawnedVMSupport.spawnVM(className, "-Xms512m", "-Xmx512m", vmargs, logOutput, true, true, args);
   }

   public static Process spawnVMWithLogMacher(String wordMatch,
                                              Runnable runnable,
                                              final String className,
                                              final String[] vmargs,
                                              final boolean logOutput,
                                              final String... args) throws Exception {
      return SpawnedVMSupport.spawnVM(wordMatch, runnable, className, "-Xms512m", "-Xmx512m", vmargs, logOutput, true, true, args);
   }

   public static Process spawnVM(final String className,
                                 final String memoryArg1,
                                 final String memoryArg2,
                                 final String[] vmargs,
                                 final boolean logOutput,
                                 final boolean logErrorOutput,
                                 final boolean useLogging,
                                 final String... args) throws Exception {
      return spawnVM(null, null, className, memoryArg1, memoryArg2, vmargs, logOutput, logErrorOutput, useLogging, args);
   }

   public static Process spawnVM(final String wordMatch,
                                 final Runnable wordRunning,
                                 final String className,
                                 final String memoryArg1,
                                 final String memoryArg2,
                                 final String[] vmargs,
                                 final boolean logOutput,
                                 final boolean logErrorOutput,
                                 final boolean useLogging,
                                 final String... args) throws Exception {
      return spawnVM(System.getProperty("java.class.path"), wordMatch, wordRunning, className, memoryArg1, memoryArg2, vmargs, logOutput, logErrorOutput, useLogging, args);

   }


   public static Process spawnVM(String classPath,
                                 String wordMatch,
                                 Runnable wordRunning,
                                 String className,
                                 String memoryArg1,
                                 String memoryArg2,
                                 String[] vmargs,
                                 boolean logOutput,
                                 boolean logErrorOutput,
                                 boolean useLogging,
                                 String... args) throws IOException, ClassNotFoundException {
      return spawnVM(classPath, wordMatch, wordRunning, className, memoryArg1,memoryArg2, vmargs, logOutput, logErrorOutput, useLogging, -1, args);
   }

   /**
    *
    * @param classPath
    * @param wordMatch
    * @param wordRunning
    * @param className
    * @param memoryArg1
    * @param memoryArg2
    * @param vmargs
    * @param logOutput
    * @param logErrorOutput
    * @param useLogging
    * @param debugPort if <=0 it means no debug
    * @param args
    * @return
    * @throws IOException
    * @throws ClassNotFoundException
    */
   public static Process spawnVM(String classPath,
                                 String wordMatch,
                                 Runnable wordRunning,
                                 String className,
                                 String memoryArg1,
                                 String memoryArg2,
                                 String[] vmargs,
                                 boolean logOutput,
                                 boolean logErrorOutput,
                                 boolean useLogging,
                                 long debugPort,
                                 String... args) throws IOException, ClassNotFoundException {
      final String javaPath = Paths.get(System.getProperty("java.home"), "bin", "java").toAbsolutePath().toString();
      ProcessBuilder builder = new ProcessBuilder();
      if (memoryArg1 == null) {
         memoryArg1 = "-Xms128m";
      }
      if (memoryArg2 == null) {
         memoryArg2 = "-Xmx128m";
      }
      builder.command(javaPath, memoryArg1, memoryArg2);
      if (debugPort > 0) {
         builder.command().add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=" + debugPort);
      }
      builder.environment().put("CLASSPATH", classPath);

      List<String> commandList = builder.command();

      if (vmargs != null) {
         for (String arg : vmargs) {
            commandList.add(arg);
         }
      }

      // The logs will be huge if you don't set this
      if (useLogging) {
         commandList.add("-Djava.util.logging.manager=org.jboss.logmanager.LogManager");
         commandList.add("-Dlogging.configuration=file:../config/logging.properties");
      }

      commandList.add("-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir", "./tmp"));
      commandList.add("-Djava.library.path=" + System.getProperty("java.library.path", "./native/bin"));

      String loggingConfigFile = System.getProperty("java.util.logging.config.file");

      if (loggingConfigFile != null) {
         commandList.add("-Djava.util.logging.config.file=" + loggingConfigFile + " ");
      }

      String jacocoAgent = System.getProperty("jacoco.agent");
      if (jacocoAgent != null && !jacocoAgent.isEmpty()) {
         commandList.add(jacocoAgent);
      }

      String loggingPlugin = System.getProperty("org.jboss.logging.Logger.pluginClass");
      if (loggingPlugin != null) {
         commandList.add("-Dorg.jboss.logging.Logger.pluginClass=" + loggingPlugin + " ");
      }

      commandList.add(className);
      for (String arg : args) {
         commandList.add(arg);
      }

      Process process = builder.start();

      SpawnedVMSupport.startLogger(logOutput, wordMatch, wordRunning, className, process);

      // Adding a reader to System.err, so the VM won't hang on a System.err.println as identified on this forum thread:
      // http://www.jboss.org/index.html?module=bb&op=viewtopic&t=151815
      ProcessLogger errorLogger = new ProcessLogger(logErrorOutput, process.getErrorStream(), className, wordMatch, wordRunning);
      errorLogger.start();

      return process;
   }

   /**
    * @param className
    * @param process
    * @throws ClassNotFoundException
    */
   public static void startLogger(final boolean print,
                                  final String wordMatch,
                                  final Runnable wordRunanble,
                                  final String className,
                                  final Process process) throws ClassNotFoundException {
      ProcessLogger outputLogger = new ProcessLogger(print, process.getInputStream(), className, wordMatch, wordRunanble);
      outputLogger.start();
   }

   /**
    * @param className
    * @param process
    * @throws ClassNotFoundException
    */
   public static void startLogger(final String className, final Process process) throws ClassNotFoundException {
      startLogger(true, null, null, className, process);
   }

   /**
    * Assert that a process exits with the expected value (or not depending if
    * the <code>sameValue</code> is expected or not). The method waits 5
    * seconds for the process to exit, then an Exception is thrown. In any case,
    * the process is destroyed before the method returns.
    */
   public static void assertProcessExits(final boolean sameValue,
                                         final int value,
                                         final Process p) throws InterruptedException, ExecutionException, TimeoutException {
      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(ActiveMQThreadFactory.defaultThreadFactory());
      Future<Integer> future = executor.submit(new Callable<Integer>() {

         @Override
         public Integer call() throws Exception {
            p.waitFor();
            return p.exitValue();
         }
      });
      try {
         int exitValue = future.get(10, SECONDS);
         if (sameValue) {
            Assert.assertSame(value, exitValue);
         } else {
            Assert.assertNotSame(value, exitValue);
         }
      } finally {
         p.destroy();
      }
   }

   /**
    * Redirect the input stream to a logger (as debug logs)
    */
   static class ProcessLogger extends Thread {

      private final InputStream is;

      private final String className;

      private final boolean print;

      private final String wordMatch;
      /**
       * This will be executed when wordMatch is within any line on the log *
       * *
       */
      private final Runnable wordRunner;

      ProcessLogger(final boolean print,
                    final InputStream is,
                    final String className,
                    String wordMatch,
                    Runnable wordRunner) throws ClassNotFoundException {
         this.is = is;
         this.print = print;
         this.className = className;
         this.wordMatch = wordMatch;
         this.wordRunner = wordRunner;
         setDaemon(true);
      }

      @Override
      public void run() {
         try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
               if (wordMatch != null && wordRunner != null) {
                  if (line.contains(wordMatch)) {
                     wordRunner.run();
                  }
               }
               if (print) {
                  System.out.println(className + ":" + line);
               }
            }
         } catch (IOException ioe) {
            ioe.printStackTrace();
         }
      }
   }
}

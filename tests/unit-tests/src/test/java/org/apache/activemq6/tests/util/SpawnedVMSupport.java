/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.tests.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import org.apache.activemq6.tests.unit.UnitTestLogger;
import org.junit.Assert;

import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert Suconic</a>
 */
public final class SpawnedVMSupport
{
   private static final UnitTestLogger log = UnitTestLogger.LOGGER;

   public static Process spawnVM(final String className, final String... args) throws Exception
   {
      return SpawnedVMSupport.spawnVM(className, new String[0], true, args);
   }

   public static Process spawnVM(final String className, final boolean logOutput, final String... args) throws Exception
   {
      return SpawnedVMSupport.spawnVM(className, new String[0], logOutput, args);
   }

   public static Process spawnVM(final String className, final String[] vmargs, final String... args) throws Exception
   {
      return SpawnedVMSupport.spawnVM(className, vmargs, true, args);
   }

   public static Process spawnVM(final String className,
                                 final String[] vmargs,
                                 final boolean logOutput,
                                 final String... args) throws Exception
   {
      return SpawnedVMSupport.spawnVM(className, "-Xms512m", "-Xmx512m", vmargs, logOutput, true, args);
   }


   public static Process spawnVM(final String className,
                                 final String memoryArg1,
                                 final String memoryArg2,
                                 final String[] vmargs,
                                 final boolean logOutput,
                                 final boolean logErrorOutput,
                                 final String... args) throws Exception
   {
      ProcessBuilder builder = new ProcessBuilder();
      builder.command("java", memoryArg1, memoryArg2, "-cp", System.getProperty("java.class.path"));

      List<String> commandList = builder.command();

      if (vmargs != null)
      {
         for (String arg : vmargs)
         {
            commandList.add(arg);
         }
      }

      commandList.add("-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir", "./tmp"));
      commandList.add("-Djava.library.path=" + System.getProperty("java.library.path", "./native/bin"));


      String loggingConfigFile = System.getProperty("java.util.logging.config.file");

      if (loggingConfigFile != null)
      {
         commandList.add("-Djava.util.logging.config.file=" + loggingConfigFile + " ");
      }

      String loggingPlugin = System.getProperty("org.jboss.logging.Logger.pluginClass");
      if (loggingPlugin != null)
      {
         commandList.add("-Dorg.jboss.logging.Logger.pluginClass=" + loggingPlugin + " ");
      }

      commandList.add(className);
      for (String arg : args)
      {
         commandList.add(arg);
      }

      Process process = builder.start();

      if (logOutput)
      {
         SpawnedVMSupport.startLogger(className, process);

      }

      // Adding a reader to System.err, so the VM won't hang on a System.err.println as identified on this forum thread:
      // http://www.jboss.org/index.html?module=bb&op=viewtopic&t=151815
      ProcessLogger errorLogger = new ProcessLogger(logErrorOutput, process.getErrorStream(), className);
      errorLogger.start();

      return process;


   }

   /**
    * @param className
    * @param process
    * @throws ClassNotFoundException
    */
   public static void startLogger(final String className, final Process process) throws ClassNotFoundException
   {
      ProcessLogger outputLogger = new ProcessLogger(true, process.getInputStream(), className);
      outputLogger.start();
   }

   /**
    * Assert that a process exits with the expected value (or not depending if
    * the <code>sameValue</code> is expected or not). The method waits 5
    * seconds for the process to exit, then an Exception is thrown. In any case,
    * the process is destroyed before the method returns.
    */
   public static void assertProcessExits(final boolean sameValue, final int value, final Process p) throws InterruptedException,
      ExecutionException,
      TimeoutException
   {
      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
      Future<Integer> future = executor.submit(new Callable<Integer>()
      {

         public Integer call() throws Exception
         {
            p.waitFor();
            return p.exitValue();
         }
      });
      try
      {
         int exitValue = future.get(10, SECONDS);
         if (sameValue)
         {
            Assert.assertSame(value, exitValue);
         }
         else
         {
            Assert.assertNotSame(value, exitValue);
         }
      }
      finally
      {
         p.destroy();
      }
   }

   /**
    * Redirect the input stream to a logger (as debug logs)
    */
   static class ProcessLogger extends Thread
   {
      private final InputStream is;

      private final String className;

      private final boolean print;

      ProcessLogger(final boolean print, final InputStream is, final String className) throws ClassNotFoundException
      {
         this.is = is;
         this.print = print;
         this.className = className;
         setDaemon(true);
      }

      @Override
      public void run()
      {
         try
         {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null)
            {
               if (print)
               {
                  System.out.println(className + ":" + line);
               }
            }
         }
         catch (IOException ioe)
         {
            ioe.printStackTrace();
         }
      }
   }
}

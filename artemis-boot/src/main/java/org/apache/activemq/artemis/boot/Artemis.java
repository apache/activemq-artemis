/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.boot;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * A main class which setups up a classpath and then passes
 * execution off to the ActiveMQ Artemis cli main.
 * </p>
 */
public class Artemis {

   private static final Logger logger = Logger.getLogger(Artemis.class.getName());

   public static void main(String[] args) throws Throwable {
      String home = System.getProperty("artemis.home");

      File fileHome = home != null ? new File(home) : null;

      String instance = System.getProperty("artemis.instance");
      File fileInstance = instance != null ? new File(instance) : null;

      Object result = execute(fileHome, fileInstance, args);
      if (result instanceof Exception) {
         // Set a nonzero status code for the exceptions caught and printed by org.apache.activemq.artemis.cli.Artemis.execute
         System.exit(1);
      }
   }

   /**
    * This is a good method for booting an embedded command
    */
   public static Object execute(File artemisHome, File artemisInstance, List<String> args) throws Throwable {
      return execute(artemisHome, artemisInstance, args.toArray(new String[args.size()]));
   }

   /**
    * This is a good method for booting an embedded command
    */
   public static Object execute(File fileHome, File fileInstance, String... args) throws Throwable {
      ArrayList<File> dirs = new ArrayList<>();
      if (fileHome != null) {
         dirs.add(new File(fileHome, "lib"));
      }
      if (fileInstance != null) {
         dirs.add(new File(fileInstance, "lib"));
      }

      ArrayList<URL> urls = new ArrayList<>();

      // Without the etc on the config, things like JGroups configuration wouldn't be loaded
      if (fileInstance != null) {
         File etcFile = new File(fileInstance, "etc");
         // Adding etc to the classLoader so modules can lookup for their configs
         urls.add(etcFile.toURI().toURL());
      }
      if (fileHome != null) {
         File etcFile = new File(fileHome, "etc");
         // Adding etc to the classLoader so modules can lookup for their configs
         urls.add(etcFile.toURI().toURL());
      }
      for (File bootdir : dirs) {
         if (bootdir.exists() && bootdir.isDirectory()) {

            // Find the jar files in the directory..
            ArrayList<File> files = new ArrayList<>();
            for (File f : bootdir.listFiles()) {
               if (f.getName().endsWith(".jar") || f.getName().endsWith(".zip")) {
                  files.add(f);
               }
            }

            // Sort the list by file name..
            Collections.sort(files, new Comparator<File>() {
               @Override
               public int compare(File file, File file1) {
                  return file.getName().compareTo(file1.getName());
               }
            });

            for (File f : files) {
               add(urls, f);
            }

         }
      }

      if (System.getProperty("java.io.tmpdir") == null && fileInstance != null) {
         System.setProperty("java.io.tmpdir", new File(fileInstance, "tmp").getCanonicalPath());
      }

      // Lets try to covert the logging.configuration setting to a valid URI
      String loggingConfig = System.getProperty("logging.configuration");
      if (loggingConfig != null) {
         System.setProperty("logging.configuration", fixupFileURI(loggingConfig));
      }

      ClassLoader originalCL = Thread.currentThread().getContextClassLoader();

      // Now setup our classloader..
      URLClassLoader loader = new URLClassLoader(urls.toArray(new URL[urls.size()]));
      Thread.currentThread().setContextClassLoader(loader);
      Class<?> clazz = loader.loadClass("org.apache.activemq.artemis.cli.Artemis");
      Method method = clazz.getMethod("execute", Boolean.TYPE, File.class, File.class, args.getClass());

      try {
         return method.invoke(null, true, fileHome, fileInstance, args);
      } catch (InvocationTargetException e) {
         throw e.getTargetException();
      } finally {
         Thread.currentThread().setContextClassLoader(originalCL);
      }

   }

   static String fixupFileURI(String value) {
      if (value != null && value.startsWith("file:")) {
         value = value.substring("file:".length());
         value = new File(value).toURI().toString();
      }
      return value;
   }

   private static void add(ArrayList<URL> urls, File file) {
      try {
         urls.add(file.toURI().toURL());
      } catch (MalformedURLException e) {
         logger.log(Level.WARNING, e.getMessage(), e);
      }
   }

}

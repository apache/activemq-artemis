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

package org.apache.activemq.artemis.tests.compatibility.base;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.activemq.artemis.tests.compatibility.GroovyRun;
import org.jboss.logging.Logger;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.SNAPSHOT;



public class ClasspathBase {


   private final Logger instanceLog = Logger.getLogger(this.getClass());

   @Rule
   public TestRule watcher = new TestWatcher() {

      @Override
      protected void starting(Description description) {
         instanceLog.info(String.format("**** start #test %s() ***", ClasspathBase.this.getClass().getName() + "::" + description.getMethodName()));
      }

      @Override
      protected void finished(Description description) {
         instanceLog.info(String.format("**** end #test %s() ***", ClasspathBase.this.getClass().getName() + "::" + description.getMethodName()));
      }
   };


   @ClassRule
   public static TemporaryFolder serverFolder;
   private static int javaVersion;

   static {
      File parent = new File("./target/tmp");
      parent.mkdirs();
      serverFolder = new TemporaryFolder(parent);
      String version = System.getProperty("java.version");
      if (version.startsWith("1.")) {
         version = version.substring(2, 3);
      } else {
         int dot = version.indexOf(".");
         if (dot != -1) {
            version = version.substring(0, dot);
         }
      }
      javaVersion = Integer.parseInt(version);
   }

   protected static Map<String, ClassLoader> loaderMap = new HashMap<>();

   private static HashSet<String> printed = new HashSet<>();

   protected ClassLoader defineClassLoader(String classPath) throws Exception {
      String[] classPathArray = classPath.split(File.pathSeparator);
      URL[] elements = new URL[classPathArray.length];
      for (int i = 0; i < classPathArray.length; i++) {
         elements[i] = new File(classPathArray[i]).toPath().toUri().toURL();
      }
      if (javaVersion > 8) {
         ClassLoader parent = (ClassLoader) ClassLoader.class.getDeclaredMethod("getPlatformClassLoader").invoke(null);
         return new URLClassLoader(elements, parent);
      }
      return new URLClassLoader(elements, null);
   }

   protected static void startServer(File folder,
                                     ClassLoader loader,
                                     String serverName,
                                     String globalMaxSize,
                                     boolean setAddressSettings,
                                     String scriptToUse,
                                     String server,
                                     String sender,
                                     String receiver) throws Exception {
      setVariable(loader, "setAddressSettings", setAddressSettings);
      evaluate(loader, scriptToUse, folder.getAbsolutePath(), serverName, server, sender, receiver, globalMaxSize);
   }

   public static void stopServer(ClassLoader loader) throws Throwable {
      execute(loader, "server.stop()");
   }

   protected ClassLoader getClasspath(String name) throws Exception {
      return getClasspath(name, false);
   }

   protected ClassLoader getClasspath(String name, boolean forceNew) throws Exception {

      if (!forceNew) {
         if (name.equals(SNAPSHOT)) {
            GroovyRun.clear();
            return VersionedBase.class.getClassLoader();
         }

         ClassLoader loader = loaderMap.get(name);
         if (loader != null && !forceNew) {
            clearGroovy(loader);
            return loader;
         }
      }

      String value = System.getProperty(name);

      if (!printed.contains(name)) {
         boolean ok = value != null && !value.trim().isEmpty();
         if (!ok) {
            System.out.println("Add \"-D" + name + "=\'CLASSPATH\'\" into your VM settings");
            System.out.println("You will see it in the output from mvn install at the compatibility-tests");
            System.out.println("... look for output from dependency-scan");

            // our dependency scan used at the pom under compatibility-tests/pom.xml will generate these, example:
            // [INFO] dependency-scan setting: -DARTEMIS-140="/Users/someuser/....."
            // copy that into your IDE setting and you should be able to debug it
         }
         Assume.assumeTrue("Cannot run these tests, no classpath found", ok);
      }

      ClassLoader loader = defineClassLoader(value);
      if (!forceNew) {
         // if we are forcing a new one, there's no point in caching it
         loaderMap.put(name, loader);
      }

      return loader;
   }

   protected static Object evaluate(ClassLoader loader, String script, String... arguments) throws Exception {
      return tclCall(loader, () -> {
         Class clazz = loader.loadClass(GroovyRun.class.getName());
         Method method = clazz.getMethod("evaluate", String.class, String[].class);
         return method.invoke(null, script, arguments);
      });
   }

   protected static void setVariable(ClassLoader loader, String name, Object object) throws Exception {
      tclCall(loader, () -> {
         Class clazz = loader.loadClass(GroovyRun.class.getName());
         Method method = clazz.getMethod("setVariable", String.class, Object.class);
         method.invoke(null, name, object);
         return null;
      });
   }

   protected static void clearGroovy(ClassLoader loader) throws Exception {
      tclCall(loader, () -> {
         Class clazz = loader.loadClass(GroovyRun.class.getName());
         Method method = clazz.getMethod("clear");
         method.invoke(null);
         return null;
      });
   }

   protected static Object setVariable(ClassLoader loader, String name) throws Exception {
      return tclCall(loader, () -> {
         Class clazz = loader.loadClass(GroovyRun.class.getName());
         Method method = clazz.getMethod("getVariable", String.class);
         return method.invoke(null, name);
      });
   }

   protected static Object execute(ClassLoader loader, String script) throws Exception {
      return tclCall(loader, () -> {
         Class clazz = loader.loadClass(GroovyRun.class.getName());
         Method method = clazz.getMethod("execute", String.class);
         return method.invoke(null, script);
      });
   }

   protected static Object tclCall(ClassLoader loader, CallIt run) throws Exception {

      ClassLoader original = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(loader);
      try {
         return run.run();
      } finally {
         Thread.currentThread().setContextClassLoader(original);
      }
   }

   public interface CallIt {

      Object run() throws Exception;
   }

}

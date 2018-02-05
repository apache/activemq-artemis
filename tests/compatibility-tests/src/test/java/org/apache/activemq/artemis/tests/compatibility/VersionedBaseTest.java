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

package org.apache.activemq.artemis.tests.compatibility;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.SNAPSHOT;

public abstract class VersionedBaseTest {

   protected final String server;
   protected final String sender;
   protected final String receiver;

   protected ClassLoader serverClassloader;
   protected ClassLoader senderClassloader;
   protected ClassLoader receiverClassloader;

   protected static Map<String, ClassLoader> loaderMap = new HashMap<>();

   public VersionedBaseTest(String server, String sender, String receiver) throws Exception {
      if (server == null) {
         server = sender;
      }
      this.server = server;
      this.sender = sender;
      this.receiver = receiver;
      this.serverClassloader = getClasspathProperty(server);
      this.senderClassloader = getClasspathProperty(sender);
      this.receiverClassloader = getClasspathProperty(receiver);
   }

   // This is a test optimization..
   // if false it will span a new VM for each classLoader used.
   // this can be a bit faster
   public static final boolean USE_CLASSLOADER = true;

   private static HashSet<String> printed = new HashSet<>();

   @ClassRule
   public static TemporaryFolder serverFolder;

   static {
      File parent = new File("./target/tmp");
      parent.mkdirs();
      serverFolder = new TemporaryFolder(parent);
   }

   @AfterClass
   public static void cleanup() {
      loaderMap.clear();
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

   protected static ClassLoader defineClassLoader(String classPath) throws MalformedURLException {
      String[] classPathArray = classPath.split(File.pathSeparator);
      URL[] elements = new URL[classPathArray.length];
      for (int i = 0; i < classPathArray.length; i++) {
         elements[i] = new File(classPathArray[i]).toPath().toUri().toURL();
      }

      return new URLClassLoader(elements, null);
   }

   protected static ClassLoader getClasspathProperty(String name) throws Exception {

      if (name.equals(SNAPSHOT)) {
         return VersionedBaseTest.class.getClassLoader();
      }

      ClassLoader loader = loaderMap.get(name);
      if (loader != null) {
         return loader;
      }

      String value = System.getProperty(name);

      if (!printed.contains(name)) {
         boolean ok = value != null && !value.trim().isEmpty();
         if (!ok) {
            System.out.println("Add \"-D" + name + "=\'CLASSPATH\'\" into your VM settings");
         } else {
            printed.add(name);
            System.out.println("****************************************************************************");
            System.out.println("* If you want to debug this test, add this parameter to your IDE run settings...");
            System.out.println("****************************************************************************");
            System.out.println("-D" + name + "=\"" + value + "\"");
            System.out.println("****************************************************************************");

         }

         Assume.assumeTrue("Cannot run these tests, no classpath found", ok);
      }

      loader = defineClassLoader(value);
      loaderMap.put(name, loader);

      return loader;
   }

   protected static List<Object[]> combinatory(Object[] rootSide, Object[] sideLeft, Object[] sideRight) {
      LinkedList<Object[]> combinations = new LinkedList<>();

      for (Object root : rootSide) {
         for (Object left : sideLeft) {
            for (Object right : sideRight) {
               combinations.add(new Object[]{root, left, right});
            }
         }
      }

      return combinations;
   }

   public void startServer(File folder, ClassLoader loader, String serverName) throws Throwable {
      startServer(folder, loader, serverName, null);
   }

   public void startServer(File folder, ClassLoader loader, String serverName, String globalMaxSize) throws Throwable {
      folder.mkdirs();

      System.out.println("Folder::" + folder);

      String scriptToUse;
      if (getServerScriptToUse() != null && getServerScriptToUse().length() != 0) {
         scriptToUse = getServerScriptToUse();
      } else if (server.startsWith("ARTEMIS")) {
         scriptToUse = "servers/artemisServer.groovy";
      } else {
         scriptToUse = "servers/hornetqServer.groovy";
      }

      evaluate(loader, scriptToUse, folder.getAbsolutePath(), serverName, server, sender, receiver, globalMaxSize);
   }
   public void stopServer(ClassLoader loader) throws Throwable {
      execute(loader, "server.stop()");
   }

   public String getServerScriptToUse() {
      return null;
   }
}

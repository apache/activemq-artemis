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

package org.apache.activemq.artemis.tests.compatibility;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.artemis.utils.FileUtil;
import org.apache.activemq.artemis.utils.RunnableEx;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
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

   public VersionedBaseTest(String server, String sender, String receiver) throws Exception {
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

   @Before
   public void startServer() throws Throwable {
      FileUtil.deleteDirectory(serverFolder.getRoot());

      serverFolder.getRoot().mkdirs();

      System.out.println("Folder::" + serverFolder.getRoot());

      String scriptToUse;
      if (server.startsWith("ARTEMIS")) {
         scriptToUse = "servers/artemisServer.groovy";
      } else {
         scriptToUse = "servers/hornetqServer.groovy";
      }

      callMain(serverClassloader, GroovyRun.class.getName(), scriptToUse, serverFolder.getRoot().getAbsolutePath(), "1", server, sender, receiver);
   }

   @After
   public void stopServer() throws Throwable {
      callExecute(serverClassloader, GroovyRun.class.getName(), "server.stop()");

      // GC help!!!
      serverClassloader = null;
      senderClassloader = null;
      receiverClassloader = null;
   }

   protected static void callMain(ClassLoader loader,
                                  String className,
                                  String script,
                                  String... arguments) throws Exception {
      tclCall(loader, () -> {
         Class clazz = loader.loadClass(className);
         Method method = clazz.getMethod("doMain", String.class, String[].class);
         method.invoke(null, script, arguments);
      });
   }

   protected static void callExecute(ClassLoader loader, String className, String script) throws Exception {
      tclCall(loader, () -> {
         Class clazz = loader.loadClass(className);
         Method method = clazz.getMethod("execute", String.class);
         method.invoke(null, script);
      });
   }

   protected static void tclCall(ClassLoader loader, RunnableEx run) throws Exception {

      ClassLoader original = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(loader);
      try {
         run.run();
      } finally {
         Thread.currentThread().setContextClassLoader(original);
      }
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

      return defineClassLoader(value);
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

}

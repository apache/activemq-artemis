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

package org.apache.activemq.artemis.tests.servercompatibility.base;

import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import groovy.transform.Memoized;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.runtime.DefaultGroovyMethods;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class CompatibilityTestScript extends Script implements CompatibilityTestScriptInterface {

   @Memoized
   private static Set<String> getInterfacePropertyNames() {
      return Arrays.stream(CompatibilityTestScriptInterface.class.getDeclaredMethods())
            .filter((m) -> m.getParameterCount() == 0)
            .filter((m) -> m.getReturnType() != Void.class)
            .map(Method::getName)
            .filter((m) -> m.startsWith("get"))
            .map((m) -> m.substring(3, 4).toLowerCase() + m.substring(4))
            .collect(Collectors.toSet());
   }

   @Override
   public void setBinding(Binding binding) {
      super.setBinding(binding);
      getInterfacePropertyNames().forEach((p) -> {
         if (!binding.hasVariable(p)) {
            throw new AssertionError("Binding property '" + p + "' not set.");
         }
      });
   }

   @Override
   public File getWorkingDir() {
      return (File) getBinding().getVariable("workingDir");
   }

   @Override
   public String getSide() {
      return (String) getBinding().getVariable("side");
   }

   private Object createLocalShell() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
      final ClassLoader cl = getClass().getClassLoader();
      final Class<?> confClass = cl.loadClass(CompilerConfiguration.class.getCanonicalName());
      final Object conf = confClass.getConstructor().newInstance();
      conf.getClass().getMethod("setScriptBaseClass", String.class).invoke(conf, getClass().getCanonicalName());
      final Class<?> bindingClass = cl.loadClass(Binding.class.getCanonicalName());
      final Class<?> shellClass = cl.loadClass(GroovyShell.class.getCanonicalName());
      return shellClass.getConstructor(ClassLoader.class, bindingClass, confClass).newInstance(cl, getBinding(), conf);
   }

   @Override
   public Object evaluate(File script) throws IOException {
      if (!script.isAbsolute()) {
         script = ScriptedCompatibilityTest.getScript(script.getPath());
      }
      try {
         final Object shell = createLocalShell();
         return shell.getClass().getMethod("evaluate", File.class).invoke(shell, script);
      } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException |
               NoSuchMethodException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public Object evaluate(String expression) throws CompilationFailedException {
      try {
         final Object shell = createLocalShell();
         return shell.getClass().getMethod("evaluate", String.class).invoke(shell, expression);
      } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException |
               NoSuchMethodException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void assertEquals(Object expected, Object given) {
      Assert.assertEquals(expected, given);
   }

   @Override
   public void assertTrue(boolean condition) {
      Assert.assertTrue(condition);
   }

   @Override
   public void assertFalse(boolean condition) {
      Assert.assertFalse(condition);
   }

   @Override
   public void assertNotNull(Object object) {
      Assert.assertNotNull(object);
   }

   @Override
   public <T> T waitForCondition(int seconds, Closure<T> condition) throws InterruptedException {
      return waitForCondition(null, seconds, condition);
   }

   @Override
   public <T> T waitForCondition(String message, int seconds, Closure<T> condition) throws InterruptedException {
      T result = condition.call();
      if (!DefaultGroovyMethods.asBoolean(result)) {
         if (message != null) {
            System.out.println(message);
         }
      }
      while (seconds > 0) {
         Thread.sleep(1000);
         seconds--;
         result = condition.call();
         if (DefaultGroovyMethods.asBoolean(result)) {
            return result;
         }
      }
      assertTrue(DefaultGroovyMethods.asBoolean(result));
      return result;
   }
}

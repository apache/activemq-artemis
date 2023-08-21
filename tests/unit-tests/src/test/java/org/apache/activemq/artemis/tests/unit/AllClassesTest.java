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
package org.apache.activemq.artemis.tests.unit;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@RunWith(value = Parameterized.class)
public class AllClassesTest {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Parameterized.Parameters(name = "classInfo={0}")
   public static Collection<?> getParameters() {
      List<Class<?>> parameters = new ArrayList<>();
      try (ScanResult result = new ClassGraph().acceptPackages("org.apache.activemq.artemis").scan()) {
         for (Map.Entry<String, ClassInfo> entry : result.getAllClasses().asMap().entrySet()) {
            if (!entry.getKey().contains("tests")
               && !entry.getValue().isInterface() && !entry.getValue().isEnum() && !entry.getValue().isAbstract() && !entry.getValue().isInnerClass()) {
               try {
                  parameters.add(entry.getValue().loadClass());
               } catch (Throwable loadThrowable) {
                  logger.debug("cannot load {} : {}", entry.getKey(), loadThrowable);
               }
            }
         }

         parameters.sort(Comparator.comparing(Class::getName));

         return parameters;
      } catch (Exception e) {
         logger.warn("Exception on loading all classes: {}", e.toString());
      }

      return parameters;
   }

   private Class<?> targetClass;

   public AllClassesTest(Class<?> targetClass) {
      this.targetClass = targetClass;
   }


   @Test(timeout = 3000)
   public void testToString() {
      Object targetInstance = null;

      try {
         targetInstance = newInstance(targetClass);
      } catch (Throwable t) {
         logger.debug("Error creating a new instance of {}: {}", targetClass.getName(), t);
      }

      Assume.assumeTrue("Cannot create " + targetClass.getName(), targetInstance != null);

      try {
         String targetOutput = targetInstance.toString();
         logger.debug("targetOutput: {}", targetOutput);
      } finally {
         if (targetInstance instanceof AutoCloseable) {
            try {
               ((AutoCloseable)targetInstance).close();
            } catch (Throwable t) {
               logger.debug("Error closing the instance of {}: {}", targetClass.getName(), t);
            }
         }
      }
   }

   private Object newInstance(Class<?> targetClass) {
      Constructor<?>[] targetConstructors = targetClass.getDeclaredConstructors();
      Arrays.sort(targetConstructors, (c1, c2) -> c2.getParameterCount() - c1.getParameterCount());
      for (Constructor<?> targetConstructor : targetConstructors) {
         List<Object> initArgs = new ArrayList<>();
         Parameter[] constructorParameters = targetConstructor.getParameters();

         for (Parameter constructorParameter : constructorParameters) {
            Object initArg;
            if (constructorParameter.getType().isAssignableFrom(byte.class)) {
               initArg = RandomUtil.randomByte();
            } else if (constructorParameter.getType().isAssignableFrom(byte[].class)) {
               initArg = RandomUtil.randomBytes();
            } else if (constructorParameter.getType().isAssignableFrom(boolean.class)) {
               initArg = RandomUtil.randomBoolean();
            } else if (constructorParameter.getType().isAssignableFrom(char.class)) {
               initArg = RandomUtil.randomChar();
            } else if (constructorParameter.getType().isAssignableFrom(double.class)) {
               initArg = RandomUtil.randomDouble();
            } else if (constructorParameter.getType().isAssignableFrom(float.class)) {
               initArg = RandomUtil.randomFloat();
            } else if (constructorParameter.getType().isAssignableFrom(int.class)) {
               initArg = RandomUtil.randomInt() / 1024;
            } else if (constructorParameter.getType().isAssignableFrom(long.class)) {
               initArg = RandomUtil.randomLong();
            } else if (constructorParameter.getType().isAssignableFrom(String.class)) {
               initArg = RandomUtil.randomString();
            } else {
               initArg = null;
            }
            initArgs.add(initArg);
         }

         try {
            return targetConstructor.newInstance(initArgs.toArray());
         } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
               logger.debug("Cannot construct {}: {}", targetClass.getName(), t);
            }
         }
      }

      return null;
   }
}

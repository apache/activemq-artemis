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
package org.apache.activemq.artemis.utils;

import java.net.URL;
import java.util.Properties;

import org.jboss.logging.Logger;

/**
 * This class will be used to perform generic class-loader operations,
 * such as load a class first using TCCL, and then the classLoader used by ActiveMQ Artemis (ClassloadingUtil.getClass().getClassLoader()).
 * <p>
 * Is't required to use a Security Block on any calls to this class.
 */

public final class ClassloadingUtil {

   private static final Logger logger = Logger.getLogger(ClassloadingUtil.class);

   private static final String INSTANTIATION_EXCEPTION_MESSAGE = "Your class must have a constructor without arguments. If it is an inner class, it must be static!";

   public static Object newInstanceFromClassLoader(final String className) {
      return newInstanceFromClassLoader(ClassloadingUtil.class, className);
   }

   public static Object newInstanceFromClassLoader(final Class<?> classOwner, final String className) {
      ClassLoader loader = classOwner.getClassLoader();
      try {
         Class<?> clazz = loader.loadClass(className);
         return clazz.newInstance();
      } catch (Throwable t) {
         if (t instanceof InstantiationException) {
            System.out.println(INSTANTIATION_EXCEPTION_MESSAGE);
         }
         loader = Thread.currentThread().getContextClassLoader();
         if (loader == null)
            throw new RuntimeException("No local context classloader", t);

         try {
            return loader.loadClass(className).newInstance();
         } catch (InstantiationException e) {
            throw new RuntimeException(INSTANTIATION_EXCEPTION_MESSAGE + " " + className, e);
         } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
         } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
         }
      }
   }

   public static Object newInstanceFromClassLoader(final String className, Object... objs) {
      return newInstanceFromClassLoader(ClassloadingUtil.class, className, objs);
   }

   public static Object newInstanceFromClassLoader(final Class<?> classOwner, final String className, Object... objs) {
      ClassLoader loader = classOwner.getClassLoader();
      try {
         Class<?>[] parametersType = new Class<?>[objs.length];
         for (int i = 0; i < objs.length; i++) {
            parametersType[i] = objs[i].getClass();
         }
         Class<?> clazz = loader.loadClass(className);
         return clazz.getConstructor(parametersType).newInstance(objs);
      } catch (Throwable t) {
         if (t instanceof InstantiationException) {
            System.out.println(INSTANTIATION_EXCEPTION_MESSAGE);
         }
         loader = Thread.currentThread().getContextClassLoader();
         if (loader == null)
            throw new RuntimeException("No local context classloader", t);

         try {
            return loader.loadClass(className).newInstance();
         } catch (InstantiationException e) {
            throw new RuntimeException(INSTANTIATION_EXCEPTION_MESSAGE + " " + className, e);
         } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
         } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
         }
      }
   }

   public static URL findResource(final String resourceName) {
      return findResource(ClassloadingUtil.class.getClassLoader(), resourceName);
   }

   public static URL findResource(ClassLoader loader, final String resourceName) {
      try {
         URL resource = loader.getResource(resourceName);
         if (resource != null)
            return resource;
      } catch (Throwable t) {
      }

      loader = Thread.currentThread().getContextClassLoader();
      if (loader == null)
         return null;

      return loader.getResource(resourceName);
   }


   public static String loadProperty(ClassLoader loader, String propertiesFile, String name) {
      Properties properties = loadProperties(loader, propertiesFile);

      return (String)properties.get(name);
   }

   public static Properties loadProperties(ClassLoader loader, String propertiesFile) {
      Properties properties = new Properties();

      try {
         URL url = findResource(loader, propertiesFile);
         if (url != null) {
            properties.load(url.openStream());
         }
      } catch (Throwable ignored) {
         logger.warn(ignored);
      }
      return properties;
   }

}

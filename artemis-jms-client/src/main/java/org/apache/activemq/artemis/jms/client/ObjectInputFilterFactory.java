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
package org.apache.activemq.artemis.jms.client;

import java.io.ObjectInputFilter;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ObjectInputFilterFactory {

   public static final String SERIAL_FILTER_PROPERTY            = "org.apache.activemq.artemis.jms.serialFilter";
   public static final String SERIAL_FILTER_CLASS_NAME_PROPERTY = "org.apache.activemq.artemis.jms.serialFilterClassName";

   private static Map<Object, ObjectInputFilter> filterCache = new ConcurrentHashMap<>();

   public static ObjectInputFilter getObjectInputFilter(ConnectionFactoryOptions options) {
      String className = getFilterClassName(options);
      if (className != null) {
         return getObjectInputFilterForClassName(className);
      }

      String pattern = getFilterPattern(options);
      if (pattern != null) {
         return getObjectInputFilterForPattern(pattern);
      }

      return null;
   }

   public static ObjectInputFilter getObjectInputFilterForPattern(String pattern) {
      if (pattern == null) {
         return null;
      }

      return filterCache.computeIfAbsent(new PatternKey(pattern),
                                         k -> ObjectInputFilter.Config.createFilter(pattern));
   }

   public static ObjectInputFilter getObjectInputFilterForClassName(String className) {
      if (className == null) {
         return null;
      }

      return filterCache.computeIfAbsent(new ClassNameKey(className), k -> {
         try {
            return (ObjectInputFilter)Class.forName(className).newInstance();
         } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class " + className + " not found.", e);
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      });
   }

   private static String getFilterClassName(ConnectionFactoryOptions options) {
      if (options != null && options.getSerialFilterClassName() != null) {
         return options.getSerialFilterClassName();
      }

      return System.getProperty(SERIAL_FILTER_CLASS_NAME_PROPERTY);
   }

   private static String getFilterPattern(ConnectionFactoryOptions options) {
      if (options != null && options.getSerialFilter() != null) {
         return options.getSerialFilter();
      }

      return System.getProperty(SERIAL_FILTER_PROPERTY);
   }

   private static class PatternKey {
      private String pattern;

      PatternKey(String pattern) {
         this.pattern = pattern;
      }

      public String getPattern() {
         return pattern;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         PatternKey that = (PatternKey) o;

         if (!Objects.equals(pattern, that.pattern)) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return pattern != null ? pattern.hashCode() : 0;
      }
   }

   private static class ClassNameKey {
      private String className;

      ClassNameKey(String className) {
         this.className = className;
      }

      public String getClassName() {
         return className;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         ClassNameKey that = (ClassNameKey) o;

         if (!Objects.equals(className, that.className)) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return className != null ? className.hashCode() : 0;
      }
   }
}

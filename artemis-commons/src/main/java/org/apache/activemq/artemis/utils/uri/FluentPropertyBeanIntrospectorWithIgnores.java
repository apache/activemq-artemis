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
package org.apache.activemq.artemis.utils.uri;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Locale;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.apache.commons.beanutils.FluentPropertyBeanIntrospector;
import org.apache.commons.beanutils.IntrospectionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

public class FluentPropertyBeanIntrospectorWithIgnores extends FluentPropertyBeanIntrospector {

   static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static ConcurrentHashSet<Pair<String, String>> ignores = new ConcurrentHashSet<>();

   public static void addIgnore(String className, String propertyName) {
      logger.trace("Adding ignore on {}/{}", className, propertyName);
      ignores.add(new Pair<>(className, propertyName));
   }

   public static boolean isIgnored(String className, String propertyName) {
      return ignores.contains(new Pair<>(className, propertyName));
   }

   @Override
   public void introspect(IntrospectionContext icontext) throws IntrospectionException {
      for (Method m : icontext.getTargetClass().getMethods()) {
         final String methodName = m.getName();
         if (methodName.startsWith(getWriteMethodPrefix()) && !methodName.equals(getWriteMethodPrefix())) {
            if (isIgnored(icontext.getTargetClass().getName(), methodName)) {
               logger.trace("{} Ignored for {}", methodName, icontext.getTargetClass().getName());
               continue;
            }

            final String propertyName = propertyName(methodName);
            introspect(icontext, m, propertyName);
            final String defaultPropertyName = defaultPropertyName(methodName);
            if (!defaultPropertyName.equals(propertyName)) {
               introspect(icontext, m, defaultPropertyName);
            }
         }
      }
   }

   private void introspect(IntrospectionContext icontext, Method writeMethod, String propertyName) {
      PropertyDescriptor pd = icontext.getPropertyDescriptor(propertyName);

      Method readMethod = null;
      if (pd != null) {
         readMethod = pd.getReadMethod();
      }
      try {
         PropertyDescriptor withFluentWrite = createFluentPropertyDescriptor(readMethod, writeMethod, propertyName);
         icontext.addPropertyDescriptor(withFluentWrite);
      } catch (IntrospectionException e) {
         logger.trace("error on add fluent descriptor for property named {}", propertyName, e);
      }
   }

   private PropertyDescriptor createFluentPropertyDescriptor(Method readMethod, Method writeMethod, String propertyName) throws IntrospectionException {
      return new PropertyDescriptor(propertyName, readMethod, writeMethod);
   }

   private String propertyName(final String methodName) {
      String propName = methodName.substring(getWriteMethodPrefix().length());
      return (propName.length() > 1) ? Character.toLowerCase(propName.charAt(0)) +
         propName.substring(1) : propName.toLowerCase(Locale.ENGLISH);
   }

   private String defaultPropertyName(final String methodName) {
      final String propertyName = methodName.substring(
         getWriteMethodPrefix().length());
      return (propertyName.length() > 1) ? Introspector.decapitalize(propertyName) : propertyName
         .toLowerCase(Locale.ENGLISH);
   }
}

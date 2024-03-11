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
         if (m.getName().startsWith(getWriteMethodPrefix())) {
            if (isIgnored(icontext.getTargetClass().getName(), m.getName())) {
               logger.trace("{} Ignored for {}", m.getName(), icontext.getTargetClass().getName());
               continue;
            }

            introspect(icontext, m, propertyName(m));
            introspect(icontext, m, defaultPropertyName(m));
         }
      }
   }

   private void introspect(IntrospectionContext icontext, Method writeMethod, String propertyName) {
      PropertyDescriptor pd = icontext.getPropertyDescriptor(propertyName);

      try {
         if (pd == null) {
            icontext.addPropertyDescriptor(createFluentPropertyDescritor(writeMethod, propertyName));
         } else if (pd.getWriteMethod() == null) {
            pd.setWriteMethod(writeMethod);
         }
      } catch (IntrospectionException e) {
         logger.trace("error for property named {}", propertyName, e);
      }
   }

   private PropertyDescriptor createFluentPropertyDescritor(Method writeMethod, String propertyName) throws IntrospectionException {
      return new PropertyDescriptor(propertyName, null, writeMethod);
   }

   private String propertyName(final Method m) {
      String methodName = m.getName().substring(getWriteMethodPrefix().length());
      return (methodName.length() > 1) ? Character.toLowerCase(methodName.charAt(0)) +
         methodName.substring(1) : methodName.toLowerCase(Locale.ENGLISH);
   }

   private String defaultPropertyName(final Method m) {
      final String methodName = m.getName().substring(
         getWriteMethodPrefix().length());
      return (methodName.length() > 1) ? Introspector.decapitalize(methodName) : methodName
         .toLowerCase(Locale.ENGLISH);
   }
}

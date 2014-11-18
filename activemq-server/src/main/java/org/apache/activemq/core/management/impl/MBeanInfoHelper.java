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
package org.apache.activemq.core.management.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;

import org.apache.activemq.api.core.management.Operation;
import org.apache.activemq.api.core.management.Parameter;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class MBeanInfoHelper
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static MBeanOperationInfo[] getMBeanOperationsInfo(final Class mbeanInterface)
   {
      List<MBeanOperationInfo> operations = new ArrayList<MBeanOperationInfo>();

      for (Method method : mbeanInterface.getMethods())
      {
         if (!MBeanInfoHelper.isGetterMethod(method) && !MBeanInfoHelper.isSetterMethod(method) &&
             !MBeanInfoHelper.isIsBooleanMethod(method))
         {
            operations.add(MBeanInfoHelper.getOperationInfo(method));
         }
      }

      return operations.toArray(new MBeanOperationInfo[0]);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static boolean isGetterMethod(final Method method)
   {
      if (!method.getName().equals("get") && method.getName().startsWith("get") &&
          method.getParameterTypes().length == 0 &&
          !method.getReturnType().equals(void.class))
      {
         return true;
      }

      return false;
   }

   private static boolean isSetterMethod(final Method method)
   {
      if (!method.getName().equals("set") && method.getName().startsWith("set") &&
          method.getParameterTypes().length == 1 &&
          method.getReturnType().equals(void.class))
      {
         return true;
      }
      else
      {
         return false;
      }
   }

   private static boolean isIsBooleanMethod(final Method method)
   {
      if (!method.getName().equals("is") && method.getName().startsWith("is") &&
          method.getParameterTypes().length == 0 &&
          method.getReturnType().equals(boolean.class))
      {
         return true;
      }
      else
      {
         return false;
      }
   }

   private static MBeanOperationInfo getOperationInfo(final Method operation)
   {
      MBeanOperationInfo info = null;
      Class<?> returnType = operation.getReturnType();

      MBeanParameterInfo[] paramsInfo = MBeanInfoHelper.getParametersInfo(operation.getParameterAnnotations(),
                                                                          operation.getParameterTypes());

      String description = operation.getName();
      int impact = MBeanOperationInfo.UNKNOWN;

      if (operation.getAnnotation(Operation.class) != null)
      {
         description = operation.getAnnotation(Operation.class).desc();
         impact = operation.getAnnotation(Operation.class).impact();
      }
      info = new MBeanOperationInfo(operation.getName(), description, paramsInfo, returnType.getName(), impact);

      return info;
   }

   private static MBeanParameterInfo[] getParametersInfo(final Annotation[][] params, final Class<?>[] paramTypes)
   {
      MBeanParameterInfo[] paramsInfo = new MBeanParameterInfo[params.length];

      for (int i = 0; i < params.length; i++)
      {
         MBeanParameterInfo paramInfo = null;
         String type = paramTypes[i].getName();
         for (Annotation anno : params[i])
         {
            if (Parameter.class.isInstance(anno))
            {
               String name = Parameter.class.cast(anno).name();
               String description = Parameter.class.cast(anno).desc();
               paramInfo = new MBeanParameterInfo(name, type, description);
            }
         }

         if (paramInfo == null)
         {
            paramInfo = new MBeanParameterInfo("p " + (i + 1), type, "parameter " + (i + 1));
         }

         paramsInfo[i] = paramInfo;
      }

      return paramsInfo;
   }

   // Inner classes -------------------------------------------------
}

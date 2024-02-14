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
package org.apache.activemq.artemis.core.server.management;

import org.apache.activemq.artemis.logs.AuditLogger;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.MBeanServerBuilder;
import javax.management.MBeanServerDelegate;

public class ArtemisMBeanServerBuilder extends MBeanServerBuilder {

   private static volatile InvocationHandler guard;

   public static void setGuard(InvocationHandler guardHandler) {
      guard = guardHandler;
   }

   public static ArtemisMBeanServerGuard getArtemisMBeanServerGuard() {
      return (ArtemisMBeanServerGuard) guard;
   }

   @Override
   public MBeanServer newMBeanServer(String defaultDomain, MBeanServer outer, MBeanServerDelegate delegate) {
      InvocationHandler handler = new MBeanInvocationHandler(super.newMBeanServer(defaultDomain, outer, delegate));
      return (MBeanServer) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{MBeanServer.class}, handler);
   }

   private static final class MBeanInvocationHandler implements InvocationHandler {

      private final MBeanServer wrapped;
      private final List<String> guarded = Collections.unmodifiableList(Arrays.asList("invoke", "getAttribute", "getAttributes", "setAttribute", "setAttributes", "queryMBeans"));

      MBeanInvocationHandler(MBeanServer mbeanServer) {
         wrapped = mbeanServer;
      }

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
         //if this is invoked via jolokia the address will be set by the filter
         //if not we can deduct it from RMI or it must be internal
         if (AuditLogger.isAnyLoggingEnabled() && AuditLogger.getRemoteAddress() == null) {
            String name = Thread.currentThread().getName();
            String url = "internal";
            if (name.startsWith("RMI TCP Connection")) {
               url = name.substring(name.indexOf('-') + 1);
            }
            AuditLogger.setRemoteAddress(url);
         }
         if (guarded.contains(method.getName())) {
            if (ArtemisMBeanServerBuilder.guard == null) {
               throw new IllegalStateException("ArtemisMBeanServerBuilder not initialized");
            }
            guard.invoke(proxy, method, args);
         }
         if (method.getName().equals("equals")
               && method.getParameterTypes().length == 1
               && method.getParameterTypes()[0] == Object.class) {
            Object target = args[0];
            if (target != null && Proxy.isProxyClass(target.getClass())) {
               InvocationHandler handler = Proxy.getInvocationHandler(target);
               if (handler instanceof MBeanInvocationHandler) {
                  args[0] = ((MBeanInvocationHandler) handler).wrapped;
               }
            }
         } else if (method.getName().equals("finalize") && method.getParameterTypes().length == 0) {
            // special case finalize, don't route through to delegate because that will get its own call
            return null;
         }
         try {
            return method.invoke(wrapped, args);
         } catch (InvocationTargetException ite) {
            throw ite.getCause();
         }
      }
   }
}

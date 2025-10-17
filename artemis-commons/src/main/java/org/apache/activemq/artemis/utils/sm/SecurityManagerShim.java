/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.utils.sm;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;

@SuppressWarnings("removal")
public class SecurityManagerShim {

   // TODO: Move to Subject specific class?
   public static Subject currentSubject() {
      AccessControlContext accessControlContext = AccessController.getContext();
      if (accessControlContext != null) {
         return Subject.getSubject(accessControlContext);
      }
      return null;
   }

   // TODO: Move to Subject specific class?
   public static <T> T callAs(final Subject subject, final Callable<T> callable) throws CompletionException {
      Objects.requireNonNull(callable);

      try {
         final PrivilegedExceptionAction<T> pa = () -> callable.call();

         return Subject.doAs(subject, pa);
      } catch (PrivilegedActionException e) {
         throw new CompletionException(e.getCause());
      } catch (Exception e) {
         throw new CompletionException(e);
      }
   }

   public static boolean isSecurityManagerEnabled() {
      return System.getSecurityManager() != null;
   }

   public static Object getAccessControlContext() {
      return AccessController.getContext();
   }

   public static<T> T doPrivileged(PrivilegedAction<T> action) {
      Objects.requireNonNull(action);

      return AccessController.doPrivileged(action);
   }

   public static<T> T doPrivileged(final PrivilegedAction<T> action, final Object context) {
      Objects.requireNonNull(action);
      Objects.requireNonNull(context, "require AccessControlContext argument");//TODO: check this is true

      final AccessControlContext acc = AccessControlContext.class.cast(context);

      return AccessController.doPrivileged(action, acc);
   }

   public static <T> T doPrivileged(final PrivilegedExceptionAction<T> exceptionAction) throws PrivilegedActionException {
      Objects.requireNonNull(exceptionAction);

      return AccessController.doPrivileged(exceptionAction);
   }

}
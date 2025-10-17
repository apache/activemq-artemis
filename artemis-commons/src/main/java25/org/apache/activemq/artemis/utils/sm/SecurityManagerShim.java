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

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;

public class SecurityManagerShim {

   // TODO: Move to Subject specific class?
   public static Subject currentSubject() {
      return Subject.current();
   }

   // TODO: Move to Subject specific class?
   public static <T> T callAs(final Subject subject, final Callable<T> callable) throws CompletionException {
      Objects.requireNonNull(callable);

      return Subject.callAs(subject, callable);
   }

   public static boolean isSecurityManagerEnabled() {
      return false; // It was removed in Java 24+
   }

   public static Object getAccessControlContext() {
      // AccessControlContext is now only useful with a SecurityManager,
      // which can never be used in Java 24+. Return null so calling
      // code can determine there is nothing to be do re: SecurityManager.
      return null;
   }

   public static<T> T doPrivileged(PrivilegedAction<T> action) {
      Objects.requireNonNull(action);

      return action.run();
   }

   public static<T> T doPrivileged(final PrivilegedAction<T> action, final Object context) {
      Objects.requireNonNull(action);

      return action.run();
   }

   public static <T> T doPrivileged(final PrivilegedExceptionAction<T> exceptionAction) throws PrivilegedActionException {
      Objects.requireNonNull(exceptionAction);

      try {
         return exceptionAction.run();
      } catch (Exception e) {
         throw new PrivilegedActionException(e);
      }
   }

}
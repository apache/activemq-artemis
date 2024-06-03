/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.extensions;

import javax.security.auth.Subject;

import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

public class SubjectDotDoAsExtension implements InvocationInterceptor {

   final Subject subject;

   public SubjectDotDoAsExtension(Subject subject) {
      this.subject = subject;
   }

   @Override
   public void interceptTestMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) throws Throwable {
      Exception e = Subject.doAs(subject, (PrivilegedExceptionAction<Exception>) () -> {
         try {
            invocation.proceed();
         } catch (Throwable e1) {
            return new Exception(e1);
         }
         return null;
      });
      if (e != null) {
         throw e;
      }
   }
}

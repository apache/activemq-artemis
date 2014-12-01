/**
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
package org.apache.activemq.integration.jboss.security;

import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;

import javax.security.auth.Subject;

import org.jboss.security.SecurityAssociation;

/** A collection of privileged actions for this package
 * @author Scott.Stark@jboss.org
 * @author <a href="mailto:alex@jboss.org">Alexey Loubyansky</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:anil.saldhana@jboss.com">anil saldhana</a>
 * @version $Revison: 1.0$
 */

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Oct 21, 2009
 */
public class AS4SecurityActions
{
   interface PrincipalInfoAction
   {
      PrincipalInfoAction PRIVILEGED = new PrincipalInfoAction()
      {
         public void push(final Principal principal, final Object credential, final Subject subject)
         {
            AccessController.doPrivileged(new PrivilegedAction()
            {
               public Object run()
               {
                  SecurityAssociation.pushSubjectContext(subject, principal, credential);
                  return null;
               }
            });
         }

         public void dup()
         {
            AccessController.doPrivileged(new PrivilegedAction()
            {
               public Object run()
               {
                  SecurityAssociation.dupSubjectContext();
                  return null;
               }
            });
         }

         public void pop()
         {
            AccessController.doPrivileged(new PrivilegedAction()
            {
               public Object run()
               {
                  SecurityAssociation.popSubjectContext();
                  return null;
               }
            });
         }
      };

      PrincipalInfoAction NON_PRIVILEGED = new PrincipalInfoAction()
      {
         public void push(final Principal principal, final Object credential, final Subject subject)
         {
            SecurityAssociation.pushSubjectContext(subject, principal, credential);
         }

         public void dup()
         {
            SecurityAssociation.dupSubjectContext();
         }

         public void pop()
         {
            SecurityAssociation.popSubjectContext();
         }
      };

      void push(Principal principal, Object credential, Subject subject);

      void dup();

      void pop();
   }

   static void pushSubjectContext(final Principal principal, final Object credential, final Subject subject)
   {
      if (System.getSecurityManager() == null)
      {
         PrincipalInfoAction.NON_PRIVILEGED.push(principal, credential, subject);
      }
      else
      {
         PrincipalInfoAction.PRIVILEGED.push(principal, credential, subject);
      }
   }

   static void popSubjectContext()
   {
      if (System.getSecurityManager() == null)
      {
         PrincipalInfoAction.NON_PRIVILEGED.pop();
      }
      else
      {
         PrincipalInfoAction.PRIVILEGED.pop();
      }
   }
}

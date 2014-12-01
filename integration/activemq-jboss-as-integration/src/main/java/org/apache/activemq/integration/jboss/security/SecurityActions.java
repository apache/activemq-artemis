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

import org.apache.activemq.integration.jboss.ActiveMQJBossLogger;
import org.jboss.security.SecurityContext;
import org.jboss.security.SecurityContextAssociation;
import org.jboss.security.SecurityContextFactory;

/** A collection of privileged actions for this package
 * @author Scott.Stark@jboss.org
 * @author <a href="mailto:alex@jboss.org">Alexey Loubyansky</a>
 * @author <a her="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revison: 1.0$
 */
class SecurityActions
{
   interface PrincipalInfoAction
   {
      PrincipalInfoAction PRIVILEGED = new PrincipalInfoAction()
      {
         public void push(final Principal principal,
                          final Object credential,
                          final Subject subject,
                          final String securityDomain)
         {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
               public Object run()
               {

                  try
                  {
                     ActiveMQJBossLogger.LOGGER.settingSecuritySubject(subject);
                     // SecurityAssociation.pushSubjectContext(subject, principal, credential);
                     SecurityContext sc = SecurityContextAssociation.getSecurityContext();
                     if (sc == null)
                     {
                        try
                        {
                           sc = SecurityContextFactory.createSecurityContext(principal,
                                                                             credential,
                                                                             subject,
                                                                             securityDomain);
                        }
                        catch (Exception e)
                        {
                           throw new RuntimeException(e);
                        }
                     }
                     else
                     {
                        sc.getUtil().createSubjectInfo(principal, credential, subject);
                     }

                     SecurityContextAssociation.setSecurityContext(sc);
                  }
                  catch (Throwable t)
                  {
                     ActiveMQJBossLogger.LOGGER.errorSettingSecurityContext(t);
                  }

                  return null;
               }
            });
         }

         public void pop()
         {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
               public Object run()
               {
                  // SecurityAssociation.popSubjectContext();
                  SecurityContextAssociation.clearSecurityContext();
                  return null;
               }
            });
         }
      };

      PrincipalInfoAction NON_PRIVILEGED = new PrincipalInfoAction()
      {
         public void push(final Principal principal,
                          final Object credential,
                          final Subject subject,
                          final String securityDomain)
         {
            // SecurityAssociation.pushSubjectContext(subject, principal, credential);
            SecurityContext sc = SecurityContextAssociation.getSecurityContext();
            if (sc == null)
            {
               try
               {
                  sc = SecurityContextFactory.createSecurityContext(principal, credential, subject, securityDomain);
               }
               catch (Exception e)
               {
                  throw new RuntimeException(e);
               }
            }
            else
            {
               sc.getUtil().createSubjectInfo(principal, credential, subject);
            }
            SecurityContextAssociation.setSecurityContext(sc);
         }

         public void pop()
         {
            // SecurityAssociation.popSubjectContext();
            SecurityContextAssociation.clearSecurityContext();
         }
      };

      void push(Principal principal, Object credential, Subject subject, String securityDomain);

      void pop();
   }

   static void pushSubjectContext(final Principal principal,
                                  final Object credential,
                                  final Subject subject,
                                  final String securityDomainName)
   {
      if (System.getSecurityManager() == null)
      {
         PrincipalInfoAction.NON_PRIVILEGED.push(principal, credential, subject, securityDomainName);
      }
      else
      {
         PrincipalInfoAction.PRIVILEGED.push(principal, credential, subject, securityDomainName);
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

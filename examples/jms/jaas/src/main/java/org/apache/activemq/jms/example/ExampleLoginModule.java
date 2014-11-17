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
package org.apache.activemq.jms.example;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.activemq.spi.core.security.JAASSecurityManager;

/**
 * A ExampleLoginModule
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ExampleLoginModule implements LoginModule
{

   private Map<String, ?> options;

   private Subject subject;

   public ExampleLoginModule()
   {
   }

   public boolean abort() throws LoginException
   {
      return true;
   }

   public boolean commit() throws LoginException
   {
      return true;
   }

   public void initialize(final Subject subject,
                          final CallbackHandler callbackHandler,
                          final Map<String, ?> sharedState,
                          final Map<String, ?> options)
   {
      this.subject = subject;
      // the credentials are passed directly to the
      // login module through the options user, pass, role
      this.options = options;
   }

   public boolean login() throws LoginException
   {
      Iterator<char[]> iterator = subject.getPrivateCredentials(char[].class).iterator();
      char[] passwordChars = iterator.next();
      String password = new String(passwordChars);
      Iterator<Principal> iterator2 = subject.getPrincipals().iterator();
      System.out.println("subject = " + subject);
      String user = iterator2.next().getName();

      boolean authenticated = user.equals(options.get("user")) && password.equals(options.get("pass"));

      if (authenticated)
      {
         Group roles = new SimpleGroup("Roles");
         roles.addMember(new JAASSecurityManager.SimplePrincipal((String)options.get("role")));
         subject.getPrincipals().add(roles);
      }
      System.out.format("JAAS authentication >>> user=%s, password=%s\n", user, password);
      System.out.println("authenticated = " + authenticated);
      return authenticated;

   }

   public Subject getSubject()
   {
      return subject;
   }

   public boolean logout() throws LoginException
   {
      return true;
   }

   public class SimpleGroup implements Group
   {
      private final String name;

      private final Set<Principal> members = new HashSet<Principal>();

      public SimpleGroup(final String name)
      {
         this.name = name;
      }

      public boolean addMember(final Principal principal)
      {
         return members.add(principal);
      }

      public boolean isMember(final Principal principal)
      {
         return members.contains(principal);
      }

      public Enumeration<? extends Principal> members()
      {
         return Collections.enumeration(members);
      }

      public boolean removeMember(final Principal principal)
      {
         return members.remove(principal);
      }

      public String getName()
      {
         return name;
      }
   }

}

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
package org.apache.activemq.spi.core.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.core.security.CheckType;
import org.apache.activemq.core.security.Role;
import org.apache.activemq.core.server.ActiveMQMessageBundle;

/**
 * A basic implementation of the ActiveMQSecurityManager. This can be used within an appserver and be deployed by
 * BasicUserCredentialsDeployer or used standalone or embedded.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ActiveMQSecurityManagerImpl implements ActiveMQSecurityManager
{

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   /**
    * the current valid users
    */
   private final Map<String, User> users = new HashMap<String, User>();

   private String defaultUser = null;

   /**
    * the roles for the users
    */
   private final Map<String, List<String>> roles = new HashMap<String, List<String>>();

   // ActiveMQComponent implementation ------------------------------------------

   public void start()
   {
   }

   public void stop()
   {
      users.clear();

      roles.clear();

      defaultUser = null;
   }

   public boolean isStarted()
   {
      return true;
   }

   // Public ---------------------------------------------------------------------

   public boolean validateUser(final String user, final String password)
   {
      if (user == null && defaultUser == null)
      {
         return false;
      }

      User theUser = users.get(user == null ? defaultUser : user);

      boolean ok = theUser != null && theUser.isValid(user == null ? defaultUser : user, password == null ? defaultUser
                                                                                                         : password);
      return ok;
   }

   public boolean validateUserAndRole(final String user,
                                      final String password,
                                      final Set<Role> roles,
                                      final CheckType checkType)
   {
      if (validateUser(user, password))
      {
         List<String> availableRoles = this.roles.get(user == null ? defaultUser : user);

         if (availableRoles == null)
         {
            return false;
         }

         for (String availableRole : availableRoles)
         {
            if (roles != null)
            {
               for (Role role : roles)
               {
                  if (role.getName().equals(availableRole) && checkType.hasRole(role))
                  {
                     return true;
                  }
               }
            }
         }
      }

      return false;
   }

   public void addUser(final String user, final String password)
   {
      if (user == null)
      {
         throw ActiveMQMessageBundle.BUNDLE.nullUser();
      }
      if (password == null)
      {
         throw ActiveMQMessageBundle.BUNDLE.nullPassword();
      }
      users.put(user, new User(user, password));
   }

   public void removeUser(final String user)
   {
      users.remove(user);
      roles.remove(user);
   }

   public void addRole(final String user, final String role)
   {
      if (roles.get(user) == null)
      {
         roles.put(user, new ArrayList<String>());
      }
      roles.get(user).add(role);
   }

   public void removeRole(final String user, final String role)
   {
      if (roles.get(user) == null)
      {
         return;
      }
      roles.get(user).remove(role);
   }

   /*
   * set the default user for null users
   */
   public void setDefaultUser(final String username)
   {
      defaultUser = username;
   }

   static class User
   {
      final String user;

      final String password;

      User(final String user, final String password)
      {
         this.user = user;
         this.password = password;
      }

      @Override
      public boolean equals(final Object o)
      {
         if (this == o)
         {
            return true;
         }
         if (o == null || getClass() != o.getClass())
         {
            return false;
         }

         User user1 = (User)o;

         if (!user.equals(user1.user))
         {
            return false;
         }

         return true;
      }

      @Override
      public int hashCode()
      {
         return user.hashCode();
      }

      public boolean isValid(final String user, final String password)
      {
         if (user == null)
         {
            return false;
         }
         return this.user.equals(user) && this.password.equals(password);
      }
   }
}

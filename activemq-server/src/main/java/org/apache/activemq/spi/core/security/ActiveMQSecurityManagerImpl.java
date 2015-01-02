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
package org.apache.activemq.spi.core.security;

import java.util.List;
import java.util.Set;

import org.apache.activemq.core.config.impl.SecurityConfiguration;
import org.apache.activemq.core.security.CheckType;
import org.apache.activemq.core.security.Role;
import org.apache.activemq.core.security.User;

/**
 * A basic implementation of the ActiveMQSecurityManager. This can be used within an appserver and be deployed by
 * BasicUserCredentialsDeployer or used standalone or embedded.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ActiveMQSecurityManagerImpl implements ActiveMQSecurityManager
{
   private final SecurityConfiguration configuration;

   public ActiveMQSecurityManagerImpl()
   {
      configuration = new SecurityConfiguration();
   }

   public ActiveMQSecurityManagerImpl(SecurityConfiguration configuration)
   {
      this.configuration = configuration;
   }

   // Public ---------------------------------------------------------------------

   public boolean validateUser(final String user, final String password)
   {
      if (user == null && configuration.getDefaultUser() == null)
      {
         return false;
      }

      String defaultUser = configuration.getDefaultUser();
      User theUser = configuration.getUser(user == null ? defaultUser : user);

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
         String defaultUser = configuration.getDefaultUser();
         List<String> availableRoles = configuration.getRole(user == null ? defaultUser : user);

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

   public SecurityConfiguration getConfiguration()
   {
      return configuration;
   }
}

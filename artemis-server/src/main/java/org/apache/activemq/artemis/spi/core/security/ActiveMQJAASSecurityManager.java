/*
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
package org.apache.activemq.artemis.spi.core.security;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.spi.core.security.jaas.JaasCredentialCallbackHandler;
import org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal;

/**
 * This implementation delegates to the JAAS security interfaces.
 *
 * The {@link Subject} returned by the login context is expecting to have a set of {@link RolePrincipal} for each
 * role of the user.
 */
public class ActiveMQJAASSecurityManager implements ActiveMQSecurityManager {

   private final boolean trace = ActiveMQServerLogger.LOGGER.isTraceEnabled();

   private String configurationName;

   public boolean validateUser(final String user, final String password) {
      try {
         getAuthenticatedSubject(user, password);
         return true;
      }
      catch (LoginException e) {
         ActiveMQServerLogger.LOGGER.debug("Couldn't validate user: " + user, e);
         return false;
      }
   }

   public boolean validateUserAndRole(final String user,
                                      final String password,
                                      final Set<Role> roles,
                                      final CheckType checkType) {
      Subject localSubject;
      try {
         localSubject = getAuthenticatedSubject(user, password);
      }
      catch (LoginException e) {
         ActiveMQServerLogger.LOGGER.debug("Couldn't validate user: " + user, e);
         return false;
      }

      boolean authorized = false;

      if (localSubject != null) {
         Set<RolePrincipal> rolesWithPermission = getPrincipalsInRole(checkType, roles);

         // Check the caller's roles
         Set<RolePrincipal> rolesForSubject = localSubject.getPrincipals(RolePrincipal.class);
         if (rolesForSubject.size() > 0 && rolesWithPermission.size() > 0) {
            Iterator<RolePrincipal> rolesForSubjectIter = rolesForSubject.iterator();
            while (!authorized && rolesForSubjectIter.hasNext()) {
               Iterator<RolePrincipal> rolesWithPermissionIter = rolesWithPermission.iterator();
               while (!authorized && rolesWithPermissionIter.hasNext()) {
                  Principal role = rolesWithPermissionIter.next();
                  authorized = rolesForSubjectIter.next().equals(role);
               }
            }
         }

         if (trace) {
            ActiveMQServerLogger.LOGGER.trace("user " + user + (authorized ? " is " : " is NOT ") + "authorized");
         }
      }

      return authorized;
   }

   private Subject getAuthenticatedSubject(final String user, final String password) throws LoginException {
      LoginContext lc = new LoginContext(configurationName, new JaasCredentialCallbackHandler(user, password));
      lc.login();
      return lc.getSubject();
   }

   private Set<RolePrincipal> getPrincipalsInRole(final CheckType checkType, final Set<Role> roles) {
      Set<RolePrincipal> principals = new HashSet<>();
      for (Role role : roles) {
         if (checkType.hasRole(role)) {
            principals.add(new RolePrincipal(role.getName()));
         }
      }
      return principals;
   }

   public void setConfigurationName(final String configurationName) {
      this.configurationName = configurationName;
   }
}

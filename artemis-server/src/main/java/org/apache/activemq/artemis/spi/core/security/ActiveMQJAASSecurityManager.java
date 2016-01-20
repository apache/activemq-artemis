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
import javax.security.cert.X509Certificate;
import java.security.Principal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.security.jaas.JaasCallbackHandler;
import org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal;
import org.apache.activemq.artemis.utils.CertificateUtil;

/**
 * This implementation delegates to the JAAS security interfaces.
 *
 * The {@link Subject} returned by the login context is expecting to have a set of {@link RolePrincipal} for each
 * role of the user.
 */
public class ActiveMQJAASSecurityManager implements ActiveMQSecurityManager2 {

   private final boolean trace = ActiveMQServerLogger.LOGGER.isTraceEnabled();

   private String configurationName;
   private SecurityConfiguration configuration;

   public ActiveMQJAASSecurityManager() {
   }

   public ActiveMQJAASSecurityManager(String configurationName) {
      this.configurationName = configurationName;
   }

   public ActiveMQJAASSecurityManager(String configurationName, SecurityConfiguration configuration) {
      this.configurationName = configurationName;
      this.configuration = configuration;
   }

   @Override
   public boolean validateUser(String user, String password) {
      return validateUser(user, password, null);
   }

   @Override
   public boolean validateUser(final String user, final String password, X509Certificate[] certificates) {
      try {
         getAuthenticatedSubject(user, password, certificates);
         return true;
      }
      catch (LoginException e) {
         ActiveMQServerLogger.LOGGER.debug("Couldn't validate user", e);
         return false;
      }
   }

   @Override
   public boolean validateUserAndRole(String user, String password, Set<Role> roles, CheckType checkType) {
      throw new UnsupportedOperationException("Invoke validateUserAndRole(String, String, Set<Role>, CheckType, String, RemotingConnection) instead");
   }

   @Override
   public boolean validateUserAndRole(final String user,
                                      final String password,
                                      final Set<Role> roles,
                                      final CheckType checkType,
                                      final String address,
                                      final RemotingConnection connection) {
      X509Certificate[] certificates = null;
      if (connection != null && connection.getTransportConnection() instanceof NettyConnection) {
         certificates = CertificateUtil.getCertsFromChannel(((NettyConnection) connection.getTransportConnection()).getChannel());
      }
      Subject localSubject;
      try {
         localSubject = getAuthenticatedSubject(user, password, certificates);
      }
      catch (LoginException e) {
         ActiveMQServerLogger.LOGGER.debug("Couldn't validate user", e);
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
               Principal subjectRole = rolesForSubjectIter.next();
               while (!authorized && rolesWithPermissionIter.hasNext()) {
                  Principal roleWithPermission = rolesWithPermissionIter.next();
                  authorized = subjectRole.equals(roleWithPermission);
               }
            }
         }

         if (trace) {
            ActiveMQServerLogger.LOGGER.trace("user " + (authorized ? " is " : " is NOT ") + "authorized");
         }
      }

      return authorized;
   }

   private Subject getAuthenticatedSubject(final String user, final String password, final X509Certificate[] certificates) throws LoginException {
      LoginContext lc = new LoginContext(configurationName, null, new JaasCallbackHandler(user, password, certificates), configuration);
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

   public void setConfiguration(SecurityConfiguration configuration) {
      this.configuration = configuration;
   }

   public SecurityConfiguration getConfiguration() {
      if (configuration == null) {
         configuration = new SecurityConfiguration();
      }

      return configuration;
   }
}

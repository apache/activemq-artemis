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
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.security.jaas.JaasCallbackHandler;
import org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal;
import org.apache.activemq.artemis.spi.core.security.jaas.UserPrincipal;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.core.remoting.CertificateUtil.getCertsFromConnection;

/**
 * This implementation delegates to the JAAS security interfaces.
 *
 * The {@link Subject} returned by the login context is expecting to have a set of {@link RolePrincipal} for each
 * role of the user.
 */
public class ActiveMQJAASSecurityManager implements ActiveMQSecurityManager3 {

   private static final Logger logger = Logger.getLogger(ActiveMQJAASSecurityManager.class);

   private static final String WILDCARD = "*";

   private String configurationName;
   private String certificateConfigurationName;
   private SecurityConfiguration configuration;
   private SecurityConfiguration certificateConfiguration;
   private String rolePrincipalClass = "org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal";

   public ActiveMQJAASSecurityManager() {
   }

   public ActiveMQJAASSecurityManager(String configurationName) {
      this.configurationName = configurationName;
   }

   public ActiveMQJAASSecurityManager(String configurationName, String certificateConfigurationName) {
      this.configurationName = configurationName;
      this.certificateConfigurationName = certificateConfigurationName;
   }

   public ActiveMQJAASSecurityManager(String configurationName, SecurityConfiguration configuration) {
      this.configurationName = configurationName;
      this.configuration = configuration;
   }

   public ActiveMQJAASSecurityManager(String configurationName,
                                      String certificateConfigurationName,
                                      SecurityConfiguration configuration,
                                      SecurityConfiguration certificateConfiguration) {
      this.configurationName = configurationName;
      this.configuration = configuration;
      this.certificateConfigurationName = certificateConfigurationName;
      this.certificateConfiguration = certificateConfiguration;
   }

   @Override
   public boolean validateUser(String user, String password) {
      throw new UnsupportedOperationException("Invoke validateUser(String, String, X509Certificate[]) instead");
   }

   @Override
   public String validateUser(final String user, final String password, RemotingConnection remotingConnection) {
      try {
         return getUserFromSubject(getAuthenticatedSubject(user, password, remotingConnection));
      } catch (LoginException e) {
         if (logger.isDebugEnabled()) {
            logger.debug("Couldn't validate user", e);
         }
         return null;
      }
   }

   public String getUserFromSubject(Subject subject) {
      String validatedUser = "";
      Set<UserPrincipal> users = subject.getPrincipals(UserPrincipal.class);

      // should only ever be 1 UserPrincipal
      for (UserPrincipal userPrincipal : users) {
         validatedUser = userPrincipal.getName();
      }
      return validatedUser;
   }

   @Override
   public boolean validateUserAndRole(String user, String password, Set<Role> roles, CheckType checkType) {
      throw new UnsupportedOperationException("Invoke validateUserAndRole(String, String, Set<Role>, CheckType, String, RemotingConnection) instead");
   }

   @Override
   public String validateUserAndRole(final String user,
                                     final String password,
                                     final Set<Role> roles,
                                     final CheckType checkType,
                                     final String address,
                                     final RemotingConnection remotingConnection) {
      Subject localSubject;
      try {
         localSubject = getAuthenticatedSubject(user, password, remotingConnection);
      } catch (LoginException e) {
         if (logger.isDebugEnabled()) {
            logger.debug("Couldn't validate user", e);
         }
         return null;
      }

      boolean authorized = false;

      if (localSubject != null) {
         Set<RolePrincipal> rolesWithPermission = getPrincipalsInRole(checkType, roles);

         // Check the caller's roles
         Set<Principal> rolesForSubject = new HashSet<>();
         try {
            rolesForSubject.addAll(localSubject.getPrincipals(Class.forName(rolePrincipalClass).asSubclass(Principal.class)));
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.failedToFindRolesForTheSubject(e);
         }
         if (rolesForSubject.size() > 0 && rolesWithPermission.size() > 0) {
            Iterator<Principal> rolesForSubjectIter = rolesForSubject.iterator();
            while (!authorized && rolesForSubjectIter.hasNext()) {
               Iterator<RolePrincipal> rolesWithPermissionIter = rolesWithPermission.iterator();
               Principal subjectRole = rolesForSubjectIter.next();
               while (!authorized && rolesWithPermissionIter.hasNext()) {
                  Principal roleWithPermission = rolesWithPermissionIter.next();
                  authorized = subjectRole.equals(roleWithPermission);
               }
            }
         }

         if (logger.isTraceEnabled()) {
            logger.trace("user " + (authorized ? " is " : " is NOT ") + "authorized");
         }
      }

      if (authorized) {
         return getUserFromSubject(localSubject);
      } else {
         return null;
      }
   }

   private Subject getAuthenticatedSubject(final String user,
                                           final String password,
                                           final RemotingConnection remotingConnection) throws LoginException {
      LoginContext lc;
      ClassLoader currentLoader = Thread.currentThread().getContextClassLoader();
      ClassLoader thisLoader = this.getClass().getClassLoader();
      try {
         if (thisLoader != currentLoader) {
            Thread.currentThread().setContextClassLoader(thisLoader);
         }
         if (certificateConfigurationName != null && certificateConfigurationName.length() > 0 && getCertsFromConnection(remotingConnection) != null) {
            lc = new LoginContext(certificateConfigurationName, null, new JaasCallbackHandler(user, password, remotingConnection), certificateConfiguration);
         } else {
            lc = new LoginContext(configurationName, null, new JaasCallbackHandler(user, password, remotingConnection), configuration);
         }
         lc.login();
         return lc.getSubject();
      } finally {
         if (thisLoader != currentLoader) {
            Thread.currentThread().setContextClassLoader(currentLoader);
         }
      }
   }

   private Set<RolePrincipal> getPrincipalsInRole(final CheckType checkType, final Set<Role> roles) {
      Set principals = new HashSet<>();
      for (Role role : roles) {
         if (checkType.hasRole(role)) {
            try {
               principals.add(createGroupPrincipal(role.getName(), rolePrincipalClass));
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.failedAddRolePrincipal(e);
            }
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

   public void setCertificateConfigurationName(final String certificateConfigurationName) {
      this.certificateConfigurationName = certificateConfigurationName;
   }

   public void setCertificateConfiguration(SecurityConfiguration certificateConfiguration) {
      this.certificateConfiguration = certificateConfiguration;
   }

   public SecurityConfiguration getConfiguration() {
      if (configuration == null) {
         configuration = new SecurityConfiguration();
      }

      return configuration;
   }

   public SecurityConfiguration getCertificateConfiguration() {
      if (certificateConfiguration == null) {
         certificateConfiguration = new SecurityConfiguration();
      }

      return certificateConfiguration;
   }

   public String getRolePrincipalClass() {
      return rolePrincipalClass;
   }

   public void setRolePrincipalClass(String rolePrincipalClass) {
      this.rolePrincipalClass = rolePrincipalClass;
   }

   public static Object createGroupPrincipal(String name, String groupClass) throws Exception {
      if (WILDCARD.equals(name)) {
         // simple match all group principal - match any name and class
         return new Principal() {
            @Override
            public String getName() {
               return WILDCARD;
            }

            @Override
            public boolean equals(Object other) {
               return true;
            }

            @Override
            public int hashCode() {
               return WILDCARD.hashCode();
            }
         };
      }
      Object[] param = new Object[]{name};

      Class<?> cls = Class.forName(groupClass);

      Constructor<?>[] constructors = cls.getConstructors();
      int i;
      Object instance;
      for (i = 0; i < constructors.length; i++) {
         Class<?>[] paramTypes = constructors[i].getParameterTypes();
         if (paramTypes.length != 0 && paramTypes[0].equals(String.class)) {
            break;
         }
      }
      if (i < constructors.length) {
         instance = constructors[i].newInstance(param);
      } else {
         instance = cls.newInstance();
         Method[] methods = cls.getMethods();
         i = 0;
         for (i = 0; i < methods.length; i++) {
            Class<?>[] paramTypes = methods[i].getParameterTypes();
            if (paramTypes.length != 0 && methods[i].getName().equals("setName") && paramTypes[0].equals(String.class)) {
               break;
            }
         }

         if (i < methods.length) {
            methods[i].invoke(instance, param);
         } else {
            throw new NoSuchMethodException();
         }
      }

      return instance;
   }

}

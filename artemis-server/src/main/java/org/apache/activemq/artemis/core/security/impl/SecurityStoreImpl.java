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
package org.apache.activemq.artemis.core.security.impl;

import javax.security.auth.Subject;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.management.impl.ManagementRemotingConnection;
import org.apache.activemq.artemis.core.remoting.CertificateUtil;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationService;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.HierarchicalRepositoryChangeListener;
import org.apache.activemq.artemis.logs.AuditLogger;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager2;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager3;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager4;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager5;
import org.apache.activemq.artemis.spi.core.security.jaas.NoCacheLoginException;
import org.apache.activemq.artemis.spi.core.security.jaas.UserPrincipal;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.lang.invoke.MethodHandles;

/**
 * The ActiveMQ Artemis SecurityStore implementation
 */
public class SecurityStoreImpl implements SecurityStore, HierarchicalRepositoryChangeListener {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final HierarchicalRepository<Set<Role>> securityRepository;

   private final ActiveMQSecurityManager securityManager;

   private final Cache<String, ConcurrentHashSet<SimpleString>> authorizationCache;

   private final Cache<String, Pair<Boolean, Subject>> authenticationCache;

   private boolean securityEnabled;

   private final String managementClusterUser;

   private final String managementClusterPassword;

   private final NotificationService notificationService;


   /**
    * @param notificationService can be <code>null</code>
    */
   public SecurityStoreImpl(final HierarchicalRepository<Set<Role>> securityRepository,
                            final ActiveMQSecurityManager securityManager,
                            final long invalidationInterval,
                            final boolean securityEnabled,
                            final String managementClusterUser,
                            final String managementClusterPassword,
                            final NotificationService notificationService,
                            final long authenticationCacheSize,
                            final long authorizationCacheSize) {
      this.securityRepository = securityRepository;
      this.securityManager = securityManager;
      this.securityEnabled = securityEnabled;
      this.managementClusterUser = managementClusterUser;
      this.managementClusterPassword = managementClusterPassword;
      this.notificationService = notificationService;
      if (authenticationCacheSize == 0) {
         authenticationCache = null;
      } else {
         authenticationCache = Caffeine.newBuilder()
                                           .maximumSize(authenticationCacheSize)
                                           .expireAfterWrite(invalidationInterval, TimeUnit.MILLISECONDS)
                                           .build();
      }
      if (authorizationCacheSize == 0) {
         authorizationCache = null;
      } else {
         authorizationCache = Caffeine.newBuilder()
                                          .maximumSize(authorizationCacheSize)
                                          .expireAfterWrite(invalidationInterval, TimeUnit.MILLISECONDS)
                                          .build();
      }
      this.securityRepository.registerListener(this);
   }

   // SecurityManager implementation --------------------------------

   @Override
   public boolean isSecurityEnabled() {
      return securityEnabled;
   }

   @Override
   public void setSecurityEnabled(boolean securityEnabled) {
      this.securityEnabled = securityEnabled;
   }

   @Override
   public void stop() {
      securityRepository.unRegisterListener(this);
   }

   @Override
   public String authenticate(final String user,
                              final String password,
                              RemotingConnection connection) throws Exception {
      return authenticate(user, password, connection, null);
   }

   @Override
   public String authenticate(final String user,
                              final String password,
                              RemotingConnection connection,
                              String securityDomain) throws Exception {
      if (securityEnabled) {

         if (managementClusterUser.equals(user)) {
            logger.trace("Authenticating cluster admin user");

            /*
             * The special user cluster user is used for creating sessions that replicate management
             * operation between nodes
             */
            if (!managementClusterPassword.equals(password)) {
               throw ActiveMQMessageBundle.BUNDLE.unableToValidateClusterUser(user);
            } else {
               return managementClusterUser;
            }
         }

         String validatedUser = null;
         boolean userIsValid = false;
         boolean check = true;

         Subject subject = null;
         Pair<Boolean, Subject> cacheEntry = getAuthenticationCacheEntry(user, password, connection);
         if (cacheEntry != null) {
            if (!cacheEntry.getA()) {
               // cached authentication failed previously so don't check again
               check = false;
            } else {
               // cached authentication succeeded previously so don't check again
               check = false;
               userIsValid = true;
               subject = cacheEntry.getB();
               validatedUser = getUserFromSubject(subject);
            }
         } else {
            if (user == null && password == null && connection instanceof ManagementRemotingConnection) {
               AccessControlContext accessControlContext = AccessController.getContext();
               if (accessControlContext != null) {
                  check = false;
                  userIsValid = true;
                  subject = Subject.getSubject(accessControlContext);
                  validatedUser = getUserFromSubject(subject);
               }
            }
         }
         if (check) {
            if (securityManager instanceof ActiveMQSecurityManager5) {
               try {
                  subject = ((ActiveMQSecurityManager5) securityManager).authenticate(user, password, connection, securityDomain);
                  putAuthenticationCacheEntry(user, password, connection, subject);
                  validatedUser = getUserFromSubject(subject);
               } catch (NoCacheLoginException e) {
                  handleNoCacheLoginException(e);
               }
            } else if (securityManager instanceof ActiveMQSecurityManager4) {
               validatedUser = ((ActiveMQSecurityManager4) securityManager).validateUser(user, password, connection, securityDomain);
            } else if (securityManager instanceof ActiveMQSecurityManager3) {
               validatedUser = ((ActiveMQSecurityManager3) securityManager).validateUser(user, password, connection);
            } else if (securityManager instanceof ActiveMQSecurityManager2) {
               userIsValid = ((ActiveMQSecurityManager2) securityManager).validateUser(user, password, CertificateUtil.getCertsFromConnection(connection));
            } else {
               userIsValid = securityManager.validateUser(user, password);
            }
         }

         // authentication failed, send a notification & throw an exception
         if (!userIsValid && validatedUser == null) {
            authenticationFailed(user, connection);
         }

         if (connection != null) {
            connection.setSubject(subject);
         }
         if (AuditLogger.isResourceLoggingEnabled()) {
            AuditLogger.userSuccesfullyAuthenticatedInAudit(subject, connection.getRemoteAddress(), connection.getID().toString());
         }

         return validatedUser;
      }

      return null;
   }

   @Override
   public void check(final SimpleString address,
                     final CheckType checkType,
                     final SecurityAuth session) throws Exception {
      check(address, null, checkType, session);
   }

   @Override
   public void check(final SimpleString address,
                     final SimpleString queue,
                     final CheckType checkType,
                     final SecurityAuth session) throws Exception {
      if (securityEnabled) {
         SimpleString bareAddress = CompositeAddress.extractAddressName(address);
         SimpleString bareQueue = CompositeAddress.extractQueueName(queue);

         logger.trace("checking access permissions to {}", bareAddress);

         // bypass permission checks for management cluster user
         String user = session.getUsername();
         if (managementClusterUser.equals(user) && session.getPassword().equals(managementClusterPassword)) {
            return;
         }

         Set<Role> roles = securityRepository.getMatch(bareAddress.toString());

         /*
          * If a valid queue is passed in and there's an exact match for the FQQN then use the FQQN instead of the address
          */
         SimpleString fqqn = null;
         if (bareQueue != null) {
            fqqn = CompositeAddress.toFullyQualified(bareAddress, bareQueue);
            if (securityRepository.containsExactMatch(fqqn.toString())) {
               roles = securityRepository.getMatch(fqqn.toString());
            }
         }

         if (checkAuthorizationCache(fqqn != null  ? fqqn : bareAddress, user, checkType)) {
            return;
         }

         final Boolean validated;
         if (securityManager instanceof ActiveMQSecurityManager5) {
            Subject subject = getSubjectForAuthorization(session, ((ActiveMQSecurityManager5) securityManager));

            /**
             * A user may authenticate successfully at first, but then later when their Subject is evicted from the
             * local cache re-authentication may fail. This could happen, for example, if the user was removed
             * from LDAP or the user's token expired.
             *
             * If the subject is null then authorization will *always* fail.
             */
            if (subject == null) {
               authenticationFailed(user, session.getRemotingConnection());
            }

            validated = ((ActiveMQSecurityManager5) securityManager).authorize(subject, roles, checkType, fqqn != null ? fqqn.toString() : bareAddress.toString());
         } else if (securityManager instanceof ActiveMQSecurityManager4) {
            validated = ((ActiveMQSecurityManager4) securityManager).validateUserAndRole(user, session.getPassword(), roles, checkType, bareAddress.toString(), session.getRemotingConnection(), session.getSecurityDomain()) != null;
         } else if (securityManager instanceof ActiveMQSecurityManager3) {
            validated = ((ActiveMQSecurityManager3) securityManager).validateUserAndRole(user, session.getPassword(), roles, checkType, bareAddress.toString(), session.getRemotingConnection()) != null;
         } else if (securityManager instanceof ActiveMQSecurityManager2) {
            validated = ((ActiveMQSecurityManager2) securityManager).validateUserAndRole(user, session.getPassword(), roles, checkType, bareAddress.toString(), session.getRemotingConnection());
         } else {
            validated = securityManager.validateUserAndRole(user, session.getPassword(), roles, checkType);
         }

         if (!validated) {
            if (notificationService != null) {
               TypedProperties props = new TypedProperties();

               props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, bareAddress);
               props.putSimpleStringProperty(ManagementHelper.HDR_CHECK_TYPE, new SimpleString(checkType.toString()));
               props.putSimpleStringProperty(ManagementHelper.HDR_USER, SimpleString.toSimpleString(user));

               Notification notification = new Notification(null, CoreNotificationType.SECURITY_PERMISSION_VIOLATION, props);

               notificationService.sendNotification(notification);
            }

            Exception ex;
            if (bareQueue == null) {
               ex = ActiveMQMessageBundle.BUNDLE.userNoPermissions(session.getUsername(), checkType, bareAddress);
            } else {
               ex = ActiveMQMessageBundle.BUNDLE.userNoPermissionsQueue(session.getUsername(), checkType, bareQueue, bareAddress);
            }
            AuditLogger.securityFailure(ex.getMessage(), ex);
            throw ex;
         }

         // if we get here we're granted, add to the cache
         ConcurrentHashSet<SimpleString> set;
         String key = createAuthorizationCacheKey(user, checkType);
         ConcurrentHashSet<SimpleString> act = getAuthorizationCacheEntry(key);
         if (act != null) {
            set = act;
         } else {
            set = new ConcurrentHashSet<>();
            putAuthorizationCacheEntry(set, key);
         }
         set.add(fqqn != null ? fqqn : bareAddress);
      }
   }

   @Override
   public void onChange() {
      invalidateAuthorizationCache();
      // we don't invalidate the authentication cache here because it's not necessary
   }

   public static String getUserFromSubject(Subject subject) {
      if (subject == null) {
         return null;
      }

      String validatedUser = "";
      Set<UserPrincipal> users = subject.getPrincipals(UserPrincipal.class);

      // should only ever be 1 UserPrincipal
      for (UserPrincipal userPrincipal : users) {
         validatedUser = userPrincipal.getName();
      }
      return validatedUser;
   }

   /**
    * Get the cached Subject. If the Subject is not in the cache then authenticate again to retrieve
    * it.
    *
    * @param session contains the authentication data
    * @return the authenticated Subject with all associated role principals or null if not
    * authenticated or JAAS is not supported by the SecurityManager.
    */
   @Override
   public Subject getSessionSubject(SecurityAuth session) {
      if (securityManager instanceof ActiveMQSecurityManager5) {
         return getSubjectForAuthorization(session, (ActiveMQSecurityManager5) securityManager);
      }
      return null;
   }

   private void authenticationFailed(String user, RemotingConnection connection) throws Exception {
      String certSubjectDN = CertificateUtil.getCertSubjectDN(connection);

      if (notificationService != null) {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(ManagementHelper.HDR_USER, SimpleString.toSimpleString(user));
         props.putSimpleStringProperty(ManagementHelper.HDR_CERT_SUBJECT_DN, SimpleString.toSimpleString(certSubjectDN));
         props.putSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS, SimpleString.toSimpleString(connection == null ? "null" : connection.getRemoteAddress()));

         Notification notification = new Notification(null, CoreNotificationType.SECURITY_AUTHENTICATION_VIOLATION, props);

         notificationService.sendNotification(notification);
      }

      Exception e = ActiveMQMessageBundle.BUNDLE.unableToValidateUser(connection == null ? "null" : connection.getRemoteAddress(), user, certSubjectDN);

      ActiveMQServerLogger.LOGGER.securityProblemWhileAuthenticating(e.getMessage());

      if (AuditLogger.isResourceLoggingEnabled()) {
         AuditLogger.userFailedAuthenticationInAudit(null, e.getMessage(), connection.getID().toString());
      }

      throw e;
   }

   /**
    * Get the cached Subject. If the Subject is not in the cache then authenticate again to retrieve it.
    *
    * @param auth contains the authentication data
    * @param securityManager used to authenticate the user if the Subject is not in the cache
    * @return the authenticated Subject with all associated role principals
    */
   private Subject getSubjectForAuthorization(SecurityAuth auth, ActiveMQSecurityManager5 securityManager) {
      Pair<Boolean, Subject> cached = getAuthenticationCacheEntry(auth.getUsername(), auth.getPassword(), auth.getRemotingConnection());

      if (cached == null && auth.getUsername() == null && auth.getPassword() == null && auth.getRemotingConnection() instanceof ManagementRemotingConnection) {
         AccessControlContext accessControlContext = AccessController.getContext();
         if (accessControlContext != null) {
            cached = new Pair<>(true, Subject.getSubject(accessControlContext));
         }
      }

      /*
       * We don't need to worry about the cached boolean being false as users always have to
       * successfully authenticate before requesting authorization for anything.
       */
      if (cached == null) {
         try {
            Subject subject = securityManager.authenticate(auth.getUsername(), auth.getPassword(), auth.getRemotingConnection(), auth.getSecurityDomain());
            putAuthenticationCacheEntry(auth.getUsername(), auth.getPassword(), auth.getRemotingConnection(), subject);
            return subject;
         } catch (NoCacheLoginException e) {
            handleNoCacheLoginException(e);
            return null;
         }
      }
      return cached.getB();
   }

   private void handleNoCacheLoginException(NoCacheLoginException e) {
      logger.debug("Skipping authentication cache due to exception: {}", e.getMessage());
   }

   private void putAuthenticationCacheEntry(String user,
                                            String password,
                                            RemotingConnection connection,
                                            Subject subject) {
      if (authenticationCache != null) {
         authenticationCache.put(createAuthenticationCacheKey(user, password, connection), new Pair<>(subject != null, subject));
      }
   }

   private Pair<Boolean, Subject> getAuthenticationCacheEntry(String user,
                                                              String password,
                                                              RemotingConnection connection) {
      if (authenticationCache == null) {
         return null;
      } else {
         return authenticationCache.getIfPresent(createAuthenticationCacheKey(user, password, connection));
      }
   }

   private void putAuthorizationCacheEntry(ConcurrentHashSet<SimpleString> set, String key) {
      if (authorizationCache != null) {
         authorizationCache.put(key, set);
      }
   }

   private ConcurrentHashSet<SimpleString> getAuthorizationCacheEntry(String key) {
      if (authorizationCache == null) {
         return null;
      } else {
         return authorizationCache.getIfPresent(key);
      }
   }

   public void invalidateAuthorizationCache() {
      if (authorizationCache != null) {
         authorizationCache.invalidateAll();
      }
   }

   public void invalidateAuthenticationCache() {
      if (authenticationCache != null) {
         authenticationCache.invalidateAll();
      }
   }

   public long getAuthenticationCacheSize() {
      if (authenticationCache == null) {
         return 0;
      } else {
         return authenticationCache.estimatedSize();
      }
   }

   public long getAuthorizationCacheSize() {
      if (authorizationCache == null) {
         return 0;
      } else {
         return authorizationCache.estimatedSize();
      }
   }

   private boolean checkAuthorizationCache(final SimpleString dest, final String user, final CheckType checkType) {
      boolean granted = false;

      ConcurrentHashSet<SimpleString> act = getAuthorizationCacheEntry(createAuthorizationCacheKey(user, checkType));
      if (act != null) {
         granted = act.contains(dest);
      }

      return granted;
   }

   private String createAuthenticationCacheKey(String username, String password, RemotingConnection connection) {
      return username + password + CertificateUtil.getCertSubjectDN(connection);
   }

   private String createAuthorizationCacheKey(String user, CheckType checkType) {
      return user + "." + checkType.name();
   }

   // for testing
   protected Cache<String, Pair<Boolean, Subject>> getAuthenticationCache() {
      return authenticationCache;
   }

   // for testing
   protected Cache<String, ConcurrentHashSet<SimpleString>> getAuthorizationCache() {
      return authorizationCache;
   }
}

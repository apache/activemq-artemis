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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
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
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;
import org.apache.activemq.artemis.utils.TypedProperties;

/**
 * The ActiveMQ Artemis SecurityStore implementation
 */
public class SecurityStoreImpl implements SecurityStore, HierarchicalRepositoryChangeListener {
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private final boolean trace = ActiveMQServerLogger.LOGGER.isTraceEnabled();

   private final HierarchicalRepository<Set<Role>> securityRepository;

   private final ActiveMQSecurityManager securityManager;

   private final ConcurrentMap<String, ConcurrentHashSet<SimpleString>> cache = new ConcurrentHashMap<String, ConcurrentHashSet<SimpleString>>();

   private final long invalidationInterval;

   private volatile long lastCheck;

   private final boolean securityEnabled;

   private final String managementClusterUser;

   private final String managementClusterPassword;

   private final NotificationService notificationService;

   // Constructors --------------------------------------------------

   /**
    * @param notificationService can be <code>null</code>
    */
   public SecurityStoreImpl(final HierarchicalRepository<Set<Role>> securityRepository,
                            final ActiveMQSecurityManager securityManager,
                            final long invalidationInterval,
                            final boolean securityEnabled,
                            final String managementClusterUser,
                            final String managementClusterPassword,
                            final NotificationService notificationService) {
      this.securityRepository = securityRepository;
      this.securityManager = securityManager;
      this.invalidationInterval = invalidationInterval;
      this.securityEnabled = securityEnabled;
      this.managementClusterUser = managementClusterUser;
      this.managementClusterPassword = managementClusterPassword;
      this.notificationService = notificationService;
      this.securityRepository.registerListener(this);
   }

   // SecurityManager implementation --------------------------------

   @Override
   public boolean isSecurityEnabled() {
      return securityEnabled;
   }

   public void stop() {
      securityRepository.unRegisterListener(this);
   }

   public void authenticate(final String user, final String password) throws Exception {
      if (securityEnabled) {

         if (managementClusterUser.equals(user)) {
            if (trace) {
               ActiveMQServerLogger.LOGGER.trace("Authenticating cluster admin user");
            }

            /*
             * The special user cluster user is used for creating sessions that replicate management
             * operation between nodes
             */
            if (!managementClusterPassword.equals(password)) {
               throw ActiveMQMessageBundle.BUNDLE.unableToValidateClusterUser(user);
            }
            else {
               return;
            }
         }

         if (!securityManager.validateUser(user, password)) {
            if (notificationService != null) {
               TypedProperties props = new TypedProperties();

               props.putSimpleStringProperty(ManagementHelper.HDR_USER, SimpleString.toSimpleString(user));

               Notification notification = new Notification(null, CoreNotificationType.SECURITY_AUTHENTICATION_VIOLATION, props);

               notificationService.sendNotification(notification);
            }

            throw ActiveMQMessageBundle.BUNDLE.unableToValidateUser(user);
         }
      }
   }

   public void check(final SimpleString address,
                     final CheckType checkType,
                     final SecurityAuth session) throws Exception {
      if (securityEnabled) {
         if (trace) {
            ActiveMQServerLogger.LOGGER.trace("checking access permissions to " + address);
         }

         String user = session.getUsername();
         if (checkCached(address, user, checkType)) {
            // OK
            return;
         }

         String saddress = address.toString();

         Set<Role> roles = securityRepository.getMatch(saddress);

         // bypass permission checks for management cluster user
         if (managementClusterUser.equals(user) && session.getPassword().equals(managementClusterPassword)) {
            return;
         }

         if (!securityManager.validateUserAndRole(user, session.getPassword(), roles, checkType)) {
            if (notificationService != null) {
               TypedProperties props = new TypedProperties();

               props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, address);
               props.putSimpleStringProperty(ManagementHelper.HDR_CHECK_TYPE, new SimpleString(checkType.toString()));
               props.putSimpleStringProperty(ManagementHelper.HDR_USER, SimpleString.toSimpleString(user));

               Notification notification = new Notification(null, CoreNotificationType.SECURITY_PERMISSION_VIOLATION, props);

               notificationService.sendNotification(notification);
            }

            throw ActiveMQMessageBundle.BUNDLE.userNoPermissions(session.getUsername(), checkType, saddress);
         }
         // if we get here we're granted, add to the cache
         ConcurrentHashSet<SimpleString> set = new ConcurrentHashSet<SimpleString>();
         ConcurrentHashSet<SimpleString> act = cache.putIfAbsent(user + "." + checkType.name(), set);
         if (act != null) {
            set = act;
         }
         set.add(address);

      }
   }

   public void onChange() {
      invalidateCache();
   }

   // Public --------------------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   private void invalidateCache() {
      cache.clear();
   }

   private boolean checkCached(final SimpleString dest, final String user, final CheckType checkType) {
      long now = System.currentTimeMillis();

      boolean granted = false;

      if (now - lastCheck > invalidationInterval) {
         invalidateCache();

         lastCheck = now;
      }
      else {
         ConcurrentHashSet<SimpleString> act = cache.get(user + "." + checkType.name());
         if (act != null) {
            granted = act.contains(dest);
         }
      }

      return granted;
   }

   // Inner class ---------------------------------------------------

}

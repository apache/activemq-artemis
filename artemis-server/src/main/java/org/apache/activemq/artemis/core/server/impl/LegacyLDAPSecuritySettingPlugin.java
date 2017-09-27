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
package org.apache.activemq.artemis.core.server.impl;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.event.EventDirContext;
import javax.naming.event.NamespaceChangeListener;
import javax.naming.event.NamingEvent;
import javax.naming.event.NamingExceptionEvent;
import javax.naming.event.ObjectChangeListener;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.SecuritySettingPlugin;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.jboss.logging.Logger;

public class LegacyLDAPSecuritySettingPlugin implements SecuritySettingPlugin {

   private static final Logger logger = Logger.getLogger(LegacyLDAPSecuritySettingPlugin.class);

   private static final long serialVersionUID = 4793109879399750045L;

   public static final String INITIAL_CONTEXT_FACTORY = "initialContextFactory";
   public static final String CONNECTION_URL = "connectionURL";
   public static final String CONNECTION_USERNAME = "connectionUsername";
   public static final String CONNECTION_PASSWORD = "connectionPassword";
   public static final String CONNECTION_PROTOCOL = "connectionProtocol";
   public static final String AUTHENTICATION = "authentication";
   public static final String ROLE_ATTRIBUTE = "roleAttribute";
   public static final String FILTER = "filter";
   public static final String DESTINATION_BASE = "destinationBase";
   public static final String ADMIN_PERMISSION_VALUE = "adminPermissionValue";
   public static final String READ_PERMISSION_VALUE = "readPermissionValue";
   public static final String WRITE_PERMISSION_VALUE = "writePermissionValue";
   public static final String ENABLE_LISTENER = "enableListener";

   private String initialContextFactory = "com.sun.jndi.ldap.LdapCtxFactory";
   private String connectionURL = "ldap://localhost:1024";
   private String connectionUsername;
   private String connectionPassword;
   private String connectionProtocol;
   private String authentication = "simple";
   private String destinationBase = "ou=destinations,o=ActiveMQ,ou=system";
   private String filter = "(cn=*)";
   private String roleAttribute = "uniqueMember";
   private String adminPermissionValue = "admin";
   private String readPermissionValue = "read";
   private String writePermissionValue = "write";
   private boolean enableListener = true;

   private DirContext context;
   private EventDirContext eventContext;
   private Map<String, Set<Role>> securityRoles;
   private HierarchicalRepository<Set<Role>> securityRepository;

   @Override
   public LegacyLDAPSecuritySettingPlugin init(Map<String, String> options) {
      if (options != null) {
         initialContextFactory = getOption(options, INITIAL_CONTEXT_FACTORY, initialContextFactory);
         connectionURL = getOption(options, CONNECTION_URL, connectionURL);
         connectionUsername = getOption(options, CONNECTION_USERNAME, connectionUsername);
         connectionPassword = getOption(options, CONNECTION_PASSWORD, connectionPassword);
         connectionProtocol = getOption(options, CONNECTION_PROTOCOL, connectionProtocol);
         authentication = getOption(options, AUTHENTICATION, authentication);
         destinationBase = getOption(options, DESTINATION_BASE, destinationBase);
         filter = getOption(options, FILTER, filter);
         roleAttribute = getOption(options, ROLE_ATTRIBUTE, roleAttribute);
         adminPermissionValue = getOption(options, ADMIN_PERMISSION_VALUE, adminPermissionValue);
         readPermissionValue = getOption(options, READ_PERMISSION_VALUE, readPermissionValue);
         writePermissionValue = getOption(options, WRITE_PERMISSION_VALUE, writePermissionValue);
         enableListener = getOption(options, ENABLE_LISTENER, Boolean.TRUE.toString()).equalsIgnoreCase(Boolean.TRUE.toString());
      }

      return this;
   }

   private String getOption(Map<String, String> options, String key, String defaultValue) {
      String result = options.get(key);
      if (result == null) {
         result = defaultValue;
      }

      return result;
   }

   public String getRoleAttribute() {
      return roleAttribute;
   }

   public SecuritySettingPlugin setRoleAttribute(String roleAttribute) {
      this.roleAttribute = roleAttribute;
      return this;
   }

   public String getFilter() {
      return filter;
   }

   public LegacyLDAPSecuritySettingPlugin setFilter(String filter) {
      this.filter = filter;
      return this;
   }

   public String getDestinationBase() {
      return destinationBase;
   }

   public LegacyLDAPSecuritySettingPlugin setDestinationBase(String destinationBase) {
      this.destinationBase = destinationBase;
      return this;
   }

   public String getAuthentication() {
      return authentication;
   }

   public LegacyLDAPSecuritySettingPlugin setAuthentication(String authentication) {
      this.authentication = authentication;
      return this;
   }

   public String getConnectionPassword() {
      return connectionPassword;
   }

   public LegacyLDAPSecuritySettingPlugin setConnectionPassword(String connectionPassword) {
      this.connectionPassword = connectionPassword;
      return this;
   }

   public String getConnectionProtocol() {
      return connectionProtocol;
   }

   public LegacyLDAPSecuritySettingPlugin setConnectionProtocol(String connectionProtocol) {
      this.connectionProtocol = connectionProtocol;
      return this;
   }

   public String getConnectionURL() {
      return connectionURL;
   }

   public LegacyLDAPSecuritySettingPlugin setConnectionURL(String connectionURL) {
      this.connectionURL = connectionURL;
      return this;
   }

   public String getConnectionUsername() {
      return connectionUsername;
   }

   public LegacyLDAPSecuritySettingPlugin setConnectionUsername(String connectionUsername) {
      this.connectionUsername = connectionUsername;
      return this;
   }

   public String getInitialContextFactory() {
      return initialContextFactory;
   }

   public String getAdminPermissionValue() {
      return adminPermissionValue;
   }

   public LegacyLDAPSecuritySettingPlugin setAdminPermissionValue(String adminPermissionValue) {
      this.adminPermissionValue = adminPermissionValue;
      return this;
   }

   public String getReadPermissionValue() {
      return readPermissionValue;
   }

   public LegacyLDAPSecuritySettingPlugin setReadPermissionValue(String readPermissionValue) {
      this.readPermissionValue = readPermissionValue;
      return this;
   }

   public String getWritePermissionValue() {
      return writePermissionValue;
   }

   public LegacyLDAPSecuritySettingPlugin setWritePermissionValue(String writePermissionValue) {
      this.writePermissionValue = writePermissionValue;
      return this;
   }

   public LegacyLDAPSecuritySettingPlugin setInitialContextFactory(String initialContextFactory) {
      this.initialContextFactory = initialContextFactory;
      return this;
   }

   public boolean isEnableListener() {
      return enableListener;
   }

   public LegacyLDAPSecuritySettingPlugin setEnableListener(boolean enableListener) {
      this.enableListener = enableListener;
      return this;
   }

   protected boolean isContextAlive() {
      boolean alive = false;
      if (context != null) {
         try {
            context.getAttributes("");
            alive = true;
         } catch (Exception e) {
         }
      }
      return alive;
   }

   protected void open() throws NamingException {
      if (isContextAlive()) {
         return;
      }

      context = createContext();
      eventContext = ((EventDirContext) context.lookup(""));

      SearchControls searchControls = new SearchControls();
      searchControls.setReturningAttributes(new String[]{roleAttribute});
      searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

      if (enableListener) {
         eventContext.addNamingListener(destinationBase, filter, searchControls, new LDAPNamespaceChangeListener());
      }
   }

   private DirContext createContext() throws NamingException {
      Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);
      if (connectionUsername != null && !"".equals(connectionUsername)) {
         env.put(Context.SECURITY_PRINCIPAL, connectionUsername);
      } else {
         throw new NamingException("Empty username is not allowed");
      }
      if (connectionPassword != null && !"".equals(connectionPassword)) {
         env.put(Context.SECURITY_CREDENTIALS, connectionPassword);
      } else {
         throw new NamingException("Empty password is not allowed");
      }
      env.put(Context.SECURITY_PROTOCOL, connectionProtocol);
      env.put(Context.PROVIDER_URL, connectionURL);
      env.put(Context.SECURITY_AUTHENTICATION, authentication);
      return new InitialDirContext(env);
   }

   @Override
   public Map<String, Set<Role>> getSecurityRoles() {
      if (securityRoles == null) {
         populateSecurityRoles();
      }
      return securityRoles;
   }

   private LegacyLDAPSecuritySettingPlugin populateSecurityRoles() {
      ActiveMQServerLogger.LOGGER.populatingSecurityRolesFromLDAP(connectionURL);
      try {
         open();
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorOpeningContextForLDAP(e);
         return this;
      }

      SearchControls searchControls = new SearchControls();
      searchControls.setReturningAttributes(new String[]{roleAttribute});
      searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

      securityRoles = new HashMap<>();
      try {
         NamingEnumeration<SearchResult> searchResults = context.search(destinationBase, filter, searchControls);
         while (searchResults.hasMore()) {
            processSearchResult(securityRoles, searchResults.next());
         }
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorPopulatingSecurityRolesFromLDAP(e);
      }

      return this;
   }

   @Override
   public void setSecurityRepository(HierarchicalRepository<Set<Role>> securityRepository) {
      this.securityRepository = securityRepository;
   }

   private void processSearchResult(Map<String, Set<Role>> securityRoles,
                                    SearchResult searchResult) throws NamingException {
      Attributes attrs = searchResult.getAttributes();
      if (attrs == null || attrs.size() == 0) {
         return;
      }
      LdapName searchResultLdapName = new LdapName(searchResult.getName());
      logger.debug("LDAP search result : " + searchResultLdapName);
      String permissionType = null;
      String destination = null;
      String destinationType = "unknown";
      for (Rdn rdn : searchResultLdapName.getRdns()) {
         if (rdn.getType().equals("cn")) {
            logger.debug("\tPermission type: " + rdn.getValue());
            permissionType = rdn.getValue().toString();
         }
         if (rdn.getType().equals("uid")) {
            logger.debug("\tDestination name: " + rdn.getValue());
            destination = rdn.getValue().toString();
         }
         if (rdn.getType().equals("ou")) {
            String rawDestinationType = rdn.getValue().toString();
            if (rawDestinationType.toLowerCase().contains("queue")) {
               destinationType = "queue";
            } else if (rawDestinationType.toLowerCase().contains("topic")) {
               destinationType = "topic";
            }
            logger.debug("\tDestination type: " + destinationType);
         }
      }
      logger.debug("\tAttributes: " + attrs);
      Attribute attr = attrs.get(roleAttribute);
      NamingEnumeration<?> e = attr.getAll();
      Set<Role> roles = securityRoles.get(destination);
      boolean exists = false;
      if (roles == null) {
         roles = new HashSet<>();
      } else {
         exists = true;
      }

      while (e.hasMore()) {
         String value = (String) e.next();
         LdapName ldapname = new LdapName(value);
         Rdn rdn = ldapname.getRdn(ldapname.size() - 1);
         String roleName = rdn.getValue().toString();
         logger.debug("\tRole name: " + roleName);
         Role role = new Role(roleName, permissionType.equalsIgnoreCase(writePermissionValue), permissionType.equalsIgnoreCase(readPermissionValue), permissionType.equalsIgnoreCase(adminPermissionValue), permissionType.equalsIgnoreCase(adminPermissionValue), permissionType.equalsIgnoreCase(adminPermissionValue), permissionType.equalsIgnoreCase(adminPermissionValue), false, // there is no permission from ActiveMQ 5.x that corresponds to the "manage" permission in ActiveMQ Artemis
                              permissionType.equalsIgnoreCase(readPermissionValue)); // the "browse" permission matches "read" from ActiveMQ 5.x
         roles.add(role);
      }

      if (!exists) {
         securityRoles.put(destination, roles);
      }
   }

   @Override
   public SecuritySettingPlugin stop() {
      try {
         eventContext.close();
      } catch (NamingException e) {
         // ignore
      }

      try {
         if (context != null) {
            context.close();
         }
      } catch (NamingException e) {
         // ignore
      }

      return this;
   }

   /**
    * Handler for new policy entries in the directory.
    *
    * @param namingEvent the new entry event that occurred
    */
   public void objectAdded(NamingEvent namingEvent) {
      Map<String, Set<Role>> newRoles = new HashMap<>();

      try {
         processSearchResult(newRoles, (SearchResult) namingEvent.getNewBinding());
         for (Map.Entry<String, Set<Role>> entry : newRoles.entrySet()) {
            Set<Role> existingRoles = securityRepository.getMatch(entry.getKey());
            for (Role role : entry.getValue()) {
               existingRoles.add(role);
            }
         }
      } catch (NamingException e) {
         ActiveMQServerLogger.LOGGER.failedToProcessEvent(e);
      }
   }

   /**
    * Handler for removed policy entries in the directory.
    *
    * @param namingEvent the removed entry event that occurred
    */
   public void objectRemoved(NamingEvent namingEvent) {
      try {
         LdapName ldapName = new LdapName(namingEvent.getOldBinding().getName());
         String match = null;
         for (Rdn rdn : ldapName.getRdns()) {
            if (rdn.getType().equals("uid")) {
               match = rdn.getValue().toString();
            }
         }

         Set<Role> roles = securityRepository.getMatch(match);

         List<Role> rolesToRemove = new ArrayList<>();

         for (Rdn rdn : ldapName.getRdns()) {
            if (rdn.getValue().equals(writePermissionValue)) {
               logger.debug("Removing write permission");
               for (Role role : roles) {
                  if (role.isSend()) {
                     rolesToRemove.add(role);
                  }
               }
            } else if (rdn.getValue().equals(readPermissionValue)) {
               logger.debug("Removing read permission");
               for (Role role : roles) {
                  if (role.isConsume()) {
                     rolesToRemove.add(role);
                  }
               }
            } else if (rdn.getValue().equals(adminPermissionValue)) {
               logger.debug("Removing admin permission");
               for (Role role : roles) {
                  if (role.isCreateDurableQueue() || role.isCreateNonDurableQueue() || role.isDeleteDurableQueue() || role.isDeleteNonDurableQueue()) {
                     rolesToRemove.add(role);
                  }
               }
            }

            for (Role roleToRemove : rolesToRemove) {
               roles.remove(roleToRemove);
            }
         }
      } catch (NamingException e) {
         ActiveMQServerLogger.LOGGER.failedToProcessEvent(e);
      }
   }

   /**
    * @param namingEvent the renaming entry event that occurred
    */
   public void objectRenamed(NamingEvent namingEvent) {

   }

   /**
    * Handler for changed policy entries in the directory.
    *
    * @param namingEvent the changed entry event that occurred
    */
   public void objectChanged(NamingEvent namingEvent) {
      objectRemoved(namingEvent);
      objectAdded(namingEvent);
   }

   /**
    * Handler for exception events from the registry.
    *
    * @param namingExceptionEvent the exception event
    */
   public void namingExceptionThrown(NamingExceptionEvent namingExceptionEvent) {
      context = null;
      ActiveMQServerLogger.LOGGER.caughtUnexpectedException(namingExceptionEvent.getException());
   }

   protected class LDAPNamespaceChangeListener implements NamespaceChangeListener, ObjectChangeListener {

      @Override
      public void namingExceptionThrown(NamingExceptionEvent evt) {
         LegacyLDAPSecuritySettingPlugin.this.namingExceptionThrown(evt);
      }

      @Override
      public void objectAdded(NamingEvent evt) {
         LegacyLDAPSecuritySettingPlugin.this.objectAdded(evt);
      }

      @Override
      public void objectRemoved(NamingEvent evt) {
         LegacyLDAPSecuritySettingPlugin.this.objectRemoved(evt);
      }

      @Override
      public void objectRenamed(NamingEvent evt) {
         LegacyLDAPSecuritySettingPlugin.this.objectRenamed(evt);
      }

      @Override
      public void objectChanged(NamingEvent evt) {
         LegacyLDAPSecuritySettingPlugin.this.objectChanged(evt);
      }
   }
}

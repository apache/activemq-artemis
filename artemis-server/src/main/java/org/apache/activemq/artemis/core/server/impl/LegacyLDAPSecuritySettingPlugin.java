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
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.SecuritySettingPlugin;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;

public class LegacyLDAPSecuritySettingPlugin implements SecuritySettingPlugin {
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

   private DirContext context;
   private Map<String, Set<Role>> securityRoles = new HashMap<>();

   @Override
   public LegacyLDAPSecuritySettingPlugin init(Map<String, String> options) {
      if (options != null) {
         initialContextFactory = options.get(INITIAL_CONTEXT_FACTORY);
         connectionURL = options.get(CONNECTION_URL);
         connectionUsername = options.get(CONNECTION_USERNAME);
         connectionPassword = options.get(CONNECTION_PASSWORD);
         connectionProtocol = options.get(CONNECTION_PROTOCOL);
         authentication = options.get(AUTHENTICATION);
         destinationBase = options.get(DESTINATION_BASE);
         filter = options.get(FILTER);
         roleAttribute = options.get(ROLE_ATTRIBUTE);
         adminPermissionValue = options.get(ADMIN_PERMISSION_VALUE);
         readPermissionValue = options.get(READ_PERMISSION_VALUE);
         writePermissionValue = options.get(WRITE_PERMISSION_VALUE);
      }

      return this;
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

   protected void open() throws NamingException {
      if (context != null) {
         return;
      }

      Hashtable<String, String> env = new Hashtable<String, String>();
      env.put(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);
      if (connectionUsername != null && !"".equals(connectionUsername)) {
         env.put(Context.SECURITY_PRINCIPAL, connectionUsername);
      }
      else {
         throw new NamingException("Empty username is not allowed");
      }
      if (connectionPassword != null && !"".equals(connectionPassword)) {
         env.put(Context.SECURITY_CREDENTIALS, connectionPassword);
      }
      else {
         throw new NamingException("Empty password is not allowed");
      }
      env.put(Context.SECURITY_PROTOCOL, connectionProtocol);
      env.put(Context.PROVIDER_URL, connectionURL);
      env.put(Context.SECURITY_AUTHENTICATION, authentication);
      context = new InitialDirContext(env);
   }

   @Override
   public Map<String, Set<Role>> getSecurityRoles() {
      return securityRoles;
   }

   @Override
   public LegacyLDAPSecuritySettingPlugin populateSecurityRoles() {
      ActiveMQServerLogger.LOGGER.populatingSecurityRolesFromLDAP(connectionURL);
      try {
         open();
      }
      catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorOpeningContextForLDAP(e);
         return this;
      }

      SearchControls searchControls = new SearchControls();
      searchControls.setReturningAttributes(new String[]{roleAttribute});
      searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

      Map<String, Set<Role>> securityRoles = new HashMap<String, Set<Role>>();
      try {
         NamingEnumeration<SearchResult> searchResults = context.search(destinationBase, filter, searchControls);
         int i = 0;
         while (searchResults.hasMore()) {
            SearchResult searchResult = searchResults.next();
            Attributes attrs = searchResult.getAttributes();
            if (attrs == null || attrs.size() == 0) {
               continue;
            }
            LdapName searchResultLdapName = new LdapName(searchResult.getName());
            ActiveMQServerLogger.LOGGER.debug("LDAP search result " + ++i + ": " + searchResultLdapName);
            String permissionType = null;
            String destination = null;
            String destinationType = "unknown";
            for (Rdn rdn : searchResultLdapName.getRdns()) {
               if (rdn.getType().equals("cn")) {
                  ActiveMQServerLogger.LOGGER.debug("\tPermission type: " + rdn.getValue());
                  permissionType = rdn.getValue().toString();
               }
               if (rdn.getType().equals("uid")) {
                  ActiveMQServerLogger.LOGGER.debug("\tDestination name: " + rdn.getValue());
                  destination = rdn.getValue().toString();
               }
               if (rdn.getType().equals("ou")) {
                  String rawDestinationType = rdn.getValue().toString();
                  if (rawDestinationType.toLowerCase().contains("queue")) {
                     destinationType = "queue";
                  }
                  else if (rawDestinationType.toLowerCase().contains("topic")) {
                     destinationType = "topic";
                  }
                  ActiveMQServerLogger.LOGGER.debug("\tDestination type: " + destinationType);
               }
            }
            ActiveMQServerLogger.LOGGER.debug("\tAttributes: " + attrs);
            Attribute attr = attrs.get(roleAttribute);
            NamingEnumeration<?> e = attr.getAll();
            Set<Role> roles = securityRoles.get(destination);
            boolean exists = false;
            if (roles == null) {
               roles = new HashSet<>();
            }
            else {
               exists = true;
            }

            while (e.hasMore()) {
               String value = (String) e.next();
               LdapName ldapname = new LdapName(value);
               Rdn rdn = ldapname.getRdn(ldapname.size() - 1);
               String roleName = rdn.getValue().toString();
               ActiveMQServerLogger.LOGGER.debug("\tRole name: " + roleName);
               Role role = new Role(roleName,
                                    permissionType.equalsIgnoreCase(writePermissionValue),
                                    permissionType.equalsIgnoreCase(readPermissionValue),
                                    permissionType.equalsIgnoreCase(adminPermissionValue),
                                    permissionType.equalsIgnoreCase(adminPermissionValue),
                                    permissionType.equalsIgnoreCase(adminPermissionValue),
                                    permissionType.equalsIgnoreCase(adminPermissionValue),
                                    false); // there is no permission from ActiveMQ 5.x that corresponds to the "manage" permission in ActiveMQ Artemis
               roles.add(role);
            }

            if (!exists) {
               securityRoles.put(destination, roles);
            }
         }
      }
      catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorPopulatingSecurityRolesFromLDAP(e);
      }

      this.securityRoles = securityRoles;
      return this;
   }
}

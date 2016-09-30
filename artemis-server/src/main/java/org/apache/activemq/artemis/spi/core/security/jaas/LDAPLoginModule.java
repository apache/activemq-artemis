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
package org.apache.activemq.artemis.spi.core.security.jaas;

import javax.naming.AuthenticationException;
import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.jboss.logging.Logger;

public class LDAPLoginModule implements LoginModule {

   private static final Logger logger = Logger.getLogger(LDAPLoginModule.class);

   private static final String INITIAL_CONTEXT_FACTORY = "initialContextFactory";
   private static final String CONNECTION_URL = "connectionURL";
   private static final String CONNECTION_USERNAME = "connectionUsername";
   private static final String CONNECTION_PASSWORD = "connectionPassword";
   private static final String CONNECTION_PROTOCOL = "connectionProtocol";
   private static final String AUTHENTICATION = "authentication";
   private static final String USER_BASE = "userBase";
   private static final String USER_SEARCH_MATCHING = "userSearchMatching";
   private static final String USER_SEARCH_SUBTREE = "userSearchSubtree";
   private static final String ROLE_BASE = "roleBase";
   private static final String ROLE_NAME = "roleName";
   private static final String ROLE_SEARCH_MATCHING = "roleSearchMatching";
   private static final String ROLE_SEARCH_SUBTREE = "roleSearchSubtree";
   private static final String USER_ROLE_NAME = "userRoleName";
   private static final String EXPAND_ROLES = "expandRoles";
   private static final String EXPAND_ROLES_MATCHING = "expandRolesMatching";

   protected DirContext context;

   private Subject subject;
   private CallbackHandler handler;
   private LDAPLoginProperty[] config;
   private String username;
   private final Set<RolePrincipal> groups = new HashSet<>();

   @Override
   public void initialize(Subject subject,
                          CallbackHandler callbackHandler,
                          Map<String, ?> sharedState,
                          Map<String, ?> options) {
      this.subject = subject;
      this.handler = callbackHandler;

      config = new LDAPLoginProperty[]{new LDAPLoginProperty(INITIAL_CONTEXT_FACTORY, (String) options.get(INITIAL_CONTEXT_FACTORY)), new LDAPLoginProperty(CONNECTION_URL, (String) options.get(CONNECTION_URL)), new LDAPLoginProperty(CONNECTION_USERNAME, (String) options.get(CONNECTION_USERNAME)), new LDAPLoginProperty(CONNECTION_PASSWORD, (String) options.get(CONNECTION_PASSWORD)), new LDAPLoginProperty(CONNECTION_PROTOCOL, (String) options.get(CONNECTION_PROTOCOL)), new LDAPLoginProperty(AUTHENTICATION, (String) options.get(AUTHENTICATION)), new LDAPLoginProperty(USER_BASE, (String) options.get(USER_BASE)), new LDAPLoginProperty(USER_SEARCH_MATCHING, (String) options.get(USER_SEARCH_MATCHING)), new LDAPLoginProperty(USER_SEARCH_SUBTREE, (String) options.get(USER_SEARCH_SUBTREE)), new LDAPLoginProperty(ROLE_BASE, (String) options.get(ROLE_BASE)), new LDAPLoginProperty(ROLE_NAME, (String) options.get(ROLE_NAME)), new LDAPLoginProperty(ROLE_SEARCH_MATCHING, (String) options.get(ROLE_SEARCH_MATCHING)), new LDAPLoginProperty(ROLE_SEARCH_SUBTREE, (String) options.get(ROLE_SEARCH_SUBTREE)), new LDAPLoginProperty(USER_ROLE_NAME, (String) options.get(USER_ROLE_NAME)), new LDAPLoginProperty(EXPAND_ROLES, (String) options.get(EXPAND_ROLES)), new LDAPLoginProperty(EXPAND_ROLES_MATCHING, (String) options.get(EXPAND_ROLES_MATCHING))};
   }

   @Override
   public boolean login() throws LoginException {

      Callback[] callbacks = new Callback[2];

      callbacks[0] = new NameCallback("User name");
      callbacks[1] = new PasswordCallback("Password", false);
      try {
         handler.handle(callbacks);
      } catch (IOException | UnsupportedCallbackException e) {
         throw (LoginException) new LoginException().initCause(e);
      }

      String password;

      username = ((NameCallback) callbacks[0]).getName();
      if (username == null)
         return false;

      if (((PasswordCallback) callbacks[1]).getPassword() != null)
         password = new String(((PasswordCallback) callbacks[1]).getPassword());
      else
         password = "";

      // authenticate will throw LoginException
      // in case of failed authentication
      authenticate(username, password);
      return true;
   }

   @Override
   public boolean logout() throws LoginException {
      username = null;
      return true;
   }

   @Override
   public boolean commit() throws LoginException {
      Set<Principal> principals = subject.getPrincipals();
      principals.add(new UserPrincipal(username));
      for (RolePrincipal gp : groups) {
         principals.add(gp);
      }
      return true;
   }

   @Override
   public boolean abort() throws LoginException {
      username = null;
      return true;
   }

   protected void closeContext() {
      if (context != null) {
         try {
            context.close();
            context = null;
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.error(e.toString());
         }
      }
   }

   protected boolean authenticate(String username, String password) throws LoginException {

      MessageFormat userSearchMatchingFormat;
      boolean userSearchSubtreeBool;

      if (logger.isDebugEnabled()) {
         logger.debug("Create the LDAP initial context.");
      }
      try {
         openContext();
      } catch (NamingException ne) {
         FailedLoginException ex = new FailedLoginException("Error opening LDAP connection");
         ex.initCause(ne);
         throw ex;
      }

      if (!isLoginPropertySet(USER_SEARCH_MATCHING))
         return false;

      userSearchMatchingFormat = new MessageFormat(getLDAPPropertyValue(USER_SEARCH_MATCHING));
      userSearchSubtreeBool = Boolean.valueOf(getLDAPPropertyValue(USER_SEARCH_SUBTREE)).booleanValue();

      try {

         String filter = userSearchMatchingFormat.format(new String[]{doRFC2254Encoding(username)});
         SearchControls constraints = new SearchControls();
         if (userSearchSubtreeBool) {
            constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
         } else {
            constraints.setSearchScope(SearchControls.ONELEVEL_SCOPE);
         }

         // setup attributes
         List<String> list = new ArrayList<>();
         if (isLoginPropertySet(USER_ROLE_NAME)) {
            list.add(getLDAPPropertyValue(USER_ROLE_NAME));
         }
         String[] attribs = new String[list.size()];
         list.toArray(attribs);
         constraints.setReturningAttributes(attribs);

         if (logger.isDebugEnabled()) {
            logger.debug("Get the user DN.");
            logger.debug("Looking for the user in LDAP with ");
            logger.debug("  base DN: " + getLDAPPropertyValue(USER_BASE));
            logger.debug("  filter: " + filter);
         }

         NamingEnumeration<SearchResult> results = context.search(getLDAPPropertyValue(USER_BASE), filter, constraints);

         if (results == null || !results.hasMore()) {
            ActiveMQServerLogger.LOGGER.warn("User " + username + " not found in LDAP.");
            throw new FailedLoginException("User " + username + " not found in LDAP.");
         }

         SearchResult result = results.next();

         if (results.hasMore()) {
            // ignore for now
         }

         String dn;
         if (result.isRelative()) {
            logger.debug("LDAP returned a relative name: " + result.getName());

            NameParser parser = context.getNameParser("");
            Name contextName = parser.parse(context.getNameInNamespace());
            Name baseName = parser.parse(getLDAPPropertyValue(USER_BASE));
            Name entryName = parser.parse(result.getName());
            Name name = contextName.addAll(baseName);
            name = name.addAll(entryName);
            dn = name.toString();
         } else {
            logger.debug("LDAP returned an absolute name: " + result.getName());

            try {
               URI uri = new URI(result.getName());
               String path = uri.getPath();

               if (path.startsWith("/")) {
                  dn = path.substring(1);
               } else {
                  dn = path;
               }
            } catch (URISyntaxException e) {
               closeContext();
               FailedLoginException ex = new FailedLoginException("Error parsing absolute name as URI.");
               ex.initCause(e);
               throw ex;
            }
         }

         if (logger.isDebugEnabled()) {
            logger.debug("Using DN [" + dn + "] for binding.");
         }

         Attributes attrs = result.getAttributes();
         if (attrs == null) {
            throw new FailedLoginException("User found, but LDAP entry malformed: " + username);
         }
         List<String> roles = null;
         if (isLoginPropertySet(USER_ROLE_NAME)) {
            roles = addAttributeValues(getLDAPPropertyValue(USER_ROLE_NAME), attrs, roles);
         }

         // check the credentials by binding to server
         if (bindUser(context, dn, password)) {
            // if authenticated add more roles
            roles = getRoles(context, dn, username, roles);
            if (logger.isDebugEnabled()) {
               logger.debug("Roles " + roles + " for user " + username);
            }
            for (String role : roles) {
               groups.add(new RolePrincipal(role));
            }
         } else {
            throw new FailedLoginException("Password does not match for user: " + username);
         }
      } catch (CommunicationException e) {
         closeContext();
         FailedLoginException ex = new FailedLoginException("Error contacting LDAP");
         ex.initCause(e);
         throw ex;
      } catch (NamingException e) {
         closeContext();
         FailedLoginException ex = new FailedLoginException("Error contacting LDAP");
         ex.initCause(e);
         throw ex;
      }

      return true;
   }

   protected List<String> getRoles(DirContext context,
                                   String dn,
                                   String username,
                                   List<String> currentRoles) throws NamingException {
      List<String> list = currentRoles;
      MessageFormat roleSearchMatchingFormat;
      boolean roleSearchSubtreeBool;
      boolean expandRolesBool;
      roleSearchMatchingFormat = new MessageFormat(getLDAPPropertyValue(ROLE_SEARCH_MATCHING));
      roleSearchSubtreeBool = Boolean.valueOf(getLDAPPropertyValue(ROLE_SEARCH_SUBTREE)).booleanValue();
      expandRolesBool = Boolean.valueOf(getLDAPPropertyValue(EXPAND_ROLES)).booleanValue();

      if (list == null) {
         list = new ArrayList<>();
      }
      if (!isLoginPropertySet(ROLE_NAME)) {
         return list;
      }
      String filter = roleSearchMatchingFormat.format(new String[]{doRFC2254Encoding(dn), doRFC2254Encoding(username)});

      SearchControls constraints = new SearchControls();
      if (roleSearchSubtreeBool) {
         constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
      } else {
         constraints.setSearchScope(SearchControls.ONELEVEL_SCOPE);
      }
      if (logger.isDebugEnabled()) {
         logger.debug("Get user roles.");
         logger.debug("Looking for the user roles in LDAP with ");
         logger.debug("  base DN: " + getLDAPPropertyValue(ROLE_BASE));
         logger.debug("  filter: " + filter);
      }
      HashSet<String> haveSeenNames = new HashSet<>();
      Queue<String> pendingNameExpansion = new LinkedList<>();
      NamingEnumeration<SearchResult> results = context.search(getLDAPPropertyValue(ROLE_BASE), filter, constraints);
      while (results.hasMore()) {
         SearchResult result = results.next();
         Attributes attrs = result.getAttributes();
         if (expandRolesBool) {
            haveSeenNames.add(result.getNameInNamespace());
            pendingNameExpansion.add(result.getNameInNamespace());
         }
         if (attrs == null) {
            continue;
         }
         list = addAttributeValues(getLDAPPropertyValue(ROLE_NAME), attrs, list);
      }
      if (expandRolesBool) {
         MessageFormat expandRolesMatchingFormat = new MessageFormat(getLDAPPropertyValue(EXPAND_ROLES_MATCHING));
         while (!pendingNameExpansion.isEmpty()) {
            String name = pendingNameExpansion.remove();
            filter = expandRolesMatchingFormat.format(new String[]{name});
            results = context.search(getLDAPPropertyValue(ROLE_BASE), filter, constraints);
            while (results.hasMore()) {
               SearchResult result = results.next();
               name = result.getNameInNamespace();
               if (!haveSeenNames.contains(name)) {
                  Attributes attrs = result.getAttributes();
                  list = addAttributeValues(getLDAPPropertyValue(ROLE_NAME), attrs, list);
                  haveSeenNames.add(name);
                  pendingNameExpansion.add(name);
               }
            }
         }
      }
      return list;
   }

   protected String doRFC2254Encoding(String inputString) {
      StringBuffer buf = new StringBuffer(inputString.length());
      for (int i = 0; i < inputString.length(); i++) {
         char c = inputString.charAt(i);
         switch (c) {
            case '\\':
               buf.append("\\5c");
               break;
            case '*':
               buf.append("\\2a");
               break;
            case '(':
               buf.append("\\28");
               break;
            case ')':
               buf.append("\\29");
               break;
            case '\0':
               buf.append("\\00");
               break;
            default:
               buf.append(c);
               break;
         }
      }
      return buf.toString();
   }

   protected boolean bindUser(DirContext context, String dn, String password) throws NamingException {
      boolean isValid = false;

      if (logger.isDebugEnabled()) {
         logger.debug("Binding the user.");
      }
      context.addToEnvironment(Context.SECURITY_PRINCIPAL, dn);
      context.addToEnvironment(Context.SECURITY_CREDENTIALS, password);
      try {
         context.getAttributes("", null);
         isValid = true;
         if (logger.isDebugEnabled()) {
            logger.debug("User " + dn + " successfully bound.");
         }
      } catch (AuthenticationException e) {
         isValid = false;
         if (logger.isDebugEnabled()) {
            logger.debug("Authentication failed for dn=" + dn);
         }
      }

      if (isLoginPropertySet(CONNECTION_USERNAME)) {
         context.addToEnvironment(Context.SECURITY_PRINCIPAL, getLDAPPropertyValue(CONNECTION_USERNAME));
      } else {
         context.removeFromEnvironment(Context.SECURITY_PRINCIPAL);
      }
      if (isLoginPropertySet(CONNECTION_PASSWORD)) {
         context.addToEnvironment(Context.SECURITY_CREDENTIALS, getLDAPPropertyValue(CONNECTION_PASSWORD));
      } else {
         context.removeFromEnvironment(Context.SECURITY_CREDENTIALS);
      }

      return isValid;
   }

   private List<String> addAttributeValues(String attrId,
                                           Attributes attrs,
                                           List<String> values) throws NamingException {

      if (attrId == null || attrs == null) {
         return values;
      }
      if (values == null) {
         values = new ArrayList<>();
      }
      Attribute attr = attrs.get(attrId);
      if (attr == null) {
         return values;
      }
      NamingEnumeration<?> e = attr.getAll();
      while (e.hasMore()) {
         String value = (String) e.next();
         values.add(value);
      }
      return values;
   }

   protected void openContext() throws NamingException {
      if (context == null) {
         try {
            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, getLDAPPropertyValue(INITIAL_CONTEXT_FACTORY));
            if (isLoginPropertySet(CONNECTION_USERNAME)) {
               env.put(Context.SECURITY_PRINCIPAL, getLDAPPropertyValue(CONNECTION_USERNAME));
            } else {
               throw new NamingException("Empty username is not allowed");
            }

            if (isLoginPropertySet(CONNECTION_PASSWORD)) {
               env.put(Context.SECURITY_CREDENTIALS, getLDAPPropertyValue(CONNECTION_PASSWORD));
            } else {
               throw new NamingException("Empty password is not allowed");
            }
            env.put(Context.SECURITY_PROTOCOL, getLDAPPropertyValue(CONNECTION_PROTOCOL));
            env.put(Context.PROVIDER_URL, getLDAPPropertyValue(CONNECTION_URL));
            env.put(Context.SECURITY_AUTHENTICATION, getLDAPPropertyValue(AUTHENTICATION));
            context = new InitialDirContext(env);

         } catch (NamingException e) {
            closeContext();
            ActiveMQServerLogger.LOGGER.error(e.toString());
            throw e;
         }
      }
   }

   private String getLDAPPropertyValue(String propertyName) {
      for (LDAPLoginProperty conf : config)
         if (conf.getPropertyName().equals(propertyName))
            return conf.getPropertyValue();
      return null;
   }

   private boolean isLoginPropertySet(String propertyName) {
      for (LDAPLoginProperty conf : config) {
         if (conf.getPropertyName().equals(propertyName) && (conf.getPropertyValue() != null && !"".equals(conf.getPropertyValue())))
            return true;
      }
      return false;
   }

}

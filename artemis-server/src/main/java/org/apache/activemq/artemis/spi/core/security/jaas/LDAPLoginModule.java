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
import javax.naming.PartialResultException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
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
import org.apache.activemq.artemis.utils.PasswordMaskingUtil;
import org.jboss.logging.Logger;

public class LDAPLoginModule implements AuditLoginModule {

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
   private static final String SASL_LOGIN_CONFIG_SCOPE = "saslLoginConfigScope";
   private static final String AUTHENTICATE_USER = "authenticateUser";
   private static final String REFERRAL = "referral";
   private static final String IGNORE_PARTIAL_RESULT_EXCEPTION = "ignorePartialResultException";
   private static final String PASSWORD_CODEC = "passwordCodec";
   private static final String CONNECTION_POOL = "connectionPool";
   private static final String CONNECTION_TIMEOUT = "connectionTimeout";
   private static final String READ_TIMEOUT = "readTimeout";

   protected DirContext context;

   private Subject subject;
   private CallbackHandler handler;
   private LDAPLoginProperty[] config;
   private String username;
   private final Set<RolePrincipal> groups = new HashSet<>();
   private boolean userAuthenticated = false;
   private boolean authenticateUser = true;
   private Subject brokerGssapiIdentity = null;
   private boolean isRoleAttributeSet = false;
   private String roleAttributeName = null;

   private String codecClass = null;

   @Override
   public void initialize(Subject subject,
                          CallbackHandler callbackHandler,
                          Map<String, ?> sharedState,
                          Map<String, ?> options) {
      this.subject = subject;
      this.handler = callbackHandler;

      config = new LDAPLoginProperty[]{new LDAPLoginProperty(INITIAL_CONTEXT_FACTORY, (String) options.get(INITIAL_CONTEXT_FACTORY)),
                                       new LDAPLoginProperty(CONNECTION_URL, (String) options.get(CONNECTION_URL)),
                                       new LDAPLoginProperty(CONNECTION_USERNAME, (String) options.get(CONNECTION_USERNAME)),
                                       new LDAPLoginProperty(CONNECTION_PASSWORD, (String) options.get(CONNECTION_PASSWORD)),
                                       new LDAPLoginProperty(CONNECTION_PROTOCOL, (String) options.get(CONNECTION_PROTOCOL)),
                                       new LDAPLoginProperty(AUTHENTICATION, (String) options.get(AUTHENTICATION)),
                                       new LDAPLoginProperty(USER_BASE, (String) options.get(USER_BASE)),
                                       new LDAPLoginProperty(USER_SEARCH_MATCHING, (String) options.get(USER_SEARCH_MATCHING)),
                                       new LDAPLoginProperty(USER_SEARCH_SUBTREE, (String) options.get(USER_SEARCH_SUBTREE)),
                                       new LDAPLoginProperty(ROLE_BASE, (String) options.get(ROLE_BASE)),
                                       new LDAPLoginProperty(ROLE_NAME, (String) options.get(ROLE_NAME)),
                                       new LDAPLoginProperty(ROLE_SEARCH_MATCHING, (String) options.get(ROLE_SEARCH_MATCHING)),
                                       new LDAPLoginProperty(ROLE_SEARCH_SUBTREE, (String) options.get(ROLE_SEARCH_SUBTREE)),
                                       new LDAPLoginProperty(USER_ROLE_NAME, (String) options.get(USER_ROLE_NAME)),
                                       new LDAPLoginProperty(EXPAND_ROLES, (String) options.get(EXPAND_ROLES)),
                                       new LDAPLoginProperty(EXPAND_ROLES_MATCHING, (String) options.get(EXPAND_ROLES_MATCHING)),
                                       new LDAPLoginProperty(PASSWORD_CODEC, (String) options.get(PASSWORD_CODEC)),
                                       new LDAPLoginProperty(SASL_LOGIN_CONFIG_SCOPE, (String) options.get(SASL_LOGIN_CONFIG_SCOPE)),
                                       new LDAPLoginProperty(AUTHENTICATE_USER, (String) options.get(AUTHENTICATE_USER)),
                                       new LDAPLoginProperty(REFERRAL, (String) options.get(REFERRAL)),
                                       new LDAPLoginProperty(IGNORE_PARTIAL_RESULT_EXCEPTION, (String) options.get(IGNORE_PARTIAL_RESULT_EXCEPTION)),
                                       new LDAPLoginProperty(CONNECTION_POOL, (String) options.get(CONNECTION_POOL)),
                                       new LDAPLoginProperty(CONNECTION_TIMEOUT, (String) options.get(CONNECTION_TIMEOUT)),
                                       new LDAPLoginProperty(READ_TIMEOUT, (String) options.get(READ_TIMEOUT))};

      if (isLoginPropertySet(AUTHENTICATE_USER)) {
         authenticateUser = Boolean.valueOf(getLDAPPropertyValue(AUTHENTICATE_USER));
      }
      isRoleAttributeSet = isLoginPropertySet(ROLE_NAME);
      roleAttributeName = getLDAPPropertyValue(ROLE_NAME);
      codecClass = getLDAPPropertyValue(PASSWORD_CODEC);
   }

   private String getPlainPassword(String password) {
      try {
         return PasswordMaskingUtil.resolveMask(password, codecClass);
      } catch (Exception e) {
         throw new IllegalArgumentException("Failed to decode password", e);
      }
   }

   @Override
   public boolean login() throws LoginException {

      if (!authenticateUser) {
         return false;
      }

      Callback[] callbacks = new Callback[2];

      callbacks[0] = new NameCallback("User name");
      callbacks[1] = new PasswordCallback("Password", false);
      try {
         handler.handle(callbacks);
      } catch (IOException | UnsupportedCallbackException e) {
         throw (LoginException) new LoginException().initCause(e);
      }

      String password = null;

      username = ((NameCallback) callbacks[0]).getName();
      if (username == null)
         return false;

      if (((PasswordCallback) callbacks[1]).getPassword() != null)
         password = new String(((PasswordCallback) callbacks[1]).getPassword());

      /**
       * https://tools.ietf.org/html/rfc4513#section-6.3.1
       *
       * Clients that use the results from a simple Bind operation to make
       * authorization decisions should actively detect unauthenticated Bind
       * requests (by verifying that the supplied password is not empty) and
       * react appropriately.
       */
      if (password == null || password.length() == 0)
         throw new FailedLoginException("Password cannot be null or empty");

      // authenticate will throw LoginException
      // in case of failed authentication
      authenticate(username, password);
      userAuthenticated = true;
      return true;
   }

   @Override
   public boolean logout() throws LoginException {
      clear();
      return true;
   }

   @Override
   public boolean commit() throws LoginException {
      boolean result = userAuthenticated;
      Set<UserPrincipal> authenticatedUsers = subject.getPrincipals(UserPrincipal.class);
      Set<Principal> principals = subject.getPrincipals();
      if (result) {
         principals.add(new UserPrincipal(username));
      }

      // assign roles to any other UserPrincipal
      for (UserPrincipal authenticatedUser : authenticatedUsers) {
         List<String> roles = new ArrayList<>();
         try {
            String dn = resolveDN(authenticatedUser.getName(), roles);
            resolveRolesForDN(context, dn, authenticatedUser.getName(), roles);
         } catch (NamingException e) {
            closeContext();
            FailedLoginException ex = new FailedLoginException("Error contacting LDAP");
            ex.initCause(e);
            throw ex;
         }
      }

      for (RolePrincipal gp : groups) {
         principals.add(gp);
      }
      clear();
      return result;
   }

   private void clear() {
      username = null;
      userAuthenticated = false;
      closeContext();
   }

   @Override
   public boolean abort() throws LoginException {
      registerFailureForAudit(username);
      clear();
      return true;
   }

   protected void closeContext() {
      if (context != null) {
         try {
            context.close();
            context = null;
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.failedToCloseContext(e);
         }
      }
   }

   protected boolean authenticate(String username, String password) throws LoginException {

      List<String> roles = new ArrayList<>();
      try {
         String dn = resolveDN(username, roles);

         // check the credentials by binding to server
         if (bindUser(context, dn, password)) {
            // if authenticated add more roles
            resolveRolesForDN(context, dn, username, roles);
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

   private void resolveRolesForDN(DirContext context, String dn, String username, List<String> roles) throws NamingException {
      addRoles(context, dn, username, roles);
      if (logger.isDebugEnabled()) {
         logger.debug("Roles " + roles + " for user " + username);
      }
      for (String role : roles) {
         groups.add(new RolePrincipal(role));
      }
   }

   private String resolveDN(String username, List<String> roles) throws FailedLoginException {
      String dn = null;

      MessageFormat userSearchMatchingFormat;
      boolean userSearchSubtreeBool;
      boolean ignorePartialResultExceptionBool;

      if (logger.isDebugEnabled()) {
         logger.debug("Create the LDAP initial context.");
      }
      try {
         openContext();
      } catch (Exception ne) {
         FailedLoginException ex = new FailedLoginException("Error opening LDAP connection");
         ex.initCause(ne);
         throw ex;
      }

      if (!isLoginPropertySet(USER_SEARCH_MATCHING)) {
         return username;
      }

      userSearchMatchingFormat = new MessageFormat(getLDAPPropertyValue(USER_SEARCH_MATCHING));
      userSearchSubtreeBool = Boolean.valueOf(getLDAPPropertyValue(USER_SEARCH_SUBTREE)).booleanValue();
      ignorePartialResultExceptionBool = Boolean.valueOf(getLDAPPropertyValue(IGNORE_PARTIAL_RESULT_EXCEPTION)).booleanValue();

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

         NamingEnumeration<SearchResult> results = null;
         try {
            results = Subject.doAs(brokerGssapiIdentity, (PrivilegedExceptionAction< NamingEnumeration<SearchResult>>) () -> context.search(getLDAPPropertyValue(USER_BASE), filter, constraints));
         } catch (PrivilegedActionException e) {
            Exception cause = e.getException();
            FailedLoginException ex = new FailedLoginException("Error executing search query to resolve DN");
            ex.initCause(cause);
            throw ex;
         }

         if (results == null || !results.hasMore()) {
            throw new FailedLoginException("User " + username + " not found in LDAP.");
         }

         SearchResult result = results.next();

         try {
            if (results.hasMore()) {
               // ignore for now
            }
         } catch (PartialResultException e) {
            // Workaround for AD servers not handling referrals correctly.
            if (ignorePartialResultExceptionBool) {
               logger.debug("PartialResultException encountered and ignored", e);
            } else {
               throw e;
            }
         }

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
         if (isLoginPropertySet(USER_ROLE_NAME)) {
            Attribute roleNames = attrs.get(getLDAPPropertyValue(USER_ROLE_NAME));
            if (roleNames != null) {
               NamingEnumeration<?> e = roleNames.getAll();
               while (e.hasMore()) {
                  String roleDnString = (String) e.next();
                  if (isRoleAttributeSet) {
                     // parse out the attribute from the group Dn
                     LdapName ldapRoleName = new LdapName(roleDnString);
                     for (int i = 0; i < ldapRoleName.size(); i++) {
                        Rdn candidate = ldapRoleName.getRdn(i);
                        if (roleAttributeName.equals(candidate.getType())) {
                           roles.add((String) candidate.getValue());
                        }
                     }
                  } else {
                     roles.add(roleDnString);
                  }
               }
            }
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

      return dn;
   }

   protected void addRoles(DirContext context,
                                   String dn,
                                   String username,
                                   List<String> currentRoles) throws NamingException {

      if (!isLoginPropertySet(ROLE_SEARCH_MATCHING)) {
         return;
      }

      MessageFormat roleSearchMatchingFormat;
      boolean roleSearchSubtreeBool;
      boolean expandRolesBool;
      boolean ignorePartialResultExceptionBool;
      roleSearchMatchingFormat = new MessageFormat(getLDAPPropertyValue(ROLE_SEARCH_MATCHING));
      roleSearchSubtreeBool = Boolean.valueOf(getLDAPPropertyValue(ROLE_SEARCH_SUBTREE)).booleanValue();
      expandRolesBool = Boolean.valueOf(getLDAPPropertyValue(EXPAND_ROLES)).booleanValue();
      ignorePartialResultExceptionBool = Boolean.valueOf(getLDAPPropertyValue(IGNORE_PARTIAL_RESULT_EXCEPTION)).booleanValue();

      final String filter = roleSearchMatchingFormat.format(new String[]{doRFC2254Encoding(dn), doRFC2254Encoding(username)});

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
      NamingEnumeration<SearchResult> results = null;
      try {
         results = Subject.doAs(brokerGssapiIdentity, (PrivilegedExceptionAction< NamingEnumeration<SearchResult>>) () -> context.search(getLDAPPropertyValue(ROLE_BASE), filter, constraints));
      } catch (PrivilegedActionException e) {
         Exception cause = e.getException();
         NamingException ex = new NamingException("Error executing search query to resolve roles");
         ex.initCause(cause);
         throw ex;
      }

      try {
         while (results.hasMore()) {
            SearchResult result = results.next();
            if (expandRolesBool) {
               haveSeenNames.add(result.getNameInNamespace());
               pendingNameExpansion.add(result.getNameInNamespace());
            }
            addRoleAttribute(result, currentRoles);
         }
      } catch (PartialResultException e) {
         // Workaround for AD servers not handling referrals correctly.
         if (ignorePartialResultExceptionBool) {
            logger.debug("PartialResultException encountered and ignored", e);
         } else {
            throw e;
         }
      }
      if (expandRolesBool) {
         MessageFormat expandRolesMatchingFormat = new MessageFormat(getLDAPPropertyValue(EXPAND_ROLES_MATCHING));
         while (!pendingNameExpansion.isEmpty()) {
            String name = pendingNameExpansion.remove();
            final String expandFilter = expandRolesMatchingFormat.format(new String[]{name});
            if (logger.isDebugEnabled()) {
               logger.debug("Get 'expanded' user roles.");
               logger.debug("Looking for the 'expanded' user roles in LDAP with ");
               logger.debug("  base DN: " + getLDAPPropertyValue(ROLE_BASE));
               logger.debug("  filter: " + expandFilter);
            }
            try {
               results = Subject.doAs(brokerGssapiIdentity, (PrivilegedExceptionAction< NamingEnumeration<SearchResult>>) () -> context.search(getLDAPPropertyValue(ROLE_BASE), expandFilter, constraints));
            } catch (PrivilegedActionException e) {
               Exception cause = e.getException();
               NamingException ex = new NamingException("Error executing search query to expand roles");
               ex.initCause(cause);
               throw ex;
            }
            try {
               while (results.hasMore()) {
                  SearchResult result = results.next();
                  name = result.getNameInNamespace();
                  if (!haveSeenNames.contains(name)) {
                     addRoleAttribute(result, currentRoles);
                     haveSeenNames.add(name);
                     pendingNameExpansion.add(name);
                  }
               }
            } catch (PartialResultException e) {
               // Workaround for AD servers not handling referrals correctly.
               if (ignorePartialResultExceptionBool) {
                  logger.debug("PartialResultException encountered and ignored", e);
               } else {
                  throw e;
               }
            }
         }
      }
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
      context.addToEnvironment(Context.SECURITY_AUTHENTICATION, "simple");
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
         context.addToEnvironment(Context.SECURITY_CREDENTIALS, getPlainPassword(getLDAPPropertyValue(CONNECTION_PASSWORD)));
      } else {
         context.removeFromEnvironment(Context.SECURITY_CREDENTIALS);
      }
      context.addToEnvironment(Context.SECURITY_AUTHENTICATION, getLDAPPropertyValue(AUTHENTICATION));

      return isValid;
   }

   private void addRoleAttribute(SearchResult searchResult, List<String> roles) throws NamingException {
      if (isRoleAttributeSet) {
         Attribute roleAttribute = searchResult.getAttributes().get(roleAttributeName);
         if (roleAttribute != null) {
            roles.add((String) roleAttribute.get());
         }
      } else {
         roles.add(searchResult.getNameInNamespace());
      }
   }


   protected void openContext() throws Exception {
      if (context == null) {
         try {
            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, getLDAPPropertyValue(INITIAL_CONTEXT_FACTORY));
            env.put(Context.SECURITY_PROTOCOL, getLDAPPropertyValue(CONNECTION_PROTOCOL));
            env.put(Context.PROVIDER_URL, getLDAPPropertyValue(CONNECTION_URL));
            env.put(Context.SECURITY_AUTHENTICATION, getLDAPPropertyValue(AUTHENTICATION));
            if (isLoginPropertySet(CONNECTION_POOL)) {
               env.put("com.sun.jndi.ldap.connect.pool", getLDAPPropertyValue(CONNECTION_POOL));
            }
            if (isLoginPropertySet(CONNECTION_TIMEOUT)) {
               env.put("com.sun.jndi.ldap.connect.timeout", getLDAPPropertyValue(CONNECTION_TIMEOUT));
            }
            if (isLoginPropertySet(READ_TIMEOUT)) {
               env.put("com.sun.jndi.ldap.read.timeout", getLDAPPropertyValue(READ_TIMEOUT));
            }

            // handle LDAP referrals
            // valid values are "throw", "ignore" and "follow"
            String referral = "ignore";
            if (getLDAPPropertyValue(REFERRAL) != null) {
               referral = getLDAPPropertyValue(REFERRAL);
            }

            env.put(Context.REFERRAL, referral);
            if (logger.isDebugEnabled()) {
               logger.debug("Referral handling: " + referral);
            }

            if ("GSSAPI".equalsIgnoreCase(getLDAPPropertyValue(AUTHENTICATION))) {

               final String configScope = isLoginPropertySet(SASL_LOGIN_CONFIG_SCOPE) ? getLDAPPropertyValue(SASL_LOGIN_CONFIG_SCOPE) : "broker-sasl-gssapi";
               try {
                  LoginContext loginContext = new LoginContext(configScope);
                  loginContext.login();
                  brokerGssapiIdentity = loginContext.getSubject();
               } catch (LoginException e) {
                  e.printStackTrace();
                  FailedLoginException ex = new FailedLoginException("Error contacting LDAP using GSSAPI in JAAS loginConfigScope: " + configScope);
                  ex.initCause(e);
                  throw ex;
               }

            } else {

               if (isLoginPropertySet(CONNECTION_USERNAME)) {
                  env.put(Context.SECURITY_PRINCIPAL, getLDAPPropertyValue(CONNECTION_USERNAME));
               } else {
                  throw new NamingException("Empty username is not allowed");
               }

               if (isLoginPropertySet(CONNECTION_PASSWORD)) {
                  env.put(Context.SECURITY_CREDENTIALS, getPlainPassword(getLDAPPropertyValue(CONNECTION_PASSWORD)));
               } else {
                  throw new NamingException("Empty password is not allowed");
               }
            }

            try {
               context = Subject.doAs(brokerGssapiIdentity, (PrivilegedExceptionAction<DirContext>) () -> new InitialDirContext(env));
            } catch (PrivilegedActionException e) {
               throw e.getException();
            }

         } catch (NamingException e) {
            closeContext();
            ActiveMQServerLogger.LOGGER.failedToOpenContext(e);
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

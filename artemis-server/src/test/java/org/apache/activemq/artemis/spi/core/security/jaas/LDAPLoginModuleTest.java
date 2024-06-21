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

import javax.naming.Context;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.security.auth.Subject;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(FrameworkRunner.class)
@CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP", port = 1024)}, allowAnonymousAccess = true)
@ApplyLdifFiles("test.ldif")
public class LDAPLoginModuleTest extends AbstractLdapTestUnit {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String PRINCIPAL = "uid=admin,ou=system";
   private static final String CREDENTIALS = "secret";

   private final String loginConfigSysPropName = "java.security.auth.login.config";
   private String oldLoginConfig;

   @Before
   public void setLoginConfigSysProperty() {
      oldLoginConfig = System.getProperty(loginConfigSysPropName, null);
      System.setProperty(loginConfigSysPropName, "src/test/resources/login.config");
   }

   @After
   public void resetLoginConfigSysProperty() {
      if (oldLoginConfig != null) {
         System.setProperty(loginConfigSysPropName, oldLoginConfig);
      }
   }

   @Test
   public void testRunning() throws Exception {

      Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.PROVIDER_URL, "ldap://localhost:1024");
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      env.put(Context.SECURITY_AUTHENTICATION, "simple");
      env.put(Context.SECURITY_PRINCIPAL, PRINCIPAL);
      env.put(Context.SECURITY_CREDENTIALS, CREDENTIALS);
      DirContext ctx = new InitialDirContext(env);

      HashSet<String> set = new HashSet<>();

      NamingEnumeration<NameClassPair> list = ctx.list("ou=system");

      while (list.hasMore()) {
         NameClassPair ncp = list.next();
         set.add(ncp.getName());
      }

      assertTrue(set.contains("uid=admin"));
      assertTrue(set.contains("ou=users"));
      assertTrue(set.contains("ou=groups"));
      assertTrue(set.contains("ou=configuration"));
      assertTrue(set.contains("prefNodeName=sysPrefRoot"));

   }

   @Test
   public void testLogin() throws Exception {
      logger.info("num session: {}", ldapServer.getLdapSessionManager().getSessions().length);

      LoginContext context = new LoginContext("LDAPLogin", callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback) {
               ((NameCallback) callbacks[i]).setName("first");
            } else if (callbacks[i] instanceof PasswordCallback) {
               ((PasswordCallback) callbacks[i]).setPassword("secret".toCharArray());
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      });
      context.login();
      context.logout();

      assertTrue("sessions still active after logout", waitFor(() -> ldapServer.getLdapSessionManager().getSessions().length == 0));
   }

   public interface Condition {
      boolean isSatisfied() throws Exception;
   }

   private boolean waitFor(final Condition condition) throws Exception {
      final long expiry = System.currentTimeMillis() + 5000;
      boolean conditionSatisified = condition.isSatisfied();
      while (!conditionSatisified && System.currentTimeMillis() < expiry) {
         TimeUnit.MILLISECONDS.sleep(100);
         conditionSatisified = condition.isSatisfied();
      }
      return conditionSatisified;
   }

   @Test
   public void testUnauthenticated() throws Exception {
      LoginContext context = new LoginContext("UnAuthenticatedLDAPLogin", callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback) {
               ((NameCallback) callbacks[i]).setName("first");
            } else if (callbacks[i] instanceof PasswordCallback) {
               ((PasswordCallback) callbacks[i]).setPassword("secret".toCharArray());
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      });
      try {
         context.login();
      } catch (LoginException le) {
         assertEquals(le.getCause().getMessage(), "Empty password is not allowed");
         return;
      }
      fail("Should have failed authenticating");
      assertTrue("sessions still active after logout", waitFor(() -> ldapServer.getLdapSessionManager().getSessions().length == 0));
   }


   @Test
   public void testAuthenticatedViaBindOnAnonConnection() throws Exception {
      LoginContext context = new LoginContext("AnonBindCheckUserLDAPLogin", callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback) {
               ((NameCallback) callbacks[i]).setName("first");
            } else if (callbacks[i] instanceof PasswordCallback) {
               ((PasswordCallback) callbacks[i]).setPassword("wrongSecret".toCharArray());
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      });
      try {
         context.login();
         fail("Should have failed authenticating");
      } catch (FailedLoginException expected) {
      }
      assertTrue("sessions still active after logout", waitFor(() -> ldapServer.getLdapSessionManager().getSessions().length == 0));
   }

   @Test
   public void testAuthenticatedOkViaBindOnAnonConnection() throws Exception {
      LoginContext context = new LoginContext("AnonBindCheckUserLDAPLogin", callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback) {
               ((NameCallback) callbacks[i]).setName("first");
            } else if (callbacks[i] instanceof PasswordCallback) {
               ((PasswordCallback) callbacks[i]).setPassword("secret".toCharArray());
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      });
      context.login();
      context.logout();
      assertTrue("sessions still active after logout", waitFor(() -> ldapServer.getLdapSessionManager().getSessions().length == 0));
   }

   @Test
   public void testCommitOnFailedLogin() throws LoginException {
      LoginModule loginModule = new LDAPLoginModule();
      JaasCallbackHandler callbackHandler = new JaasCallbackHandler(null, null, null);

      loginModule.initialize(new Subject(), callbackHandler, null, new HashMap<>());

      // login should return false due to null username
      assertFalse(loginModule.login());

      // since login failed commit should return false as well
      assertFalse(loginModule.commit());
   }

   @Test
   public void testPropertyConfigMap() throws Exception {
      LDAPLoginModule loginModule = new LDAPLoginModule();
      JaasCallbackHandler callbackHandler = new JaasCallbackHandler(null, null, null);

      Field configMap = null;
      HashMap<String, Object> options = new HashMap<>();
      for (Field field: loginModule.getClass().getDeclaredFields()) {
         if (Modifier.isStatic(field.getModifiers()) && Modifier.isFinal(field.getModifiers()) && field.getType().isAssignableFrom(String.class)) {
            field.setAccessible(true);
            options.put((String)field.get(loginModule), "SET");
         }
         if (field.getName().equals("config")) {
            field.setAccessible(true);
            configMap = field;
         }
      }
      loginModule.initialize(new Subject(), callbackHandler, null, options);

      Set<LDAPLoginProperty> ldapProps = (Set<LDAPLoginProperty>) configMap.get(loginModule);
      for (String key: options.keySet()) {
         assertTrue("val set: " + key, presentIn(ldapProps, key));
      }
   }

   @Test
   public void testEmptyPassword() throws Exception {
      LoginContext context = new LoginContext("LDAPLogin", callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback) {
               ((NameCallback) callbacks[i]).setName("first");
            } else if (callbacks[i] instanceof PasswordCallback) {
               ((PasswordCallback) callbacks[i]).setPassword("".toCharArray());
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      });
      try {
         context.login();
         fail("Should have thrown a FailedLoginException");
      } catch (FailedLoginException fle) {
         assertEquals("Password cannot be null or empty", fle.getMessage());
      }
      context.logout();
   }

   @Test
   public void testNullPassword() throws Exception {
      LoginContext context = new LoginContext("LDAPLogin", callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback) {
               ((NameCallback) callbacks[i]).setName("first");
            } else if (callbacks[i] instanceof PasswordCallback) {
               ((PasswordCallback) callbacks[i]).setPassword(null);
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      });
      try {
         context.login();
         fail("Should have thrown a FailedLoginException");
      } catch (FailedLoginException fle) {
         assertEquals("Password cannot be null or empty", fle.getMessage());
      }
      context.logout();
   }
   @Test
   public void testEnvironmentProperties() throws Exception {
      HashMap<String, Object> options = new HashMap<>();

      // set module configs
      for (LDAPLoginModule.ConfigKey configKey: LDAPLoginModule.ConfigKey.values()) {
         if (configKey.getName().equals("initialContextFactory")) {
            options.put(configKey.getName(), "com.sun.jndi.ldap.LdapCtxFactory");
         } else if (configKey.getName().equals("connectionURL")) {
            options.put(configKey.getName(), "ldap://localhost:1024");
         } else if (configKey.getName().equals("referral")) {
            options.put(configKey.getName(), "ignore");
         } else if (configKey.getName().equals("connectionTimeout")) {
            options.put(configKey.getName(), "10000");
         } else if (configKey.getName().equals("readTimeout")) {
            options.put(configKey.getName(), "11000");
         } else if (configKey.getName().equals("authentication")) {
            options.put(configKey.getName(), "simple");
         } else if (configKey.getName().equals("connectionUsername")) {
            options.put(configKey.getName(), PRINCIPAL);
         } else if (configKey.getName().equals("connectionPassword")) {
            options.put(configKey.getName(), CREDENTIALS);
         } else if (configKey.getName().equals("connectionProtocol")) {
            options.put(configKey.getName(), "s");
         } else if (configKey.getName().equals("debug")) {
            options.put(configKey.getName(), "true");
         } else {
            options.put(configKey.getName(), configKey.getName() + "_value_set");
         }
      }

      // add extra configs
      options.put("com.sun.jndi.ldap.tls.cbtype", "tls-server-end-point");
      options.put("randomConfig", "some-value");

      // add non-strings configs
      options.put("non.string.1", new Object());
      options.put("non.string.2", 1);

      // create context
      LDAPLoginModule loginModule = new LDAPLoginModule();
      loginModule.initialize(new Subject(), null, null, options);
      loginModule.openContext();

      // get created environment
      Hashtable<?, ?> environment = loginModule.context.getEnvironment();
      // cleanup
      loginModule.closeContext();

      // module config keys should not be passed to environment
      for (LDAPLoginModule.ConfigKey configKey: LDAPLoginModule.ConfigKey.values()) {
         assertEquals("value should not be set for key: " + configKey.getName(), null, environment.get(configKey.getName()));
      }

      // extra, non-module configs should be passed to environment
      assertEquals("value should be set for key: " + "com.sun.jndi.ldap.tls.cbtype", "tls-server-end-point", environment.get("com.sun.jndi.ldap.tls.cbtype"));
      assertEquals("value should be set for key: " + "randomConfig", "some-value", environment.get("randomConfig"));

      // non-string configs should not be passed to environment
      assertEquals("value should not be set for key: " + "non.string.1", null, environment.get("non.string.1"));
      assertEquals("value should not be set for key: " + "non.string.2", null, environment.get("non.string.2"));

      // environment configs should be set
      assertEquals("value should be set for key: " + Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory", environment.get(Context.INITIAL_CONTEXT_FACTORY));
      assertEquals("value should be set for key: " + Context.PROVIDER_URL, "ldap://localhost:1024", environment.get(Context.PROVIDER_URL));
      assertEquals("value should be set for key: " + Context.REFERRAL, "ignore", environment.get(Context.REFERRAL));
      assertEquals("value should be set for key: " + "com.sun.jndi.ldap.connect.timeout", "10000", environment.get("com.sun.jndi.ldap.connect.timeout"));
      assertEquals("value should be set for key: " + "com.sun.jndi.ldap.read.timeout", "11000", environment.get("com.sun.jndi.ldap.read.timeout"));
      assertEquals("value should be set for key: " + Context.SECURITY_AUTHENTICATION, "simple", environment.get(Context.SECURITY_AUTHENTICATION));
      assertEquals("value should be set for key: " + Context.SECURITY_PRINCIPAL, PRINCIPAL, environment.get(Context.SECURITY_PRINCIPAL));
      assertEquals("value should be set for key: " + Context.SECURITY_CREDENTIALS, CREDENTIALS, environment.get(Context.SECURITY_CREDENTIALS));
      assertEquals("value should be set for key: " + Context.SECURITY_PROTOCOL, "s", environment.get(Context.SECURITY_PROTOCOL));
   }

   private boolean presentIn(Set<LDAPLoginProperty> ldapProps, String propertyName) {
      for (LDAPLoginProperty conf : ldapProps) {
         if (conf.getPropertyName().equals(propertyName) && (conf.getPropertyValue() != null && !"".equals(conf.getPropertyValue())))
            return true;
      }
      return false;
   }

}

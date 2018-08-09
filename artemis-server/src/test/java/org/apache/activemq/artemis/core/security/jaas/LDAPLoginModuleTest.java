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
package org.apache.activemq.artemis.core.security.jaas;

import javax.naming.Context;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.spi.core.security.jaas.JaasCallbackHandler;
import org.apache.activemq.artemis.spi.core.security.jaas.LDAPLoginModule;
import org.apache.activemq.artemis.spi.core.security.jaas.LDAPLoginProperty;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(FrameworkRunner.class)
@CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP", port = 1024)})
@ApplyLdifFiles("test.ldif")
public class LDAPLoginModuleTest extends AbstractLdapTestUnit {

   private static final Logger logger = Logger.getLogger(LDAPLoginModuleTest.class);

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
      logger.info("num session: " + ldapServer.getLdapSessionManager().getSessions().length);

      LoginContext context = new LoginContext("LDAPLogin", new CallbackHandler() {
         @Override
         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (int i = 0; i < callbacks.length; i++) {
               if (callbacks[i] instanceof NameCallback) {
                  ((NameCallback) callbacks[i]).setName("first");
               } else if (callbacks[i] instanceof PasswordCallback) {
                  ((PasswordCallback) callbacks[i]).setPassword("secret".toCharArray());
               } else {
                  throw new UnsupportedCallbackException(callbacks[i]);
               }
            }
         }
      });
      context.login();
      context.logout();

      assertTrue("sessions still active after logout", waitFor(() -> ldapServer.getLdapSessionManager().getSessions().length == 0));
   }

   @Test
   public void testLoginPooled() throws Exception {
      CallbackHandler callbackHandler = callbacks -> {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback) {
               ((NameCallback) callbacks[i]).setName("first");
            } else if (callbacks[i] instanceof PasswordCallback) {
               ((PasswordCallback) callbacks[i]).setPassword("secret".toCharArray());
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      };

      LoginContext context = new LoginContext("LDAPLoginPooled", callbackHandler);
      context.login();
      context.logout();

      // again
      context.login();
      context.logout();

      // new context
      context = new LoginContext("LDAPLoginPooled", callbackHandler);
      context.login();
      context.logout();

      Executor pool = Executors.newCachedThreadPool();
      for (int i = 0; i < 20; i++) {
         pool.execute(() -> {
            try {
               LoginContext context1 = new LoginContext("LDAPLoginPooled", callbackHandler);
               context1.login();
               context1.logout();
            } catch (Exception ignored) {
            }
         });
      }

      /*
       * The number of sessions here is variable due to the pool used to create the LoginContext objects and the pooling
       * for the LDAP connections (which are managed by the JVM implementation). We really just need to confirm that
       * there are still connections to the LDAP server open even after all the LoginContext objects are closed as that
       * will indicate the LDAP connection pooling is working.
       */
      assertTrue("not enough active sessions after logout", waitFor(() -> ldapServer.getLdapSessionManager().getSessions().length >= 5));

      ((ExecutorService) pool).shutdown();
      ((ExecutorService) pool).awaitTermination(2, TimeUnit.SECONDS);
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
      LoginContext context = new LoginContext("UnAuthenticatedLDAPLogin", new CallbackHandler() {
         @Override
         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (int i = 0; i < callbacks.length; i++) {
               if (callbacks[i] instanceof NameCallback) {
                  ((NameCallback) callbacks[i]).setName("first");
               } else if (callbacks[i] instanceof PasswordCallback) {
                  ((PasswordCallback) callbacks[i]).setPassword("secret".toCharArray());
               } else {
                  throw new UnsupportedCallbackException(callbacks[i]);
               }
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
   public void testCommitOnFailedLogin() throws LoginException {
      LoginModule loginModule = new LDAPLoginModule();
      JaasCallbackHandler callbackHandler = new JaasCallbackHandler(null, null, null);

      loginModule.initialize(new Subject(), callbackHandler, null, new HashMap<String, Object>());

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

      LDAPLoginProperty[] ldapProps = (LDAPLoginProperty[]) configMap.get(loginModule);
      for (String key: options.keySet()) {
         assertTrue("val set: " + key, presentInArray(ldapProps, key));
      }
   }

   @Test
   public void testEmptyPassword() throws Exception {
      LoginContext context = new LoginContext("LDAPLogin", new CallbackHandler() {
         @Override
         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (int i = 0; i < callbacks.length; i++) {
               if (callbacks[i] instanceof NameCallback) {
                  ((NameCallback) callbacks[i]).setName("first");
               } else if (callbacks[i] instanceof PasswordCallback) {
                  ((PasswordCallback) callbacks[i]).setPassword("".toCharArray());
               } else {
                  throw new UnsupportedCallbackException(callbacks[i]);
               }
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
      LoginContext context = new LoginContext("LDAPLogin", new CallbackHandler() {
         @Override
         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (int i = 0; i < callbacks.length; i++) {
               if (callbacks[i] instanceof NameCallback) {
                  ((NameCallback) callbacks[i]).setName("first");
               } else if (callbacks[i] instanceof PasswordCallback) {
                  ((PasswordCallback) callbacks[i]).setPassword(null);
               } else {
                  throw new UnsupportedCallbackException(callbacks[i]);
               }
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

   private boolean presentInArray(LDAPLoginProperty[] ldapProps, String propertyName) {
      for (LDAPLoginProperty conf : ldapProps) {
         if (conf.getPropertyName().equals(propertyName) && (conf.getPropertyValue() != null && !"".equals(conf.getPropertyValue())))
            return true;
      }
      return false;
   }

}

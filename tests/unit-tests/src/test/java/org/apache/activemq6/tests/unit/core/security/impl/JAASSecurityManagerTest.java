/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.tests.unit.core.security.impl;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.IOException;
import java.security.Principal;
import java.security.acl.Group;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.activemq6.core.security.CheckType;
import org.apache.activemq6.core.security.Role;
import org.apache.activemq6.spi.core.security.JAASSecurityManager;
import org.apache.activemq6.tests.util.UnitTestCase;
import org.jboss.security.SimpleGroup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * tests the JAASSecurityManager
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JAASSecurityManagerTest extends UnitTestCase
{
   private JAASSecurityManager securityManager;

   private static final String USER = "user";

   private static final String PASSWORD = "password";

   private static final String INVALID_PASSWORD = "invalidPassword";

   private static final String ROLE = "role";

   private static final String INVALID_ROLE = "invalidRole";

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      securityManager = new JAASSecurityManager();

      final String domainName = SimpleLogingModule.class.getName();
      // pass the correct user/pass and a role as options to the login module
      final Map<String, String> options = new HashMap<String, String>();
      options.put("user", JAASSecurityManagerTest.USER);
      options.put("pass", JAASSecurityManagerTest.PASSWORD);
      options.put("role", JAASSecurityManagerTest.ROLE);

      securityManager.setConfigurationName(domainName);
      securityManager.setCallbackHandler(new CallbackHandler()
      {
         public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException
         {
            // empty callback, auth info are directly passed as options to the login module
         }
      });
      securityManager.setConfiguration(new SimpleConfiguration(domainName, options));

   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      securityManager = null;

      super.tearDown();
   }

   @Test
   public void testValidatingUser()
   {
      Assert.assertTrue(securityManager.validateUser(JAASSecurityManagerTest.USER, JAASSecurityManagerTest.PASSWORD));
      Assert.assertFalse(securityManager.validateUser(JAASSecurityManagerTest.USER,
                                                      JAASSecurityManagerTest.INVALID_PASSWORD));
   }

   @Test
   public void testValidatingUserAndRole()
   {
      Set<Role> roles = new HashSet<Role>();
      roles.add(new Role(JAASSecurityManagerTest.ROLE, true, true, true, true, true, true, true));

      Assert.assertTrue(securityManager.validateUserAndRole(JAASSecurityManagerTest.USER,
                                                            JAASSecurityManagerTest.PASSWORD,
                                                            roles,
                                                            CheckType.CREATE_DURABLE_QUEUE));

      roles.clear();
      roles.add(new Role(JAASSecurityManagerTest.INVALID_ROLE, true, true, true, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole(JAASSecurityManagerTest.USER,
                                                             JAASSecurityManagerTest.PASSWORD,
                                                             roles,
                                                             CheckType.CREATE_DURABLE_QUEUE));
   }

   public static class SimpleLogingModule implements LoginModule
   {

      private Map<String, ?> options;

      private Subject subject;

      public SimpleLogingModule()
      {
      }

      public boolean abort() throws LoginException
      {
         return true;
      }

      public boolean commit() throws LoginException
      {
         return true;
      }

      public void initialize(final Subject subject,
                             final CallbackHandler callbackHandler,
                             final Map<String, ?> sharedState,
                             final Map<String, ?> options)
      {
         this.subject = subject;
         this.options = options;
      }

      public boolean login() throws LoginException
      {
         Iterator<char[]> iterator = subject.getPrivateCredentials(char[].class).iterator();
         char[] passwordChars = iterator.next();
         String password = new String(passwordChars);
         Iterator<Principal> iterator2 = subject.getPrincipals().iterator();
         String user = iterator2.next().getName();

         boolean authenticated = user.equals(options.get("user")) && password.equals(options.get("pass"));

         if (authenticated)
         {
            Group roles = new SimpleGroup("Roles");
            roles.addMember(new JAASSecurityManager.SimplePrincipal((String) options.get("role")));
            subject.getPrincipals().add(roles);
         }
         return authenticated;

      }

      public Subject getSubject()
      {
         return subject;
      }

      public boolean logout() throws LoginException
      {
         return true;
      }
   }

   public static class SimpleConfiguration extends Configuration
   {
      private final Map<String, ?> options;

      private final String loginModuleName;

      public SimpleConfiguration(final String loginModuleName, final Map<String, ?> options)
      {
         this.loginModuleName = loginModuleName;
         this.options = options;
      }

      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(final String name)
      {
         AppConfigurationEntry entry = new AppConfigurationEntry(loginModuleName,
                                                                 LoginModuleControlFlag.REQUIRED,
                                                                 options);
         return new AppConfigurationEntry[]{entry};
      }

      @Override
      public void refresh()
      {
      }
   }
}

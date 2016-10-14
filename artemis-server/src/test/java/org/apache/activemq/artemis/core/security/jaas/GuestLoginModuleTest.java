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

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal;
import org.apache.activemq.artemis.spi.core.security.jaas.UserPrincipal;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

public class GuestLoginModuleTest extends Assert {

   private static final Logger logger = Logger.getLogger(GuestLoginModuleTest.class);

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = GuestLoginModuleTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            try {
               path = URLDecoder.decode(resource.getFile(), StandardCharsets.UTF_8.name());
               System.setProperty("java.security.auth.login.config", path);
            } catch (UnsupportedEncodingException e) {
               logger.error(e.getMessage(), e);
               throw new RuntimeException(e);
            }
         }
      }
   }

   @Test
   public void testLogin() throws LoginException {
      LoginContext context = new LoginContext("GuestLogin", new CallbackHandler() {
         @Override
         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            assertEquals("Should have no Callbacks", 0, callbacks.length);
         }
      });
      context.login();

      Subject subject = context.getSubject();

      assertEquals("Should have two principals", 2, subject.getPrincipals().size());
      assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
      assertTrue("User principal is 'foo'", subject.getPrincipals(UserPrincipal.class).contains(new UserPrincipal("foo")));

      assertEquals("Should have one group principal", 1, subject.getPrincipals(RolePrincipal.class).size());
      assertTrue("Role principal is 'bar'", subject.getPrincipals(RolePrincipal.class).contains(new RolePrincipal("bar")));

      context.logout();

      assertEquals("Should have zero principals", 0, subject.getPrincipals().size());
   }

   @Test
   public void testLoginWithDefaults() throws LoginException {
      LoginContext context = new LoginContext("GuestLoginWithDefaults", new CallbackHandler() {
         @Override
         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            assertEquals("Should have no Callbacks", 0, callbacks.length);
         }
      });
      context.login();

      Subject subject = context.getSubject();

      assertEquals("Should have two principals", 2, subject.getPrincipals().size());
      assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
      assertTrue("User principal is 'guest'", subject.getPrincipals(UserPrincipal.class).contains(new UserPrincipal("guest")));

      assertEquals("Should have one group principal", 1, subject.getPrincipals(RolePrincipal.class).size());
      assertTrue("Role principal is 'guests'", subject.getPrincipals(RolePrincipal.class).contains(new RolePrincipal("guests")));

      context.logout();

      assertEquals("Should have zero principals", 0, subject.getPrincipals().size());
   }
}

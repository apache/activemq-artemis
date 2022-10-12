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
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.activemq.artemis.core.server.impl.ServerStatus;
import org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal;
import org.apache.activemq.artemis.spi.core.security.jaas.UserPrincipal;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.junit.Assert;
import org.junit.Test;

public class PropertiesLoginModuleTest extends Assert {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = PropertiesLoginModuleTest.class.getClassLoader().getResource("login.config");
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
      LoginContext context = new LoginContext("PropertiesLogin", new UserPassHandler("first", "secret"));

      context.login();

      Subject subject = context.getSubject();

      assertEquals("Should have three principals", 3, subject.getPrincipals().size());
      assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
      assertEquals("Should have two group principals", 2, subject.getPrincipals(RolePrincipal.class).size());

      context.logout();

      assertEquals("Should have zero principals", 0, subject.getPrincipals().size());
   }

   @Test
   public void testLoginMasked() throws LoginException {
      LoginContext context = new LoginContext("PropertiesLogin", new UserPassHandler("third", "helloworld"));
      context.login();
      context.logout();
   }

   @Test
   public void testLoginReload() throws Exception {
      File targetPropDir = new File("target/loginReloadTest");
      File usersFile = new File(targetPropDir, "users.properties");
      File rolesFile = new File(targetPropDir, "roles.properties");

      //Set up initial properties
      FileUtils.copyFile(new File(getClass().getResource("/users.properties").toURI()), usersFile);
      FileUtils.copyFile(new File(getClass().getResource("/roles.properties").toURI()), rolesFile);

      LoginContext context = new LoginContext("PropertiesLoginReload", new UserPassHandler("first", "secret"));
      context.login();
      Subject subject = context.getSubject();

      //test initial principals
      assertEquals("Should have three principals", 3, subject.getPrincipals().size());
      assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
      assertEquals("Should have two group principals", 2, subject.getPrincipals(RolePrincipal.class).size());

      String hashOriginal = genHash(usersFile);
      assertTrue("Contains hash", ServerStatus.getInstance().asJson().contains(hashOriginal));

      context.logout();

      assertEquals("Should have zero principals", 0, subject.getPrincipals().size());

      //Modify the file and test that the properties are reloaded
      Thread.sleep(1000);
      FileUtils.copyFile(new File(getClass().getResource("/usersReload.properties").toURI()), usersFile);
      FileUtils.copyFile(new File(getClass().getResource("/rolesReload.properties").toURI()), rolesFile);
      FileUtils.touch(usersFile);
      FileUtils.touch(rolesFile);

      //Use new password to verify  users file was reloaded
      context = new LoginContext("PropertiesLoginReload", new UserPassHandler("first", "secrets"));
      context.login();
      subject = context.getSubject();

      //Check that the principals changed
      assertEquals("Should have three principals", 2, subject.getPrincipals().size());
      assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
      assertEquals("Should have one group principals", 1, subject.getPrincipals(RolePrincipal.class).size());

      context.logout();

      String hashUpdated = genHash(usersFile);
      assertTrue("Contains hashUpdated", ServerStatus.getInstance().asJson().contains(hashUpdated));
      assertNotEquals(hashOriginal, hashUpdated);

      assertEquals("Should have zero principals", 0, subject.getPrincipals().size());
   }

   private String genHash(File usersFile) {
      Checksum hash = new Adler32();

      try (FileReader fileReader = new FileReader(usersFile); BufferedReader bufferedReader = new BufferedReader(fileReader)) {
         String line = null;
         while ((line = bufferedReader.readLine()) != null) {
            if (!line.startsWith("#") && !line.isBlank()) {
               hash.update(line.getBytes(StandardCharsets.UTF_8));
            }
         }
      } catch (Exception expectedEof) {
      }
      return String.valueOf(hash.getValue());
   }

   @Test
   public void testBadUseridLogin() throws Exception {
      LoginContext context = new LoginContext("PropertiesLogin", new UserPassHandler("BAD", "secret"));

      try {
         context.login();
         fail("Should have thrown a FailedLoginException");
      } catch (FailedLoginException doNothing) {
      }

   }

   @Test
   public void testBadPWLogin() throws Exception {
      LoginContext context = new LoginContext("PropertiesLogin", new UserPassHandler("first", "BAD"));

      try {
         context.login();
         fail("Should have thrown a FailedLoginException");
      } catch (FailedLoginException doNothing) {
      }

   }

   private static class UserPassHandler implements CallbackHandler {

      private final String user;
      private final String pass;

      private UserPassHandler(final String user, final String pass) {
         this.user = user;
         this.pass = pass;
      }

      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
         for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof NameCallback) {
               ((NameCallback) callbacks[i]).setName(user);
            } else if (callbacks[i] instanceof PasswordCallback) {
               ((PasswordCallback) callbacks[i]).setPassword(pass.toCharArray());
            } else {
               throw new UnsupportedCallbackException(callbacks[i]);
            }
         }
      }
   }
}

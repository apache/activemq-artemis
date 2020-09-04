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

import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.security.impl.SecurityStoreImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.jboss.logging.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class JAASSecurityManagerTest {
   private static final Logger log = Logger.getLogger(JAASSecurityManagerTest.class);

   @Parameterized.Parameters(name = "newLoader=({0})")
   public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] {{true}, {false}});
   }

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = PropertiesLoginModuleTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            try {
               path = URLDecoder.decode(resource.getFile(), StandardCharsets.UTF_8.name());
               System.setProperty("java.security.auth.login.config", path);
            } catch (UnsupportedEncodingException e) {
               throw new RuntimeException(e);
            }
         }
      }
   }

   @Parameterized.Parameter
   public boolean usingNewLoader;

   @Rule
   public TemporaryFolder tmpDir = new TemporaryFolder();

   @Test
   public void testLoginClassloading() throws Exception {
      ClassLoader existingLoader = Thread.currentThread().getContextClassLoader();
      log.debug("loader: " + existingLoader);
      try {
         if (usingNewLoader) {
            URLClassLoader simulatedLoader = new URLClassLoader(new URL[]{tmpDir.getRoot().toURI().toURL()}, null);
            Thread.currentThread().setContextClassLoader(simulatedLoader);
         }
         ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("PropertiesLogin");

         Subject result = securityManager.authenticate("first", "secret", null, null);

         assertNotNull(result);
         assertEquals("first", SecurityStoreImpl.getUserFromSubject(result));

         Role role = new Role("programmers", true, true, true, true, true, true, true, true, true, true);
         Set<Role> roles = new HashSet<>();
         roles.add(role);
         boolean authorizationResult = securityManager.authorize(result, roles, CheckType.SEND, "someaddress");

         assertTrue(authorizationResult);

      } finally {
         Thread.currentThread().setContextClassLoader(existingLoader);
      }
   }

}

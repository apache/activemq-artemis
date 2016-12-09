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
package org.apache.activemq.artemis.tests.unit.core.security.impl;

import java.util.HashSet;

import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * tests ActiveMQSecurityManagerImpl
 */
public class ActiveMQSecurityManagerImplTest extends ActiveMQTestBase {

   private ActiveMQSecurityManagerImpl securityManager;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      securityManager = new ActiveMQSecurityManagerImpl();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      securityManager = null;

      super.tearDown();
   }

   @Test
   public void testDefaultSecurity() {
      securityManager.getConfiguration().addUser("guest", "password");
      securityManager.getConfiguration().addRole("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      Assert.assertTrue(securityManager.validateUser(null, null));
      Assert.assertTrue(securityManager.validateUser("guest", "password"));
      Assert.assertFalse(securityManager.validateUser(null, "wrongpass"));
      HashSet<Role> roles = new HashSet<>();
      roles.add(new Role("guest", true, true, true, true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.CREATE_DURABLE_QUEUE));
      Assert.assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.SEND));
      Assert.assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.CONSUME));
      roles = new HashSet<>();
      roles.add(new Role("guest", true, true, false, true, true, true, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CREATE_DURABLE_QUEUE));
      Assert.assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.SEND));
      Assert.assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.CONSUME));
      roles = new HashSet<>();
      roles.add(new Role("guest", true, false, false, true, true, true, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CREATE_DURABLE_QUEUE));
      Assert.assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.SEND));
      Assert.assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CONSUME));
      roles = new HashSet<>();
      roles.add(new Role("guest", false, false, false, true, true, true, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CREATE_DURABLE_QUEUE));
      Assert.assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.SEND));
      Assert.assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CONSUME));
   }

   @Test
   public void testAddingUsers() {
      securityManager.getConfiguration().addUser("newuser1", "newpassword1");
      Assert.assertTrue(securityManager.validateUser("newuser1", "newpassword1"));
      Assert.assertFalse(securityManager.validateUser("newuser1", "guest"));
      Assert.assertFalse(securityManager.validateUser("newuser1", null));
      try {
         securityManager.getConfiguration().addUser("newuser2", null);
         Assert.fail("password cannot be null");
      } catch (IllegalArgumentException e) {
         // pass
      }
      try {
         securityManager.getConfiguration().addUser(null, "newpassword2");
         Assert.fail("password cannot be null");
      } catch (IllegalArgumentException e) {
         // pass
      }
   }

   @Test
   public void testRemovingUsers() {
      securityManager.getConfiguration().addUser("newuser1", "newpassword1");
      Assert.assertTrue(securityManager.validateUser("newuser1", "newpassword1"));
      securityManager.getConfiguration().removeUser("newuser1");
      Assert.assertFalse(securityManager.validateUser("newuser1", "newpassword1"));
   }

   @Test
   public void testRemovingInvalidUsers() {
      securityManager.getConfiguration().addUser("newuser1", "newpassword1");
      Assert.assertTrue(securityManager.validateUser("newuser1", "newpassword1"));
      securityManager.getConfiguration().removeUser("nonuser");
      Assert.assertTrue(securityManager.validateUser("newuser1", "newpassword1"));
   }

   @Test
   public void testAddingRoles() {
      securityManager.getConfiguration().addUser("newuser1", "newpassword1");
      securityManager.getConfiguration().addRole("newuser1", "role1");
      securityManager.getConfiguration().addRole("newuser1", "role2");
      securityManager.getConfiguration().addRole("newuser1", "role3");
      securityManager.getConfiguration().addRole("newuser1", "role4");
      HashSet<Role> roles = new HashSet<>();
      roles.add(new Role("role1", true, true, true, true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<>();
      roles.add(new Role("role2", true, true, true, true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<>();
      roles.add(new Role("role3", true, true, true, true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<>();
      roles.add(new Role("role4", true, true, true, true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<>();
      roles.add(new Role("role5", true, true, true, true, true, true, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
   }

   @Test
   public void testRemovingRoles() {
      securityManager.getConfiguration().addUser("newuser1", "newpassword1");
      securityManager.getConfiguration().addRole("newuser1", "role1");
      securityManager.getConfiguration().addRole("newuser1", "role2");
      securityManager.getConfiguration().addRole("newuser1", "role3");
      securityManager.getConfiguration().addRole("newuser1", "role4");
      securityManager.getConfiguration().removeRole("newuser1", "role2");
      securityManager.getConfiguration().removeRole("newuser1", "role4");
      HashSet<Role> roles = new HashSet<>();
      roles.add(new Role("role1", true, true, true, true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<>();
      roles.add(new Role("role2", true, true, true, true, true, true, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<>();
      roles.add(new Role("role3", true, true, true, true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<>();
      roles.add(new Role("role4", true, true, true, true, true, true, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<>();
      roles.add(new Role("role5", true, true, true, true, true, true, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
   }
}

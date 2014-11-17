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
package org.apache.activemq.tests.unit.core.security.impl;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.HashSet;

import org.junit.Assert;

import org.apache.activemq.core.security.CheckType;
import org.apache.activemq.core.security.Role;
import org.apache.activemq.spi.core.security.HornetQSecurityManagerImpl;
import org.apache.activemq.tests.util.UnitTestCase;

/**
 * tests HornetQSecurityManagerImpl
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class HornetQSecurityManagerImplTest extends UnitTestCase
{
   private HornetQSecurityManagerImpl securityManager;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      securityManager = new HornetQSecurityManagerImpl();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      securityManager = null;

      super.tearDown();
   }

   @Test
   public void testDefaultSecurity()
   {
      securityManager.addUser("guest", "guest");
      securityManager.addRole("guest", "guest");
      securityManager.setDefaultUser("guest");
      Assert.assertTrue(securityManager.validateUser(null, null));
      Assert.assertTrue(securityManager.validateUser("guest", "guest"));
      HashSet<Role> roles = new HashSet<Role>();
      roles.add(new Role("guest", true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.CREATE_DURABLE_QUEUE));
      Assert.assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.SEND));
      Assert.assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.CONSUME));
      roles = new HashSet<Role>();
      roles.add(new Role("guest", true, true, false, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CREATE_DURABLE_QUEUE));
      Assert.assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.SEND));
      Assert.assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.CONSUME));
      roles = new HashSet<Role>();
      roles.add(new Role("guest", true, false, false, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CREATE_DURABLE_QUEUE));
      Assert.assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.SEND));
      Assert.assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CONSUME));
      roles = new HashSet<Role>();
      roles.add(new Role("guest", false, false, false, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CREATE_DURABLE_QUEUE));
      Assert.assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.SEND));
      Assert.assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CONSUME));
   }

   @Test
   public void testAddingUsers()
   {
      securityManager.addUser("newuser1", "newpassword1");
      Assert.assertTrue(securityManager.validateUser("newuser1", "newpassword1"));
      Assert.assertFalse(securityManager.validateUser("newuser1", "guest"));
      Assert.assertFalse(securityManager.validateUser("newuser1", null));
      try
      {
         securityManager.addUser("newuser2", null);
         Assert.fail("password cannot be null");
      }
      catch (IllegalArgumentException e)
      {
         // pass
      }
      try
      {
         securityManager.addUser(null, "newpassword2");
         Assert.fail("password cannot be null");
      }
      catch (IllegalArgumentException e)
      {
         // pass
      }
   }

   @Test
   public void testRemovingUsers()
   {
      securityManager.addUser("newuser1", "newpassword1");
      Assert.assertTrue(securityManager.validateUser("newuser1", "newpassword1"));
      securityManager.removeUser("newuser1");
      Assert.assertFalse(securityManager.validateUser("newuser1", "newpassword1"));
   }

   @Test
   public void testRemovingInvalidUsers()
   {
      securityManager.addUser("newuser1", "newpassword1");
      Assert.assertTrue(securityManager.validateUser("newuser1", "newpassword1"));
      securityManager.removeUser("nonuser");
      Assert.assertTrue(securityManager.validateUser("newuser1", "newpassword1"));
   }

   @Test
   public void testAddingRoles()
   {
      securityManager.addUser("newuser1", "newpassword1");
      securityManager.addRole("newuser1", "role1");
      securityManager.addRole("newuser1", "role2");
      securityManager.addRole("newuser1", "role3");
      securityManager.addRole("newuser1", "role4");
      HashSet<Role> roles = new HashSet<Role>();
      roles.add(new Role("role1", true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<Role>();
      roles.add(new Role("role2", true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<Role>();
      roles.add(new Role("role3", true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<Role>();
      roles.add(new Role("role4", true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<Role>();
      roles.add(new Role("role5", true, true, true, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
   }

   @Test
   public void testRemovingRoles()
   {
      securityManager.addUser("newuser1", "newpassword1");
      securityManager.addRole("newuser1", "role1");
      securityManager.addRole("newuser1", "role2");
      securityManager.addRole("newuser1", "role3");
      securityManager.addRole("newuser1", "role4");
      securityManager.removeRole("newuser1", "role2");
      securityManager.removeRole("newuser1", "role4");
      HashSet<Role> roles = new HashSet<Role>();
      roles.add(new Role("role1", true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<Role>();
      roles.add(new Role("role2", true, true, true, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<Role>();
      roles.add(new Role("role3", true, true, true, true, true, true, true));
      Assert.assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<Role>();
      roles.add(new Role("role4", true, true, true, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
      roles = new HashSet<Role>();
      roles.add(new Role("role5", true, true, true, true, true, true, true));
      Assert.assertFalse(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.SEND));
   }
}

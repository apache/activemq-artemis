/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.management;


import org.junit.Assert;
import org.junit.Test;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.List;

public class JMXAccessControlListTest {

   @Test
   public void testBasicDomain() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToWhiteList("org.myDomain", null);
      controlList.addToWhiteList("org.myDomain.foo", null);
      Assert.assertTrue(controlList.isInWhiteList(new ObjectName("org.myDomain:*")));
      Assert.assertTrue(controlList.isInWhiteList(new ObjectName("org.myDomain.foo:*")));
      Assert.assertFalse(controlList.isInWhiteList(new ObjectName("org.myDomain.bar:*")));
   }

   @Test
   public void testBasicDomainWithProperty() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToWhiteList("org.myDomain", "type=foo");
      controlList.addToWhiteList("org.myDomain.foo", "type=bar");
      Assert.assertFalse(controlList.isInWhiteList(new ObjectName("org.myDomain:*")));
      Assert.assertFalse(controlList.isInWhiteList(new ObjectName("org.myDomain.foo:*")));
      Assert.assertFalse(controlList.isInWhiteList(new ObjectName("org.myDomain.bar:*")));
      Assert.assertFalse(controlList.isInWhiteList(new ObjectName("org.myDomain:subType=foo")));

      Assert.assertTrue(controlList.isInWhiteList(new ObjectName("org.myDomain:type=foo")));
      Assert.assertTrue(controlList.isInWhiteList(new ObjectName("org.myDomain:subType=bar,type=foo")));
   }

   @Test
   public void testBasicDomainWithWildCardProperty() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToWhiteList("org.myDomain", "type=*");
      Assert.assertFalse(controlList.isInWhiteList(new ObjectName("org.myDomain:*")));
      Assert.assertFalse(controlList.isInWhiteList(new ObjectName("org.myDomain.foo:*")));
      Assert.assertFalse(controlList.isInWhiteList(new ObjectName("org.myDomain.bar:*")));
      Assert.assertTrue(controlList.isInWhiteList(new ObjectName("org.myDomain:type=foo")));
   }

   @Test
   public void testBasicRole() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToRoleAccess("org.myDomain", null,"listSomething", "admin");
      List<String> roles = controlList.getRolesForObject(new ObjectName("org.myDomain:*"), "listSomething");
      Assert.assertArrayEquals(roles.toArray(), new String[]{"admin"});
   }

   @Test
   public void testBasicRoleWithKey() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToRoleAccess("org.myDomain", "type=foo","listSomething", "admin");
      controlList.addToRoleAccess("org.myDomain", null,"listSomething", "view");
      List<String> roles = controlList.getRolesForObject(new ObjectName("org.myDomain:type=foo"), "listSomething");
      Assert.assertArrayEquals(roles.toArray(), new String[]{"admin"});
   }

   @Test
   public void testBasicRoleWithKeyContainingQuotes() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToRoleAccess("org.myDomain", "type=foo","listSomething", "admin");
      controlList.addToRoleAccess("org.myDomain", null,"listSomething", "view");
      List<String> roles = controlList.getRolesForObject(new ObjectName("org.myDomain:type=\"foo\""), "listSomething");
      Assert.assertArrayEquals(roles.toArray(), new String[]{"admin"});
   }

   @Test
   public void testBasicRoleWithWildcardKey() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToRoleAccess("org.myDomain", "type=*","listSomething", "admin");
      controlList.addToRoleAccess("org.myDomain", null,"listSomething", "view");
      List<String> roles = controlList.getRolesForObject(new ObjectName("org.myDomain:type=foo"), "listSomething");
      Assert.assertArrayEquals(roles.toArray(), new String[]{"admin"});
   }

   @Test
   public void testMutipleBasicRoles() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToRoleAccess("org.myDomain", null, "listSomething", "admin", "view", "update");
      List<String> roles = controlList.getRolesForObject(new ObjectName("org.myDomain:*"), "listSomething");
      Assert.assertArrayEquals(roles.toArray(), new String[]{"admin", "view", "update"});
   }

   @Test
   public void testBasicRoleWithPrefix() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToRoleAccess("org.myDomain", null,"list*", "admin");
      List<String> roles = controlList.getRolesForObject(new ObjectName("org.myDomain:*"), "listSomething");
      Assert.assertArrayEquals(roles.toArray(), new String[]{"admin"});
   }

   @Test
   public void testBasicRoleWithBoth() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToRoleAccess("org.myDomain", null,"listSomething", "admin");
      controlList.addToRoleAccess("org.myDomain", null,"list*", "view");
      List<String> roles = controlList.getRolesForObject(new ObjectName("org.myDomain:*"), "listSomething");
      Assert.assertArrayEquals(roles.toArray(), new String[]{"admin"});
      roles = controlList.getRolesForObject(new ObjectName("org.myDomain:*"), "listSomethingMore");
      Assert.assertArrayEquals(roles.toArray(), new String[]{"view"});
   }

   @Test
   public void testBasicRoleWithDefaultsPrefix() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToDefaultAccess("setSomething","admin");
      controlList.addToRoleAccess("org.myDomain", null,"list*", "view");
      List<String> roles = controlList.getRolesForObject(new ObjectName("org.myDomain.foo:*"), "setSomething");
      Assert.assertArrayEquals(roles.toArray(), new String[]{"admin"});
   }

   @Test
   public void testBasicRoleWithDefaultsWildcardPrefix() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToDefaultAccess("setSomething","admin");
      controlList.addToDefaultAccess("set*","admin");
      controlList.addToRoleAccess("org.myDomain", null,"list*", "view");
      List<String> roles = controlList.getRolesForObject(new ObjectName("org.myDomain.foo:*"), "setSomethingMore");
      Assert.assertArrayEquals(roles.toArray(), new String[]{"admin"});
   }

   @Test
   public void testBasicRoleWithDefaultscatchAllPrefix() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToDefaultAccess("setSomething","admin");
      controlList.addToDefaultAccess("*","admin");
      controlList.addToRoleAccess("org.myDomain", null,"list*", "view");
      List<String> roles = controlList.getRolesForObject(new ObjectName("org.myDomain.foo:*"), "setSomethingMore");
      Assert.assertArrayEquals(roles.toArray(), new String[]{"admin"});
   }

   @Test
   public void testKeylessDomain() throws MalformedObjectNameException {
      JMXAccessControlList controlList = new JMXAccessControlList();
      controlList.addToRoleAccess("org.myDomain.foo", null,"list*", "amq","monitor");
      controlList.addToRoleAccess("org.myDomain.foo", null,"get*", "amq","monitor");
      controlList.addToRoleAccess("org.myDomain.foo", null,"is*", "amq","monitor");
      controlList.addToRoleAccess("org.myDomain.foo", null,"set*", "amq");
      controlList.addToRoleAccess("org.myDomain.foo", null,"*", "amq");

      List<String> roles = controlList.getRolesForObject(new ObjectName("org.myDomain.foo:foo=bar"), "listFoo");
      Assert.assertNotNull(roles);
      Assert.assertEquals(roles.size(), 2);
      Assert.assertEquals(roles.get(0), "amq");
      Assert.assertEquals(roles.get(1), "monitor");

      roles = controlList.getRolesForObject(new ObjectName("org.myDomain.foo:foo=bar"), "getFoo");
      Assert.assertNotNull(roles);
      Assert.assertEquals(roles.size(), 2);
      Assert.assertEquals(roles.get(0), "amq");
      Assert.assertEquals(roles.get(1), "monitor");

      roles = controlList.getRolesForObject(new ObjectName("org.myDomain.foo:foo=bar"), "isFoo");
      Assert.assertNotNull(roles);
      Assert.assertEquals(roles.size(), 2);
      Assert.assertEquals(roles.get(0), "amq");
      Assert.assertEquals(roles.get(1), "monitor");

      roles = controlList.getRolesForObject(new ObjectName("org.myDomain.foo:foo=bar"), "setFoo");
      Assert.assertNotNull(roles);
      Assert.assertEquals(roles.size(), 1);
      Assert.assertEquals(roles.get(0), "amq");

      roles = controlList.getRolesForObject(new ObjectName("org.myDomain.foo:foo=bar"), "createFoo");
      Assert.assertNotNull(roles);
      Assert.assertEquals(roles.size(), 1);
      Assert.assertEquals(roles.get(0), "amq");

   }
}

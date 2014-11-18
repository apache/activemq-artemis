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
package org.apache.activemq.tests.unit.core.deployers.impl;

import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.core.deployers.DeploymentManager;
import org.apache.activemq.core.deployers.impl.SecurityDeployer;
import org.apache.activemq.core.security.Role;
import org.apache.activemq.core.settings.HierarchicalRepository;
import org.apache.activemq.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class SecurityDeployerTest extends UnitTestCase
{
   private SecurityDeployer deployer;

   private final String conf = "<security-setting match=\"jms.topic.testTopic\">\n" + "      <permission type=\"createDurableQueue\" roles=\"durpublisher\"/>\n"
      + "      <permission type=\"deleteDurableQueue\" roles=\"durpublisher\"/>\n"
      + "      <permission type=\"consume\" roles=\"guest,publisher,durpublisher\"/>\n"
      + "      <permission type=\"send\" roles=\"guest,publisher,durpublisher\"/>\n"
      + "      <permission type=\"manage\" roles=\"guest,publisher,durpublisher\"/>\n"
      + "   </security-setting>";

   private final String confWithWhiteSpace1 = "<security-setting match=\"jms.topic.testTopic\">\n"
      + "      <permission type=\"createDurableQueue\" roles=\"guest, publisher, durpublisher\"/>\n"
      + "      <permission type=\"createNonDurableQueue\" roles=\"guest, publisher, durpublisher\"/>\n"
      + "      <permission type=\"deleteNonDurableQueue\" roles=\"guest, publisher, durpublisher\"/>\n"
      + "      <permission type=\"deleteDurableQueue\" roles=\"guest, publisher, durpublisher\"/>\n"

      + "      <permission type=\"consume\" roles=\"guest, publisher, durpublisher\"/>\n"
      + "      <permission type=\"send\" roles=\"guest, publisher, durpublisher\"/>\n"
      + "      <permission type=\"manage\" roles=\"guest, publisher, durpublisher\"/>\n"
      + "      <permission type=\"manage\" roles=\"guest, publisher, durpublisher\"/>\n"
      + "   </security-setting>";

   private final String confWithWhiteSpace2 = "<security-setting match=\"jms.topic.testTopic\">\n"
      + "      <permission type=\"createDurableQueue\" roles=\"guest , publisher , durpublisher\"/>\n"
      + "      <permission type=\"createNonDurableQueue\" roles=\"guest , publisher , durpublisher\"/>\n"
      + "      <permission type=\"deleteNonDurableQueue\" roles=\"guest , publisher , durpublisher\"/>\n"
      + "      <permission type=\"deleteDurableQueue\" roles=\"guest , publisher , durpublisher\"/>\n"

      + "      <permission type=\"consume\" roles=\"guest , publisher , durpublisher\"/>\n"
      + "      <permission type=\"send\" roles=\"guest , publisher , durpublisher\"/>\n"
      + "      <permission type=\"manage\" roles=\"guest , publisher , durpublisher\"/>\n"
      + "   </security-setting>";

   private final String conf2 = "<security-setting match=\"jms.topic.testQueue\">\n"
      + "      <permission type=\"createNonDurableQueue\" roles=\"durpublisher\"/>\n"
      + "      <permission type=\"deleteNonDurableQueue\" roles=\"durpublisher\"/>\n"
      + "      <permission type=\"consume\" roles=\"guest,publisher,durpublisher\"/>\n"
      + "      <permission type=\"send\" roles=\"guest,publisher,durpublisher\"/>\n"
      + "   </security-setting>";

   private final String noRoles = "   <securityfoo match=\"queues.testQueue\">\n" + "   </securityfoo>";

   private HierarchicalRepository<Set<Role>> repository;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      repository = new HierarchicalObjectRepository<Set<Role>>();
      DeploymentManager deploymentManager = new FakeDeploymentManager();
      deployer = new SecurityDeployer(deploymentManager, repository);
   }

   @Test
   public void testSingle() throws Exception
   {
      Element e = org.apache.activemq.utils.XMLUtil.stringToElement(conf);
      deployer.deploy(e);
      HashSet<Role> roles = (HashSet<Role>) repository.getMatch("jms.topic.testTopic");
      Assert.assertNotNull(roles);
      Assert.assertEquals(3, roles.size());
      for (Role role : roles)
      {
         if (role.getName().equals("guest"))
         {
            Assert.assertTrue(role.isConsume());
            Assert.assertFalse(role.isCreateDurableQueue());
            Assert.assertFalse(role.isCreateNonDurableQueue());
            Assert.assertFalse(role.isDeleteDurableQueue());
            Assert.assertFalse(role.isDeleteNonDurableQueue());
            Assert.assertTrue(role.isManage());
            Assert.assertTrue(role.isSend());
         }
         else if (role.getName().equals("publisher"))
         {
            Assert.assertTrue(role.isConsume());
            Assert.assertFalse(role.isCreateDurableQueue());
            Assert.assertFalse(role.isCreateNonDurableQueue());
            Assert.assertFalse(role.isDeleteDurableQueue());
            Assert.assertFalse(role.isDeleteNonDurableQueue());
            Assert.assertTrue(role.isManage());
            Assert.assertTrue(role.isSend());
         }
         else if (role.getName().equals("durpublisher"))
         {
            Assert.assertTrue(role.isConsume());
            Assert.assertTrue(role.isCreateDurableQueue());
            Assert.assertFalse(role.isCreateNonDurableQueue());
            Assert.assertTrue(role.isDeleteDurableQueue());
            Assert.assertFalse(role.isDeleteNonDurableQueue());
            Assert.assertTrue(role.isManage());
            Assert.assertTrue(role.isSend());
         }
         else
         {
            Assert.fail("unexpected role");
         }
      }
   }

   @Test
   public void testWithWhiteSpace1() throws Exception
   {
      testWithWhiteSpace(confWithWhiteSpace1);
   }

   @Test
   public void testWithWhiteSpace2() throws Exception
   {
      testWithWhiteSpace(confWithWhiteSpace2);
   }

   private void testWithWhiteSpace(String conf) throws Exception
   {
      Element e = org.apache.activemq.utils.XMLUtil.stringToElement(confWithWhiteSpace1);
      deployer.deploy(e);
      HashSet<Role> roles = (HashSet<Role>) repository.getMatch("jms.topic.testTopic");
      Assert.assertNotNull(roles);
      Assert.assertEquals(3, roles.size());
      for (Role role : roles)
      {
         if (role.getName().equals("guest"))
         {
            Assert.assertTrue(role.isConsume());
            Assert.assertTrue(role.isCreateDurableQueue());
            Assert.assertTrue(role.isCreateNonDurableQueue());
            Assert.assertTrue(role.isDeleteDurableQueue());
            Assert.assertTrue(role.isDeleteNonDurableQueue());
            Assert.assertTrue(role.isManage());
            Assert.assertTrue(role.isSend());
         }
         else if (role.getName().equals("publisher"))
         {
            Assert.assertTrue(role.isConsume());
            Assert.assertTrue(role.isCreateDurableQueue());
            Assert.assertTrue(role.isCreateNonDurableQueue());
            Assert.assertTrue(role.isDeleteDurableQueue());
            Assert.assertTrue(role.isDeleteNonDurableQueue());
            Assert.assertTrue(role.isManage());
            Assert.assertTrue(role.isSend());
         }
         else if (role.getName().equals("durpublisher"))
         {
            Assert.assertTrue(role.isConsume());
            Assert.assertTrue(role.isCreateDurableQueue());
            Assert.assertTrue(role.isCreateNonDurableQueue());
            Assert.assertTrue(role.isDeleteDurableQueue());
            Assert.assertTrue(role.isDeleteNonDurableQueue());
            Assert.assertTrue(role.isManage());
            Assert.assertTrue(role.isSend());
         }
         else
         {
            Assert.fail("unexpected role");
         }
      }
   }

   @Test
   public void testMultiple() throws Exception
   {
      deployer.deploy(org.apache.activemq.utils.XMLUtil.stringToElement(conf));
      deployer.deploy(org.apache.activemq.utils.XMLUtil.stringToElement(conf2));
      HashSet<Role> roles = (HashSet<Role>) repository.getMatch("jms.topic.testTopic");
      Assert.assertNotNull(roles);
      Assert.assertEquals(3, roles.size());
      for (Role role : roles)
      {
         if (role.getName().equals("guest"))
         {
            Assert.assertTrue(role.isConsume());
            Assert.assertFalse(role.isCreateDurableQueue());
            Assert.assertFalse(role.isCreateNonDurableQueue());
            Assert.assertFalse(role.isDeleteDurableQueue());
            Assert.assertFalse(role.isDeleteNonDurableQueue());
            Assert.assertTrue(role.isManage());
            Assert.assertTrue(role.isSend());
         }
         else if (role.getName().equals("publisher"))
         {
            Assert.assertTrue(role.isConsume());
            Assert.assertFalse(role.isCreateDurableQueue());
            Assert.assertFalse(role.isCreateNonDurableQueue());
            Assert.assertFalse(role.isDeleteDurableQueue());
            Assert.assertFalse(role.isDeleteNonDurableQueue());
            Assert.assertTrue(role.isManage());
            Assert.assertTrue(role.isSend());
         }
         else if (role.getName().equals("durpublisher"))
         {
            Assert.assertTrue(role.isConsume());
            Assert.assertTrue(role.isCreateDurableQueue());
            Assert.assertFalse(role.isCreateNonDurableQueue());
            Assert.assertTrue(role.isDeleteDurableQueue());
            Assert.assertFalse(role.isDeleteNonDurableQueue());
            Assert.assertTrue(role.isManage());
            Assert.assertTrue(role.isSend());
         }
         else
         {
            Assert.fail("unexpected role");
         }
      }
      roles = (HashSet<Role>) repository.getMatch("jms.topic.testQueue");
      Assert.assertNotNull(roles);
      Assert.assertEquals(3, roles.size());
      for (Role role : roles)
      {
         if (role.getName().equals("guest"))
         {
            Assert.assertTrue(role.isConsume());
            Assert.assertFalse(role.isCreateDurableQueue());
            Assert.assertFalse(role.isCreateNonDurableQueue());
            Assert.assertFalse(role.isDeleteDurableQueue());
            Assert.assertFalse(role.isDeleteNonDurableQueue());
            Assert.assertFalse(role.isManage());
            Assert.assertTrue(role.isSend());
         }
         else if (role.getName().equals("publisher"))
         {
            Assert.assertTrue(role.isConsume());
            Assert.assertFalse(role.isCreateDurableQueue());
            Assert.assertFalse(role.isCreateNonDurableQueue());
            Assert.assertFalse(role.isDeleteDurableQueue());
            Assert.assertFalse(role.isDeleteNonDurableQueue());
            Assert.assertFalse(role.isManage());
            Assert.assertTrue(role.isSend());
         }
         else if (role.getName().equals("durpublisher"))
         {
            Assert.assertTrue(role.isConsume());
            Assert.assertFalse(role.isCreateDurableQueue());
            Assert.assertTrue(role.isCreateNonDurableQueue());
            Assert.assertFalse(role.isDeleteDurableQueue());
            Assert.assertTrue(role.isDeleteNonDurableQueue());
            Assert.assertFalse(role.isManage());
            Assert.assertTrue(role.isSend());
         }
         else
         {
            Assert.fail("unexpected role");
         }
      }
   }

   @Test
   public void testNoRolesAdded() throws Exception
   {
      deployer.deploy(org.apache.activemq.utils.XMLUtil.stringToElement(noRoles));
      HashSet<Role> roles = (HashSet<Role>) repository.getMatch("jms.topic.testQueue");
      Assert.assertNull(roles);
   }

   @Test
   public void testDeployFromConfigurationFile() throws Exception
   {
      String xml = "<configuration xmlns='urn:activemq'> " + "<security-settings>"
         + "   <security-setting match=\"jms.topic.testTopic\">"
         + "      <permission type=\"createDurableQueue\" roles=\"durpublisher\"/>"
         + "      <permission type=\"deleteDurableQueue\" roles=\"durpublisher\"/>"
         + "      <permission type=\"consume\" roles=\"guest,publisher,durpublisher\"/>"
         + "      <permission type=\"send\" roles=\"guest,publisher,durpublisher\"/>"
         + "      <permission type=\"manage\" roles=\"guest,publisher,durpublisher\"/>"
         + "   </security-setting>"
         + "</security-settings>"
         + "</configuration>";

      Element rootNode = org.apache.activemq.utils.XMLUtil.stringToElement(xml);
      deployer.validate(rootNode);
      NodeList securityNodes = rootNode.getElementsByTagName("security-setting");
      Assert.assertEquals(1, securityNodes.getLength());

      deployer.deploy(securityNodes.item(0));
      HashSet<Role> roles = (HashSet<Role>) repository.getMatch("jms.topic.testTopic");
      Assert.assertNotNull(roles);
      Assert.assertEquals(3, roles.size());
   }
}

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

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.activemq.core.deployers.impl.XmlDeployer;
import org.apache.activemq.tests.util.UnitTestCase;
import org.apache.activemq.utils.XMLUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * tests the abstract xml deployer class
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class XMLDeployerTest extends UnitTestCase
{
   private static final String conf1 = "<configuration>\n" + "   <test name=\"test1\">content1</test>\n"
      + "   <test name=\"test2\">content2</test>\n"
      + "   <test name=\"test3\">content3</test>\n"
      + "   <test name=\"test4\">content4</test>\n"
      + "</configuration>";

   private static final String conf2 = "<configuration>\n" + "   <test name=\"test1\">content1</test>\n"
      + "   <test name=\"test2\">contenthaschanged2</test>\n"
      + "   <test name=\"test3\">contenthaschanged3</test>\n"
      + "   <test name=\"test4\">content4</test>\n"
      + "</configuration>";

   private static final String conf3 = "<configuration>\n" + "   <test name=\"test1\">content1</test>\n"
      + "   <test name=\"test2\">contenthaschanged2</test>\n"
      + "</configuration>";

   private static final String conf4 = "<configuration>\n" + "   <test name=\"test1\">content1</test>\n"
      + "   <test name=\"test2\">content2</test>\n"
      + "   <test name=\"test3\">content3</test>\n"
      + "   <test name=\"test4\">content4</test>\n"
      + "   <test name=\"test5\">content5</test>\n"
      + "   <test name=\"test6\">content6</test>\n"
      + "</configuration>";

   private URI url;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      url = new URI("http://localhost");
   }

   @Test
   public void testDeploy() throws Exception
   {
      Element e = XMLUtil.stringToElement(XMLDeployerTest.conf1);
      TestDeployer testDeployer = new TestDeployer();
      testDeployer.setElement(e);
      testDeployer.deploy(url);
      Assert.assertEquals(testDeployer.getDeployments(), 4);
      Assert.assertNotNull(testDeployer.getNodes().get("test1"));
      Assert.assertNotNull(testDeployer.getNodes().get("test2"));
      Assert.assertNotNull(testDeployer.getNodes().get("test3"));
      Assert.assertNotNull(testDeployer.getNodes().get("test4"));
      Assert.assertEquals(testDeployer.getNodes().get("test1").getTextContent(), "content1");
      Assert.assertEquals(testDeployer.getNodes().get("test2").getTextContent(), "content2");
      Assert.assertEquals(testDeployer.getNodes().get("test3").getTextContent(), "content3");
      Assert.assertEquals(testDeployer.getNodes().get("test4").getTextContent(), "content4");
   }

   @Test
   public void testRedeploy() throws Exception
   {
      Element e = XMLUtil.stringToElement(XMLDeployerTest.conf1);
      TestDeployer testDeployer = new TestDeployer();
      testDeployer.setElement(e);
      testDeployer.deploy(url);
      e = org.apache.activemq.utils.XMLUtil.stringToElement(XMLDeployerTest.conf2);
      testDeployer.setElement(e);
      testDeployer.redeploy(url);
      Assert.assertEquals(testDeployer.getDeployments(), 4);
      Assert.assertNotNull(testDeployer.getNodes().get("test1"));
      Assert.assertNotNull(testDeployer.getNodes().get("test2"));
      Assert.assertNotNull(testDeployer.getNodes().get("test3"));
      Assert.assertNotNull(testDeployer.getNodes().get("test4"));
      Assert.assertEquals(testDeployer.getNodes().get("test1").getTextContent(), "content1");
      Assert.assertEquals(testDeployer.getNodes().get("test2").getTextContent(), "contenthaschanged2");
      Assert.assertEquals(testDeployer.getNodes().get("test3").getTextContent(), "contenthaschanged3");
      Assert.assertEquals(testDeployer.getNodes().get("test4").getTextContent(), "content4");
   }

   @Test
   public void testRedeployRemovingNodes() throws Exception
   {
      Element e = XMLUtil.stringToElement(XMLDeployerTest.conf1);
      TestDeployer testDeployer = new TestDeployer();
      testDeployer.setElement(e);
      testDeployer.deploy(url);
      e = org.apache.activemq.utils.XMLUtil.stringToElement(XMLDeployerTest.conf3);
      testDeployer.setElement(e);
      testDeployer.redeploy(url);
      Assert.assertEquals(testDeployer.getDeployments(), 2);
      Assert.assertNotNull(testDeployer.getNodes().get("test1"));
      Assert.assertNotNull(testDeployer.getNodes().get("test2"));
      Assert.assertNull(testDeployer.getNodes().get("test3"));
      Assert.assertNull(testDeployer.getNodes().get("test4"));
      Assert.assertEquals(testDeployer.getNodes().get("test1").getTextContent(), "content1");
      Assert.assertEquals(testDeployer.getNodes().get("test2").getTextContent(), "contenthaschanged2");
   }

   @Test
   public void testRedeployAddingNodes() throws Exception
   {
      Element e = XMLUtil.stringToElement(XMLDeployerTest.conf1);
      TestDeployer testDeployer = new TestDeployer();
      testDeployer.setElement(e);
      testDeployer.deploy(url);
      e = org.apache.activemq.utils.XMLUtil.stringToElement(XMLDeployerTest.conf4);
      testDeployer.setElement(e);
      testDeployer.redeploy(url);
      Assert.assertEquals(testDeployer.getDeployments(), 6);
      Assert.assertNotNull(testDeployer.getNodes().get("test1"));
      Assert.assertNotNull(testDeployer.getNodes().get("test2"));
      Assert.assertNotNull(testDeployer.getNodes().get("test3"));
      Assert.assertNotNull(testDeployer.getNodes().get("test4"));
      Assert.assertNotNull(testDeployer.getNodes().get("test5"));
      Assert.assertNotNull(testDeployer.getNodes().get("test6"));
      Assert.assertEquals(testDeployer.getNodes().get("test1").getTextContent(), "content1");
      Assert.assertEquals(testDeployer.getNodes().get("test2").getTextContent(), "content2");
      Assert.assertEquals(testDeployer.getNodes().get("test3").getTextContent(), "content3");
      Assert.assertEquals(testDeployer.getNodes().get("test4").getTextContent(), "content4");
      Assert.assertEquals(testDeployer.getNodes().get("test5").getTextContent(), "content5");
      Assert.assertEquals(testDeployer.getNodes().get("test6").getTextContent(), "content6");
   }

   @Test
   public void testUndeploy() throws Exception
   {
      Element e = org.apache.activemq.utils.XMLUtil.stringToElement(XMLDeployerTest.conf1);
      TestDeployer testDeployer = new TestDeployer();
      testDeployer.setElement(e);
      testDeployer.deploy(url);
      testDeployer.undeploy(url);
      Assert.assertEquals(testDeployer.getDeployments(), 0);
      Assert.assertNull(testDeployer.getNodes().get("test1"));
      Assert.assertNull(testDeployer.getNodes().get("test2"));
      Assert.assertNull(testDeployer.getNodes().get("test3"));
      Assert.assertNull(testDeployer.getNodes().get("test4"));
   }

   class TestDeployer extends XmlDeployer
   {
      private String elementname = "test";

      Element element = null;

      private int deployments = 0;

      ArrayList<String> contents = new ArrayList<String>();

      HashMap<String, Node> nodes = new HashMap<String, Node>();

      public TestDeployer()
      {
         super(null);
      }

      public HashMap<String, Node> getNodes()
      {
         return nodes;
      }

      public ArrayList<String> getContents()
      {
         return contents;
      }

      public int getDeployments()
      {
         return deployments;
      }

      public String getElementname()
      {
         return elementname;
      }

      public void setElementname(final String elementname)
      {
         this.elementname = elementname;
      }

      public Element getElement()
      {
         return element;
      }

      public void setElement(final Element element)
      {
         this.element = element;
      }

      @Override
      public String[] getElementTagName()
      {
         return new String[]{elementname};
      }

      @Override
      public String[] getConfigFileNames()
      {
         return new String[]{"test"};
      }

      @Override
      public String[] getDefaultConfigFileNames()
      {
         return new String[0];
      }

      @Override
      public void validate(final Node rootNode) throws Exception
      {
      }

      @Override
      public void deploy(final Node node) throws Exception
      {
         deployments++;
         contents.add(node.getTextContent());
         nodes.put(node.getAttributes().getNamedItem(XmlDeployer.NAME_ATTR).getNodeValue(), node);
      }

      @Override
      public void undeploy(final Node node) throws Exception
      {
         deployments--;
         nodes.remove(node.getAttributes().getNamedItem(XmlDeployer.NAME_ATTR).getNodeValue());
      }

      @Override
      protected Element getRootElement(final URI url)
      {
         return element;
      }
   }
}

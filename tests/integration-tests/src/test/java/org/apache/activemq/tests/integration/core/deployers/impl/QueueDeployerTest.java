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
package org.apache.activemq.tests.integration.core.deployers.impl;
import org.junit.Before;

import org.junit.Test;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.deployers.DeploymentManager;
import org.apache.activemq.core.deployers.impl.FileDeploymentManager;
import org.apache.activemq.core.deployers.impl.QueueDeployer;
import org.apache.activemq.core.postoffice.Binding;
import org.apache.activemq.core.postoffice.Bindings;
import org.apache.activemq.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * A QueueDeployerTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class QueueDeployerTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private QueueDeployer deployer;

   private HornetQServer server;

   @Test
   public void testParseQueueConfiguration() throws Exception
   {
      String xml = "<configuration xmlns='urn:hornetq'>"
                   + "   <queues>"
                   + "      <queue name='foo'>"
                   + "         <address>bar</address>"
                   + "         <filter string='speed > 88' />"
                   + "         <durable>false</durable>"
                   + "      </queue>"
                   + "   </queues>"
                   + "</configuration>";

      Element rootNode = org.apache.activemq.utils.XMLUtil.stringToElement(xml);
      deployer.validate(rootNode);
      NodeList queueNodes = rootNode.getElementsByTagName("queue");
      assertEquals(1, queueNodes.getLength());
      deployer.deploy(queueNodes.item(0));

      Bindings bindings = server.getPostOffice().getBindingsForAddress(SimpleString.toSimpleString("bar"));
      assertEquals(1, bindings.getBindings().size());
      Binding binding = bindings.getBindings().iterator().next();
      assertTrue(binding instanceof LocalQueueBinding);
      LocalQueueBinding queueBinding = (LocalQueueBinding)binding;

      assertEquals("foo", queueBinding.getQueue().getName().toString());
      assertEquals("speed > 88", queueBinding.getQueue().getFilter().getFilterString().toString());
      assertEquals(false, queueBinding.getQueue().isDurable());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      server = createServer(true);
      DeploymentManager deploymentManager = new FileDeploymentManager(500);
      deployer = new QueueDeployer(deploymentManager, server);
      server.start();
   }
}

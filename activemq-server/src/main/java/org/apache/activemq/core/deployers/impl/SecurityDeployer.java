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
package org.apache.activemq.core.deployers.impl;

import java.util.Set;

import org.apache.activemq.api.core.Pair;
import org.apache.activemq.core.deployers.DeploymentManager;
import org.apache.activemq.core.security.Role;
import org.apache.activemq.core.settings.HierarchicalRepository;
import org.w3c.dom.Node;

/**
 * Deploys the security settings into a security repository and adds them to the security store.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class SecurityDeployer extends XmlDeployer
{
   private static final String QUEUES_XML = "activemq-queues.xml";

   private static final String MATCH = "match";

   private final FileConfigurationParser parser = new FileConfigurationParser();

   /**
    * The repository to add to
    */
   private final HierarchicalRepository<Set<Role>> securityRepository;

   public SecurityDeployer(final DeploymentManager deploymentManager,
                           final HierarchicalRepository<Set<Role>> securityRepository)
   {
      super(deploymentManager);

      this.securityRepository = securityRepository;
   }

   /**
    * the names of the elements to deploy
    *
    * @return the names of the elements todeploy
    */
   @Override
   public String[] getElementTagName()
   {
      return new String[]{FileConfigurationParser.SECURITY_ELEMENT_NAME};
   }

   @Override
   public void validate(final Node rootNode) throws Exception
   {
      org.apache.activemq.utils.XMLUtil.validate(rootNode, "schema/activemq-configuration.xsd");
   }

   /**
    * the key attribute for the element, usually 'name' but can be overridden
    *
    * @return the key attribute
    */
   @Override
   public String getKeyAttribute()
   {
      return SecurityDeployer.MATCH;
   }

   /**
    * deploy an element
    *
    * @param node the element to deploy
    * @throws Exception
    */
   @Override
   public void deploy(final Node node) throws Exception
   {
      Pair<String, Set<Role>> securityMatch = parser.parseSecurityRoles(node);
      securityRepository.addMatch(securityMatch.getA(), securityMatch.getB());
   }

   /**
    * undeploys an element
    *
    * @param node the element to undeploy
    * @throws Exception
    */
   @Override
   public void undeploy(final Node node) throws Exception
   {
      String match = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
      securityRepository.removeMatch(match);
   }

   /**
    * The name of the configuration file name to look for for deployment
    *
    * @return The name of the config file
    */
   @Override
   public String[] getDefaultConfigFileNames()
   {
      return new String[]{"activemq-configuration.xml", SecurityDeployer.QUEUES_XML};
   }
}

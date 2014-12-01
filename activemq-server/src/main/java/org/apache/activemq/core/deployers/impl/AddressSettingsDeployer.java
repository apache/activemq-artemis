/**
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
package org.apache.activemq.core.deployers.impl;

import org.apache.activemq.api.core.Pair;
import org.apache.activemq.core.deployers.DeploymentManager;
import org.apache.activemq.core.settings.HierarchicalRepository;
import org.apache.activemq.core.settings.impl.AddressSettings;
import org.w3c.dom.Node;

/**
 * A deployer for creating a set of queue settings and adding them to a repository
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class AddressSettingsDeployer extends XmlDeployer
{
   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final FileConfigurationParser parser = new FileConfigurationParser();

   public AddressSettingsDeployer(final DeploymentManager deploymentManager,
                                  final HierarchicalRepository<AddressSettings> addressSettingsRepository)
   {
      super(deploymentManager);
      this.addressSettingsRepository = addressSettingsRepository;
   }

   /**
    * the names of the elements to deploy
    *
    * @return the names of the elements to deploy
    */
   @Override
   public String[] getElementTagName()
   {
      return new String[]{"address-setting"};
   }

   @Override
   public void validate(final Node rootNode) throws Exception
   {
      org.apache.activemq.utils.XMLUtil.validate(rootNode, "schema/activemq-configuration.xsd");
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

      Pair<String, AddressSettings> setting = parser.parseAddressSettings(node);

      addressSettingsRepository.addMatch(setting.getA(), setting.getB());
   }

   @Override
   public String[] getDefaultConfigFileNames()
   {
      return new String[]{"activemq-configuration.xml", "activemq-queues.xml"};
   }

   /**
    * Undeploys an element.
    *
    * @param node the element to undeploy
    * @throws Exception
    */
   @Override
   public void undeploy(final Node node) throws Exception
   {
      String match = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();

      addressSettingsRepository.removeMatch(match);
   }

   /**
    * the key attribute for the element, usually 'name' but can be overridden
    *
    * @return the key attribute
    */
   @Override
   public String getKeyAttribute()
   {
      return "match";
   }

}

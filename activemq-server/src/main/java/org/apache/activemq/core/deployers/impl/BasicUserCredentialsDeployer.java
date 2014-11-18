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

import org.apache.activemq.core.deployers.DeploymentManager;
import org.apache.activemq.spi.core.security.HornetQSecurityManager;
import org.apache.activemq.utils.PasswordMaskingUtil;
import org.apache.activemq.utils.SensitiveDataCodec;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * deployer for adding security loaded from the file "hornetq-users.xml"
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class BasicUserCredentialsDeployer extends XmlDeployer
{
   private final HornetQSecurityManager hornetQSecurityManager;

   private static final String PASSWORD_ATTRIBUTE = "password";

   private static final String ROLES_NODE = "role";

   private static final String ROLE_ATTR_NAME = "name";

   private static final String DEFAULT_USER = "defaultuser";

   private static final String USER = "user";

   private static final String MASK_PASSWORD = "mask-password";

   private static final String PASSWORD_CODEC = "password-codec";

   private boolean maskPassword = false;

   private SensitiveDataCodec<String> passwordCodec;

   public BasicUserCredentialsDeployer(final DeploymentManager deploymentManager,
                                       final HornetQSecurityManager hornetQSecurityManager)
   {
      super(deploymentManager);

      this.hornetQSecurityManager = hornetQSecurityManager;
   }

   @Override
   public String[] getElementTagName()
   {
      return new String[]{MASK_PASSWORD, PASSWORD_CODEC, DEFAULT_USER, USER};
   }

   @Override
   public void validate(final Node rootNode) throws Exception
   {
      org.apache.activemq.utils.XMLUtil.validate(rootNode, "schema/activemq-users.xsd");
   }

   @Override
   public void deploy(final Node node) throws Exception
   {
      String nodeName = node.getNodeName();

      if (MASK_PASSWORD.equals(nodeName))
      {
         String value = node.getTextContent().trim();

         maskPassword = Boolean.parseBoolean(value);

         if (maskPassword)
         {
            passwordCodec = PasswordMaskingUtil.getDefaultCodec();
         }
         return;
      }

      if (PASSWORD_CODEC.equals(nodeName))
      {
         if (maskPassword)
         {
            String codecDesc = node.getTextContent();

            passwordCodec = PasswordMaskingUtil.getCodec(codecDesc);
         }
         return;
      }

      String username = node.getAttributes().getNamedItem("name").getNodeValue();
      String password = node.getAttributes()
         .getNamedItem(BasicUserCredentialsDeployer.PASSWORD_ATTRIBUTE)
         .getNodeValue();

      if (maskPassword)
      {
         if ((password != null) && (!"".equals(password.trim())))
         {
            password = passwordCodec.decode(password);
         }
      }

      // add the user
      hornetQSecurityManager.addUser(username, password);

      if (BasicUserCredentialsDeployer.DEFAULT_USER.equalsIgnoreCase(nodeName))
      {
         hornetQSecurityManager.setDefaultUser(username);
      }
      NodeList children = node.getChildNodes();
      for (int i = 0; i < children.getLength(); i++)
      {
         Node child = children.item(i);
         // and add any roles
         if (BasicUserCredentialsDeployer.ROLES_NODE.equalsIgnoreCase(child.getNodeName()))
         {
            String role = child.getAttributes()
               .getNamedItem(BasicUserCredentialsDeployer.ROLE_ATTR_NAME)
               .getNodeValue();
            hornetQSecurityManager.addRole(username, role);
         }
      }
   }

   @Override
   public void undeploy(final Node node) throws Exception
   {
      String username = node.getAttributes().getNamedItem("name").getNodeValue();
      hornetQSecurityManager.removeUser(username);
   }

   @Override
   public String[] getDefaultConfigFileNames()
   {
      return new String[]{"activemq-users.xml"};
   }
}

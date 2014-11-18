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
package org.apache.activemq.jms.server.impl;

import org.apache.activemq.core.deployers.DeploymentManager;
import org.apache.activemq.core.deployers.impl.XmlDeployer;
import org.apache.activemq.jms.server.JMSServerConfigParser;
import org.apache.activemq.jms.server.JMSServerManager;
import org.apache.activemq.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq.jms.server.config.TopicConfiguration;
import org.w3c.dom.Node;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSServerDeployer extends XmlDeployer
{
   private final JMSServerConfigParser parser;

   private final JMSServerManager jmsServerManager;

   protected static final String CONNECTOR_REF_ELEMENT = "connector-ref";

   protected static final String DISCOVERY_GROUP_ELEMENT = "discovery-group-ref";

   protected static final String ENTRIES_NODE_NAME = "entries";

   protected static final String ENTRY_NODE_NAME = "entry";

   protected static final String CONNECTORS_NODE_NAME = "connectors";

   protected static final String CONNECTION_FACTORY_NODE_NAME = "connection-factory";

   protected static final String QUEUE_NODE_NAME = "queue";

   protected static final String QUEUE_SELECTOR_NODE_NAME = "selector";

   protected static final String TOPIC_NODE_NAME = "topic";

   protected static final String JMX_DOMAIN_NAME = "jmx-domain";

   protected static final boolean DEFAULT_QUEUE_DURABILITY = true;

   public JMSServerDeployer(final JMSServerManager jmsServerManager,
                            final DeploymentManager deploymentManager)
   {
      super(deploymentManager);

      this.jmsServerManager = jmsServerManager;

      parser = new JMSServerConfigParserImpl();
   }

   /**
    * the names of the elements to deploy
    *
    * @return the names of the elements todeploy
    */
   @Override
   public String[] getElementTagName()
   {
      return new String[]{JMSServerDeployer.QUEUE_NODE_NAME,
         JMSServerDeployer.TOPIC_NODE_NAME,
         JMSServerDeployer.CONNECTION_FACTORY_NODE_NAME};
   }

   @Override
   public void validate(final Node rootNode) throws Exception
   {
      org.apache.activemq.utils.XMLUtil.validate(rootNode, "schema/activemq-jms.xsd");
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
      createAndBindObject(node);
   }

   /**
    * Creates the object to bind, this will either be a JBossConnectionFActory, ActiveMQQueue or
    * ActiveMQTopic.
    *
    * @param node the config
    * @throws Exception
    */
   private void createAndBindObject(final Node node) throws Exception
   {
      if (node.getNodeName().equals(JMSServerDeployer.CONNECTION_FACTORY_NODE_NAME))
      {
         deployConnectionFactory(node);
      }
      else if (node.getNodeName().equals(JMSServerDeployer.QUEUE_NODE_NAME))
      {
         deployQueue(node);
      }
      else if (node.getNodeName().equals(JMSServerDeployer.TOPIC_NODE_NAME))
      {
         deployTopic(node);
      }
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
      if (node.getNodeName().equals(JMSServerDeployer.CONNECTION_FACTORY_NODE_NAME))
      {
         String cfName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         jmsServerManager.destroyConnectionFactory(cfName);
      }
      else if (node.getNodeName().equals(JMSServerDeployer.QUEUE_NODE_NAME))
      {
         String queueName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         jmsServerManager.removeQueueFromJNDI(queueName);
      }
      else if (node.getNodeName().equals(JMSServerDeployer.TOPIC_NODE_NAME))
      {
         String topicName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         jmsServerManager.removeTopicFromJNDI(topicName);
      }
   }

   @Override
   public String[] getDefaultConfigFileNames()
   {
      return new String[]{"activemq-jms.xml"};
   }


   /**
    * @param node
    * @throws Exception
    */
   private void deployTopic(final Node node) throws Exception
   {
      TopicConfiguration topicConfig = parser.parseTopicConfiguration(node);
      jmsServerManager.createTopic(false, topicConfig.getName(), topicConfig.getBindings());
   }

   /**
    * @param node
    * @throws Exception
    */
   private void deployQueue(final Node node) throws Exception
   {
      JMSQueueConfiguration queueconfig = parser.parseQueueConfiguration(node);
      jmsServerManager.createQueue(false, queueconfig.getName(), queueconfig.getSelector(), queueconfig.isDurable(), queueconfig.getBindings());
   }

   /**
    * @param node
    * @throws Exception
    */
   private void deployConnectionFactory(final Node node) throws Exception
   {
      ConnectionFactoryConfiguration cfConfig = parser.parseConnectionFactoryConfiguration(node);
      jmsServerManager.createConnectionFactory(false, cfConfig, cfConfig.getBindings());
   }


}

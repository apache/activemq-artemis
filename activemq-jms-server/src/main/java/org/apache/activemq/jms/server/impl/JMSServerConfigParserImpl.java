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
package org.apache.activemq.jms.server.impl;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;

import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.core.config.impl.Validators;
import org.apache.activemq.jms.server.ActiveMQJMSServerLogger;
import org.apache.activemq.jms.server.JMSServerConfigParser;
import org.apache.activemq.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.jms.server.config.JMSConfiguration;
import org.apache.activemq.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq.jms.server.config.TopicConfiguration;
import org.apache.activemq.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.jms.server.config.impl.TopicConfigurationImpl;
import org.apache.activemq.utils.XMLConfigurationUtil;
import org.apache.activemq.utils.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * JMS Configuration File Parser.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public final class JMSServerConfigParserImpl implements JMSServerConfigParser
{
   protected static final String NAME_ATTR = "name";

   public JMSServerConfigParserImpl()
   {
   }

   /**
    * Parse the JMS Configuration XML as a JMSConfiguration object
    */
   public JMSConfiguration parseConfiguration(final InputStream stream) throws Exception
   {
      Reader reader = new InputStreamReader(stream);
      String xml = org.apache.activemq.utils.XMLUtil.readerToString(reader);
      xml = XMLUtil.replaceSystemProps(xml);
      return parseConfiguration(XMLUtil.stringToElement(xml));
   }

   /**
    * Parse the JMS Configuration XML as a JMSConfiguration object
    */
   public JMSConfiguration parseConfiguration(final Node rootnode) throws Exception
   {

      ArrayList<JMSQueueConfiguration> queues = new ArrayList<JMSQueueConfiguration>();
      ArrayList<TopicConfiguration> topics = new ArrayList<TopicConfiguration>();
      ArrayList<ConnectionFactoryConfiguration> cfs = new ArrayList<ConnectionFactoryConfiguration>();
      String domain = ActiveMQDefaultConfiguration.getDefaultJmxDomain();

      Element e = (Element) rootnode;

      org.apache.activemq.utils.XMLUtil.validate(rootnode, "schema/activemq-jms.xsd");

      String[] elements = new String[]{JMSServerDeployer.QUEUE_NODE_NAME,
         JMSServerDeployer.TOPIC_NODE_NAME};
      for (String element : elements)
      {
         NodeList children = e.getElementsByTagName(element);
         for (int i = 0; i < children.getLength(); i++)
         {
            Node node = children.item(i);
            Node keyNode = node.getAttributes().getNamedItem(JMSServerConfigParserImpl.NAME_ATTR);
            if (keyNode == null)
            {
               ActiveMQJMSServerLogger.LOGGER.jmsConfigMissingKey(node);
               continue;
            }

            if (node.getNodeName().equals(JMSServerDeployer.TOPIC_NODE_NAME))
            {
               topics.add(parseTopicConfiguration(node));
            }
            else if (node.getNodeName().equals(JMSServerDeployer.QUEUE_NODE_NAME))
            {
               queues.add(parseQueueConfiguration(node));
            }
         }
      }

      domain = XMLConfigurationUtil.getString(e, JMSServerDeployer.JMX_DOMAIN_NAME, ActiveMQDefaultConfiguration.getDefaultJmxDomain(), Validators.NO_CHECK);


      JMSConfiguration value = newConfig(queues, topics, domain);

      return value;
   }

   /**
    * Parse the topic node as a TopicConfiguration object
    *
    * @param node
    * @return topic configuration
    * @throws Exception
    */
   public TopicConfiguration parseTopicConfiguration(final Node node) throws Exception
   {
      String topicName = node.getAttributes().getNamedItem(JMSServerConfigParserImpl.NAME_ATTR).getNodeValue();

      return newTopic(topicName);
   }

   /**
    * Parse the Queue Configuration node as a QueueConfiguration object
    *
    * @param node
    * @return jms queue configuration
    * @throws Exception
    */
   public JMSQueueConfiguration parseQueueConfiguration(final Node node) throws Exception
   {
      Element e = (Element) node;
      NamedNodeMap atts = node.getAttributes();
      String queueName = atts.getNamedItem(JMSServerConfigParserImpl.NAME_ATTR).getNodeValue();
      String selectorString = null;
      boolean durable = XMLConfigurationUtil.getBoolean(e, "durable", JMSServerDeployer.DEFAULT_QUEUE_DURABILITY);
      NodeList children = node.getChildNodes();
      for (int i = 0; i < children.getLength(); i++)
      {
         Node child = children.item(i);

         if (JMSServerDeployer.QUEUE_SELECTOR_NODE_NAME.equals(children.item(i).getNodeName()))
         {
            Node selectorNode = children.item(i);
            Node attNode = selectorNode.getAttributes().getNamedItem("string");
            selectorString = attNode.getNodeValue();
         }
      }

      return newQueue(queueName, selectorString, durable);
   }

   /**
    * hook for integration layers
    *
    * @param topicName
    * @return
    */
   protected TopicConfiguration newTopic(final String topicName)
   {
      return new TopicConfigurationImpl()
         .setName(topicName);
   }

   /**
    * hook for integration layers
    *
    * @param queueName
    * @param selectorString
    * @param durable
    * @return
    */
   protected JMSQueueConfiguration newQueue(final String queueName,
                                            final String selectorString,
                                            final boolean durable)
   {
      return new JMSQueueConfigurationImpl().
         setName(queueName).
         setSelector(selectorString).
         setDurable(durable);
   }

   /**
    * hook for integration layers
    *
    * @param queues
    * @param topics
    * @param domain
    * @return
    */
   protected JMSConfiguration newConfig(final ArrayList<JMSQueueConfiguration> queues,
                                        final ArrayList<TopicConfiguration> topics, String domain)
   {
      return new JMSConfigurationImpl()
         .setQueueConfigurations(queues)
         .setTopicConfigurations(topics)
         .setDomain(domain);
   }
}

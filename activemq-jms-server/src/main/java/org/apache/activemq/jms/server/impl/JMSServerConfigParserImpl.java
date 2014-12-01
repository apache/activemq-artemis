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
import java.util.List;

import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.config.impl.Validators;
import org.apache.activemq.jms.server.ActiveMQJMSServerBundle;
import org.apache.activemq.jms.server.ActiveMQJMSServerLogger;
import org.apache.activemq.jms.server.JMSServerConfigParser;
import org.apache.activemq.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.jms.server.config.JMSConfiguration;
import org.apache.activemq.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq.jms.server.config.TopicConfiguration;
import org.apache.activemq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
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
         JMSServerDeployer.TOPIC_NODE_NAME,
         JMSServerDeployer.CONNECTION_FACTORY_NODE_NAME};
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

            if (node.getNodeName().equals(JMSServerDeployer.CONNECTION_FACTORY_NODE_NAME))
            {
               cfs.add(parseConnectionFactoryConfiguration(node));
            }
            else if (node.getNodeName().equals(JMSServerDeployer.TOPIC_NODE_NAME))
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


      JMSConfiguration value = newConfig(queues, topics, cfs, domain);

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
      NodeList children = node.getChildNodes();
      ArrayList<String> jndiNames = new ArrayList<String>();
      for (int i = 0; i < children.getLength(); i++)
      {
         Node child = children.item(i);

         if (JMSServerDeployer.ENTRY_NODE_NAME.equals(children.item(i).getNodeName()))
         {
            String jndiElement = child.getAttributes().getNamedItem("name").getNodeValue();
            jndiNames.add(jndiElement);
         }
      }

      String[] strBindings = jndiNames.toArray(new String[jndiNames.size()]);

      return newTopic(topicName, strBindings);

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
      ArrayList<String> jndiNames = new ArrayList<String>();
      for (int i = 0; i < children.getLength(); i++)
      {
         Node child = children.item(i);

         if (JMSServerDeployer.ENTRY_NODE_NAME.equals(children.item(i).getNodeName()))
         {
            String jndiName = child.getAttributes().getNamedItem("name").getNodeValue();
            jndiNames.add(jndiName);
         }
         else if (JMSServerDeployer.QUEUE_SELECTOR_NODE_NAME.equals(children.item(i).getNodeName()))
         {
            Node selectorNode = children.item(i);
            Node attNode = selectorNode.getAttributes().getNamedItem("string");
            selectorString = attNode.getNodeValue();
         }
      }

      String[] jndiArray = jndiNames.toArray(new String[jndiNames.size()]);
      return newQueue(queueName, selectorString, durable, jndiArray);
   }

   /**
    * Parse the Connection Configuration node as a ConnectionFactoryConfiguration object
    *
    * @param node
    * @return ConnectionFactoryConfiguration
    * @throws Exception
    */
   public ConnectionFactoryConfiguration parseConnectionFactoryConfiguration(final Node node) throws Exception
   {
      if (!node.getNodeName().equals(JMSServerDeployer.CONNECTION_FACTORY_NODE_NAME))
      {
         // sanity check, this shouldn't ever happen
         throw ActiveMQJMSServerBundle.BUNDLE.invalidNodeParsingCF(node.getNodeName());
      }
      Element e = (Element) node;

      String name = node.getAttributes().getNamedItem(JMSServerConfigParserImpl.NAME_ATTR).getNodeValue();

      String fact = e.getAttribute("signature");
      boolean isXA = XMLConfigurationUtil.getBoolean(e,
                                                     "xa",
                                                     ActiveMQClient.DEFAULT_XA);

      JMSFactoryType factType = resolveFactoryType(fact, isXA);

      long clientFailureCheckPeriod = XMLConfigurationUtil.getLong(e,
                                                                   "client-failure-check-period",
                                                                   ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                                   Validators.MINUS_ONE_OR_GT_ZERO);
      long connectionTTL = XMLConfigurationUtil.getLong(e,
                                                        "connection-ttl",
                                                        ActiveMQClient.DEFAULT_CONNECTION_TTL,
                                                        Validators.MINUS_ONE_OR_GE_ZERO);
      long callTimeout = XMLConfigurationUtil.getLong(e,
                                                      "call-timeout",
                                                      ActiveMQClient.DEFAULT_CALL_TIMEOUT,
                                                      Validators.GE_ZERO);
      long callFailoverTimeout = XMLConfigurationUtil.getLong(e,
                                                              "call-failover-timeout",
                                                              ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                                                              Validators.MINUS_ONE_OR_GT_ZERO);
      String clientID = XMLConfigurationUtil.getString(e, "client-id", null, Validators.NO_CHECK);
      int dupsOKBatchSize = XMLConfigurationUtil.getInteger(e,
                                                            "dups-ok-batch-size",
                                                            ActiveMQClient.DEFAULT_ACK_BATCH_SIZE,
                                                            Validators.GT_ZERO);
      int transactionBatchSize = XMLConfigurationUtil.getInteger(e,
                                                                 "transaction-batch-size",
                                                                 ActiveMQClient.DEFAULT_ACK_BATCH_SIZE,
                                                                 Validators.GT_ZERO);
      int consumerWindowSize = XMLConfigurationUtil.getInteger(e,
                                                               "consumer-window-size",
                                                               ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                               Validators.MINUS_ONE_OR_GE_ZERO);
      int producerWindowSize = XMLConfigurationUtil.getInteger(e,
                                                               "producer-window-size",
                                                               ActiveMQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
                                                               Validators.MINUS_ONE_OR_GT_ZERO);
      int consumerMaxRate = XMLConfigurationUtil.getInteger(e,
                                                            "consumer-max-rate",
                                                            ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE,
                                                            Validators.MINUS_ONE_OR_GT_ZERO);
      int confirmationWindowSize = XMLConfigurationUtil.getInteger(e,
                                                                   "confirmation-window-size",
                                                                   ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                                   Validators.MINUS_ONE_OR_GT_ZERO);
      int producerMaxRate = XMLConfigurationUtil.getInteger(e,
                                                            "producer-max-rate",
                                                            ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE,
                                                            Validators.MINUS_ONE_OR_GT_ZERO);
      boolean cacheLargeMessagesClient = XMLConfigurationUtil.getBoolean(e,
                                                                         "cache-large-message-client",
                                                                         ActiveMQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT);
      int minLargeMessageSize = XMLConfigurationUtil.getInteger(e,
                                                                "min-large-message-size",
                                                                ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                                Validators.GT_ZERO);

      boolean compressLargeMessages = XMLConfigurationUtil.getBoolean(e,
                                                                      "compress-large-messages",
                                                                      ActiveMQClient.DEFAULT_COMPRESS_LARGE_MESSAGES);

      boolean blockOnAcknowledge = XMLConfigurationUtil.getBoolean(e,
                                                                   "block-on-acknowledge",
                                                                   ActiveMQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      boolean blockOnNonDurableSend = XMLConfigurationUtil.getBoolean(e,
                                                                      "block-on-non-durable-send",
                                                                      ActiveMQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      boolean blockOnDurableSend = XMLConfigurationUtil.getBoolean(e,
                                                                   "block-on-durable-send",
                                                                   ActiveMQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      boolean autoGroup = XMLConfigurationUtil.getBoolean(e, "auto-group", ActiveMQClient.DEFAULT_AUTO_GROUP);
      boolean preAcknowledge = XMLConfigurationUtil.getBoolean(e,
                                                               "pre-acknowledge",
                                                               ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE);
      long retryInterval = XMLConfigurationUtil.getLong(e,
                                                        "retry-interval",
                                                        ActiveMQClient.DEFAULT_RETRY_INTERVAL,
                                                        Validators.GT_ZERO);
      double retryIntervalMultiplier = XMLConfigurationUtil.getDouble(e,
                                                                      "retry-interval-multiplier",
                                                                      ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                                      Validators.GT_ZERO);
      long maxRetryInterval = XMLConfigurationUtil.getLong(e,
                                                           "max-retry-interval",
                                                           ActiveMQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                           Validators.GT_ZERO);
      int reconnectAttempts = XMLConfigurationUtil.getInteger(e,
                                                              "reconnect-attempts",
                                                              ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS,
                                                              Validators.MINUS_ONE_OR_GE_ZERO);
      boolean failoverOnInitialConnection = XMLConfigurationUtil.getBoolean(e,
                                                                            "failover-on-initial-connection",
                                                                            ActiveMQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION);

      boolean useGlobalPools = XMLConfigurationUtil.getBoolean(e,
                                                               "use-global-pools",
                                                               ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS);
      int scheduledThreadPoolMaxSize = XMLConfigurationUtil.getInteger(e,
                                                                       "scheduled-thread-pool-max-size",
                                                                       ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                                                       Validators.MINUS_ONE_OR_GT_ZERO);
      int threadPoolMaxSize = XMLConfigurationUtil.getInteger(e,
                                                              "thread-pool-max-size",
                                                              ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                                                              Validators.MINUS_ONE_OR_GT_ZERO);
      String connectionLoadBalancingPolicyClassName = XMLConfigurationUtil.getString(e,
                                                                                     "connection-load-balancing-policy-class-name",
                                                                                     ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                                                     Validators.NOT_NULL_OR_EMPTY);
      boolean ha = XMLConfigurationUtil.getBoolean(e, "ha", ActiveMQClient.DEFAULT_HA);

      String groupid = XMLConfigurationUtil.getString(e, "group-id", null, Validators.NO_CHECK);
      List<String> jndiBindings = new ArrayList<String>();
      List<String> connectorNames = new ArrayList<String>();
      String discoveryGroupName = null;

      NodeList children = node.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (JMSServerDeployer.ENTRIES_NODE_NAME.equals(child.getNodeName()))
         {
            NodeList entries = child.getChildNodes();
            for (int i = 0; i < entries.getLength(); i++)
            {
               Node entry = entries.item(i);
               if (JMSServerDeployer.ENTRY_NODE_NAME.equals(entry.getNodeName()))
               {
                  String jndiName = entry.getAttributes().getNamedItem("name").getNodeValue();

                  jndiBindings.add(jndiName);
               }
            }
         }
         else if (JMSServerDeployer.CONNECTORS_NODE_NAME.equals(child.getNodeName()))
         {
            NodeList entries = child.getChildNodes();
            for (int i = 0; i < entries.getLength(); i++)
            {
               Node entry = entries.item(i);
               if (JMSServerDeployer.CONNECTOR_REF_ELEMENT.equals(entry.getNodeName()))
               {
                  String connectorName = entry.getAttributes().getNamedItem("connector-name").getNodeValue();

                  connectorNames.add(connectorName);
               }
            }
         }
         else if (JMSServerDeployer.DISCOVERY_GROUP_ELEMENT.equals(child.getNodeName()))
         {
            discoveryGroupName = child.getAttributes().getNamedItem("discovery-group-name").getNodeValue();

         }
      }

      ConnectionFactoryConfiguration cfConfig;

      String[] strbindings = jndiBindings.toArray(new String[jndiBindings.size()]);

      if (discoveryGroupName != null)
      {
         cfConfig = new ConnectionFactoryConfigurationImpl()
            .setDiscoveryGroupName(discoveryGroupName);
      }
      else
      {
         ArrayList<String> connectors = new ArrayList<String>(connectorNames.size());
         for (String connectorName : connectorNames)
         {
            connectors.add(connectorName);
         }
         cfConfig = new ConnectionFactoryConfigurationImpl()
            .setConnectorNames(connectors);
      }

      cfConfig
         .setName(name)
         .setHA(ha)
         .setBindings(strbindings)
         .setFactoryType(factType)
         .setClientID(clientID)
         .setClientFailureCheckPeriod(clientFailureCheckPeriod)
         .setConnectionTTL(connectionTTL)
         .setCallTimeout(callTimeout)
         .setCallFailoverTimeout(callFailoverTimeout)
         .setCacheLargeMessagesClient(cacheLargeMessagesClient)
         .setMinLargeMessageSize(minLargeMessageSize)
         .setCompressLargeMessages(compressLargeMessages)
         .setConsumerWindowSize(consumerWindowSize)
         .setConsumerMaxRate(consumerMaxRate)
         .setConfirmationWindowSize(confirmationWindowSize)
         .setProducerWindowSize(producerWindowSize)
         .setProducerMaxRate(producerMaxRate)
         .setBlockOnAcknowledge(blockOnAcknowledge)
         .setBlockOnDurableSend(blockOnDurableSend)
         .setBlockOnNonDurableSend(blockOnNonDurableSend)
         .setAutoGroup(autoGroup)
         .setPreAcknowledge(preAcknowledge)
         .setLoadBalancingPolicyClassName(connectionLoadBalancingPolicyClassName)
         .setTransactionBatchSize(transactionBatchSize)
         .setDupsOKBatchSize(dupsOKBatchSize)
         .setUseGlobalPools(useGlobalPools)
         .setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize)
         .setThreadPoolMaxSize(threadPoolMaxSize)
         .setRetryInterval(retryInterval)
         .setRetryIntervalMultiplier(retryIntervalMultiplier)
         .setMaxRetryInterval(maxRetryInterval)
         .setReconnectAttempts(reconnectAttempts)
         .setFailoverOnInitialConnection(failoverOnInitialConnection)
         .setGroupID(groupid);

      return cfConfig;
   }

   private JMSFactoryType resolveFactoryType(String fact, boolean isXA) throws ActiveMQException
   {
      if ("".equals(fact))
      {
         fact = "generic";
      }
      if (isXA)
      {
         if ("generic".equals(fact))
         {
            return JMSFactoryType.XA_CF;
         }
         if ("queue".equals(fact))
         {
            return JMSFactoryType.QUEUE_XA_CF;
         }
         if ("topic".equals(fact))
         {
            return JMSFactoryType.TOPIC_XA_CF;
         }
      }
      else
      {
         if ("generic".equals(fact))
         {
            return JMSFactoryType.CF;
         }
         if ("queue".equals(fact))
         {
            return JMSFactoryType.QUEUE_CF;
         }
         if ("topic".equals(fact))
         {
            return JMSFactoryType.TOPIC_CF;
         }
      }
      throw ActiveMQJMSServerBundle.BUNDLE.invalidSignatureParsingCF(fact);
   }

   /**
    * hook for integration layers
    *
    * @param topicName
    * @param strBindings
    * @return
    */
   protected TopicConfiguration newTopic(final String topicName, final String[] strBindings)
   {
      return new TopicConfigurationImpl()
         .setName(topicName)
         .setBindings(strBindings);
   }

   /**
    * hook for integration layers
    *
    * @param queueName
    * @param selectorString
    * @param durable
    * @param jndiArray
    * @return
    */
   protected JMSQueueConfiguration newQueue(final String queueName,
                                            final String selectorString,
                                            final boolean durable,
                                            final String[] jndiArray)
   {
      return new JMSQueueConfigurationImpl().
         setName(queueName).
         setSelector(selectorString).
         setDurable(durable).
         setBindings(jndiArray);
   }

   /**
    * hook for integration layers
    *
    * @param queues
    * @param topics
    * @param cfs
    * @param domain
    * @return
    */
   protected JMSConfiguration newConfig(final ArrayList<JMSQueueConfiguration> queues,
                                        final ArrayList<TopicConfiguration> topics,
                                        final ArrayList<ConnectionFactoryConfiguration> cfs, String domain)
   {
      return new JMSConfigurationImpl()
         .setConnectionFactoryConfigurations(cfs)
         .setQueueConfigurations(queues)
         .setTopicConfigurations(topics)
         .setDomain(domain);
   }
}

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
package org.apache.activemq6.tests.integration.jms.server.config;

import org.junit.Test;

import java.io.InputStream;
import java.net.URL;

import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.jms.server.JMSServerConfigParser;
import org.apache.activemq6.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq6.jms.server.config.JMSConfiguration;
import org.apache.activemq6.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq6.jms.server.config.TopicConfiguration;
import org.apache.activemq6.jms.server.impl.JMSServerConfigParserImpl;
import org.apache.activemq6.tests.util.ServiceTestBase;

/**
 * A JMSServerConfigParserTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JMSServerConfigParserTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------


   @Test
   public void testParsing() throws Exception
   {
      Configuration config = createDefaultConfig()
         // anything so the parsing will work
         .addConnectorConfiguration("netty", new TransportConfiguration());

      JMSServerConfigParser parser = new JMSServerConfigParserImpl();

      String conf = "hornetq-jms-for-JMSServerDeployerTest.xml";
      URL confURL = Thread.currentThread().getContextClassLoader().getResource(conf);

      InputStream stream = confURL.openStream();

      JMSConfiguration jmsconfig = parser.parseConfiguration(stream);
      stream.close();

      ConnectionFactoryConfiguration cfConfig = jmsconfig.getConnectionFactoryConfigurations().get(0);

      assertEquals(1234, cfConfig.getClientFailureCheckPeriod());
      assertEquals(5678, cfConfig.getCallTimeout());
      assertEquals(12345, cfConfig.getConsumerWindowSize());
      assertEquals(6789, cfConfig.getConsumerMaxRate());
      assertEquals(123456, cfConfig.getConfirmationWindowSize());
      assertEquals(7712652, cfConfig.getProducerWindowSize());
      assertEquals(789, cfConfig.getProducerMaxRate());
      assertEquals(12, cfConfig.getMinLargeMessageSize());
      assertEquals(true, cfConfig.isCompressLargeMessages());
      assertEquals("TestClientID", cfConfig.getClientID());
      assertEquals(3456, cfConfig.getDupsOKBatchSize());
      assertEquals(4567, cfConfig.getTransactionBatchSize());
      assertEquals(true, cfConfig.isBlockOnAcknowledge());
      assertEquals(false, cfConfig.isBlockOnNonDurableSend());
      assertEquals(true, cfConfig.isBlockOnDurableSend());
      assertEquals(false, cfConfig.isAutoGroup());
      assertEquals(true, cfConfig.isPreAcknowledge());
      assertEquals(2345, cfConfig.getConnectionTTL());
      assertEquals("FooClass", cfConfig.getLoadBalancingPolicyClassName());
      assertEquals(34, cfConfig.getReconnectAttempts());
      assertEquals(5, cfConfig.getRetryInterval());
      assertEquals(6.0, cfConfig.getRetryIntervalMultiplier(), 0.000001);
      assertEquals(300, cfConfig.getMaxRetryInterval());
      assertEquals(true, cfConfig.isCacheLargeMessagesClient());


      assertEquals(1, jmsconfig.getQueueConfigurations().size());

      JMSQueueConfiguration queueConfig = jmsconfig.getQueueConfigurations().get(0);
      assertEquals("fullConfigurationQueue", queueConfig.getName());
      assertEquals(2, queueConfig.getBindings().length);
      assertEquals("/fullConfigurationQueue", queueConfig.getBindings()[0]);
      assertEquals("/queue/fullConfigurationQueue", queueConfig.getBindings()[1]);


      assertEquals(1, jmsconfig.getTopicConfigurations().size());
      TopicConfiguration topicConfig = jmsconfig.getTopicConfigurations().get(0);
      assertEquals("fullConfigurationTopic", topicConfig.getName());
      assertEquals(2, topicConfig.getBindings().length);
      assertEquals("/fullConfigurationTopic", topicConfig.getBindings()[0]);
      assertEquals("/topic/fullConfigurationTopic", topicConfig.getBindings()[1]);


   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

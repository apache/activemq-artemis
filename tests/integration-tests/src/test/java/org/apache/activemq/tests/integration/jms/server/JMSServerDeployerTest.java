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
package org.apache.activemq.tests.integration.jms.server;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import java.net.URI;

import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.UDPBroadcastGroupConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.deployers.DeploymentManager;
import org.apache.activemq.core.deployers.impl.FileDeploymentManager;
import org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.jms.server.JMSServerManager;
import org.apache.activemq.jms.server.impl.JMSServerDeployer;
import org.apache.activemq.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.unit.util.InVMNamingContext;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;

/**
 * A JMSServerDeployerTest
 *
 * @author jmesnil
 */
public class JMSServerDeployerTest extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   private JMSServerManager jmsServer;

   private Context context;

   private DeploymentManager deploymentManager;

   private Configuration config;

   // Public --------------------------------------------------------

   @Test
   public void testValidateEmptyConfiguration() throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager);

      String xml = "<configuration xmlns='urn:activemq'> " + "</configuration>";

      Element rootNode = org.apache.activemq.utils.XMLUtil.stringToElement(xml);
      deployer.validate(rootNode);
   }

   @Test
   public void testDeployUnusualQueueNames() throws Exception
   {
      doTestDeployQueuesWithUnusualNames("queue.with.dots.in.name", "/myqueue");

      doTestDeployQueuesWithUnusualNames("queue with spaces in name", "/myqueue2");

      doTestDeployQueuesWithUnusualNames("queue/with/slashes/in/name", "/myqueue3");

      doTestDeployQueuesWithUnusualNames("queue\\with\\backslashes\\in\\name", "/myqueue4");

      doTestDeployQueuesWithUnusualNames("queue with # chars and * chars in name",
                                         "queue with &#35; chars and &#42; chars in name",
                                         "/myqueue5");
   }

   @Test
   public void testDeployUnusualTopicNames() throws Exception
   {
      doTestDeployTopicsWithUnusualNames("topic.with.dots.in.name", "/mytopic");

      doTestDeployTopicsWithUnusualNames("topic with spaces in name", "/mytopic2");

      doTestDeployTopicsWithUnusualNames("topic/with/slashes/in/name", "/mytopic3");

      doTestDeployTopicsWithUnusualNames("topic\\with\\backslashes\\in\\name", "/mytopic4");

      doTestDeployTopicsWithUnusualNames("topic with # chars and * chars in name",
                                         "topic with &#35; chars and &#42; chars in name",
                                         "/mytopic5");
   }

   private void doTestDeployQueuesWithUnusualNames(final String queueName,
                                                   final String htmlEncodedName,
                                                   final String jndiName) throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager);

      String xml = "<queue name=\"" + htmlEncodedName + "\">" + "<entry name=\"" + jndiName + "\"/>" + "</queue>";

      Element rootNode = org.apache.activemq.utils.XMLUtil.stringToElement(xml);

      deployer.deploy(rootNode);

      Queue queue = (Queue) context.lookup(jndiName);
      Assert.assertNotNull(queue);
      Assert.assertEquals(queueName, queue.getQueueName());
   }

   private void doTestDeployTopicsWithUnusualNames(final String topicName,
                                                   final String htmlEncodedName,
                                                   final String jndiName) throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager);

      String xml =

         "<topic name=\"" + htmlEncodedName + "\">" + "<entry name=\"" + jndiName + "\"/>" + "</topic>";

      Element rootNode = org.apache.activemq.utils.XMLUtil.stringToElement(xml);

      deployer.deploy(rootNode);

      Topic topic = (Topic) context.lookup(jndiName);
      Assert.assertNotNull(topic);
      Assert.assertEquals(topicName, topic.getTopicName());
   }

   private void doTestDeployQueuesWithUnusualNames(final String queueName, final String jndiName) throws Exception
   {
      doTestDeployQueuesWithUnusualNames(queueName, queueName, jndiName);
   }

   private void doTestDeployTopicsWithUnusualNames(final String topicName, final String jndiName) throws Exception
   {
      doTestDeployTopicsWithUnusualNames(topicName, topicName, jndiName);
   }

   @Test
   public void testDeployFullConfiguration() throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager);

      String conf = "activemq-jms-for-JMSServerDeployerTest.xml";
      URI confURL = Thread.currentThread().getContextClassLoader().getResource(conf).toURI();

      String[] connectionFactoryBindings = new String[]{"/fullConfigurationConnectionFactory",
         "/acme/fullConfigurationConnectionFactory",
         "java:/xyz/tfullConfigurationConnectionFactory",
         "java:/connectionfactories/acme/fullConfigurationConnectionFactory"};
      String[] queueBindings = new String[]{"/fullConfigurationQueue", "/queue/fullConfigurationQueue"};
      String[] topicBindings = new String[]{"/fullConfigurationTopic", "/topic/fullConfigurationTopic"};

      for (String binding : connectionFactoryBindings)
      {
         UnitTestCase.checkNoBinding(context, binding);
      }
      for (String binding : queueBindings)
      {
         UnitTestCase.checkNoBinding(context, binding);
      }
      for (String binding : topicBindings)
      {
         UnitTestCase.checkNoBinding(context, binding);
      }

      deployer.deploy(confURL);

      for (String binding : connectionFactoryBindings)
      {
         UnitTestCase.checkBinding(context, binding);
      }
      for (String binding : queueBindings)
      {
         UnitTestCase.checkBinding(context, binding);
      }
      for (String binding : topicBindings)
      {
         UnitTestCase.checkBinding(context, binding);
      }

      for (String binding : connectionFactoryBindings)
      {
         ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) context.lookup(binding);
         Assert.assertNotNull(cf);
         Assert.assertEquals(1234, cf.getClientFailureCheckPeriod());
         Assert.assertEquals(5678, cf.getCallTimeout());
         Assert.assertEquals(12345, cf.getConsumerWindowSize());
         Assert.assertEquals(6789, cf.getConsumerMaxRate());
         Assert.assertEquals(123456, cf.getConfirmationWindowSize());
         Assert.assertEquals(7712652, cf.getProducerWindowSize());
         Assert.assertEquals(789, cf.getProducerMaxRate());
         Assert.assertEquals(12, cf.getMinLargeMessageSize());
         Assert.assertEquals("TestClientID", cf.getClientID());
         Assert.assertEquals(3456, cf.getDupsOKBatchSize());
         Assert.assertEquals(4567, cf.getTransactionBatchSize());
         Assert.assertEquals(true, cf.isBlockOnAcknowledge());
         Assert.assertEquals(false, cf.isBlockOnNonDurableSend());
         Assert.assertEquals(true, cf.isBlockOnDurableSend());
         Assert.assertEquals(false, cf.isAutoGroup());
         Assert.assertEquals(true, cf.isPreAcknowledge());
         Assert.assertEquals(2345, cf.getConnectionTTL());
         assertEquals(true, cf.isFailoverOnInitialConnection());
         Assert.assertEquals(34, cf.getReconnectAttempts());
         Assert.assertEquals(5, cf.getRetryInterval());
         Assert.assertEquals(6.0, cf.getRetryIntervalMultiplier(), 0.000001);
         Assert.assertEquals(true, cf.isCacheLargeMessagesClient());
      }

      for (String binding : queueBindings)
      {
         Queue queue = (Queue) context.lookup(binding);
         Assert.assertNotNull(queue);
         Assert.assertEquals("fullConfigurationQueue", queue.getQueueName());
      }

      for (String binding : topicBindings)
      {
         Topic topic = (Topic) context.lookup(binding);
         Assert.assertNotNull(topic);
         Assert.assertEquals("fullConfigurationTopic", topic.getTopicName());
      }
   }

   @Test
   public void testDeployFullConfiguration2() throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager);

      String conf = "activemq-jms-for-JMSServerDeployerTest2.xml";
      URI confURL = Thread.currentThread().getContextClassLoader().getResource(conf).toURI();

      String[] connectionFactoryBindings = new String[]{"/fullConfigurationConnectionFactory",
         "/acme/fullConfigurationConnectionFactory",
         "java:/xyz/tfullConfigurationConnectionFactory",
         "java:/connectionfactories/acme/fullConfigurationConnectionFactory"};
      String[] queueBindings = new String[]{"/fullConfigurationQueue", "/queue/fullConfigurationQueue"};
      String[] topicBindings = new String[]{"/fullConfigurationTopic", "/topic/fullConfigurationTopic"};

      for (String binding : connectionFactoryBindings)
      {
         UnitTestCase.checkNoBinding(context, binding);
      }
      for (String binding : queueBindings)
      {
         UnitTestCase.checkNoBinding(context, binding);
      }
      for (String binding : topicBindings)
      {
         UnitTestCase.checkNoBinding(context, binding);
      }

      deployer.deploy(confURL);

      for (String binding : connectionFactoryBindings)
      {
         UnitTestCase.checkBinding(context, binding);
      }
      for (String binding : queueBindings)
      {
         UnitTestCase.checkBinding(context, binding);
      }
      for (String binding : topicBindings)
      {
         UnitTestCase.checkBinding(context, binding);
      }

      for (String binding : connectionFactoryBindings)
      {
         ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) context.lookup(binding);
         Assert.assertNotNull(cf);
         Assert.assertEquals(1234, cf.getClientFailureCheckPeriod());
         Assert.assertEquals(5678, cf.getCallTimeout());
         Assert.assertEquals(12345, cf.getConsumerWindowSize());
         Assert.assertEquals(6789, cf.getConsumerMaxRate());
         Assert.assertEquals(123456, cf.getConfirmationWindowSize());
         Assert.assertEquals(7712652, cf.getProducerWindowSize());
         Assert.assertEquals(789, cf.getProducerMaxRate());
         Assert.assertEquals(12, cf.getMinLargeMessageSize());
         Assert.assertEquals("TestClientID", cf.getClientID());
         Assert.assertEquals(3456, cf.getDupsOKBatchSize());
         Assert.assertEquals(4567, cf.getTransactionBatchSize());
         Assert.assertEquals(true, cf.isBlockOnAcknowledge());
         Assert.assertEquals(false, cf.isBlockOnNonDurableSend());
         Assert.assertEquals(true, cf.isBlockOnDurableSend());
         Assert.assertEquals(false, cf.isAutoGroup());
         Assert.assertEquals(true, cf.isPreAcknowledge());
         Assert.assertEquals(2345, cf.getConnectionTTL());
         assertEquals(true, cf.isFailoverOnInitialConnection());
         Assert.assertEquals(34, cf.getReconnectAttempts());
         Assert.assertEquals(5, cf.getRetryInterval());
         Assert.assertEquals(6.0, cf.getRetryIntervalMultiplier(), 0.000001);
         Assert.assertEquals(true, cf.isCacheLargeMessagesClient());
      }

      for (String binding : queueBindings)
      {
         Queue queue = (Queue) context.lookup(binding);
         Assert.assertNotNull(queue);
         Assert.assertEquals("fullConfigurationQueue", queue.getQueueName());
      }

      for (String binding : topicBindings)
      {
         Topic topic = (Topic) context.lookup(binding);
         Assert.assertNotNull(topic);
         Assert.assertEquals("fullConfigurationTopic", topic.getTopicName());
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      DiscoveryGroupConfiguration dcg = new DiscoveryGroupConfiguration()
         .setName("mygroup")
         .setRefreshTimeout(5432)
         .setDiscoveryInitialWaitTimeout(5432)
         .setBroadcastEndpointFactoryConfiguration(new UDPBroadcastGroupConfiguration()
                                                      .setGroupAddress("243.7.7.7")
                                                      .setGroupPort(12345)
                                                      .setLocalBindAddress("172.16.8.10"));

      config = createBasicConfig()
         .addConnectorConfiguration("netty", new TransportConfiguration(NettyConnectorFactory.class.getName()))
         .addDiscoveryGroupConfiguration("mygroup", dcg);

      ActiveMQServer server = createServer(false, config);

      deploymentManager = new FileDeploymentManager(config.getFileDeployerScanPeriod());

      jmsServer = new JMSServerManagerImpl(server);
      context = new InVMNamingContext();
      jmsServer.setContext(context);
      jmsServer.start();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      jmsServer.stop();
      jmsServer = null;
      context = null;
      deploymentManager = null;
      config = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
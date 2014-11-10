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
package org.hornetq.tests.integration.jms.server.config;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;

import org.junit.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.JMSConfiguration;
import org.hornetq.jms.server.config.TopicConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.hornetq.jms.server.config.impl.JMSConfigurationImpl;
import org.hornetq.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.hornetq.jms.server.config.impl.TopicConfigurationImpl;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMNamingContext;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A JMSConfigurationTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class JMSConfigurationTest extends ServiceTestBase
{

   @Test
   public void testSetupJMSConfiguration() throws Exception
   {
      Context context = new InVMNamingContext();

      Configuration coreConfiguration = createDefaultConfig(false);
      HornetQServer coreServer = new HornetQServerImpl(coreConfiguration);

      JMSConfiguration jmsConfiguration = new JMSConfigurationImpl();
      jmsConfiguration.setContext(context);
      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName());
      List<TransportConfiguration> transportConfigs = new ArrayList<TransportConfiguration>();
      transportConfigs.add(connectorConfig);

      ConnectionFactoryConfiguration cfConfig = new ConnectionFactoryConfigurationImpl()
         .setName(RandomUtil.randomString())
         .setConnectorNames(registerConnectors(coreServer, transportConfigs))
         .setBindings("/cf/binding1", "/cf/binding2");

      jmsConfiguration.getConnectionFactoryConfigurations().add(cfConfig);
      JMSQueueConfigurationImpl queueConfig = new JMSQueueConfigurationImpl()
         .setName(RandomUtil.randomString())
         .setDurable(false)
         .setBindings(
            "/queue/binding1",
            "/queue/binding2");
      jmsConfiguration.getQueueConfigurations().add(queueConfig);
      TopicConfiguration topicConfig = new TopicConfigurationImpl()
         .setName(RandomUtil.randomString())
         .setBindings(
            "/topic/binding1",
            "/topic/binding2");
      jmsConfiguration.getTopicConfigurations().add(topicConfig);

      JMSServerManager server = new JMSServerManagerImpl(coreServer, jmsConfiguration);
      server.start();

      for (String binding : cfConfig.getBindings())
      {
         Object o = context.lookup(binding);
         Assert.assertNotNull(o);
         Assert.assertTrue(o instanceof ConnectionFactory);
         ConnectionFactory cf = (ConnectionFactory)o;
         Connection connection = cf.createConnection();
         connection.close();
      }

      for (String binding : queueConfig.getBindings())
      {
         Object o = context.lookup(binding);
         Assert.assertNotNull(o);
         Assert.assertTrue(o instanceof Queue);
         Queue queue = (Queue)o;
         Assert.assertEquals(queueConfig.getName(), queue.getQueueName());
      }

      for (String binding : topicConfig.getBindings())
      {
         Object o = context.lookup(binding);
         Assert.assertNotNull(o);
         Assert.assertTrue(o instanceof Topic);
         Topic topic = (Topic)o;
         Assert.assertEquals(topicConfig.getName(), topic.getTopicName());
      }

      server.stop();
   }
}

/*
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
package org.apache.activemq.artemis.tests.integration.jms.server.config;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.TopicConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.TopicConfigurationImpl;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import java.util.ArrayList;
import java.util.List;

public class JMSConfigurationTest extends ActiveMQTestBase
{
   @Test
   public void testSetupJMSConfiguration() throws Exception
   {
      Context context = new InVMNamingContext();

      ActiveMQServer coreServer = new ActiveMQServerImpl(createDefaultInVMConfig());

      JMSConfiguration jmsConfiguration = new JMSConfigurationImpl();
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

      server.setRegistry(new JndiBindingRegistry(context));
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

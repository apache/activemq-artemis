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
package org.apache.activemq.tests.util;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.jms.ActiveMQJMSClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.unit.util.InVMNamingContext;
import org.junit.After;
import org.junit.Before;

/**
 * A JMSBaseTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class JMSClusteredTestBase extends ServiceTestBase
{

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   protected MBeanServer mBeanServer1;

   protected ActiveMQServer server1;

   protected JMSServerManagerImpl jmsServer1;

   protected MBeanServer mBeanServer2;

   protected ActiveMQServer server2;

   protected JMSServerManagerImpl jmsServer2;

   protected ConnectionFactory cf1;

   protected ConnectionFactory cf2;

   protected InVMNamingContext context1;

   protected InVMNamingContext context2;

   protected static final int MAX_HOPS = 1;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * @throws Exception
    */
   protected Queue createQueue(final String name) throws Exception
   {
      jmsServer2.createQueue(false, name, null, true, "/queue/" + name);
      jmsServer1.createQueue(false, name, null, true, "/queue/" + name);

      assertTrue(waitForBindings(server1, "jms.queue." + name, false, 1, 0, 10000));
      assertTrue(waitForBindings(server2, "jms.queue." + name, false, 1, 0, 10000));

      return (Queue) context1.lookup("/queue/" + name);
   }

   protected Topic createTopic(final String name) throws Exception
   {
      return createTopic(name, false);
   }

   protected Topic createTopic(final String name, boolean durable) throws Exception
   {
      jmsServer2.createTopic(durable, name, "/topic/" + name);
      jmsServer1.createTopic(durable, name, "/topic/" + name);

      return (Topic) context1.lookup("/topic/" + name);
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      setupServer2();
      setupServer1();

      jmsServer1.start();
      jmsServer1.activated();
      waitForServer(jmsServer1.getActiveMQServer());

      jmsServer2.start();
      jmsServer2.activated();
      waitForServer(jmsServer2.getActiveMQServer());

      waitForTopology(jmsServer1.getActiveMQServer(), 2);

      waitForTopology(jmsServer2.getActiveMQServer(), 2);

      cf1 = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                                                             generateInVMParams(0)));
      cf2 = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                                                             generateInVMParams(1)));
   }

   /**
    * @throws Exception
    */
   private void setupServer2() throws Exception
   {
      Configuration conf2 = createConfigServer2();

      JMSConfigurationImpl jmsconfig = new JMSConfigurationImpl();

      mBeanServer2 = MBeanServerFactory.createMBeanServer();
      server2 = ActiveMQServers.newActiveMQServer(conf2, mBeanServer2, enablePersistence());
      jmsServer2 = new JMSServerManagerImpl(server2, jmsconfig);
      context2 = new InVMNamingContext();
      jmsServer2.setContext(context2);
   }

   /**
    * @return
    */
   protected Configuration createConfigServer2()
   {
      List<String> toOtherServerPair = new ArrayList<String>();
      toOtherServerPair.add("toServer1");

      Configuration conf2 = createDefaultConfig(1, generateInVMParams(1), INVM_ACCEPTOR_FACTORY);
      conf2.setSecurityEnabled(false);
      conf2.setPersistenceEnabled(false);

      conf2.getConnectorConfigurations().put("toServer1",
                                             new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                        generateInVMParams(0)));
      conf2.getConnectorConfigurations().put("server2",
                                             new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                        generateInVMParams(1)));

      conf2.getClusterConfigurations().add(new ClusterConnectionConfiguration()
         .setName("to-server1")
         .setAddress("jms")
         .setConnectorName("server2")
         .setRetryInterval(1000)
         .setMaxHops(MAX_HOPS)
         .setConfirmationWindowSize(1024)
         .setStaticConnectors(toOtherServerPair));

      return conf2;
   }

   /**
    * @throws Exception
    */
   private void setupServer1() throws Exception
   {
      Configuration conf1 = createConfigServer1();

      JMSConfigurationImpl jmsconfig = new JMSConfigurationImpl();

      mBeanServer1 = MBeanServerFactory.createMBeanServer();
      server1 = ActiveMQServers.newActiveMQServer(conf1, mBeanServer1, enablePersistence());
      jmsServer1 = new JMSServerManagerImpl(server1, jmsconfig);
      context1 = new InVMNamingContext();
      jmsServer1.setContext(context1);
   }

   protected boolean enablePersistence()
   {
      return false;
   }

   /**
    * @return
    */
   protected Configuration createConfigServer1()
   {
      List<String> toOtherServerPair = new ArrayList<String>();
      toOtherServerPair.add("toServer2");

      Configuration conf1 = createDefaultConfig(0, generateInVMParams(0), INVM_ACCEPTOR_FACTORY);

      conf1.setSecurityEnabled(false);
      conf1.setPersistenceEnabled(false);

      conf1.getConnectorConfigurations().put("toServer2",
                                             new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                        generateInVMParams(1)));
      conf1.getConnectorConfigurations().put("server1",
                                             new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                        generateInVMParams(0)));

      conf1.getClusterConfigurations().add(new ClusterConnectionConfiguration()
         .setName("to-server2")
         .setAddress("jms")
         .setConnectorName("server1")
         .setRetryInterval(1000)
         .setMaxHops(MAX_HOPS)
         .setConfirmationWindowSize(1024)
         .setStaticConnectors(toOtherServerPair));

      return conf1;
   }

   @Override
   @After
   public void tearDown() throws Exception
   {

      try
      {
         jmsServer2.stop();

         server2.stop();

         context2.close();
      }
      catch (Throwable e)
      {
         log.warn("Can't stop server2", e);
      }

      ((ActiveMQConnectionFactory) cf1).close();

      ((ActiveMQConnectionFactory) cf2).close();

      server2 = null;

      jmsServer2 = null;

      context2 = null;

      cf1 = null;

      try
      {
         jmsServer1.stop();

         server1.stop();

         context1.close();
      }
      catch (Throwable e)
      {
         log.warn("Can't stop server1", e);
      }

      server1 = null;

      jmsServer1 = null;

      context1 = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   protected Map<String, Object> generateInVMParams(final int node)
   {
      Map<String, Object> params = new HashMap<String, Object>();

      params.put(org.apache.activemq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, node);

      return params;
   }


}

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
package org.apache.activemq.core.config.impl;

import java.util.Collections;

import org.apache.activemq.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.core.config.BridgeConfiguration;
import org.apache.activemq.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.DivertConfiguration;
import org.apache.activemq.core.config.FileDeploymentManager;
import org.apache.activemq.core.config.HAPolicyConfiguration;
import org.apache.activemq.core.config.ha.LiveOnlyPolicyConfiguration;
import org.apache.activemq.core.security.Role;
import org.apache.activemq.core.server.JournalType;
import org.apache.activemq.core.settings.impl.SlowConsumerPolicy;
import org.junit.Assert;
import org.junit.Test;

public class FileConfigurationTest extends ConfigurationImplTest
{
   @Override
   @Test
   public void testDefaults()
   {
      // Check they match the values from the test file
      Assert.assertEquals("SomeNameForUseOnTheApplicationServer", conf.getName());
      Assert.assertEquals(false, conf.isPersistenceEnabled());
      Assert.assertEquals(true, conf.isClustered());
      Assert.assertEquals(12345, conf.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(54321, conf.getThreadPoolMaxSize());
      Assert.assertEquals(false, conf.isSecurityEnabled());
      Assert.assertEquals(5423, conf.getSecurityInvalidationInterval());
      Assert.assertEquals(true, conf.isWildcardRoutingEnabled());
      Assert.assertEquals(new SimpleString("Giraffe"), conf.getManagementAddress());
      Assert.assertEquals(new SimpleString("Whatever"), conf.getManagementNotificationAddress());
      Assert.assertEquals("Frog", conf.getClusterUser());
      Assert.assertEquals("Wombat", conf.getClusterPassword());
      Assert.assertEquals(false, conf.isJMXManagementEnabled());
      Assert.assertEquals("gro.qtenroh", conf.getJMXDomain());
      Assert.assertEquals(true, conf.isMessageCounterEnabled());
      Assert.assertEquals(5, conf.getMessageCounterMaxDayHistory());
      Assert.assertEquals(123456, conf.getMessageCounterSamplePeriod());
      Assert.assertEquals(12345, conf.getConnectionTTLOverride());
      Assert.assertEquals(98765, conf.getTransactionTimeout());
      Assert.assertEquals(56789, conf.getTransactionTimeoutScanPeriod());
      Assert.assertEquals(10111213, conf.getMessageExpiryScanPeriod());
      Assert.assertEquals(8, conf.getMessageExpiryThreadPriority());
      Assert.assertEquals(127, conf.getIDCacheSize());
      Assert.assertEquals(true, conf.isPersistIDCache());
      Assert.assertEquals(true, conf.isPersistDeliveryCountBeforeDelivery());
      Assert.assertEquals("pagingdir", conf.getPagingDirectory());
      Assert.assertEquals("somedir", conf.getBindingsDirectory());
      Assert.assertEquals(false, conf.isCreateBindingsDir());

      Assert.assertEquals("max concurrent io", 17, conf.getPageMaxConcurrentIO());
      Assert.assertEquals("somedir2", conf.getJournalDirectory());
      Assert.assertEquals(false, conf.isCreateJournalDir());
      Assert.assertEquals(JournalType.NIO, conf.getJournalType());
      Assert.assertEquals(10000, conf.getJournalBufferSize_NIO());
      Assert.assertEquals(1000, conf.getJournalBufferTimeout_NIO());
      Assert.assertEquals(56546, conf.getJournalMaxIO_NIO());

      Assert.assertEquals(false, conf.isJournalSyncTransactional());
      Assert.assertEquals(true, conf.isJournalSyncNonTransactional());
      Assert.assertEquals(12345678, conf.getJournalFileSize());
      Assert.assertEquals(100, conf.getJournalMinFiles());
      Assert.assertEquals(123, conf.getJournalCompactMinFiles());
      Assert.assertEquals(33, conf.getJournalCompactPercentage());
      Assert.assertEquals(true, conf.isGracefulShutdownEnabled());
      Assert.assertEquals(12345, conf.getGracefulShutdownTimeout());

      Assert.assertEquals("largemessagesdir", conf.getLargeMessagesDirectory());
      Assert.assertEquals(95, conf.getMemoryWarningThreshold());

      Assert.assertEquals(2, conf.getIncomingInterceptorClassNames().size());
      Assert.assertTrue(conf.getIncomingInterceptorClassNames()
                           .contains("org.apache.activemq.tests.unit.core.config.impl.TestInterceptor1"));
      Assert.assertTrue(conf.getIncomingInterceptorClassNames()
                           .contains("org.apache.activemq.tests.unit.core.config.impl.TestInterceptor2"));


      Assert.assertEquals(2, conf.getConnectorConfigurations().size());

      TransportConfiguration tc = conf.getConnectorConfigurations().get("connector1");
      Assert.assertNotNull(tc);
      Assert.assertEquals("org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory", tc.getFactoryClassName());
      Assert.assertEquals("mylocal", tc.getParams().get("localAddress"));
      Assert.assertEquals("99", tc.getParams().get("localPort"));
      Assert.assertEquals("localhost1", tc.getParams().get("host"));
      Assert.assertEquals("5678", tc.getParams().get("port"));

      tc = conf.getConnectorConfigurations().get("connector2");
      Assert.assertNotNull(tc);
      Assert.assertEquals("org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory", tc.getFactoryClassName());
      Assert.assertEquals("5", tc.getParams().get("serverId"));

      Assert.assertEquals(2, conf.getAcceptorConfigurations().size());
      for (TransportConfiguration ac : conf.getAcceptorConfigurations())
      {
         if (ac.getFactoryClassName().equals("org.apache.activemq.core.remoting.impl.netty.NettyAcceptorFactory"))
         {
            Assert.assertEquals("456", ac.getParams().get("tcpNoDelay"));
            Assert.assertEquals("44", ac.getParams().get("connectionTtl"));
         }
         else
         {
            Assert.assertEquals("org.apache.activemq.core.remoting.impl.invm.InVMAcceptorFactory",
                                ac.getFactoryClassName());
            Assert.assertEquals("0", ac.getParams().get("serverId"));
         }
      }

      Assert.assertEquals(2, conf.getBroadcastGroupConfigurations().size());
      for (BroadcastGroupConfiguration bc : conf.getBroadcastGroupConfigurations())
      {
         UDPBroadcastEndpointFactory udpBc = (UDPBroadcastEndpointFactory) bc.getEndpointFactory();
         if (bc.getName().equals("bg1"))
         {
            Assert.assertEquals("bg1", bc.getName());
            Assert.assertEquals(10999, udpBc.getLocalBindPort());
            Assert.assertEquals("192.168.0.120", udpBc.getGroupAddress());
            Assert.assertEquals(11999, udpBc.getGroupPort());
            Assert.assertEquals(12345, bc.getBroadcastPeriod());
            Assert.assertEquals("connector1", bc.getConnectorInfos().get(0));
         }
         else
         {
            Assert.assertEquals("bg2", bc.getName());
            Assert.assertEquals(12999, udpBc.getLocalBindPort());
            Assert.assertEquals("192.168.0.121", udpBc.getGroupAddress());
            Assert.assertEquals(13999, udpBc.getGroupPort());
            Assert.assertEquals(23456, bc.getBroadcastPeriod());
            Assert.assertEquals("connector2", bc.getConnectorInfos().get(0));
         }
      }

      Assert.assertEquals(2, conf.getDiscoveryGroupConfigurations().size());
      DiscoveryGroupConfiguration dc = conf.getDiscoveryGroupConfigurations().get("dg1");
      Assert.assertEquals("dg1", dc.getName());
      Assert.assertEquals("192.168.0.120", ((UDPBroadcastEndpointFactory) dc.getBroadcastEndpointFactory()).getGroupAddress());
      assertEquals("172.16.8.10", ((UDPBroadcastEndpointFactory) dc.getBroadcastEndpointFactory()).getLocalBindAddress());
      Assert.assertEquals(11999, ((UDPBroadcastEndpointFactory) dc.getBroadcastEndpointFactory()).getGroupPort());
      Assert.assertEquals(12345, dc.getRefreshTimeout());

      dc = conf.getDiscoveryGroupConfigurations().get("dg2");
      Assert.assertEquals("dg2", dc.getName());
      Assert.assertEquals("192.168.0.121", ((UDPBroadcastEndpointFactory) dc.getBroadcastEndpointFactory()).getGroupAddress());
      assertEquals("172.16.8.11", ((UDPBroadcastEndpointFactory) dc.getBroadcastEndpointFactory()).getLocalBindAddress());
      Assert.assertEquals(12999, ((UDPBroadcastEndpointFactory) dc.getBroadcastEndpointFactory()).getGroupPort());
      Assert.assertEquals(23456, dc.getRefreshTimeout());

      Assert.assertEquals(2, conf.getDivertConfigurations().size());
      for (DivertConfiguration dic : conf.getDivertConfigurations())
      {
         if (dic.getName().equals("divert1"))
         {
            Assert.assertEquals("divert1", dic.getName());
            Assert.assertEquals("routing-name1", dic.getRoutingName());
            Assert.assertEquals("address1", dic.getAddress());
            Assert.assertEquals("forwarding-address1", dic.getForwardingAddress());
            Assert.assertEquals("speed > 88", dic.getFilterString());
            Assert.assertEquals("org.foo.Transformer", dic.getTransformerClassName());
            Assert.assertEquals(true, dic.isExclusive());
         }
         else
         {
            Assert.assertEquals("divert2", dic.getName());
            Assert.assertEquals("routing-name2", dic.getRoutingName());
            Assert.assertEquals("address2", dic.getAddress());
            Assert.assertEquals("forwarding-address2", dic.getForwardingAddress());
            Assert.assertEquals("speed < 88", dic.getFilterString());
            Assert.assertEquals("org.foo.Transformer2", dic.getTransformerClassName());
            Assert.assertEquals(false, dic.isExclusive());
         }
      }

      Assert.assertEquals(2, conf.getBridgeConfigurations().size());
      for (BridgeConfiguration bc : conf.getBridgeConfigurations())
      {
         if (bc.getName().equals("bridge1"))
         {
            Assert.assertEquals("bridge1", bc.getName());
            Assert.assertEquals("queue1", bc.getQueueName());
            Assert.assertEquals("minLargeMessageSize", 4, bc.getMinLargeMessageSize());
            assertEquals("check-period", 31, bc.getClientFailureCheckPeriod());
            assertEquals("connection time-to-live", 370, bc.getConnectionTTL());
            Assert.assertEquals("bridge-forwarding-address1", bc.getForwardingAddress());
            Assert.assertEquals("sku > 1", bc.getFilterString());
            Assert.assertEquals("org.foo.BridgeTransformer", bc.getTransformerClassName());
            Assert.assertEquals(3, bc.getRetryInterval());
            Assert.assertEquals(0.2, bc.getRetryIntervalMultiplier(), 0.0001);
            assertEquals("max retry interval", 10002, bc.getMaxRetryInterval());
            Assert.assertEquals(2, bc.getReconnectAttempts());
            Assert.assertEquals(true, bc.isUseDuplicateDetection());
            Assert.assertEquals("connector1", bc.getStaticConnectors().get(0));
            Assert.assertEquals(null, bc.getDiscoveryGroupName());
         }
         else
         {
            Assert.assertEquals("bridge2", bc.getName());
            Assert.assertEquals("queue2", bc.getQueueName());
            Assert.assertEquals("bridge-forwarding-address2", bc.getForwardingAddress());
            Assert.assertEquals(null, bc.getFilterString());
            Assert.assertEquals(null, bc.getTransformerClassName());
            Assert.assertEquals(null, bc.getStaticConnectors());
            Assert.assertEquals("dg1", bc.getDiscoveryGroupName());
         }
      }

      Assert.assertEquals(2, conf.getClusterConfigurations().size());

      HAPolicyConfiguration pc = conf.getHAPolicyConfiguration();
      assertNotNull(pc);
      assertTrue(pc instanceof LiveOnlyPolicyConfiguration);
      LiveOnlyPolicyConfiguration lopc = (LiveOnlyPolicyConfiguration) pc;
      assertNotNull(lopc.getScaleDownConfiguration());
      assertEquals(lopc.getScaleDownConfiguration().getGroupName(), "boo!");
      assertEquals(lopc.getScaleDownConfiguration().getDiscoveryGroup(), "dg1");

      for (ClusterConnectionConfiguration ccc : conf.getClusterConfigurations())
      {
         if (ccc.getName().equals("cluster-connection1"))
         {
            Assert.assertEquals("cluster-connection1", ccc.getName());
            Assert.assertEquals("clusterConnectionConf minLargeMessageSize", 321, ccc.getMinLargeMessageSize());
            assertEquals("check-period", 331, ccc.getClientFailureCheckPeriod());
            assertEquals("connection time-to-live", 3370, ccc.getConnectionTTL());
            Assert.assertEquals("queues1", ccc.getAddress());
            Assert.assertEquals(3, ccc.getRetryInterval());
            Assert.assertEquals(true, ccc.isDuplicateDetection());
            Assert.assertEquals(false, ccc.isForwardWhenNoConsumers());
            Assert.assertEquals(1, ccc.getMaxHops());
            Assert.assertEquals(123, ccc.getCallTimeout());
            Assert.assertEquals(123, ccc.getCallFailoverTimeout());
            assertEquals("multiplier", 0.25, ccc.getRetryIntervalMultiplier(), 0.00001);
            assertEquals("max retry interval", 10000, ccc.getMaxRetryInterval());
            assertEquals(72, ccc.getReconnectAttempts());
            Assert.assertEquals("connector1", ccc.getStaticConnectors().get(0));
            Assert.assertEquals("connector2", ccc.getStaticConnectors().get(1));
            Assert.assertEquals(null, ccc.getDiscoveryGroupName());
         }
         else
         {
            Assert.assertEquals("cluster-connection2", ccc.getName());
            Assert.assertEquals("queues2", ccc.getAddress());
            Assert.assertEquals(4, ccc.getRetryInterval());
            Assert.assertEquals(456, ccc.getCallTimeout());
            Assert.assertEquals(456, ccc.getCallFailoverTimeout());
            Assert.assertEquals(false, ccc.isDuplicateDetection());
            Assert.assertEquals(true, ccc.isForwardWhenNoConsumers());
            Assert.assertEquals(2, ccc.getMaxHops());
            Assert.assertEquals(Collections.emptyList(), ccc.getStaticConnectors());
            Assert.assertEquals("dg1", ccc.getDiscoveryGroupName());
         }
      }


      assertEquals(2, conf.getAddressesSettings().size());

      assertTrue(conf.getAddressesSettings().get("a1") != null);
      assertTrue(conf.getAddressesSettings().get("a2") != null);

      assertEquals("a1.1", conf.getAddressesSettings().get("a1").getDeadLetterAddress().toString());
      assertEquals("a1.2", conf.getAddressesSettings().get("a1").getExpiryAddress().toString());
      assertEquals(1, conf.getAddressesSettings().get("a1").getRedeliveryDelay());
      assertEquals(81781728121878L, conf.getAddressesSettings().get("a1").getMaxSizeBytes());
      assertEquals(81738173872337L, conf.getAddressesSettings().get("a1").getPageSizeBytes());
      assertEquals(10, conf.getAddressesSettings().get("a1").getPageCacheMaxSize());
      assertEquals(4, conf.getAddressesSettings().get("a1").getMessageCounterHistoryDayLimit());
      assertEquals(10, conf.getAddressesSettings().get("a1").getSlowConsumerThreshold());
      assertEquals(5, conf.getAddressesSettings().get("a1").getSlowConsumerCheckPeriod());
      assertEquals(SlowConsumerPolicy.NOTIFY, conf.getAddressesSettings().get("a1").getSlowConsumerPolicy());
      assertEquals(true, conf.getAddressesSettings().get("a1").isAutoCreateJmsQueues());
      assertEquals(true, conf.getAddressesSettings().get("a1").isAutoDeleteJmsQueues());

      assertEquals("a2.1", conf.getAddressesSettings().get("a2").getDeadLetterAddress().toString());
      assertEquals("a2.2", conf.getAddressesSettings().get("a2").getExpiryAddress().toString());
      assertEquals(5, conf.getAddressesSettings().get("a2").getRedeliveryDelay());
      assertEquals(932489234928324L, conf.getAddressesSettings().get("a2").getMaxSizeBytes());
      assertEquals(7126716262626L, conf.getAddressesSettings().get("a2").getPageSizeBytes());
      assertEquals(20, conf.getAddressesSettings().get("a2").getPageCacheMaxSize());
      assertEquals(8, conf.getAddressesSettings().get("a2").getMessageCounterHistoryDayLimit());
      assertEquals(20, conf.getAddressesSettings().get("a2").getSlowConsumerThreshold());
      assertEquals(15, conf.getAddressesSettings().get("a2").getSlowConsumerCheckPeriod());
      assertEquals(SlowConsumerPolicy.KILL, conf.getAddressesSettings().get("a2").getSlowConsumerPolicy());
      assertEquals(false, conf.getAddressesSettings().get("a2").isAutoCreateJmsQueues());
      assertEquals(false, conf.getAddressesSettings().get("a2").isAutoDeleteJmsQueues());


      assertEquals(2, conf.getQueueConfigurations().size());

      assertEquals("queue1", conf.getQueueConfigurations().get(0).getName());
      assertEquals("address1", conf.getQueueConfigurations().get(0).getAddress());
      assertEquals("color='red'", conf.getQueueConfigurations().get(0).getFilterString());
      assertEquals(false, conf.getQueueConfigurations().get(0).isDurable());

      assertEquals("queue2", conf.getQueueConfigurations().get(1).getName());
      assertEquals("address2", conf.getQueueConfigurations().get(1).getAddress());
      assertEquals("color='blue'", conf.getQueueConfigurations().get(1).getFilterString());
      assertEquals(false, conf.getQueueConfigurations().get(1).isDurable());

      assertEquals(2, conf.getSecurityRoles().size());

      assertTrue(conf.getSecurityRoles().containsKey("a1"));

      assertTrue(conf.getSecurityRoles().containsKey("a2"));

      Role a1Role = conf.getSecurityRoles().get("a1").toArray(new Role[1])[0];

      assertFalse(a1Role.isSend());
      assertFalse(a1Role.isConsume());
      assertFalse(a1Role.isCreateDurableQueue());
      assertFalse(a1Role.isDeleteDurableQueue());
      assertTrue(a1Role.isCreateNonDurableQueue());
      assertFalse(a1Role.isDeleteNonDurableQueue());
      assertFalse(a1Role.isManage());

      Role a2Role = conf.getSecurityRoles().get("a2").toArray(new Role[1])[0];

      assertFalse(a2Role.isSend());
      assertFalse(a2Role.isConsume());
      assertFalse(a2Role.isCreateDurableQueue());
      assertFalse(a2Role.isDeleteDurableQueue());
      assertFalse(a2Role.isCreateNonDurableQueue());
      assertTrue(a2Role.isDeleteNonDurableQueue());
      assertFalse(a2Role.isManage());


   }

   @Override
   protected Configuration createConfiguration() throws Exception
   {
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager("ConfigurationTest-full-config.xml");
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();
      return fc;
   }
}

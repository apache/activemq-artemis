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
package org.apache.activemq.artemis.tests.integration.management;

import org.apache.activemq.artemis.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.artemis.api.core.ChannelBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.jgroups.JChannelManager;
import org.apache.activemq.artemis.api.core.management.JGroupsChannelBroadcastGroupControl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.jgroups.JChannel;
import org.jgroups.conf.PlainConfigurator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.json.JsonArray;
import java.util.ArrayList;
import java.util.List;

public class JGroupsChannelBroadcastGroupControlTest extends ManagementTestBase {

   private ActiveMQServer server;
   BroadcastGroupConfiguration broadcastGroupConfig;

   JGroupsChannelBroadcastGroupControl broadcastGroupControl;

   @After
   public void cleanupJChannel() {
      JChannelManager.getInstance().clear();
   }

   @Before
   public void prepareJChannel() {
      JChannelManager.getInstance().setLoopbackMessages(true);
   }

   @Rule
   public ThreadLeakCheckRule threadLeakCheckRule = new ThreadLeakCheckRule();

   private final String jgroupsConfigString = "UDP(oob_thread_pool.max_threads=300;" + "bind_addr=127.0.0.1;oob_thread_pool.keep_alive_time=1000;" + "max_bundle_size=31k;mcast_send_buf_size=640000;" + "internal_thread_pool.keep_alive_time=60000;" + "internal_thread_pool.rejection_policy=discard;" + "mcast_recv_buf_size=25000000;bind_port=55200;" + "internal_thread_pool.queue_max_size=100;" + "mcast_port=45688;thread_pool.min_threads=20;" + "oob_thread_pool.rejection_policy=discard;" + "thread_pool.max_threads=300;enable_diagnostics=false;" + "thread_pool.enabled=true;internal_thread_pool.queue_enabled=true;" + "ucast_recv_buf_size=20000000;ucast_send_buf_size=640000;" + "internal_thread_pool.enabled=true;oob_thread_pool.enabled=true;" + "ip_ttl=2;thread_pool.rejection_policy=discard;thread_pool.keep_alive_time=5000;" + "internal_thread_pool.max_threads=10;thread_pool.queue_enabled=true;" + "mcast_addr=230.0.0.4;singleton_name=udp;max_bundle_timeout=30;" + "oob_thread_pool.queue_enabled=false;internal_thread_pool.min_threads=1;" + "bundler_type=old;oob_thread_pool.min_threads=20;" + "thread_pool.queue_max_size=1000):PING(num_initial_members=3;" + "timeout=2000):MERGE3(min_interval=20000;max_interval=100000)" + ":FD_SOCK(bind_addr=127.0.0.1;start_port=54200):FD_ALL(interval=3000;" + "timeout=15000):VERIFY_SUSPECT(bind_addr=127.0.0.1;" + "timeout=1500):pbcast.NAKACK2(max_msg_batch_size=100;" + "xmit_table_msgs_per_row=10000;xmit_table_max_compaction_time=10000;" + "xmit_table_num_rows=100;xmit_interval=1000):UNICAST3(xmit_table_msgs_per_row=10000;" + "xmit_table_max_compaction_time=10000;xmit_table_num_rows=20)" + ":pbcast.STABLE(desired_avg_gossip=50000;max_bytes=400000;" + "stability_delay=1000):pbcast.GMS(print_local_addr=true;" + "view_bundling=true;join_timeout=3000;view_ack_collection_timeout=5000;" + "resume_task_timeout=7500):UFC(max_credits=1m;min_threshold=0.40)" + ":MFC(max_credits=1m;min_threshold=0.40):FRAG2(frag_size=30k)" + ":RSVP(resend_interval=500;ack_on_delivery=false;timeout=60000)";


   @Test
   public void testAttributes() throws Exception {
      ChannelBroadcastEndpointFactory udpCfg = (ChannelBroadcastEndpointFactory) broadcastGroupConfig.getEndpointFactory();
      Assert.assertEquals(broadcastGroupConfig.getName(), broadcastGroupControl.getName());
      Assert.assertEquals(udpCfg.getChannelName(), broadcastGroupControl.getChannelName());
      Assert.assertEquals(broadcastGroupConfig.getBroadcastPeriod(), broadcastGroupControl.getBroadcastPeriod());

      Object[] connectorPairs = broadcastGroupControl.getConnectorPairs();
      Assert.assertEquals(1, connectorPairs.length);

      String connectorPairData = (String) connectorPairs[0];
      Assert.assertEquals(broadcastGroupConfig.getConnectorInfos().get(0), connectorPairData);
      String jsonString = broadcastGroupControl.getConnectorPairsAsJSON();
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(1, array.size());
      Assert.assertEquals(broadcastGroupConfig.getConnectorInfos().get(0), array.getString(0));

      Assert.assertTrue(broadcastGroupControl.isStarted());
   }

   protected JGroupsChannelBroadcastGroupControl createManagementControl(final String name) throws Exception {
      return ManagementControlHelper.createJgroupsChannelBroadcastGroupControl(name, mbeanServer);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      TransportConfiguration connectorConfiguration = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
      List<String> connectorInfos = new ArrayList<>();
      connectorInfos.add(connectorConfiguration.getName());
      PlainConfigurator configurator = new PlainConfigurator(jgroupsConfigString);
      JChannel channel = new JChannel(configurator);

      String channelName1 = "channel1";
      ChannelBroadcastEndpointFactory endpointFactory = new ChannelBroadcastEndpointFactory(channel, channelName1);
      broadcastGroupConfig = new BroadcastGroupConfiguration().setName(RandomUtil.randomString()).setBroadcastPeriod(RandomUtil.randomPositiveInt()).setConnectorInfos(connectorInfos).setEndpointFactory(endpointFactory);

      Configuration config = createDefaultInVMConfig().setJMXManagementEnabled(true).addConnectorConfiguration(connectorConfiguration.getName(), connectorConfiguration).addBroadcastGroupConfiguration(broadcastGroupConfig);
      server = addServer(ActiveMQServers.newActiveMQServer(config, mbeanServer, false));
      server.start();

      broadcastGroupControl = createManagementControl(broadcastGroupConfig.getName());
   }
}

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
package org.apache.activemq.artemis.tests.integration.broadcast;

import org.apache.activemq.artemis.api.core.BroadcastEndpoint;
import org.apache.activemq.artemis.api.core.BroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.ChannelBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.jgroups.JChannelManager;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.jgroups.JChannel;
import org.jgroups.conf.PlainConfigurator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class JGroupsBroadcastTest {

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
   public void testRefCount() throws Exception {
      JChannel channel = null;
      JChannel newChannel = null;

      try {

         PlainConfigurator configurator = new PlainConfigurator(jgroupsConfigString);
         channel = new JChannel(configurator);

         String channelName1 = "channel1";

         BroadcastEndpointFactory jgroupsBroadcastCfg1 = new ChannelBroadcastEndpointFactory(channel, channelName1);

         BroadcastEndpoint channelEndpoint1 = jgroupsBroadcastCfg1.createBroadcastEndpoint();

         BroadcastEndpoint channelEndpoint2 = jgroupsBroadcastCfg1.createBroadcastEndpoint();

         BroadcastEndpoint channelEndpoint3 = jgroupsBroadcastCfg1.createBroadcastEndpoint();

         channelEndpoint1.close(true);

         Assert.assertTrue(channel.isOpen());

         channelEndpoint2.close(true);

         Assert.assertTrue(channel.isOpen());

         channelEndpoint3.close(true);

         Assert.assertTrue(channel.isOpen());

         channel.close();

         //after we close the last endpoint reference counting will close the channel so once we create a new one the
         // channel wrapper is recreated
         try {
            channelEndpoint2.openClient();
            Assert.fail("this should be closed");
         } catch (Exception e) {
         }

         newChannel = new JChannel(configurator);

         jgroupsBroadcastCfg1 = new ChannelBroadcastEndpointFactory(newChannel, channelName1);

         channelEndpoint1 = jgroupsBroadcastCfg1.createBroadcastEndpoint();

         channelEndpoint1.openClient();

      } catch (Exception e) {
         e.printStackTrace();
         throw e;
      } finally {
         try {
            channel.close();
         } catch (Throwable ignored) {

         }
         try {
            newChannel.close();
         } catch (Throwable ignored) {

         }
      }
   }

}

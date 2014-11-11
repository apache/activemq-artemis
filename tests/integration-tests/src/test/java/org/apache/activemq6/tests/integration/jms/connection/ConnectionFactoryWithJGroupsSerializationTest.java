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
package org.apache.activemq6.tests.integration.jms.connection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import javax.jms.Queue;

import org.apache.activemq6.api.core.BroadcastEndpointFactoryConfiguration;
import org.apache.activemq6.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq6.api.core.JGroupsBroadcastGroupConfiguration;
import org.apache.activemq6.api.jms.JMSFactoryType;
import org.apache.activemq6.jms.client.HornetQConnectionFactory;
import org.apache.activemq6.tests.util.JMSTestBase;
import org.jgroups.JChannel;
import org.jgroups.conf.PlainConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConnectionFactoryWithJGroupsSerializationTest extends JMSTestBase
{

   protected static HornetQConnectionFactory jmsCf1;
   protected static HornetQConnectionFactory jmsCf2;

   private final String jgroupsConfigString = "UDP(oob_thread_pool.max_threads=300;"
                                            + "bind_addr=127.0.0.1;oob_thread_pool.keep_alive_time=1000;"
                                            + "max_bundle_size=31k;mcast_send_buf_size=640000;"
                                            + "internal_thread_pool.keep_alive_time=60000;"
                                            + "internal_thread_pool.rejection_policy=discard;"
                                            + "mcast_recv_buf_size=25000000;bind_port=55200;"
                                            + "internal_thread_pool.queue_max_size=100;"
                                            + "mcast_port=45688;thread_pool.min_threads=20;"
                                            + "oob_thread_pool.rejection_policy=discard;"
                                            + "thread_pool.max_threads=300;enable_diagnostics=false;"
                                            + "thread_pool.enabled=true;internal_thread_pool.queue_enabled=true;"
                                            + "ucast_recv_buf_size=20000000;ucast_send_buf_size=640000;"
                                            + "internal_thread_pool.enabled=true;oob_thread_pool.enabled=true;"
                                            + "ip_ttl=2;thread_pool.rejection_policy=discard;thread_pool.keep_alive_time=5000;"
                                            + "internal_thread_pool.max_threads=10;thread_pool.queue_enabled=true;"
                                            + "mcast_addr=230.0.0.4;singleton_name=udp;max_bundle_timeout=30;"
                                            + "oob_thread_pool.queue_enabled=false;internal_thread_pool.min_threads=1;"
                                            + "bundler_type=old;oob_thread_pool.min_threads=20;"
                                            + "thread_pool.queue_max_size=1000):PING(num_initial_members=3;"
                                            + "timeout=2000):MERGE3(min_interval=20000;max_interval=100000)"
                                            + ":FD_SOCK(bind_addr=127.0.0.1;start_port=54200):FD_ALL(interval=3000;"
                                            + "timeout=15000):VERIFY_SUSPECT(bind_addr=127.0.0.1;"
                                            + "timeout=1500):pbcast.NAKACK2(max_msg_batch_size=100;"
                                            + "xmit_table_msgs_per_row=10000;xmit_table_max_compaction_time=10000;"
                                            + "xmit_table_num_rows=100;xmit_interval=1000):UNICAST3(xmit_table_msgs_per_row=10000;"
                                            + "xmit_table_max_compaction_time=10000;xmit_table_num_rows=20)"
                                            + ":pbcast.STABLE(desired_avg_gossip=50000;max_bytes=400000;"
                                            + "stability_delay=1000):pbcast.GMS(print_local_addr=true;"
                                            + "view_bundling=true;join_timeout=3000;view_ack_collection_timeout=5000;"
                                            + "resume_task_timeout=7500):UFC(max_credits=1m;min_threshold=0.40)"
                                            + ":MFC(max_credits=1m;min_threshold=0.40):FRAG2(frag_size=30k)"
                                            + ":RSVP(resend_interval=500;ack_on_delivery=false;timeout=60000)";

   JChannel channel = null;
   Queue testQueue = null;

   @Override
   @Before
   public void setUp() throws Exception
   {
      try
      {
         super.setUp();

         PlainConfigurator configurator = new PlainConfigurator(jgroupsConfigString);
         channel = new JChannel(configurator);

         String channelName1 = "channel1";
         String channelName2 = "channel2";

         JGroupsBroadcastGroupConfiguration jgroupsBroadcastCfg1 = new JGroupsBroadcastGroupConfiguration(channel, channelName1);
         JGroupsBroadcastGroupConfiguration jgroupsBroadcastCfg2 = new JGroupsBroadcastGroupConfiguration(jgroupsConfigString, channelName2);

         DiscoveryGroupConfiguration dcConfig1 = new DiscoveryGroupConfiguration()
            .setName("dg1")
            .setRefreshTimeout(5000)
            .setDiscoveryInitialWaitTimeout(5000)
            .setBroadcastEndpointFactoryConfiguration(jgroupsBroadcastCfg1);

         DiscoveryGroupConfiguration dcConfig2 = new DiscoveryGroupConfiguration()
            .setName("dg2")
            .setRefreshTimeout(5000)
            .setDiscoveryInitialWaitTimeout(5000)
            .setBroadcastEndpointFactoryConfiguration(jgroupsBroadcastCfg2);

         jmsServer.getHornetQServer().getConfiguration().getDiscoveryGroupConfigurations().put(dcConfig1.getName(), dcConfig1);
         jmsServer.getHornetQServer().getConfiguration().getDiscoveryGroupConfigurations().put(dcConfig2.getName(), dcConfig2);

         jmsServer.createConnectionFactory("ConnectionFactory1",
                                           false,
                                           JMSFactoryType.CF,
                                           dcConfig1.getName(),
                                           "/ConnectionFactory1");


         jmsServer.createConnectionFactory("ConnectionFactory2",
                                           false,
                                           JMSFactoryType.CF,
                                           dcConfig2.getName(),
                                           "/ConnectionFactory2");

         testQueue = createQueue("testQueueFor1389");
      }
      catch (Exception e)
      {
         e.printStackTrace();
         throw e;
      }
   }

   // Public --------------------------------------------------------

   //HORNETQ-1389
   //Here we deploy two Connection Factories with JGroups discovery groups.
   //The first one uses a runtime JChannel object, which is the case before the fix.
   //The second one uses the raw jgroups config string, which is the case after fix.
   //So the first one will get serialization exception in the test
   //while the second will not.
   @Test
   public void testSerialization() throws Exception
   {
      jmsCf1 = (HornetQConnectionFactory) namingContext.lookup("/ConnectionFactory1");
      jmsCf2 = (HornetQConnectionFactory) namingContext.lookup("/ConnectionFactory2");

      try
      {
         serialize(jmsCf1);
      }
      catch (java.io.NotSerializableException e)
      {
         //this is expected
      }

      //now cf2 should be OK
      byte[] x = serialize(jmsCf2);
      HornetQConnectionFactory jmsCf2Copy = deserialize(x, HornetQConnectionFactory.class);
      assertNotNull(jmsCf2Copy);
      BroadcastEndpointFactoryConfiguration broadcastEndpoint = jmsCf2Copy.getDiscoveryGroupConfiguration().getBroadcastEndpointFactoryConfiguration();
      assertTrue(broadcastEndpoint instanceof JGroupsBroadcastGroupConfiguration);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (channel != null)
      {
         channel.close();
      }

      super.tearDown();
   }

   private static <T extends Serializable> byte[] serialize(T obj) throws IOException
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.close();
      return baos.toByteArray();
   }

   private static <T extends Serializable> T deserialize(byte[] b, Class<T> cl) throws IOException, ClassNotFoundException
   {
      ByteArrayInputStream bais = new ByteArrayInputStream(b);
      ObjectInputStream ois = new ObjectInputStream(bais);
      Object o = ois.readObject();
      return cl.cast(o);
   }

}

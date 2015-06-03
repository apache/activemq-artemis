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
package org.apache.activemq.artemis.tests.integration.discovery;

import java.net.InetAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.BroadcastEndpoint;
import org.apache.activemq.artemis.api.core.BroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.artemis.core.cluster.DiscoveryGroup;
import org.apache.activemq.artemis.core.server.cluster.impl.BroadcastGroupImpl;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DiscoveryStayAliveTest extends DiscoveryBaseTest
{


   ScheduledExecutorService scheduledExecutorService;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
                                                                 new ActiveMQThreadFactory("ActiveMQ-scheduled-threads",
                                                                                           false,
                                                                                           Thread.currentThread().getContextClassLoader()));

   }

   public void tearDown() throws Exception
   {
      scheduledExecutorService.shutdown();
      super.tearDown();
   }

   @Test
   public void testDiscoveryRunning() throws Throwable
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;


      final DiscoveryGroup dg = newDiscoveryGroup(RandomUtil.randomString(),
                                                  RandomUtil.randomString(),
                                                  null,
                                                  groupAddress,
                                                  groupPort,
                                                  timeout);

      final AtomicInteger errors = new AtomicInteger(0);
      Thread t = new Thread()
      {
         public void run()
         {
            try
            {
               dg.internalRunning();
            }
            catch (Throwable e)
            {
               e.printStackTrace();
               errors.incrementAndGet();
            }

         }
      };
      t.start();


      BroadcastGroupImpl bg = new BroadcastGroupImpl(new FakeNodeManager("test-nodeID"),
                                                     RandomUtil.randomString(),
                                                     1, scheduledExecutorService, new UDPBroadcastEndpointFactory().setGroupAddress(address1).
                                                        setGroupPort(groupPort));

      bg.start();

      bg.addConnector(generateTC());


      for (int i = 0; i < 10; i++)
      {
         BroadcastEndpointFactory factoryEndpoint = new UDPBroadcastEndpointFactory().setGroupAddress(address1).
            setGroupPort(groupPort);
         sendBadData(factoryEndpoint);

         Thread.sleep(100);
         assertTrue(t.isAlive());
         assertEquals(0, errors.get());
      }

      bg.stop();
      dg.stop();

      t.join(5000);

      Assert.assertFalse(t.isAlive());

   }


   private static void sendBadData(BroadcastEndpointFactory factoryEndpoint) throws Exception
   {
      BroadcastEndpoint endpoint = factoryEndpoint.createBroadcastEndpoint();


      ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(500);

      buffer.writeString("This is a test1!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
      buffer.writeString("This is a test2!!!!!!!!!!!!!!!!!!!!!!!!!!!!");


      byte[] bytes = new byte[buffer.writerIndex()];

      buffer.readBytes(bytes);

      // messing up with the string!!!
      for (int i = bytes.length - 10; i < bytes.length; i++)
      {
         bytes[i] = 0;
      }


      endpoint.openBroadcaster();

      endpoint.broadcast(bytes);
   }
}

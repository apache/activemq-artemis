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
package org.apache.activemq.artemis.tests.integration.cluster.failover;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.CountDownSessionFailureListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReplicatedDistributionTest extends ClusterTestBase {

   private static final SimpleString ADDRESS = new SimpleString("test.SomeAddress");
   private ClientSession sessionOne;
   private ClientSession sessionThree;
   private ClientConsumer consThree;
   private ClientProducer producer;

   @Test
   public void testRedistribution() throws Exception {
      commonTestCode();

      for (int i = 0; i < 50; i++) {
         ClientMessage msg = consThree.receive(15000);

         Assert.assertNotNull(msg);

         // System.out.println(i + " msg = " + msg);

         int received = msg.getIntProperty("key");

         Assert.assertEquals(i, received);

         msg.acknowledge();
      }

      sessionThree.commit();

      // consThree.close();

      // TODO: Remove this sleep: If a node fail,
      // Redistribution may loose messages between the nodes.
      Thread.sleep(500);

      fail(sessionThree);

      // sessionThree.close();
      //
      // setupSessionFactory(2, -1, true);
      //
      // sessionThree = sfs[2].createSession(true, true);
      //
      // sessionThree.start();

      // consThree = sessionThree.createConsumer(ADDRESS);

      for (int i = 50; i < 100; i++) {
         ClientMessage msg = consThree.receive(15000);

         Assert.assertNotNull(msg);

         // System.out.println(i + " msg = " + msg);

         int received = (Integer) msg.getObjectProperty(new SimpleString("key"));

         Assert.assertEquals(i, received);

         msg.acknowledge();
      }

      Assert.assertNull(consThree.receiveImmediate());

      sessionThree.commit();

      sessionOne.start();

      ClientConsumer consOne = sessionOne.createConsumer(ReplicatedDistributionTest.ADDRESS);

      Assert.assertNull(consOne.receiveImmediate());
   }

   @Test
   public void testSimpleRedistribution() throws Exception {
      commonTestCode();

      for (int i = 0; i < 100; i++) {
         ClientMessage msg = consThree.receive(15000);

         Assert.assertNotNull(msg);

         // System.out.println(i + " msg = " + msg);

         int received = msg.getIntProperty("key");

         if (i != received) {
            // Shouldn't this be a failure?
            instanceLog.warn(i + "!=" + received);
         }
         msg.acknowledge();
      }

      sessionThree.commit();

      sessionOne.start();

      ClientConsumer consOne = sessionOne.createConsumer(ReplicatedDistributionTest.ADDRESS);

      Assert.assertNull(consOne.receiveImmediate());
   }

   private void commonTestCode() throws Exception {
      waitForBindings(3, "test.SomeAddress", 1, 1, true);
      waitForBindings(1, "test.SomeAddress", 1, 1, false);

      producer = sessionOne.createProducer(ReplicatedDistributionTest.ADDRESS);

      for (int i = 0; i < 100; i++) {
         ClientMessage msg = sessionOne.createMessage(true);
         msg.putIntProperty(new SimpleString("key"), i);
         producer.send(msg);
      }
      sessionOne.commit();

   }

   /**
    * @param session
    * @throws InterruptedException
    */
   private void fail(final ClientSession session) throws InterruptedException {

      final CountDownLatch latch = new CountDownLatch(1);

      session.addFailureListener(new CountDownSessionFailureListener(latch, session));

      RemotingConnection conn = ((ClientSessionInternal) session).getConnection();

      // Simulate failure on connection
      conn.fail(new ActiveMQNotConnectedException());

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      Assert.assertTrue(ok);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      setupLiveServer(1, true, haType(), true, false);
      setupLiveServer(3, true, haType(), true, false);
      setupBackupServer(2, 3, true, haType(), true);

      final String address = ReplicatedDistributionTest.ADDRESS.toString();
      // notice the abuse of the method call, '3' is not a backup for '1'
      setupClusterConnectionWithBackups("test", address, MessageLoadBalancingType.ON_DEMAND, 1, true, 1, new int[]{3});
      setupClusterConnectionWithBackups("test", address, MessageLoadBalancingType.ON_DEMAND, 1, true, 3, new int[]{2, 1});
      setupClusterConnectionWithBackups("test", address, MessageLoadBalancingType.ON_DEMAND, 1, true, 2, new int[]{3});

      AddressSettings as = new AddressSettings().setRedistributionDelay(0);

      for (int i : new int[]{1, 2, 3}) {
         getServer(i).getAddressSettingsRepository().addMatch("test.*", as);
         getServer(i).start();
      }

      setupSessionFactory(1, -1, true, true);
      setupSessionFactory(3, 2, true, true);

      sessionOne = sfs[1].createSession(true, true);
      sessionThree = sfs[3].createSession(false, false);

      sessionOne.createQueue(new QueueConfiguration(ReplicatedDistributionTest.ADDRESS));
      sessionThree.createQueue(new QueueConfiguration(ReplicatedDistributionTest.ADDRESS));

      consThree = sessionThree.createConsumer(ReplicatedDistributionTest.ADDRESS);

      sessionThree.start();
   }

   @Override
   protected HAType haType() {
      return HAType.SharedNothingReplication;
   }
}

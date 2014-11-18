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
package org.apache.activemq.tests.integration.discovery;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.api.core.BroadcastEndpoint;
import org.apache.activemq.api.core.BroadcastEndpointFactory;
import org.apache.activemq.api.core.JGroupsBroadcastGroupConfiguration;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.UDPBroadcastGroupConfiguration;
import org.apache.activemq.api.core.management.CoreNotificationType;
import org.apache.activemq.core.cluster.DiscoveryEntry;
import org.apache.activemq.core.cluster.DiscoveryGroup;
import org.apache.activemq.core.cluster.DiscoveryListener;
import org.apache.activemq.core.server.ActiveMQComponent;
import org.apache.activemq.core.server.NodeManager;
import org.apache.activemq.core.server.cluster.BroadcastGroup;
import org.apache.activemq.core.server.cluster.impl.BroadcastGroupImpl;
import org.apache.activemq.core.server.management.Notification;
import org.apache.activemq.core.server.management.NotificationService;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.integration.SimpleNotificationService;
import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.UnitTestCase;
import org.apache.activemq.utils.UUIDGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * This will test Discovery test on JGroups and UDP.
 * <p/>
 * In some configurations IPV6 may be a challenge. To make sure this test works, you may add this
 * property to your JVM settings: {@literal -Djgroups.bind_addr=::1}
 * <p/>
 * Or ultimately you may also turn off IPV6: {@literal -Djava.net.preferIPv4Stack=true}
 * <p/>
 * Note when you are not sure about your IP settings of your test machine, you should make sure
 * that the jgroups.bind_addr and java.net.preferXXStack by defining them explicitly, for example
 * if you would like to use IPV6, set BOTH properties to your JVM like the following:
 * -Djgroups.bind_addr=::1 -Djava.net.preferIPv6Addresses=true
 * <p/>
 * or if you prefer IPV4:
 * -Djgroups.bind_addr=localhost -Djava.net.preferIPv4Stack=true
 * <p/>
 * Also: Make sure you add integration-tests/src/tests/resources to your project path on the
 * tests/integration-tests
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class DiscoveryTest extends UnitTestCase
{
   private static final String TEST_JGROUPS_CONF_FILE = "test-jgroups-file_ping.xml";

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private final String address1 = getUDPDiscoveryAddress();

   private final String address2 = getUDPDiscoveryAddress(1);

   private final String address3 = getUDPDiscoveryAddress(2);

   BroadcastGroup bg = null, bg1 = null, bg2 = null, bg3 = null;
   DiscoveryGroup dg = null, dg1 = null, dg2 = null, dg3 = null;


   @Override
   @After
   public void tearDown() throws Exception
   {
      /** This file path is defined at {@link #TEST_JGROUPS_CONF_FILE} */
      deleteDirectory(new File("/tmp/amqtest.ping.dir"));
      for (ActiveMQComponent component : new ActiveMQComponent[]{bg, bg1, bg2, bg3, dg, dg1, dg2, dg3})
      {
         stopComponent(component);
      }
      super.tearDown();
   }


   @Test
   public void testSimpleBroadcast() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      final String nodeID = RandomUtil.randomString();

      bg = new BroadcastGroupImpl(new FakeNodeManager(nodeID),
                                  RandomUtil.randomString(),
                                  0, null, new UDPBroadcastGroupConfiguration().setGroupAddress(address1).setGroupPort(groupPort).createBroadcastEndpointFactory());

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
                             RandomUtil.randomString(),
                             null,
                             groupAddress,
                             groupPort,
                             timeout);

      dg.start();

      verifyBroadcast(bg, dg);

      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);
   }


   @Test
   public void testSimpleBroadcastJGropus() throws Exception
   {
      final String nodeID = RandomUtil.randomString();

      bg = new BroadcastGroupImpl(new FakeNodeManager(nodeID), "broadcast", 100, null,
                                  new JGroupsBroadcastGroupConfiguration(TEST_JGROUPS_CONF_FILE, "tst").createBroadcastEndpointFactory());

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg = new DiscoveryGroup(nodeID + "1", "broadcast", 5000L,
                              new JGroupsBroadcastGroupConfiguration(TEST_JGROUPS_CONF_FILE, "tst").createBroadcastEndpointFactory(),
                              null);

      dg.start();

      verifyBroadcast(bg, dg);
      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);
   }

   /**
    * Create one broadcaster and 100 receivers. Make sure broadcasting works.
    * Then stop 99 of the receivers, the last one could still be working.
    *
    * @throws Exception
    */
   @Test
   public void testJGropusChannelReferenceCounting() throws Exception
   {
      JGroupsBroadcastGroupConfiguration jgroupsConfig =
         new JGroupsBroadcastGroupConfiguration(TEST_JGROUPS_CONF_FILE, "tst");
      BroadcastEndpointFactory factory = jgroupsConfig.createBroadcastEndpointFactory();
      BroadcastEndpoint broadcaster = factory.createBroadcastEndpoint();
      broadcaster.openBroadcaster();

      int num = 100;
      BroadcastEndpoint[] receivers = new BroadcastEndpoint[num];
      for (int i = 0; i < num; i++)
      {
         receivers[i] = factory.createBroadcastEndpoint();
         receivers[i].openClient();
      }

      final byte[] data = new byte[]{1, 2, 3, 4, 5};
      broadcaster.broadcast(data);

      for (int i = 0; i < num; i++)
      {
         byte[] received = receivers[i].receiveBroadcast(5000, TimeUnit.MILLISECONDS);
         assertNotNull(received);
         assertEquals(5, received.length);
         assertEquals(1, received[0]);
         assertEquals(2, received[1]);
         assertEquals(3, received[2]);
         assertEquals(4, received[3]);
         assertEquals(5, received[4]);
      }

      for (int i = 0; i < num - 1; i++)
      {
         receivers[i].close(false);
      }

      byte[] data1 = receivers[num - 1].receiveBroadcast(5, TimeUnit.SECONDS);
      assertNull(data1);

      broadcaster.broadcast(data);
      data1 = receivers[num - 1].receiveBroadcast(5, TimeUnit.SECONDS);

      assertNotNull(data1);
      assertEquals(5, data1.length);
      assertEquals(1, data1[0]);
      assertEquals(2, data1[1]);
      assertEquals(3, data1[2]);
      assertEquals(4, data1[3]);
      assertEquals(5, data1[4]);

      receivers[num - 1].close(false);
      broadcaster.close(true);
   }

   /**
    * Create one broadcaster and 50 receivers. Make sure broadcasting works.
    * Then stop all of the receivers, and create 50 new ones. Make sure the
    * 50 new ones are receiving data from the broadcasting.
    *
    * @throws Exception
    */
   @Test
   public void testJGropusChannelReferenceCounting1() throws Exception
   {
      JGroupsBroadcastGroupConfiguration jgroupsConfig =
         new JGroupsBroadcastGroupConfiguration(TEST_JGROUPS_CONF_FILE, "tst");
      BroadcastEndpointFactory factory = jgroupsConfig.createBroadcastEndpointFactory();
      BroadcastEndpoint broadcaster = factory.createBroadcastEndpoint();
      broadcaster.openBroadcaster();

      int num = 50;
      BroadcastEndpoint[] receivers = new BroadcastEndpoint[num];
      for (int i = 0; i < num; i++)
      {
         receivers[i] = factory.createBroadcastEndpoint();
         receivers[i].openClient();
      }

      final byte[] data = new byte[]{1, 2, 3, 4, 5};
      broadcaster.broadcast(data);

      for (int i = 0; i < num; i++)
      {
         byte[] received = receivers[i].receiveBroadcast(5000, TimeUnit.MILLISECONDS);
         assertNotNull(received);
         assertEquals(5, received.length);
         assertEquals(1, received[0]);
         assertEquals(2, received[1]);
         assertEquals(3, received[2]);
         assertEquals(4, received[3]);
         assertEquals(5, received[4]);
      }

      for (int i = 0; i < num; i++)
      {
         receivers[i].close(false);
      }

      //new ones
      for (int i = 0; i < num; i++)
      {
         receivers[i] = factory.createBroadcastEndpoint();
         receivers[i].openClient();
      }

      broadcaster.broadcast(data);

      for (int i = 0; i < num; i++)
      {
         byte[] received = receivers[i].receiveBroadcast(5000, TimeUnit.MILLISECONDS);
         assertNotNull(received);
         assertEquals(5, received.length);
         assertEquals(1, received[0]);
         assertEquals(2, received[1]);
         assertEquals(3, received[2]);
         assertEquals(4, received[3]);
         assertEquals(5, received[4]);
      }

      for (int i = 0; i < num; i++)
      {
         receivers[i].close(false);
      }
      broadcaster.close(true);
   }

   /**
    * Create one broadcaster and 50 receivers. Then stop half of the receivers.
    * Then add the half back, plus some more. Make sure all receivers receive data.
    *
    * @throws Exception
    */
   @Test
   public void testJGropusChannelReferenceCounting2() throws Exception
   {
      JGroupsBroadcastGroupConfiguration jgroupsConfig =
         new JGroupsBroadcastGroupConfiguration(TEST_JGROUPS_CONF_FILE, "tst");
      BroadcastEndpointFactory factory = jgroupsConfig.createBroadcastEndpointFactory();
      BroadcastEndpoint broadcaster = factory.createBroadcastEndpoint();
      broadcaster.openBroadcaster();

      int num = 50;
      BroadcastEndpoint[] receivers = new BroadcastEndpoint[num];
      for (int i = 0; i < num; i++)
      {
         receivers[i] = factory.createBroadcastEndpoint();
         receivers[i].openClient();
      }

      for (int i = 0; i < num / 2; i++)
      {
         receivers[i].close(false);
      }

      for (int i = 0; i < num / 2; i++)
      {
         receivers[i] = factory.createBroadcastEndpoint();
         receivers[i].openClient();
      }

      int num2 = 10;
      BroadcastEndpoint[] moreReceivers = new BroadcastEndpoint[num2];

      for (int i = 0; i < num2; i++)
      {
         moreReceivers[i] = factory.createBroadcastEndpoint();
         moreReceivers[i].openClient();
      }

      final byte[] data = new byte[]{1, 2, 3, 4, 5};
      broadcaster.broadcast(data);

      for (int i = 0; i < num; i++)
      {
         byte[] received = receivers[i].receiveBroadcast(5000, TimeUnit.MILLISECONDS);
         assertNotNull(received);
         assertEquals(5, received.length);
         assertEquals(1, received[0]);
         assertEquals(2, received[1]);
         assertEquals(3, received[2]);
         assertEquals(4, received[3]);
         assertEquals(5, received[4]);
      }

      for (int i = 0; i < num2; i++)
      {
         byte[] received = moreReceivers[i].receiveBroadcast(5000, TimeUnit.MILLISECONDS);
         assertNotNull(received);
         assertEquals(5, received.length);
         assertEquals(1, received[0]);
         assertEquals(2, received[1]);
         assertEquals(3, received[2]);
         assertEquals(4, received[3]);
         assertEquals(5, received[4]);
      }

      for (int i = 0; i < num; i++)
      {
         receivers[i].close(false);
      }

      for (int i = 0; i < num2; i++)
      {
         moreReceivers[i].close(false);
      }

      broadcaster.close(true);
   }

   @Test
   public void testStraightSendReceiveJGroups() throws Exception
   {
      BroadcastEndpoint broadcaster = null;
      BroadcastEndpoint client = null;
      try
      {
         JGroupsBroadcastGroupConfiguration jgroupsConfig =
            new JGroupsBroadcastGroupConfiguration(TEST_JGROUPS_CONF_FILE, "tst");
         broadcaster = jgroupsConfig.createBroadcastEndpointFactory().createBroadcastEndpoint();

         broadcaster.openBroadcaster();

         client = jgroupsConfig.createBroadcastEndpointFactory().createBroadcastEndpoint();

         client.openClient();

         Thread.sleep(1000);

         byte[] randomBytes = "PQP".getBytes();

         broadcaster.broadcast(randomBytes);

         byte[] btreceived = client.receiveBroadcast(5, TimeUnit.SECONDS);

         System.out.println("BTReceived = " + btreceived);

         assertNotNull(btreceived);

         assertEquals(randomBytes.length, btreceived.length);

         for (int i = 0; i < randomBytes.length; i++)
         {
            assertEquals(randomBytes[i], btreceived[i]);
         }
      }
      finally
      {
         try
         {
            if (broadcaster != null)
               broadcaster.close(true);

            if (client != null)
               client.close(false);
         }
         catch (Exception ignored)
         {
            ignored.printStackTrace();
         }
      }

   }

   @Test
   public void testSimpleBroadcastSpecificNIC() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      final String nodeID = RandomUtil.randomString();

      // We need to choose a real NIC on the local machine - note this will silently pass if the machine
      // has no usable NIC!

      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

      InetAddress localAddress = null;

   outer:
      while (networkInterfaces.hasMoreElements())
      {
         NetworkInterface networkInterface = networkInterfaces.nextElement();
         if (networkInterface.isLoopback() || networkInterface.isVirtual() ||
            !networkInterface.isUp() ||
            !networkInterface.supportsMulticast())
         {
            continue;
         }

         Enumeration<InetAddress> en = networkInterface.getInetAddresses();

         while (en.hasMoreElements())
         {
            InetAddress ia = en.nextElement();

            if (ia.getAddress().length == 4)
            {
               localAddress = ia;

               break outer;
            }
         }
      }

      if (localAddress == null)
      {
         log.warn("Can't find address to use");

         return;
      }

      log.info("Local address is " + localAddress);

      bg = newBroadcast(nodeID,
                        RandomUtil.randomString(),
                        localAddress,
                        6552,
                        groupAddress,
                        groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
                             RandomUtil.randomString(),
                             localAddress,
                             groupAddress,
                             groupPort,
                             timeout);

      dg.start();

      verifyBroadcast(bg, dg);

      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

   }

   @Test
   public void testSimpleBroadcastWithStopStartDiscoveryGroup() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      final String nodeID = RandomUtil.randomString();

      bg = newBroadcast(nodeID,
                        RandomUtil.randomString(),
                        null,
                        -1,
                        groupAddress,
                        groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
                             RandomUtil.randomString(),
                             null,
                             groupAddress,
                             groupPort,
                             timeout);

      dg.start();

      verifyBroadcast(bg, dg);
      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      bg.stop();

      dg.stop();

      dg.start();

      bg.start();

      verifyBroadcast(bg, dg);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);
   }

   @Test
   public void testIgnoreTrafficFromOwnNode() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      bg = newBroadcast(nodeID,
                        RandomUtil.randomString(),
                        null,
                        -1,
                        groupAddress,
                        groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg = newDiscoveryGroup(nodeID,
                             RandomUtil.randomString(),
                             null,
                             groupAddress,
                             groupPort,
                             timeout);

      dg.start();

      verifyNonBroadcast(bg, dg);
      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();

      Assert.assertNotNull(entries);

      Assert.assertEquals(0, entries.size());

   }

   @Test
   public void testSimpleBroadcastDifferentPort() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(getUDPDiscoveryAddress());
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      bg = newBroadcast(RandomUtil.randomString(),
                        RandomUtil.randomString(),
                        null,
                        -1,
                        groupAddress,
                        groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      final int port2 = getUDPDiscoveryPort(1);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
                             RandomUtil.randomString(),
                             null,
                             groupAddress,
                             port2,
                             timeout);

      dg.start();

      verifyNonBroadcast(bg, dg);
   }

   @Test
   public void testSimpleBroadcastDifferentAddressAndPort() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      bg = newBroadcast(RandomUtil.randomString(),
                        RandomUtil.randomString(),
                        null,
                        -1,
                        groupAddress,
                        groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      final InetAddress groupAddress2 = InetAddress.getByName(address2);
      final int port2 = getUDPDiscoveryPort(1);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
                             RandomUtil.randomString(),
                             null,
                             groupAddress2,
                             port2,
                             timeout);

      dg.start();

      verifyNonBroadcast(bg, dg);
   }

   @Test
   public void testMultipleGroups() throws Exception
   {
      final int groupPort1 = getUDPDiscoveryPort();

      final int groupPort2 = getUDPDiscoveryPort(1);

      final int groupPort3 = getUDPDiscoveryPort(2);

      final InetAddress groupAddress1 = InetAddress.getByName(address1);

      final InetAddress groupAddress2 = InetAddress.getByName(address2);

      final InetAddress groupAddress3 = InetAddress.getByName(address3);

      final int timeout = 5000;

      String node1 = UUIDGenerator.getInstance().generateStringUUID();

      String node2 = UUIDGenerator.getInstance().generateStringUUID();

      String node3 = UUIDGenerator.getInstance().generateStringUUID();

      bg1 = newBroadcast(node1,
                         RandomUtil.randomString(),
                         null,
                         -1,
                         groupAddress1,
                         groupPort1);

      bg2 = newBroadcast(node2,
                         RandomUtil.randomString(),
                         null,
                         -1,
                         groupAddress2,
                         groupPort2);

      bg3 = newBroadcast(node3,
                         RandomUtil.randomString(),
                         null,
                         -1,
                         groupAddress3,
                         groupPort3);

      bg2.start();
      bg1.start();
      bg3.start();

      TransportConfiguration live1 = generateTC("live1");

      TransportConfiguration live2 = generateTC("live2");

      TransportConfiguration live3 = generateTC("live3");

      bg1.addConnector(live1);
      bg2.addConnector(live2);
      bg3.addConnector(live3);

      dg1 = newDiscoveryGroup("group-1::" + RandomUtil.randomString(),
                              "group-1::" + RandomUtil.randomString(),
                              null,
                              groupAddress1,
                              groupPort1,
                              timeout);
      dg1.start();

      dg2 = newDiscoveryGroup("group-2::" + RandomUtil.randomString(),
                              "group-2::" + RandomUtil.randomString(),
                              null,
                              groupAddress2,
                              groupPort2,
                              timeout);
      dg2.start();

      dg3 = newDiscoveryGroup("group-3::" + RandomUtil.randomString(),
                              "group-3::" + RandomUtil.randomString(),
                              null,
                              groupAddress3,
                              groupPort3,
                              timeout);
      dg3.start();

      bg1.broadcastConnectors();

      bg2.broadcastConnectors();

      bg3.broadcastConnectors();

      boolean ok = dg1.waitForBroadcast(timeout);
      Assert.assertTrue(ok);
      List<DiscoveryEntry> entries = dg1.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      ok = dg2.waitForBroadcast(timeout);
      Assert.assertTrue(ok);
      entries = dg2.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live2), entries);

      ok = dg3.waitForBroadcast(timeout);
      Assert.assertTrue(ok);
      entries = dg3.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live3), entries);
   }

   // -- fix this test
   // public void testBroadcastNullBackup() throws Exception
   // {
   // final InetAddress groupAddress = InetAddress.getByName(address1);
   // final int groupPort = getUDPDiscoveryPort();
   // final int timeout = 500;
   //
   // String nodeID = RandomUtil.randomString();
   //
   // BroadcastGroup bg = new BroadcastGroupImpl(nodeID,
   // RandomUtil.randomString(),
   // null,
   // -1,
   // groupAddress,
   // groupPort,
   // true);
   //
   // bg.start();
   //
   // TransportConfiguration live1 = generateTC();
   //
   // Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration,
   // TransportConfiguration>(live1,
   // null);
   //
   // bg.addConnectorPair(connectorPair);
   //
   // DiscoveryGroup dg = new DiscoveryGroup(RandomUtil.randomString(),
   // RandomUtil.randomString(),
   // null,
   // groupAddress,
   // groupPort,
   // timeout,
   // Executors.newFixedThreadPool(1));
   //
   // dg.start();
   //
   // bg.broadcastConnectors();
   //
   // boolean ok = dg.waitForBroadcast(1000);
   //
   // Assert.assertTrue(ok);
   // }

   @Test
   public void testDiscoveryListenersCalled() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      bg = newBroadcast(nodeID,
                        RandomUtil.randomString(),
                        null,
                        -1,
                        groupAddress,
                        groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
                             RandomUtil.randomString(),
                             null,
                             groupAddress,
                             groupPort,
                             timeout);

      MyListener listener1 = new MyListener();
      MyListener listener2 = new MyListener();
      MyListener listener3 = new MyListener();

      dg.registerListener(listener1);
      dg.registerListener(listener2);
      dg.registerListener(listener3);

      dg.start();

      verifyBroadcast(bg, dg);

      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      Assert.assertTrue(listener3.called);

      listener1.called = false;
      listener2.called = false;
      listener3.called = false;

      verifyBroadcast(bg, dg);

      // Won't be called since connectors haven't changed
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      Assert.assertFalse(listener3.called);
   }

   /**
    * @param discoveryGroup
    * @throws Exception
    */
   private static void verifyBroadcast(BroadcastGroup broadcastGroup, DiscoveryGroup discoveryGroup) throws Exception
   {
      broadcastGroup.broadcastConnectors();
      Assert.assertTrue("broadcast received", discoveryGroup.waitForBroadcast(2000));
   }

   /**
    * @param discoveryGroup
    * @throws Exception
    */
   private static void verifyNonBroadcast(BroadcastGroup broadcastGroup, DiscoveryGroup discoveryGroup) throws Exception
   {
      broadcastGroup.broadcastConnectors();
      Assert.assertFalse("NO broadcast received", discoveryGroup.waitForBroadcast(2000));
   }

   @Test
   public void testConnectorsUpdatedMultipleBroadcasters() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String node1 = RandomUtil.randomString();
      String node2 = RandomUtil.randomString();
      String node3 = RandomUtil.randomString();

      bg1 = newBroadcast(node1,
                         RandomUtil.randomString(),
                         null,
                         -1,
                         groupAddress,
                         groupPort);
      bg1.start();

      bg2 = newBroadcast(node2,
                         RandomUtil.randomString(),
                         null,
                         -1,
                         groupAddress,
                         groupPort);
      bg2.start();

      bg3 = newBroadcast(node3,
                         RandomUtil.randomString(),
                         null,
                         -1,
                         groupAddress,
                         groupPort);
      bg3.start();

      TransportConfiguration live1 = generateTC();
      bg1.addConnector(live1);

      TransportConfiguration live2 = generateTC();
      bg2.addConnector(live2);

      TransportConfiguration live3 = generateTC();
      bg3.addConnector(live3);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
                             RandomUtil.randomString(),
                             null,
                             groupAddress,
                             groupPort,
                             timeout);

      MyListener listener1 = new MyListener();
      dg.registerListener(listener1);
      MyListener listener2 = new MyListener();
      dg.registerListener(listener2);

      dg.start();

      verifyBroadcast(bg1, dg);
      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      verifyBroadcast(bg2, dg);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2), entries);
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      verifyBroadcast(bg3, dg);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      verifyBroadcast(bg1, dg);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      verifyBroadcast(bg2, dg);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      verifyBroadcast(bg3, dg);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg2.removeConnector(live2);
      verifyBroadcast(bg2, dg);

      // Connector2 should still be there since not timed out yet

      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      Thread.sleep(timeout * 2);

      bg1.broadcastConnectors();
      boolean ok = dg.waitForBroadcast(1000);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);

      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live3), entries);
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg1.removeConnector(live1);
      bg3.removeConnector(live3);

      Thread.sleep(timeout * 2);

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);

      entries = dg.getDiscoveryEntries();
      Assert.assertNotNull(entries);
      Assert.assertEquals(0, entries.size());
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);

      entries = dg.getDiscoveryEntries();
      Assert.assertNotNull(entries);
      Assert.assertEquals(0, entries.size());
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
   }

   @Test
   public void testMultipleDiscoveryGroups() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      bg = newBroadcast(nodeID,
                        RandomUtil.randomString(),
                        null,
                        -1,
                        groupAddress,
                        groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg1 = newDiscoveryGroup(RandomUtil.randomString(),
                              RandomUtil.randomString(),
                              null,
                              groupAddress,
                              groupPort,
                              timeout);

      dg2 = newDiscoveryGroup(RandomUtil.randomString(),
                              RandomUtil.randomString(),
                              null,
                              groupAddress,
                              groupPort,
                              timeout);

      dg3 = newDiscoveryGroup(RandomUtil.randomString(),
                              RandomUtil.randomString(),
                              null,
                              groupAddress,
                              groupPort,
                              timeout);

      dg1.start();
      dg2.start();
      dg3.start();

      bg.broadcastConnectors();

      boolean ok = dg1.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      List<DiscoveryEntry> entries = dg1.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      ok = dg2.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg2.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      ok = dg3.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg3.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      bg.stop();

      dg1.stop();
      dg2.stop();
      dg3.stop();
   }

   @Test
   public void testDiscoveryGroupNotifications() throws Exception
   {
      SimpleNotificationService notifService = new SimpleNotificationService();
      SimpleNotificationService.Listener notifListener = new SimpleNotificationService.Listener();
      notifService.addNotificationListener(notifListener);

      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      dg = newDiscoveryGroup(RandomUtil.randomString(),
                             RandomUtil.randomString(),
                             null,
                             groupAddress,
                             groupPort,
                             timeout, notifService);

      Assert.assertEquals(0, notifListener.getNotifications().size());

      dg.start();

      Assert.assertEquals(1, notifListener.getNotifications().size());
      Notification notif = notifListener.getNotifications().get(0);
      Assert.assertEquals(CoreNotificationType.DISCOVERY_GROUP_STARTED, notif.getType());
      Assert.assertEquals(dg.getName(), notif.getProperties()
         .getSimpleStringProperty(new SimpleString("name"))
         .toString());

      dg.stop();

      Assert.assertEquals(2, notifListener.getNotifications().size());
      notif = notifListener.getNotifications().get(1);
      Assert.assertEquals(CoreNotificationType.DISCOVERY_GROUP_STOPPED, notif.getType());
      Assert.assertEquals(dg.getName(), notif.getProperties()
         .getSimpleStringProperty(new SimpleString("name"))
         .toString());
   }

   @Test
   public void testBroadcastGroupNotifications() throws Exception
   {
      SimpleNotificationService notifService = new SimpleNotificationService();
      SimpleNotificationService.Listener notifListener = new SimpleNotificationService.Listener();
      notifService.addNotificationListener(notifListener);

      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();

      bg = newBroadcast(RandomUtil.randomString(),
                        RandomUtil.randomString(),
                        null,
                        -1,
                        groupAddress,
                        groupPort);

      bg.setNotificationService(notifService);

      Assert.assertEquals(0, notifListener.getNotifications().size());

      bg.start();

      Assert.assertEquals(1, notifListener.getNotifications().size());
      Notification notif = notifListener.getNotifications().get(0);
      Assert.assertEquals(CoreNotificationType.BROADCAST_GROUP_STARTED, notif.getType());
      Assert.assertEquals(bg.getName(), notif.getProperties()
         .getSimpleStringProperty(new SimpleString("name"))
         .toString());

      bg.stop();

      Assert.assertEquals(2, notifListener.getNotifications().size());
      notif = notifListener.getNotifications().get(1);
      Assert.assertEquals(CoreNotificationType.BROADCAST_GROUP_STOPPED, notif.getType());
      Assert.assertEquals(bg.getName(), notif.getProperties()
         .getSimpleStringProperty(new SimpleString("name"))
         .toString());
   }

   /**
    * https://issues.jboss.org/browse/HORNETQ-1389
    * @throws Exception
    */
   @Test
   public void testJGroupsBroadcastGroupConfigurationSerializable() throws Exception
   {
      JGroupsBroadcastGroupConfiguration jgroupsConfig =
         new JGroupsBroadcastGroupConfiguration(TEST_JGROUPS_CONF_FILE, "somChannel");
      ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
      ObjectOutputStream objectOut = new ObjectOutputStream(byteOut);
      objectOut.writeObject(jgroupsConfig);

      byte[] serializedData = byteOut.toByteArray();
      ByteArrayInputStream byteIn = new ByteArrayInputStream(serializedData);
      ObjectInputStream objectIn = new ObjectInputStream(byteIn);

      Object object = objectIn.readObject();
      assertNotNull(object);
      assertTrue(object instanceof JGroupsBroadcastGroupConfiguration);
   }

   private TransportConfiguration generateTC(String debug)
   {
      String className = "org.foo.bar." + debug + "|" + UUIDGenerator.getInstance().generateStringUUID() + "";
      String name = UUIDGenerator.getInstance().generateStringUUID();
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(UUIDGenerator.getInstance().generateStringUUID(), 123);
      params.put(UUIDGenerator.getInstance().generateStringUUID(), UUIDGenerator.getInstance().generateStringUUID());
      params.put(UUIDGenerator.getInstance().generateStringUUID(), true);
      TransportConfiguration tc = new TransportConfiguration(className, params, name);
      return tc;
   }

   private TransportConfiguration generateTC()
   {
      return generateTC("");
   }

   private static class MyListener implements DiscoveryListener
   {
      volatile boolean called;

      public void connectorsChanged(List<DiscoveryEntry> newConnectors)
      {
         called = true;
      }
   }

   private static void assertEqualsDiscoveryEntries(List<TransportConfiguration> expected, List<DiscoveryEntry> actual)
   {
      assertNotNull(actual);

      List<TransportConfiguration> sortedExpected = new ArrayList<TransportConfiguration>(expected);
      Collections.sort(sortedExpected, new Comparator<TransportConfiguration>()
      {

         public int compare(TransportConfiguration o1, TransportConfiguration o2)
         {
            return o2.toString().compareTo(o1.toString());
         }
      });
      List<DiscoveryEntry> sortedActual = new ArrayList<DiscoveryEntry>(actual);
      Collections.sort(sortedActual, new Comparator<DiscoveryEntry>()
      {
         public int compare(DiscoveryEntry o1, DiscoveryEntry o2)
         {
            return o2.getConnector().toString().compareTo(o1.getConnector().toString());
         }
      });
      if (sortedExpected.size() != sortedActual.size())
      {
         dump(sortedExpected, sortedActual);
      }
      assertEquals(sortedExpected.size(), sortedActual.size());
      for (int i = 0; i < sortedExpected.size(); i++)
      {
         if (!sortedExpected.get(i).equals(sortedActual.get(i).getConnector()))
         {
            dump(sortedExpected, sortedActual);
         }
         assertEquals(sortedExpected.get(i), sortedActual.get(i).getConnector());
      }
   }

   private static void dump(List<TransportConfiguration> sortedExpected, List<DiscoveryEntry> sortedActual)
   {
      System.out.println("wrong broadcasts received");
      System.out.println("expected");
      System.out.println("----------------------------");
      for (TransportConfiguration transportConfiguration : sortedExpected)
      {
         System.out.println("transportConfiguration = " + transportConfiguration);
      }
      System.out.println("----------------------------");
      System.out.println("actual");
      System.out.println("----------------------------");
      for (DiscoveryEntry discoveryEntry : sortedActual)
      {
         System.out.println("transportConfiguration = " + discoveryEntry.getConnector());
      }
      System.out.println("----------------------------");
   }

   /**
    * This method is here just to facilitate creating the Broadcaster for this test
    */
   private BroadcastGroupImpl newBroadcast(final String nodeID,
                                           final String name,
                                           final InetAddress localAddress,
                                           int localPort,
                                           final InetAddress groupAddress,
                                           final int groupPort) throws Exception
   {
      return new BroadcastGroupImpl(new FakeNodeManager(nodeID), name, 0, null, new UDPBroadcastGroupConfiguration()
         .setGroupAddress(groupAddress.getHostAddress())
         .setGroupPort(groupPort)
         .setLocalBindAddress(localAddress != null ? localAddress.getHostAddress() : null)
         .setLocalBindPort(localPort)
         .createBroadcastEndpointFactory());
   }

   private DiscoveryGroup newDiscoveryGroup(final String nodeID, final String name, final InetAddress localBindAddress,
                                            final InetAddress groupAddress, final int groupPort, final long timeout) throws Exception
   {
      return newDiscoveryGroup(nodeID, name, localBindAddress, groupAddress, groupPort, timeout, null);
   }

   private DiscoveryGroup newDiscoveryGroup(final String nodeID, final String name, final InetAddress localBindAddress,
                                            final InetAddress groupAddress, final int groupPort, final long timeout, NotificationService notif) throws Exception
   {
      return new DiscoveryGroup(nodeID, name, timeout, new UDPBroadcastGroupConfiguration()
         .setGroupAddress(groupAddress.getHostAddress())
         .setGroupPort(groupPort)
         .setLocalBindAddress(localBindAddress != null ? localBindAddress.getHostAddress() : null)
         .createBroadcastEndpointFactory(), notif);
   }


   private final class FakeNodeManager extends NodeManager
   {

      public FakeNodeManager(String nodeID)
      {
         super(false, null);
         this.setNodeID(nodeID);
      }

      @Override
      public void awaitLiveNode() throws Exception
      {
      }

      @Override
      public void startBackup() throws Exception
      {
      }

      @Override
      public void startLiveNode() throws Exception
      {
      }

      @Override
      public void pauseLiveServer() throws Exception
      {
      }

      @Override
      public void crashLiveServer() throws Exception
      {
      }

      @Override
      public void releaseBackup() throws Exception
      {
      }

      @Override
      public SimpleString readNodeId() throws ActiveMQIllegalStateException, IOException
      {
         return null;
      }

      @Override
      public boolean isAwaitingFailback() throws Exception
      {
         return false;
      }

      @Override
      public boolean isBackupLive() throws Exception
      {
         return false;
      }

      @Override
      public void interrupt()
      {
      }
   }

}

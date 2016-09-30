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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.artemis.core.cluster.DiscoveryEntry;
import org.apache.activemq.artemis.core.cluster.DiscoveryGroup;
import org.apache.activemq.artemis.core.cluster.DiscoveryListener;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.cluster.BroadcastGroup;
import org.apache.activemq.artemis.core.server.cluster.impl.BroadcastGroupImpl;
import org.apache.activemq.artemis.core.server.management.NotificationService;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.junit.Assert;

public class DiscoveryBaseTest extends ActiveMQTestBase {

   protected static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   protected final String address1 = getUDPDiscoveryAddress();

   protected final String address2 = getUDPDiscoveryAddress(1);

   protected final String address3 = getUDPDiscoveryAddress(2);

   /**
    * @param discoveryGroup
    * @throws Exception
    */
   protected static void verifyBroadcast(BroadcastGroup broadcastGroup,
                                         DiscoveryGroup discoveryGroup) throws Exception {
      broadcastGroup.broadcastConnectors();
      Assert.assertTrue("broadcast not received", discoveryGroup.waitForBroadcast(2000));
   }

   /**
    * @param discoveryGroup
    * @throws Exception
    */
   protected static void verifyNonBroadcast(BroadcastGroup broadcastGroup,
                                            DiscoveryGroup discoveryGroup) throws Exception {
      broadcastGroup.broadcastConnectors();
      Assert.assertFalse("NO broadcast received", discoveryGroup.waitForBroadcast(2000));
   }

   protected TransportConfiguration generateTC() {
      return generateTC("");
   }

   protected TransportConfiguration generateTC(String debug) {
      String className = "org.foo.bar." + debug + "|" + UUIDGenerator.getInstance().generateStringUUID() + "";
      String name = UUIDGenerator.getInstance().generateStringUUID();
      Map<String, Object> params = new HashMap<>();
      params.put(UUIDGenerator.getInstance().generateStringUUID(), 123);
      params.put(UUIDGenerator.getInstance().generateStringUUID(), UUIDGenerator.getInstance().generateStringUUID());
      params.put(UUIDGenerator.getInstance().generateStringUUID(), true);
      TransportConfiguration tc = new TransportConfiguration(className, params, name);
      return tc;
   }

   protected static class MyListener implements DiscoveryListener {

      volatile boolean called;

      @Override
      public void connectorsChanged(List<DiscoveryEntry> newConnectors) {
         called = true;
      }
   }

   protected static void assertEqualsDiscoveryEntries(List<TransportConfiguration> expected,
                                                      List<DiscoveryEntry> actual) {
      assertNotNull(actual);

      List<TransportConfiguration> sortedExpected = new ArrayList<>(expected);
      Collections.sort(sortedExpected, new Comparator<TransportConfiguration>() {

         @Override
         public int compare(TransportConfiguration o1, TransportConfiguration o2) {
            return o2.toString().compareTo(o1.toString());
         }
      });
      List<DiscoveryEntry> sortedActual = new ArrayList<>(actual);
      Collections.sort(sortedActual, new Comparator<DiscoveryEntry>() {
         @Override
         public int compare(DiscoveryEntry o1, DiscoveryEntry o2) {
            return o2.getConnector().toString().compareTo(o1.getConnector().toString());
         }
      });
      if (sortedExpected.size() != sortedActual.size()) {
         dump(sortedExpected, sortedActual);
      }
      assertEquals(sortedExpected.size(), sortedActual.size());
      for (int i = 0; i < sortedExpected.size(); i++) {
         if (!sortedExpected.get(i).equals(sortedActual.get(i).getConnector())) {
            dump(sortedExpected, sortedActual);
         }
         assertEquals(sortedExpected.get(i), sortedActual.get(i).getConnector());
      }
   }

   protected static void dump(List<TransportConfiguration> sortedExpected, List<DiscoveryEntry> sortedActual) {
      System.out.println("wrong broadcasts received");
      System.out.println("expected");
      System.out.println("----------------------------");
      for (TransportConfiguration transportConfiguration : sortedExpected) {
         System.out.println("transportConfiguration = " + transportConfiguration);
      }
      System.out.println("----------------------------");
      System.out.println("actual");
      System.out.println("----------------------------");
      for (DiscoveryEntry discoveryEntry : sortedActual) {
         System.out.println("transportConfiguration = " + discoveryEntry.getConnector());
      }
      System.out.println("----------------------------");
   }

   /**
    * This method is here just to facilitate creating the Broadcaster for this test
    */
   protected BroadcastGroupImpl newBroadcast(final String nodeID,
                                             final String name,
                                             final InetAddress localAddress,
                                             int localPort,
                                             final InetAddress groupAddress,
                                             final int groupPort) throws Exception {
      return new BroadcastGroupImpl(new FakeNodeManager(nodeID), name, 0, null, new UDPBroadcastEndpointFactory().setGroupAddress(groupAddress.getHostAddress()).setGroupPort(groupPort).setLocalBindAddress(localAddress != null ? localAddress.getHostAddress() : "localhost").setLocalBindPort(localPort));
   }

   protected DiscoveryGroup newDiscoveryGroup(final String nodeID,
                                              final String name,
                                              final InetAddress localBindAddress,
                                              final InetAddress groupAddress,
                                              final int groupPort,
                                              final long timeout) throws Exception {
      return newDiscoveryGroup(nodeID, name, localBindAddress, groupAddress, groupPort, timeout, null);
   }

   protected DiscoveryGroup newDiscoveryGroup(final String nodeID,
                                              final String name,
                                              final InetAddress localBindAddress,
                                              final InetAddress groupAddress,
                                              final int groupPort,
                                              final long timeout,
                                              NotificationService notif) throws Exception {
      return new DiscoveryGroup(nodeID, name, timeout, new UDPBroadcastEndpointFactory().setGroupAddress(groupAddress.getHostAddress()).setGroupPort(groupPort).setLocalBindAddress(localBindAddress != null ? localBindAddress.getHostAddress() : "localhost"), notif);
   }

   protected final class FakeNodeManager extends NodeManager {

      public FakeNodeManager(String nodeID) {
         super(false, null);
         this.setNodeID(nodeID);
      }

      @Override
      public void awaitLiveNode() throws Exception {
      }

      @Override
      public void awaitLiveStatus() throws Exception {
      }

      @Override
      public void startBackup() throws Exception {
      }

      @Override
      public ActivateCallback startLiveNode() throws Exception {
         return new ActivateCallback() {
            @Override
            public void preActivate() {
            }

            @Override
            public void activated() {
            }

            @Override
            public void deActivate() {
            }

            @Override
            public void activationComplete() {
            }
         };
      }

      @Override
      public void pauseLiveServer() throws Exception {
      }

      @Override
      public void crashLiveServer() throws Exception {
      }

      @Override
      public void releaseBackup() throws Exception {
      }

      @Override
      public SimpleString readNodeId() {
         return null;
      }

      @Override
      public boolean isAwaitingFailback() throws Exception {
         return false;
      }

      @Override
      public boolean isBackupLive() throws Exception {
         return false;
      }

      @Override
      public void interrupt() {
      }
   }

}

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
package org.apache.activemq.artemis.core.server.group.impl;

import javax.management.ObjectName;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.management.impl.ActiveMQServerControlImpl;
import org.apache.activemq.artemis.core.messagecounter.MessageCounterManager;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Divert;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.core.server.cluster.BroadcastGroup;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.junit.Assert;
import org.junit.Test;

/**
 * this is testing the case for resending notifications from RemotingGroupHandler
 * There is a small window where you could receive notifications wrongly
 * this test will make sure the component would play well with that notification
 */
public class ClusteredResetMockTest extends ActiveMQTestBase {

   public static final SimpleString ANYCLUSTER = SimpleString.toSimpleString("anycluster");

   @Test
   public void testMultipleSenders() throws Throwable {

      int NUMBER_OF_SENDERS = 100;
      ReusableLatch latchSends = new ReusableLatch(NUMBER_OF_SENDERS);

      FakeManagement fake = new FakeManagement(latchSends);
      RemoteGroupingHandler handler = new RemoteGroupingHandler(fake, SimpleString.toSimpleString("tst1"), SimpleString.toSimpleString("tst2"), 50000, 499);
      handler.start();

      Sender[] sn = new Sender[NUMBER_OF_SENDERS];

      for (int i = 0; i < sn.length; i++) {
         sn[i] = new Sender("grp" + i, handler);
         sn[i].start();
      }

      try {

         // Waiting two requests to arrive
         Assert.assertTrue(latchSends.await(1, TimeUnit.MINUTES));

         // we will ask a resend.. need to add 2 back
         for (int i = 0; i < NUMBER_OF_SENDERS; i++) {
            // There is no countUp(NUMBER_OF_SENDERS); adding two back on the reusable latch
            latchSends.countUp();
         }

         fake.pendingNotifications.clear();

         handler.resendPending();

         assertTrue(latchSends.await(10, TimeUnit.SECONDS));

         HashSet<SimpleString> codesAsked = new HashSet<>();

         for (Notification notification : fake.pendingNotifications) {
            codesAsked.add(notification.getProperties().getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID));
         }

         for (Sender snItem : sn) {
            assertTrue(codesAsked.contains(snItem.code));
         }

         for (int i = NUMBER_OF_SENDERS - 1; i >= 0; i--) {

            // Sending back the response as Notifications would be doing
            Response response = new Response(sn[i].code, ANYCLUSTER);
            handler.proposed(response);
         }

         for (Sender sni : sn) {
            sni.join();
            if (sni.ex != null) {
               throw sni.ex;
            }
         }
      } finally {

         for (Sender sni : sn) {
            sni.interrupt();
         }
      }

   }

   class Sender extends Thread {

      SimpleString code;
      public RemoteGroupingHandler handler;

      Throwable ex;

      Sender(String code, RemoteGroupingHandler handler) {
         super("Sender::" + code);
         this.code = SimpleString.toSimpleString(code);
         this.handler = handler;
      }

      @Override
      public void run() {
         Proposal proposal = new Proposal(code, ANYCLUSTER);

         try {
            Response response = handler.propose(proposal);
            if (response == null) {
               ex = new NullPointerException("expected value on " + getName());
            } else if (!response.getGroupId().equals(code)) {
               ex = new IllegalStateException("expected code=" + code + " but it was " + response.getGroupId());
            }
         } catch (Throwable ex) {
            ex.printStackTrace();
            this.ex = ex;
         }

      }
   }

   class FakeManagement implements ManagementService {

      public ConcurrentHashSet<Notification> pendingNotifications = new ConcurrentHashSet<>();

      final ReusableLatch latch;

      FakeManagement(ReusableLatch latch) {
         this.latch = latch;
      }

      @Override
      public MessageCounterManager getMessageCounterManager() {
         return null;
      }

      @Override
      public SimpleString getManagementAddress() {
         return null;
      }

      @Override
      public SimpleString getManagementNotificationAddress() {
         return null;
      }

      @Override
      public ObjectNameBuilder getObjectNameBuilder() {
         return null;
      }

      @Override
      public void setStorageManager(StorageManager storageManager) {

      }

      @Override
      public ActiveMQServerControlImpl registerServer(PostOffice postOffice,
                                                      SecurityStore securityStore,
                                                      StorageManager storageManager,
                                                      Configuration configuration,
                                                      HierarchicalRepository<AddressSettings> addressSettingsRepository,
                                                      HierarchicalRepository<Set<Role>> securityRepository,
                                                      ResourceManager resourceManager,
                                                      RemotingService remotingService,
                                                      ActiveMQServer messagingServer,
                                                      QueueFactory queueFactory,
                                                      ScheduledExecutorService scheduledThreadPool,
                                                      PagingManager pagingManager,
                                                      boolean backup) throws Exception {
         return null;
      }

      @Override
      public void unregisterServer() throws Exception {

      }

      @Override
      public void registerInJMX(ObjectName objectName, Object managedResource) throws Exception {

      }

      @Override
      public void unregisterFromJMX(ObjectName objectName) throws Exception {

      }

      @Override
      public void registerInRegistry(String resourceName, Object managedResource) {

      }

      @Override
      public void unregisterFromRegistry(String resourceName) {

      }

      @Override
      public void registerAddress(AddressInfo addressInfo) throws Exception {

      }

      @Override
      public void unregisterAddress(SimpleString address) throws Exception {

      }

      @Override
      public void registerQueue(Queue queue, SimpleString address, StorageManager storageManager) throws Exception {

      }

      @Override
      public void unregisterQueue(SimpleString name, SimpleString address, RoutingType routingType) throws Exception {

      }

      @Override
      public void registerAcceptor(Acceptor acceptor, TransportConfiguration configuration) throws Exception {

      }

      @Override
      public void unregisterAcceptors() {

      }

      @Override
      public void registerDivert(Divert divert, DivertConfiguration config) throws Exception {

      }

      @Override
      public void unregisterDivert(SimpleString name, SimpleString address) throws Exception {

      }

      @Override
      public void registerBroadcastGroup(BroadcastGroup broadcastGroup,
                                         BroadcastGroupConfiguration configuration) throws Exception {

      }

      @Override
      public void unregisterBroadcastGroup(String name) throws Exception {

      }

      @Override
      public void registerBridge(Bridge bridge, BridgeConfiguration configuration) throws Exception {

      }

      @Override
      public void unregisterBridge(String name) throws Exception {

      }

      @Override
      public void registerCluster(ClusterConnection cluster,
                                  ClusterConnectionConfiguration configuration) throws Exception {

      }

      @Override
      public void unregisterCluster(String name) throws Exception {

      }

      @Override
      public Object getResource(String resourceName) {
         return null;
      }

      @Override
      public Object[] getResources(Class<?> resourceType) {
         return new Object[0];
      }

      @Override
      public ServerMessage handleMessage(ServerMessage message) throws Exception {
         return null;
      }

      @Override
      public void start() throws Exception {

      }

      @Override
      public void stop() throws Exception {

      }

      @Override
      public boolean isStarted() {
         return false;
      }

      @Override
      public void sendNotification(Notification notification) throws Exception {
         pendingNotifications.add(notification);
         latch.countDown();
      }

      @Override
      public void enableNotifications(boolean enable) {

      }

      @Override
      public void addNotificationListener(NotificationListener listener) {

      }

      @Override
      public void removeNotificationListener(NotificationListener listener) {

      }
   }

}

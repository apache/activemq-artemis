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
package org.apache.activemq.artemis.tests.extras.byteman;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.tests.integration.cluster.failover.FailoverTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(BMUnitRunner.class)
public class LargeMessageReplicationTest extends FailoverTestBase {


   private static final String DIVERT_ADDRESS = "jms.queue.testQueue";
   private static final String DIVERT_FORWARD_ADDRESS = "jms.queue.divertedQueue";
   private ClientSessionFactoryInternal sf;

   private static AtomicLong copyThread = new AtomicLong(-1);
   private static List<byte[]> sourceBytes = new ArrayList<>();
   private static byte[] originalBuffer;
   private static boolean isOk;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      isOk = true;
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return getNettyAcceptorTransportConfiguration(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return getNettyConnectorTransportConfiguration(live);
   }

   @Override
   protected void createConfigs() throws Exception {
      createReplicatedConfigs();

      liveConfig.setJournalFileSize(10240000);
      backupConfig.setJournalFileSize(10240000);
      addQueue(liveConfig, DIVERT_ADDRESS, DIVERT_ADDRESS);
      addQueue(liveConfig, DIVERT_FORWARD_ADDRESS, DIVERT_FORWARD_ADDRESS);
      addDivert(liveConfig, DIVERT_ADDRESS, DIVERT_FORWARD_ADDRESS, false);
      addDivert(backupConfig, DIVERT_ADDRESS, DIVERT_FORWARD_ADDRESS, false);
   }

   private void addQueue(Configuration serverConfig, String address, String name) {

      List<CoreAddressConfiguration> addrConfigs = serverConfig.getAddressConfigurations();
      CoreAddressConfiguration addrCfg = new CoreAddressConfiguration();
      addrCfg.setName(address);
      addrCfg.addRoutingType(RoutingType.ANYCAST);
      CoreQueueConfiguration qConfig = new CoreQueueConfiguration();
      qConfig.setName(name);
      qConfig.setAddress(address);
      addrCfg.addQueueConfiguration(qConfig);
      addrConfigs.add(addrCfg);
   }

   private void addDivert(Configuration serverConfig, String source, String target, boolean exclusive) {
      List<DivertConfiguration> divertConfigs = serverConfig.getDivertConfigurations();
      DivertConfiguration newDivert = new DivertConfiguration();
      newDivert.setName("myDivert");
      newDivert.setAddress(source);
      newDivert.setForwardingAddress(target);
      newDivert.setExclusive(exclusive);
      divertConfigs.add(newDivert);
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "record large message copy thread",
         targetClass = "org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl",
         targetMethod = "copy(long)",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.LargeMessageReplicationTest.copyThread()"), @BMRule(
         name = "record byte array in addBytes",
         targetClass = "org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl",
         targetMethod = "addBytes(byte[])",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.LargeMessageReplicationTest.addBytesIn($1)"), @BMRule(
         name = "record byte array used for reading large message",
         targetClass = "^org.apache.activemq.artemis.core.io.SequentialFile",
         isInterface = true,
         targetMethod = "read(java.nio.ByteBuffer)",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.LargeMessageReplicationTest.originBuff($1)")})
   //https://issues.apache.org/jira/browse/ARTEMIS-1220
   public void testDivertCopyMessageBuffer() throws Exception {
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.HOST_PROP_NAME, "localhost");
      TransportConfiguration tc = createTransportConfiguration(true, false, params);
      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithHA(tc)).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setReconnectAttempts(-1);
      sf = createSessionFactoryAndWaitForTopology(locator, 2);
      int minLarge = locator.getMinLargeMessageSize();

      ClientSession session = sf.createSession(false, false);
      addClientSession(session);
      session.start();

      ClientProducer producer = session.createProducer(DIVERT_ADDRESS);
      ClientMessage message = createLargeMessage(session, 3 * minLarge);
      producer.send(message);

      session.commit();

      ClientConsumer consumer = session.createConsumer(DIVERT_ADDRESS);
      ClientMessage receivedFromSourceQueue = consumer.receive(5000);
      assertNotNull(receivedFromSourceQueue);
      receivedFromSourceQueue.acknowledge();
      session.commit();

      crash(session);

      ClientConsumer consumer1 = session.createConsumer(DIVERT_FORWARD_ADDRESS);
      ClientMessage receivedFromTargetQueue = consumer1.receive(5000);
      assertNotNull(receivedFromTargetQueue);
      receivedFromTargetQueue.acknowledge();

      session.commit();

      checkBufferNotReused();
   }

   private void checkBufferNotReused() throws Exception {
      assertNotNull("Didn't catch the original buffer!", originalBuffer);
      assertTrue("Didn't catch the read buffer!", sourceBytes.size() > 0);
      for (byte[] array : sourceBytes) {
         assertFalse("Buffer reused!", originalBuffer == array);
      }
   }

   private ClientMessage createLargeMessage(ClientSession session, final int largeSize) {
      ClientMessage message = session.createMessage(true);
      ActiveMQBuffer bodyBuffer = message.getBodyBuffer();
      final int propSize = 10240;
      while (bodyBuffer.writerIndex() < largeSize) {
         byte[] prop = new byte[propSize];
         bodyBuffer.writeBytes(prop);
      }
      return message;
   }

   private static void copyThread() {
      System.out.println("_************************ " + Thread.currentThread().getId());
      copyThread.set(Thread.currentThread().getId());
   }

   private static void addBytesIn(final byte[] bytes) {
      if (copyThread.get() != -1 && copyThread.get() == Thread.currentThread().getId()) {
         sourceBytes.add(bytes);
      }
   }

   private static void originBuff(final ByteBuffer buff) {
      if (copyThread.get() != -1 && copyThread.get() == Thread.currentThread().getId()) {
         originalBuffer = buff.array();
      }
   }
}

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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.client.impl.ClientProducerCredits;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionSendMessage;
import org.apache.activemq.artemis.jms.bridge.ConnectionFactoryFactory;
import org.apache.activemq.artemis.jms.bridge.QualityOfServiceMode;
import org.apache.activemq.artemis.jms.bridge.impl.JMSBridgeImpl;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.tests.extras.jms.bridge.BridgeTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class JMSBridgeReconnectionTest extends BridgeTestBase {

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "trace clientsessionimpl send",
         targetClass = "org.apache.activemq.artemis.core.protocol.core.impl.ChannelImpl",
         targetMethod = "send",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.JMSBridgeReconnectionTest.pause($1);"), @BMRule(
         name = "trace sendRegularMessage",
         targetClass = "org.apache.activemq.artemis.core.client.impl.ClientProducerImpl",
         targetMethod = "sendRegularMessage",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.JMSBridgeReconnectionTest.pause2($2,$3,$4);")})
   public void performCrashDestinationStopBridge() throws Exception {
      activeMQServer = jmsServer1;
      ConnectionFactoryFactory factInUse0 = cff0;
      ConnectionFactoryFactory factInUse1 = cff1;
      final JMSBridgeImpl bridge = new JMSBridgeImpl(factInUse0, factInUse1, sourceQueueFactory, targetQueueFactory, null, null, null, null, null, 1000, -1, QualityOfServiceMode.DUPLICATES_OK, 10, -1, null, null, false).setBridgeName("test-bridge");

      addActiveMQComponent(bridge);
      bridge.setTransactionManager(newTransactionManager());
      bridge.start();
      final CountDownLatch latch = new CountDownLatch(20);
      Thread clientThread = new Thread(new Runnable() {
         @Override
         public void run() {
            while (bridge.isStarted()) {
               try {
                  sendMessages(cf0, sourceQueue, 0, 1, false, false);
                  latch.countDown();
               } catch (Exception e) {
                  e.printStackTrace();
               }
            }
         }
      });

      clientThread.start();

      stopLatch.await(10000, TimeUnit.MILLISECONDS);

      bridge.stop();

      clientThread.join(5000);

      assertTrue(!clientThread.isAlive());
   }

   public static void pause(Packet packet) {
      if (packet.getType() == PacketImpl.SESS_SEND) {
         SessionSendMessage sendMessage = (SessionSendMessage) packet;
         if (sendMessage.getMessage().containsProperty("__AMQ_CID") && count < 0 && !stopped) {
            try {
               activeMQServer.stop();
            } catch (Exception e) {
               e.printStackTrace();
            }
            stopped = true;
            try {
               Thread.sleep(5000);
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
            stopLatch.countDown();
         }
      }
   }

   static JMSServerManager activeMQServer;
   static boolean stopped = false;
   static int count = 20;
   static CountDownLatch stopLatch = new CountDownLatch(1);

   public static void pause2(Message msgI, boolean sendBlocking, final ClientProducerCredits theCredits) {
      if (msgI.containsProperty("__AMQ_CID")) {
         count--;
      }
   }
}

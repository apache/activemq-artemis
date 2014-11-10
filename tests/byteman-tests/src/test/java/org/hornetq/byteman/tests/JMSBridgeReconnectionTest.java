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
package org.hornetq.byteman.tests;

import org.hornetq.core.client.impl.ClientProducerCredits;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.SessionSendMessage;
import org.hornetq.jms.bridge.ConnectionFactoryFactory;
import org.hornetq.jms.bridge.QualityOfServiceMode;
import org.hornetq.jms.bridge.impl.JMSBridgeImpl;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.tests.integration.jms.bridge.BridgeTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(BMUnitRunner.class)
public class JMSBridgeReconnectionTest extends BridgeTestBase
{

   @Test
   @BMRules
         (
               rules =
                     {
                           @BMRule
                                 (
                                       name = "trace clientsessionimpl send",
                                       targetClass = "org.hornetq.core.protocol.core.impl.ChannelImpl",
                                       targetMethod = "send",
                                       targetLocation = "ENTRY",
                                       action = "org.hornetq.byteman.tests.JMSBridgeReconnectionTest.pause($1);"
                                 ),
                           @BMRule
                                 (
                                       name = "trace sendRegularMessage",
                                       targetClass = "org.hornetq.core.client.impl.ClientProducerImpl",
                                       targetMethod = "sendRegularMessage",
                                       targetLocation = "ENTRY",
                                       action = "org.hornetq.byteman.tests.JMSBridgeReconnectionTest.pause2($1,$2,$3);"
                                 )
                     }
         )
   public void performCrashDestinationStopBridge() throws Exception
   {
      hornetQServer = jmsServer1;
      ConnectionFactoryFactory factInUse0 = cff0;
      ConnectionFactoryFactory factInUse1 = cff1;
      final JMSBridgeImpl bridge =
            new JMSBridgeImpl(factInUse0,
                  factInUse1,
                  sourceQueueFactory,
                  targetQueueFactory,
                  null,
                  null,
                  null,
                  null,
                  null,
                  1000,
                  -1,
                  QualityOfServiceMode.DUPLICATES_OK,
                  10,
                  -1,
                  null,
                  null,
                  false);

      addHornetQComponent(bridge);
      bridge.setTransactionManager(newTransactionManager());
      bridge.start();
      final CountDownLatch latch = new CountDownLatch(20);
      Thread clientThread = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            while (bridge.isStarted())
            {
               try
               {
                  sendMessages(cf0, sourceQueue, 0, 1, false, false);
                  latch.countDown();
               }
               catch (Exception e)
               {
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

   public static void pause(Packet packet)
   {
      if (packet.getType() == PacketImpl.SESS_SEND)
      {
         SessionSendMessage sendMessage = (SessionSendMessage) packet;
         if (sendMessage.getMessage().containsProperty("__HQ_CID") && count < 0 && !stopped)
         {
            try
            {
               hornetQServer.stop();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
            stopped = true;
            try
            {
               Thread.sleep(5000);
            }
            catch (InterruptedException e)
            {
               e.printStackTrace();
            }
            stopLatch.countDown();
         }
      }
   }

   static JMSServerManager hornetQServer;
   static boolean stopped = false;
   static int count = 20;
   static CountDownLatch stopLatch = new CountDownLatch(1);
   public static void pause2(MessageInternal msgI, boolean sendBlocking, final ClientProducerCredits theCredits)
   {
      if (msgI.containsProperty("__HQ_CID"))
      {
         count--;
      }
   }
}

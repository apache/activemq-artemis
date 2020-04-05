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
package org.apache.activemq.artemis.tests.extras.byteman.critical.analyzer;

import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzerPolicy;
import org.junit.Before;

public abstract class CriticalAnalyzerFaultInjectionTestBase extends JMSTestBase {

   // Critical Analyzer Settings
   private static long CHECK_PERIOD = 100;
   private static long TIMEOUT = 3000;

   protected SimpleString address = SimpleString.toSimpleString("faultInjectionTestAddress");

   private Thread t;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server.addAddressInfo(new AddressInfo(address, RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));
      conn = nettyCf.createConnection();
   }

   /*
   Checks every 100ms timesout after 3000ms.  Test should wait no longer than 3100s + Shutdown time.
    */
   @Override
   protected Configuration createDefaultConfig(boolean netty) throws Exception {
      return super.createDefaultConfig(netty).setCriticalAnalyzerPolicy(CriticalAnalyzerPolicy.SHUTDOWN).setCriticalAnalyzer(true).setCriticalAnalyzerCheckPeriod(CHECK_PERIOD).setCriticalAnalyzerTimeout(TIMEOUT).setJournalType(JournalType.NIO);
   }

   @Override
   public boolean usePersistence() {
      return true;
   }

   void testSendDurableMessage() throws Exception {
      try {
         t = new Thread(() -> {
            try {
               Session s = conn.createSession(true, Session.SESSION_TRANSACTED);

               Queue jmsQueue = s.createQueue(address.toString());
               MessageProducer p = s.createProducer(jmsQueue);
               p.setDeliveryMode(DeliveryMode.PERSISTENT);
               conn.start();
               for (int i = 0; i < 10; i++) {
                  p.send(s.createTextMessage("payload"));
               }
               s.commit();
            } catch (Throwable e) {
               e.printStackTrace();
            }
         });
         t.start();

         Wait.assertFalse(server::isStarted);
      } finally {
         t.interrupt();
      }
   }
}

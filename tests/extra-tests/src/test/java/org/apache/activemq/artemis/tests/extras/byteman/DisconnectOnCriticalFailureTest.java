/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.extras.byteman;

import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(BMUnitRunner.class)
public class DisconnectOnCriticalFailureTest extends JMSTestBase {

   private static AtomicBoolean corruptPacket = new AtomicBoolean(false);

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "Corrupt Decoding",
         targetClass = "org.apache.activemq.artemis.core.protocol.core.impl.PacketDecoder",
         targetMethod = "decode(byte)",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.DisconnectOnCriticalFailureTest.doThrow();")})
   public void testSendDisconnect() throws Exception {
      createQueue("queue1");
      final Connection producerConnection = nettyCf.createConnection();
      final CountDownLatch latch = new CountDownLatch(1);

      try {
         producerConnection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException e) {
               latch.countDown();
            }
         });

         corruptPacket.set(true);
         producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         assertTrue(latch.await(5, TimeUnit.SECONDS));
      } finally {
         corruptPacket.set(false);

         if (producerConnection != null) {
            producerConnection.close();
         }
      }
   }

   public static void doThrow() {
      if (corruptPacket.get()) {
         corruptPacket.set(false);
         throw new IllegalArgumentException("Invalid type: -84");
      }
   }
}

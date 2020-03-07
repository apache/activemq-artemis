/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.mqtt.imported;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class MQTTQueueCleanTest extends MQTTTestSupport {

   private static final Logger LOG = LoggerFactory.getLogger(MQTTQueueCleanTest.class);

   @Test
   public void testQueueCleanWhenConnectionSynExeConnectAndDisconnect() throws Exception {
      Random random = new Random();
      Set<MQTTClientProvider> clientProviders = new HashSet<>(11);
      int repeatCount = 0;
      String address = "clean/test";
      String clientId = "sameClientId";
      String queueName = "::sameClientId.clean.test";
      //The abnormal scene does not necessarily occur, repeating 100 times to ensure the recurrence of the abnormality
      while (repeatCount < 100) {
         repeatCount++;
         int subConnectionCount = random.nextInt(50) + 1;
         int sC = 0;
         try {
            //Reconnect at least twice to reproduce the problem
            while (sC < subConnectionCount) {
               sC++;
               MQTTClientProvider clientProvider = getMQTTClientProvider();
               clientProvider.setClientId(clientId);
               initializeConnection(clientProvider);
               clientProviders.add(clientProvider);
               clientProvider.subscribe(address, AT_LEAST_ONCE);
            }
         } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
         } finally {
            for (MQTTClientProvider clientProvider : clientProviders) {
               clientProvider.disconnect();
            }
            clientProviders.clear();
            assertTrue(Wait.waitFor(() -> server.locateQueue(SimpleString.toSimpleString(queueName)) == null, 5000, 10));
         }
      }
   }

}

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

package org.apache.activemq.artemis.tests.integration.addressing;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test is simulating a dead lock that may happen while removing addresses.
 */
public class TwoWaysRemoveAddressTest extends ActiveMQTestBase {

   private static Logger logger = Logger.getLogger(TwoWaysRemoveAddressTest.class);

   @Test(timeout = 60_000)
   public void testDeadLock() throws Throwable  {
      ActiveMQServer server = addServer(createServer(false));
      server.start();

      final int retries = 10;
      CyclicBarrier barrier = new CyclicBarrier(2);

      AtomicInteger errors = new AtomicInteger(0);

      Thread createAndDestroy1 = new Thread() {
         @Override
         public void run() {

            try {
               barrier.await(10, TimeUnit.SECONDS);

               for (int i = 0; i < retries; i++) {
                  logger.debug("Removed queue on thread 1 ::" + i);
                  server.createQueue(new QueueConfiguration("queueName_1_" + i).setAddress("address_1_" + i).setRoutingType(RoutingType.ANYCAST));
                  server.destroyQueue(SimpleString.toSimpleString("queueName_1_" + i));
               }
            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
               errors.incrementAndGet();
            }
         }
      };

      Thread createAndDestroy2 = new Thread() {
         @Override
         public void run() {

            try {
               barrier.await(10, TimeUnit.SECONDS);

               for (int i = 0; i < retries; i++) {
                  logger.debug("Removed queue on thread 2 ::" + i);
                  server.createQueue(new QueueConfiguration("queueName_2_" + i).setAddress("address_2_" + i).setRoutingType(RoutingType.ANYCAST));
                  server.removeAddressInfo(SimpleString.toSimpleString("address_2_" + i), null, true);
               }
            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
               errors.incrementAndGet();
            }
         }
      };

      createAndDestroy1.start();
      createAndDestroy2.start();

      createAndDestroy1.join(10_000);
      createAndDestroy2.join(10_000);

      Assert.assertFalse(createAndDestroy1.isAlive());
      Assert.assertFalse(createAndDestroy2.isAlive());


      Assert.assertEquals(0, errors.get());

   }

}

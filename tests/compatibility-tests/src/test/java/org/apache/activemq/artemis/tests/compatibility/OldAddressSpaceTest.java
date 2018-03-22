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
package org.apache.activemq.artemis.tests.compatibility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.utils.FileUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.ONE_FIVE;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.SNAPSHOT;

@RunWith(Parameterized.class)
public class OldAddressSpaceTest extends VersionedBaseTest {

   @Parameterized.Parameters(name = "server={0}, producer={1}, consumer={2}")
   public static Collection getParameters() {
      List<Object[]> combinations = new ArrayList<>();
      combinations.addAll(combinatory(new Object[]{SNAPSHOT}, new Object[]{ONE_FIVE, SNAPSHOT}, new Object[]{ONE_FIVE, SNAPSHOT}));
      return combinations;
   }

   public OldAddressSpaceTest(String server, String sender, String receiver) throws Exception {
      super(server, sender, receiver);
   }


   @Before
   public void setUp() throws Throwable {
      FileUtil.deleteDirectory(serverFolder.getRoot());
   }

   @After
   public void stopTest() throws Exception {
      execute(serverClassloader, "server.stop()");
   }

   @Test
   public void testClientSenderServerAddressSettings() throws Throwable {
      evaluate(serverClassloader, "oldAddressSpace/artemisServer.groovy", serverFolder.getRoot().getAbsolutePath(), server);

      CountDownLatch subscriptionCreated = new CountDownLatch(1);
      CountDownLatch receiverLatch = new CountDownLatch(1);
      CountDownLatch senderLatch = new CountDownLatch(1);

      setVariable(receiverClassloader, "latch", receiverLatch);
      setVariable(receiverClassloader, "subscriptionCreated", subscriptionCreated);

      AtomicInteger errors = new AtomicInteger(0);
      Thread t1 = new Thread() {
         @Override
         public void run() {
            try {
               evaluate(receiverClassloader, "oldAddressSpace/receiveMessages.groovy", receiver);
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }
         }
      };
      t1.start();

      Assert.assertTrue(subscriptionCreated.await(10, TimeUnit.SECONDS));

      setVariable(senderClassloader, "senderLatch", senderLatch);
      Thread t2 = new Thread() {
         @Override
         public void run() {
            try {
               evaluate(senderClassloader, "oldAddressSpace/sendMessagesAddress.groovy", sender);
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }
         }
      };
      t2.start();


      try {
         Assert.assertTrue("Sender is blocking by mistake", senderLatch.await(10, TimeUnit.SECONDS));
         Assert.assertTrue("Receiver did not receive messages", receiverLatch.await(10, TimeUnit.SECONDS));
      } finally {

         t1.join(TimeUnit.SECONDS.toMillis(1));
         t2.join(TimeUnit.SECONDS.toMillis(1));

         if (t1.isAlive()) {
            t1.interrupt();
         }

         if (t2.isAlive()) {
            t2.interrupt();
         }
      }

   }

}
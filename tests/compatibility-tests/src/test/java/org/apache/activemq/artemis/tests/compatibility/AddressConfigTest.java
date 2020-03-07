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

import org.apache.activemq.artemis.tests.compatibility.base.VersionedBase;
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
public class AddressConfigTest extends VersionedBase {

   // this will ensure that all tests in this class are run twice,
   // once with "true" passed to the class' constructor and once with "false"
   @Parameterized.Parameters(name = "server={0}, producer={1}, consumer={2}")
   public static Collection getParameters() {
      // we don't need every single version ever released..
      // if we keep testing current one against 2.4 and 1.4.. we are sure the wire and API won't change over time
      List<Object[]> combinations = new ArrayList<>();

      /*
      // during development sometimes is useful to comment out the combinations
      // and add the ones you are interested.. example:
       */
      //      combinations.add(new Object[]{SNAPSHOT, ONE_FIVE, ONE_FIVE});
      //      combinations.add(new Object[]{ONE_FIVE, ONE_FIVE, ONE_FIVE});

      combinations.addAll(combinatory(new Object[]{SNAPSHOT}, new Object[]{ONE_FIVE, SNAPSHOT}, new Object[]{ONE_FIVE, SNAPSHOT}));
      return combinations;
   }

   public AddressConfigTest(String server, String sender, String receiver) throws Exception {
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
      evaluate(serverClassloader, "addressConfig/artemisServer.groovy", serverFolder.getRoot().getAbsolutePath());


      CountDownLatch latch = new CountDownLatch(1);

      setVariable(receiverClassloader, "latch", latch);

      AtomicInteger errors = new AtomicInteger(0);

      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               evaluate(receiverClassloader, "addressConfig/receiveMessages.groovy", "receive");
            } catch (Throwable e) {
               errors.incrementAndGet();
            }
         }
      };
      t.start();


      Thread t2 = new Thread() {
         @Override
         public void run() {
            try {
               evaluate(senderClassloader, "addressConfig/sendMessagesAddress.groovy", "send");
            } catch (Throwable e) {
               errors.incrementAndGet();
            }
         }
      };
      t2.start();


      try {
         Assert.assertTrue("Sender is blocking by mistake", latch.await(10, TimeUnit.SECONDS));
      } finally {

         t.join(TimeUnit.SECONDS.toMillis(1));
         t2.join(TimeUnit.SECONDS.toMillis(1));

         if (t.isAlive()) {
            t.interrupt();
         }

         if (t2.isAlive()) {
            t2.interrupt();
         }
      }

   }

}

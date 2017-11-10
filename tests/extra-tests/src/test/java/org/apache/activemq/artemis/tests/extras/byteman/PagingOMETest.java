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

import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class PagingOMETest extends ActiveMQTestBase {

   private ServerLocator locator;
   private ActiveMQServer server;
   private ClientSessionFactory sf;
   static final int MESSAGE_SIZE = 1024; // 1k

   private static final int RECEIVE_TIMEOUT = 5000;

   private static final int PAGE_MAX = 100 * 1024;

   private static final int PAGE_SIZE = 10 * 1024;

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   static boolean failureActive = false;

   public static void refCheck() {
      if (failureActive) {
         throw new OutOfMemoryError("fake error");
      }
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      failureActive = false;
      locator = createInVMNonHALocator();
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "fakeOME",
         targetClass = "org.apache.activemq.artemis.core.paging.cursor.PagedReferenceImpl",
         targetMethod = "getPagedMessage",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.PagingOMETest.refCheck()")})
   public void testPageCleanup() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultConfig(false);

      config.setJournalSyncNonTransactional(false);

      HashMap<String, AddressSettings> map = new HashMap<>();
      AddressSettings value = new AddressSettings();
      map.put(ADDRESS.toString(), value);
      server = createServer(true, config, PAGE_SIZE, PAGE_MAX, map);

      server.start();

      final int numberOfMessages = 2;

      locator = createInVMNonHALocator();

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(false);
      locator.setConsumerWindowSize(0);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      Queue queue = server.locateQueue(ADDRESS);
      queue.getPageSubscription().getPagingStore().startPaging();

      Assert.assertTrue(queue.getPageSubscription().getPagingStore().isPaging());

      ClientProducer producer = session.createProducer(PagingOMETest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }
      session.commit();

      session = sf.createSession(false, false, false);

      session.start();

      Wait.assertTrue(() -> numberOfMessages == queue.getMessageCount());

      // The consumer has to be created after the queue.getMessageCount assertion
      // otherwise delivery could alter the messagecount and give us a false failure
      ClientConsumer consumer = session.createConsumer(PagingOMETest.ADDRESS);
      ClientMessage msg = null;

      msg = consumer.receive(1000);

      failureActive = true;
      msg.individualAcknowledge();
      try {
         session.commit();
         Assert.fail("exception expected");
      } catch (Exception expected) {
      }
      failureActive = false;
      session.rollback();

      session.close();

      sf.close();

      locator.close();

      server.stop();

      server.start();

      locator = createInVMNonHALocator();

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(false);
      locator.setConsumerWindowSize(0);

      sf = createSessionFactory(locator);

      session = sf.createSession(false, false, false);

      consumer = session.createConsumer(PagingOMETest.ADDRESS);

      session.start();

      for (int i = 0; i < numberOfMessages; i++) {
         msg = consumer.receive(1000);
         Assert.assertNotNull(msg);
         msg.individualAcknowledge();
      }
      Assert.assertNull(consumer.receiveImmediate());
      session.commit();

      session.close();
      sf.close();
      server.stop();

   }
}

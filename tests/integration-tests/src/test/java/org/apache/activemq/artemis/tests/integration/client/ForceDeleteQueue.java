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

package org.apache.activemq.artemis.tests.integration.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ForceDeleteQueue extends ActiveMQTestBase {

   ActiveMQServer server;
   String protocol = "openwire";
   String uri = "tcp://localhost:61616";

   public ForceDeleteQueue(String protocol) {
      this.protocol = protocol;
   }

   @Parameterized.Parameters(name = "protocol={0}")
   public static Collection<Object[]> data() {
      Object[][] params = new Object[][]{{"openwire"}, {"core"}, {"amqp"}};
      return Arrays.asList(params);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      if (protocol.equals("openwire")) {
         uri = "tcp://localhost:61616?jms.prefetchPolicy.all=5000";
      }

      server = createServer(true, true);
      server.getAddressSettingsRepository().addMatch("#",
                                                     new AddressSettings().setMaxDeliveryAttempts(2));

      server.start();
   }

   @Test
   public void testForceDelete() throws Exception {
      SimpleString queueName = SimpleString.toSimpleString("testForceDelete");
      server.addAddressInfo(new AddressInfo(queueName, RoutingType.ANYCAST));
      server.createQueue(queueName, RoutingType.ANYCAST, queueName, null, true, false);

      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, uri);
      Connection conn = factory.createConnection();

      AssertionLoggerHandler.startCapture();
      try {
         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue(queueName.toString());
         MessageProducer producer = session.createProducer(queue);
         for (int i = 0; i < 1000; i++) {
            TextMessage message = session.createTextMessage("Text " + i);
            producer.send(message);
         }
         session.commit();

         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(queueName);

         Wait.assertEquals(1000, serverQueue::getMessageCount);

         conn.close();

         conn = factory.createConnection();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         conn.start();

         LinkedListIterator<MessageReference> queueiterator =  serverQueue.browserIterator();
         ArrayList<Long> listQueue = new ArrayList<>(1000);

         while (queueiterator.hasNext()) {
            MessageReference ref = queueiterator.next();

            listQueue.add(ref.getMessageID());
         }

         queueiterator.close();

         MessageConsumer consumer = session.createConsumer(queue);

         Wait.assertTrue(() -> serverQueue.getDeliveringCount() > 100);

         for (Long l : listQueue) {
            // this is forcing an artificial situation where the message was removed during a failure condition
            server.getStorageManager().deleteMessage(l);
         }

         server.destroyQueue(queueName, null, false);

         for (RemotingConnection connection : server.getRemotingService().getConnections()) {
            connection.fail(new ActiveMQException("failure"));
         }


         Assert.assertFalse(AssertionLoggerHandler.findText("Cannot find add info"));


      } finally {
         AssertionLoggerHandler.stopCapture();
         try {
            conn.close();
         } catch (Throwable ignored) {
         }
      }

   }

}

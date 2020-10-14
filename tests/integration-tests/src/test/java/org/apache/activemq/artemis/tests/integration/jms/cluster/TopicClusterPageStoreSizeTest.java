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
package org.apache.activemq.artemis.tests.integration.jms.cluster;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.tests.util.JMSClusteredTestBase;
import org.junit.Test;

public class TopicClusterPageStoreSizeTest extends JMSClusteredTestBase {

   public static final String TOPIC = "jms.t1";

   @Test
   public void testPageStoreSizeWithClusteredDurableSub() throws Exception {
      doTestPageStoreSizeWithClusteredDurableSub(false);
   }

   @Test
   public void testPageStoreSizeWithClusteredDurableSubWithPaging() throws Exception {
      doTestPageStoreSizeWithClusteredDurableSub(true);
   }

   private void doTestPageStoreSizeWithClusteredDurableSub(boolean forcePaging) throws Exception {

      Connection conn1 = cf1.createConnection();

      conn1.setClientID("someClient1");

      Connection conn2 = cf2.createConnection();

      conn2.setClientID("someClient2");

      conn1.start();

      conn2.start();

      Topic topic1 = createTopic(TOPIC, true);

      Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod1 = session1.createProducer(null);
      prod1.setDeliveryMode(DeliveryMode.PERSISTENT);

      MessageConsumer cons1 = session1.createDurableSubscriber(topic1, "sub1");
      MessageConsumer cons2 = session2.createDurableSubscriber(topic1, "sub2");

      waitForBindings(server1, TOPIC, true, 1, 1, 2000);
      waitForBindings(server2, TOPIC, true, 1, 1, 2000);
      waitForBindings(server1, TOPIC, false, 1, 1, 2000);
      waitForBindings(server2, TOPIC, false, 1, 1, 2000);

      if (forcePaging) {
         for (SimpleString psName : server1.getPagingManager().getStoreNames()) {
            server1.getPagingManager().getPageStore(psName).startPaging();
         }
         for (SimpleString psName : server2.getPagingManager().getStoreNames()) {
            server2.getPagingManager().getPageStore(psName).startPaging();
         }
      }

      prod1.send(topic1, session1.createTextMessage("someMessage"));

      TextMessage m2 = (TextMessage) cons2.receive(5000);
      assertNotNull(m2);
      TextMessage m1 = (TextMessage) cons1.receive(5000);
      assertTrue(m1.getJMSDestination().toString().contains(TOPIC));

      assertNotNull(m1);

      conn1.close();
      conn2.close();

      for (SimpleString psName : server1.getPagingManager().getStoreNames()) {
         assertTrue("non negative size: " + psName, server1.getPagingManager().getPageStore(psName).getAddressSize() >= 0);
      }

      for (SimpleString psName : server2.getPagingManager().getStoreNames()) {
         assertTrue("non negative size: " + psName, server2.getPagingManager().getPageStore(psName).getAddressSize() >= 0);
      }
   }
}

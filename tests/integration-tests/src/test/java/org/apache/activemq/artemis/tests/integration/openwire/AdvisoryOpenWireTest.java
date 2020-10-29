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
package org.apache.activemq.artemis.tests.integration.openwire;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AdvisoryOpenWireTest extends BasicOpenWireTest {

   @Override
   @Before
   public void setUp() throws Exception {
      //this system property is used to construct the executor in
      //org.apache.activemq.transport.AbstractInactivityMonitor.createExecutor()
      //and affects the pool's shutdown time. (default is 30 sec)
      //set it to 2 to make tests shutdown quicker.
      System.setProperty("org.apache.activemq.transport.AbstractInactivityMonitor.keepAliveTime", "2");
      this.realStore = true;
      super.setUp();
   }

   @Test
   public void testTempTopicLeak() throws Exception {
      Connection connection = null;

      try {
         connection = factory.createConnection();
         connection.start();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         TemporaryTopic temporaryTopic = session.createTemporaryTopic();
         temporaryTopic.delete();

         Object[] queueResources = server.getManagementService().getResources(QueueControl.class);

         for (Object queueResource : queueResources) {

            if (((QueueControl) queueResource).getAddress().equals("ActiveMQ.Advisory.TempTopic")) {
               QueueControl queueControl = (QueueControl) queueResource;
               Wait.waitFor(() -> queueControl.getMessageCount() == 0);
               assertNotNull("addressControl for temp advisory", queueControl);

               Wait.assertEquals(0, queueControl::getMessageCount);
               Wait.assertEquals(2, queueControl::getMessagesAdded);
            }
         }
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test
   public void testTempQueueLeak() throws Exception {
      Connection connection = null;

      try {
         connection = factory.createConnection();
         connection.start();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         TemporaryQueue temporaryQueue = session.createTemporaryQueue();
         temporaryQueue.delete();

         Object[] queueResources = server.getManagementService().getResources(QueueControl.class);

         for (Object queueResource : queueResources) {

            if (((QueueControl) queueResource).getAddress().equals("ActiveMQ.Advisory.TempQueue")) {
               QueueControl queueControl = (QueueControl) queueResource;
               Wait.waitFor(() -> queueControl.getMessageCount() == 0);
               assertNotNull("addressControl for temp advisory", queueControl);
               Wait.assertEquals(0, queueControl::getMessageCount);
               Wait.assertEquals(2, queueControl::getMessagesAdded);

            }
         }
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test
   public void testTempQueueLeakManyConnections() throws Exception {
      final Connection[] connections = new Connection[20];

      try {
         for (int i = 0; i < connections.length; i++) {
            connections[i] = factory.createConnection();
            connections[i].start();
         }

         Session session = connections[0].createSession(false, Session.AUTO_ACKNOWLEDGE);

         for (int i = 0; i < connections.length; i++) {
            TemporaryQueue temporaryQueue = session.createTemporaryQueue();
            temporaryQueue.delete();
         }

         Object[] addressResources = server.getManagementService().getResources(AddressControl.class);

         for (Object addressResource : addressResources) {

            if (((AddressControl) addressResource).getAddress().equals("ActiveMQ.Advisory.TempQueue")) {
               AddressControl addressControl = (AddressControl) addressResource;
               Wait.waitFor(() -> addressControl.getMessageCount() == 0);
               assertNotNull("addressControl for temp advisory", addressControl);
               Wait.assertEquals(0, addressControl::getMessageCount);
            }
         }


         //sleep a bit to allow message count to go down.
      } finally {
         for (Connection conn : connections) {
            if (conn != null) {
               conn.close();
            }
         }
      }
   }

   @Test
   public void testConnectionAdvisory() throws Exception {
      final Connection[] connections = new Connection[20];

      connections[0] = factory.createConnection();
      connections[0].start();

      final CountDownLatch numConnectionsCreatedViaAdvisoryNotificationsLatch = new CountDownLatch(19);
      connections[0].createSession(false, Session.AUTO_ACKNOWLEDGE)
         .createConsumer(AdvisorySupport.getConnectionAdvisoryTopic()).setMessageListener(message -> numConnectionsCreatedViaAdvisoryNotificationsLatch.countDown());

      try {
         for (int i = 1; i < connections.length; i++) {
            connections[i] = factory.createConnection();
            connections[i].start();
         }

         Session session = connections[0].createSession(false, Session.AUTO_ACKNOWLEDGE);
         session.close();

         assertTrue("Got all the advisories on time", numConnectionsCreatedViaAdvisoryNotificationsLatch.await(5, TimeUnit.SECONDS));

      } finally {
         for (Connection conn : connections) {
            if (conn != null) {
               conn.close();
            }
         }
      }

   }


}

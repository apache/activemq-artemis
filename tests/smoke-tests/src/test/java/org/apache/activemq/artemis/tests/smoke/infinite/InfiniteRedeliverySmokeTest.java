/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.smoke.infinite;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.io.File;

import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InfiniteRedeliverySmokeTest extends SmokeTestBase {

   private static final Logger logger = Logger.getLogger(InfiniteRedeliverySmokeTest.class);

   public static final String SERVER_NAME_0 = "infinite-redelivery";

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      startServer(SERVER_NAME_0, 0, 30000);
   }

   @Test
   public void testValidateRedeliveries() throws Exception {
      ConnectionFactory factory = new ActiveMQConnectionFactory();
      Connection connection = factory.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

      Queue queue = session.createQueue("testQueue");
      MessageProducer producer = session.createProducer(queue);
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);


      TextMessage message = session.createTextMessage("this is a test");
      for (int i = 0; i < 5000; i++) {
         producer.send(message);
      }
      session.commit();

      connection.start();

      MessageConsumer consumer = session.createConsumer(queue);

      File journalLocation = new File(getServerLocation(SERVER_NAME_0) + "/data/journal");
      SequentialFileFactory fileFactory = new NIOSequentialFileFactory(journalLocation, 1);

      for (int i = 0; i < 500; i++) {
         if (i % 10 == 0) logger.debug("Redelivery " + i);
         for (int j = 0; j < 5000; j++) {
            Assert.assertNotNull(consumer.receive(5000));
         }
         session.rollback();

         int numberOfFiles = fileFactory.listFiles("amq").size();

         // it should be actually 10, However if a future rule changes it to allow removing files I'm ok with that
         Assert.assertTrue("there are not enough files on journal", numberOfFiles >= 2);
         // it should be max 10 actually, I'm just leaving some space for future changes,
         // as the real test I'm after here is the broker should clean itself up
         Wait.assertTrue("there are too many files created", () -> fileFactory.listFiles("amq").size() <= 20);

      }
   }

   @Test
   public void testValidateJournalOnRollbackSend() throws Exception {
      ConnectionFactory factory = new ActiveMQConnectionFactory();
      Connection connection = factory.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

      Queue queue = session.createQueue("testQueue");
      MessageProducer producer = session.createProducer(queue);
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);



      File journalLocation = new File(getServerLocation(SERVER_NAME_0) + "/data/journal");
      SequentialFileFactory fileFactory = new NIOSequentialFileFactory(journalLocation, 1);
      TextMessage message = session.createTextMessage("This is a test");
      producer.send(message); // we will always have one message behind
      connection.start();
      MessageConsumer consumer = session.createConsumer(queue);
      for (int i = 0; i < 500; i++) {
         if (i % 10 == 0) logger.debug("Rollback send " + i);
         for (int j = 0; j < 5000; j++) {
            producer.send(message);
         }
         if (i % 100 == 0) {
            session.commit();
            for (int c = 0; c < 5000; c++) {
               Assert.assertNotNull(consumer.receive(5000));
            }
            session.commit();
            Assert.assertNotNull(consumer.receive(5000)); // there's one message behind
            session.rollback(); // we will keep the one message behind
         } else {
            session.rollback();
         }
         int numberOfFiles = fileFactory.listFiles("amq").size();
         // it should be actually 10, However if a future rule changes it to allow removing files I'm ok with that
         Assert.assertTrue("there are not enough files on journal", numberOfFiles >= 2);
         // it should be max 10 actually, I'm just leaving some space for future changes,
         // as the real test I'm after here is the broker should clean itself up
         Wait.assertTrue(() -> fileFactory.listFiles("amq").size() <= 20);
         Assert.assertTrue("there are too many files created", numberOfFiles <= 20);
      }
   }
}

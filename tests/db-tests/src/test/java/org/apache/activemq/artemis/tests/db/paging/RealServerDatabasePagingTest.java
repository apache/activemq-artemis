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

package org.apache.activemq.artemis.tests.db.paging;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.tests.db.common.Database;
import org.apache.activemq.artemis.tests.db.common.ParameterDBTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.utils.TestParameters.testProperty;

public class RealServerDatabasePagingTest extends ParameterDBTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String TEST_NAME = "PGDB";

   private static final int MAX_MESSAGES = Integer.parseInt(testProperty(TEST_NAME, "MAX_MESSAGES", "200"));

   private static final int SOAK_MAX_MESSAGES = Integer.parseInt(testProperty(TEST_NAME, "SOAK_MAX_MESSAGES", "100000"));

   private static final int MESSAGE_SIZE = Integer.parseInt(testProperty(TEST_NAME, "MESSAGE_SIZE", "1000"));
   private static final int SOAK_MESSAGE_SIZE = Integer.parseInt(testProperty(TEST_NAME, "SOAK_MESSAGE_SIZE", "1000"));

   private static final int COMMIT_INTERVAL = Integer.parseInt(testProperty(TEST_NAME, "COMMIT_INTERVAL", "1000"));

   Process serverProcess;

   @Parameterized.Parameters(name = "db={0}")
   public static Collection<Object[]> parameters() {
      return convertParameters(Database.selectedList());
   }


   @Before
   public void before() throws Exception {
      serverProcess = startServer(database.getName(), 0, 60_000);
   }


   @Test
   public void testPaging() throws Exception {
      testPaging("CORE", MAX_MESSAGES, MESSAGE_SIZE);
      testPaging("AMQP", MAX_MESSAGES, MESSAGE_SIZE);
      testPaging("OPENWIRE", MAX_MESSAGES, MESSAGE_SIZE);
   }


   @Test
   public void testSoakPaging() throws Exception {
      testPaging("AMQP", SOAK_MAX_MESSAGES, SOAK_MESSAGE_SIZE);
   }

   private void testPaging(String protocol, int messages, int messageSize) throws Exception {
      logger.info("performing paging test on protocol={} and db={}", protocol, database);

      final String queueName = "QUEUE_" + RandomUtil.randomString() + "_" + protocol + "_" + database;

      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      byte[] messageLoad = RandomUtil.randomBytes(messageSize);

      try (Connection connection = connectionFactory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue(queueName);
         MessageProducer producer = session.createProducer(queue);
         for (int i = 0; i < messages; i++) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(messageLoad);
            message.setIntProperty("i", i);
            producer.send(message);
            if (i % COMMIT_INTERVAL == 0) {
               logger.info("Sent {} messages", i);
               session.commit();
            }
         }
         session.commit();

      }

      serverProcess.destroyForcibly();
      serverProcess.waitFor(1, TimeUnit.MINUTES);
      Assert.assertFalse(serverProcess.isAlive());

      serverProcess = startServer(database.getName(), 0, 60_000);

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue(queueName);
         MessageConsumer consumer = session.createConsumer(queue);
         for (int i = 0; i < messages; i++) {
            BytesMessage message = (BytesMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertEquals(i, message.getIntProperty("i"));
            Assert.assertEquals(messageSize, message.getBodyLength());
            byte[] bytesOutput = new byte[(int)message.getBodyLength()];
            message.readBytes(bytesOutput);
            Assert.assertArrayEquals(messageLoad, bytesOutput);
            if (i % COMMIT_INTERVAL == 0) {
               logger.info("Received {}", i);
               session.commit();
            }
         }
         session.commit();
         Assert.assertNull(consumer.receiveNoWait());
      }


   }

}

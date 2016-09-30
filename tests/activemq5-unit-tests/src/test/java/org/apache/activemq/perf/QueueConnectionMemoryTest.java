/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.perf;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.leveldb.LevelDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class QueueConnectionMemoryTest extends SimpleQueueTest {

   private static final transient Logger LOG = LoggerFactory.getLogger(QueueConnectionMemoryTest.class);

   @Override
   protected void setUp() throws Exception {
   }

   @Override
   protected void tearDown() throws Exception {

   }

   @Override
   protected Destination createDestination(Session s, String destinationName) throws JMSException {
      return s.createTemporaryQueue();
   }

   @Override
   public void testPerformance() throws JMSException {
      // just cancel super class test
   }

   @Override
   protected void configureBroker(BrokerService answer, String uri) throws Exception {
      LevelDBStore adaptor = new LevelDBStore();
      answer.setPersistenceAdapter(adaptor);
      answer.addConnector(uri);
      answer.setDeleteAllMessagesOnStartup(true);
   }

   public void testMemory() throws Exception {
      if (broker == null) {
         broker = createBroker(bindAddress);
      }
      factory = createConnectionFactory(bindAddress);
      Connection con = factory.createConnection();
      Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
      createDestination(session, destinationName);
      con.close();
      for (int i = 0; i < 3; i++) {
         Connection connection = factory.createConnection();
         connection.start();
         Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination dest = s.createTemporaryQueue();
         s.createConsumer(dest);
         LOG.debug("Created connnection: " + i);
         Thread.sleep(1000);
      }

      Thread.sleep(Integer.MAX_VALUE);
   }
}

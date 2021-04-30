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

package org.apache.activemq.artemis.tests.integration.cli;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import java.io.File;

import org.apache.activemq.artemis.cli.commands.tools.RecoverMessages;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Test;

public class RecoverTest extends JMSTestBase {

   @Override
   protected boolean usePersistence() {
      return true;
   }

   @Test
   public void testRecoverCoreNoTx() throws Exception {
      testRecover(false, "CORE");
   }

   @Test
   public void testRecoverCORETx() throws Exception {
      testRecover(true, "CORE");
   }


   @Test
   public void testRecoverAMQPNoTx() throws Exception {
      testRecover(false, "AMQP");
   }

   @Test
   public void testRecoverAMQPTx() throws Exception {
      testRecover(true, "AMQP");
   }

   @Test
   public void testRecoverOpenWireNoTx() throws Exception {
      testRecover(false, "OPENWIRE");
   }

   @Test
   public void testRecoverOpenWireTx() throws Exception {
      testRecover(true, "OPENWIRE");
   }


   public void testRecover(boolean useTX, String protocol) throws Exception {

      createQueue(true, "TestQueue");
      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      Connection connection = factory.createConnection();
      Session session = connection.createSession(useTX, useTX ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue("TestQueue");
      MessageProducer producer =  session.createProducer(queue);
      for (int i = 0; i < 1000; i++) {
         producer.send(session.createTextMessage("test1"));
      }

      if (useTX) {
         session.commit();
      }

      // Using compacting here, will kind of duplicate all the records into reclaimed files
      // this might cause extra challenges on recovering the data
      server.getStorageManager().getMessageJournal().scheduleCompactAndBlock(60_000);

      MessageConsumer consumer = session.createConsumer(queue);
      connection.start();

      for (int i = 0; i < 1000; i++) {
         Assert.assertNotNull(consumer.receive(5000));
      }

      if (useTX) {
         session.commit();
      }

      connection.close();

      server.stop();

      File newJournalLocation = new File(server.getConfiguration().getJournalLocation().getParentFile(), "recovered");

      RecoverMessages.recover(server.getConfiguration(), newJournalLocation, true);

      server.getConfiguration().setJournalDirectory(newJournalLocation.getAbsolutePath());

      server.start();

      factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      connection = factory.createConnection();
      session = connection.createSession(useTX, useTX ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);

      connection.start();

      consumer = session.createConsumer(queue);

      for (int i = 0; i < 1000; i++) {
         Assert.assertNotNull(consumer.receive(5000));
      }

      Assert.assertNull(consumer.receiveNoWait());

      if (useTX) {
         session.commit();
      }

      connection.close();
   }

}

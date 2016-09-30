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

package org.apache.activemq.artemis.tests.integration.client;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.management.JMSQueueControl;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.SlowConsumerPolicy;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MultipleProducersTest extends JMSTestBase {

   public Connection conn;
   public Queue queueOne = null;
   public Queue queueTwo = null;
   public Session session = null;

   public SimpleString dlq = new SimpleString("jms.queue.DLQ");
   public SimpleString expiryQueue = new SimpleString("jms.queue.ExpiryQueue");

   public SimpleString queueOneName = new SimpleString("jms.queue.queueOne");
   public SimpleString queueTwoName = new SimpleString("jms.queue.queueTwo");
   public JMSQueueControl control = null;
   public long queueOneMsgCount = 0;
   public long queueTwoMsgCount = 0;

   @Before
   public void iniTest() throws Exception {

   }

   @Test
   public void wrongQueue() throws Exception {

      conn = cf.createConnection();

      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();

      AddressSettings addressSettings = new AddressSettings();

      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);
      addressSettings.setExpiryAddress(dlq);
      addressSettings.setDeadLetterAddress(expiryQueue);
      addressSettings.setRedeliveryDelay(0);
      addressSettings.setMessageCounterHistoryDayLimit(2);
      addressSettings.setLastValueQueue(false);
      addressSettings.setMaxDeliveryAttempts(10);
      addressSettings.setMaxSizeBytes(1048576);
      addressSettings.setPageCacheMaxSize(5);
      addressSettings.setPageSizeBytes(2097152);
      addressSettings.setRedistributionDelay(-1);
      addressSettings.setSendToDLAOnNoRoute(false);
      addressSettings.setSlowConsumerCheckPeriod(5);
      addressSettings.setSlowConsumerPolicy(SlowConsumerPolicy.NOTIFY);
      addressSettings.setSlowConsumerThreshold(-1);

      repos.setDefault(addressSettings);

      queueOne = createQueue("queueOne");

      queueTwo = createQueue("queueTwo");

      try {
         while (true) {
            sendMessage(queueOne, session);
         }
      } catch (Throwable t) {
         //         t.printStackTrace();
         // expected
      }

      session.close();

      conn.close();

      session = null;
      conn = null;

      conn = cf.createConnection();
      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // send a message to a queue which is already full
      // result an exception
      try {
         sendMessage(queueOne, session);
         Assert.fail("Exception expected");
      } catch (Exception t) {
      }

      // send 5 message to queueTwo
      // there should be 5 messages on queueTwo
      for (int i = 0; i < 5; i++) {
         sendMessage(queueTwo, session);
      }

      // before sending any messages to queueOne it has to be drained.
      // after draining queueOne send 5 message to queueOne
      queueTwoMsgCount = server.locateQueue(queueTwoName).getMessageCount();

      control = (JMSQueueControl) server.getManagementService().getResource(ResourceNames.JMS_QUEUE + queueOne.getQueueName());

      control.removeMessages(null);

      for (int i = 0; i < 5; i++) {
         sendMessage(queueOne, session);
      }

      // at the end of the test there should be 5 message on queueOne and 5 messages on queueTwo

      session.close();

      conn.close();

      queueOneMsgCount = server.locateQueue(queueOneName).getMessageCount();

      queueTwoMsgCount = server.locateQueue(queueTwoName).getMessageCount();

      Assert.assertEquals("queueTwo message count", 5, queueTwoMsgCount);
      Assert.assertEquals("queueOne message count", 5, queueOneMsgCount);

   }

   private void sendMessage(Queue queue, Session session) throws Exception {

      MessageProducer mp = session.createProducer(queue);

      try {
         mp.setDisableMessageID(true);
         mp.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         mp.setPriority(Message.DEFAULT_PRIORITY);
         mp.setTimeToLive(Message.DEFAULT_TIME_TO_LIVE);

         mp.send(session.createTextMessage("This is message for " + queue.getQueueName()));
      } finally {

         mp.close();
      }
   }
}

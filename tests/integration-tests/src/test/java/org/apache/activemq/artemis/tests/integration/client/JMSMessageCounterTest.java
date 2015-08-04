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
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.management.JMSQueueControl;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Test;

public class JMSMessageCounterTest extends JMSTestBase {

   @Override
   protected boolean usePersistence() {
      return true;
   }

   @Test
   public void testMessageCounter() throws Exception {
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Queue queue = createQueue(true, "Test");

      MessageProducer producer = sess.createProducer(queue);
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         TextMessage mess = sess.createTextMessage("msg" + i);
         producer.send(mess);
      }

      conn.close();

      JMSQueueControl control = (JMSQueueControl) server.getManagementService().getResource(ResourceNames.JMS_QUEUE + queue.getQueueName());
      assertNotNull(control);

      System.out.println(control.listMessageCounterAsHTML());

      jmsServer.stop();

      restartServer();

      control = (JMSQueueControl) server.getManagementService().getResource(ResourceNames.JMS_QUEUE + queue.getQueueName());
      assertNotNull(control);

      System.out.println(control.listMessageCounterAsHTML());
   }

}

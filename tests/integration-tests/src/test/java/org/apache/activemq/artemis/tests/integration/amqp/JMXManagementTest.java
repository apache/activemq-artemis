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
package org.apache.activemq.artemis.tests.integration.amqp;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Map;

public class JMXManagementTest extends JMSClientTestSupport {

   @Test
   public void testListDeliveringMessages() throws Exception {
      SimpleString queue = new SimpleString(getQueueName());

      Connection connection1 = createConnection();
      Connection connection2 = createConnection();
      Session prodSession = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session consSession = connection2.createSession(true, Session.SESSION_TRANSACTED);

      javax.jms.Queue jmsQueue = prodSession.createQueue(queue.toString());

      QueueControl queueControl = createManagementControl(queue, queue);

      MessageProducer producer = prodSession.createProducer(jmsQueue);
      final int num = 20;

      for (int i = 0; i < num; i++) {
         TextMessage message = prodSession.createTextMessage("hello" + i);
         producer.send(message);
      }

      connection2.start();
      MessageConsumer consumer = consSession.createConsumer(jmsQueue);

      for (int i = 0; i < num; i++) {
         TextMessage msgRec = (TextMessage) consumer.receive(5000);
         assertNotNull(msgRec);
         assertEquals(msgRec.getText(), "hello" + i);
      }

      //before commit
      assertEquals(num, queueControl.getDeliveringCount());

      Map<String, Map<String, Object>[]> result = queueControl.listDeliveringMessages();
      assertEquals(1, result.size());

      Map<String, Object>[] msgMaps = result.entrySet().iterator().next().getValue();

      assertEquals(num, msgMaps.length);

      consSession.commit();
      result = queueControl.listDeliveringMessages();

      assertEquals(0, result.size());

      consSession.close();
      prodSession.close();

      connection1.close();
      connection2.close();
   }

   protected QueueControl createManagementControl(final SimpleString address,
                                                  final SimpleString queue) throws Exception {
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, queue, RoutingType.ANYCAST, this.mBeanServer);

      return queueControl;
   }
}

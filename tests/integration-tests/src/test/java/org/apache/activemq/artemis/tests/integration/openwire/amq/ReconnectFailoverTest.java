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
package org.apache.activemq.artemis.tests.integration.openwire.amq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.integration.openwire.BasicOpenWireTest;
import org.junit.Assert;
import org.junit.Test;

public class ReconnectFailoverTest extends BasicOpenWireTest {

   @Test
   public void testReconnectOnFailoverWithClientID() throws Exception {
      ConnectionFactory failoverFactory = new ActiveMQConnectionFactory("failover:(tcp://localhost:61616)");
      Connection connection = failoverFactory.createConnection();
      try {
         connection.setClientID("foo");
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         server.getRemotingService().getConnections().forEach(c -> c.getTransportConnection().forceClose());
         Queue tempQueue = session.createTemporaryQueue();
         MessageProducer producer = session.createProducer(tempQueue);
         for (int i = 0; i < 10; i++) {
            producer.send(session.createTextMessage("hello"));
         }
         connection.start();
         MessageConsumer consumer = session.createConsumer(tempQueue);
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            Assert.assertNotNull(message);
            Assert.assertEquals("hello", message.getText());
         }
      } finally {
         connection.close();
      }
   }

}

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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.openwire.BasicOpenWireTest;
import org.junit.Assert;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import java.util.Map;

public class ProducerAutoCreateQueueTest extends BasicOpenWireTest {

   @Override
   protected void extraServerConfig(Configuration serverConfig) {
      String match = "#";
      Map<String, AddressSettings> asMap = serverConfig.getAddressesSettings();
      asMap.get(match).setAutoCreateAddresses(true).setAutoCreateQueues(true);
   }

   @Test
   public void testProducerBlockWontGetTimeout() throws Exception {
      Connection connection = null;
      try {
         connection = factory.createConnection("admin", "password");
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue trash = session.createQueue("trash");
         final MessageProducer producer = session.createProducer(trash);
         producer.send(session.createTextMessage("foo"));
         Assert.assertNotNull(server.getPostOffice().getBinding(new SimpleString("trash")));
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test
   public void testAutoCreateSendToTopic() throws Exception {
      Connection connection = null;
      try {
         connection = factory.createConnection("admin", "password");
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic trash = session.createTopic("trash");
         final MessageProducer producer = session.createProducer(trash);
         producer.send(session.createTextMessage("foo"));
      } finally {
         if (connection != null) {
            connection.close();
         }
      }

      assertNotNull(server.getAddressInfo(new SimpleString("trash")));
      assertEquals(0, server.getTotalMessageCount());
   }

   @Test
   public void testAutoCreateSendToQueue() throws Exception {
      Connection connection = null;
      try {
         connection = factory.createConnection("admin", "password");
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue trash = session.createQueue("trash");
         final MessageProducer producer = session.createProducer(trash);
         producer.send(session.createTextMessage("foo"));
      } finally {
         if (connection != null) {
            connection.close();
         }
      }

      assertNotNull(server.getAddressInfo(new SimpleString("trash")));
      assertNotNull(server.locateQueue(new SimpleString("trash")));
      assertEquals(1, server.getTotalMessageCount());
   }

   @Test
   public void testAutoDelete() throws Exception {
      Connection connection = null;
      try {
         connection = factory.createConnection("admin", "password");
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue trash = session.createQueue("trash");
         final MessageProducer producer = session.createProducer(trash);
         producer.send(session.createTextMessage("foo"));
         Assert.assertNotNull(server.locateQueue(new SimpleString("trash")));
         MessageConsumer consumer = session.createConsumer(trash);
         connection.start();
         assertNotNull(consumer.receive(1000));
      } finally {
         if (connection != null) {
            connection.close();
         }
      }

      assertNull(server.locateQueue(new SimpleString("trash")));
   }

   @Test
   public void testAutoDeleteNegative() throws Exception {
      server.getAddressSettingsRepository().addMatch("trash", new AddressSettings().setAutoDeleteQueues(false).setAutoDeleteAddresses(false));
      Connection connection = null;
      try {
         connection = factory.createConnection("admin", "password");
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue trash = session.createQueue("trash");
         final MessageProducer producer = session.createProducer(trash);
         producer.send(session.createTextMessage("foo"));
         Assert.assertNotNull(server.locateQueue(new SimpleString("trash")));
         MessageConsumer consumer = session.createConsumer(trash);
         connection.start();
         assertNotNull(consumer.receive(1000));
      } finally {
         if (connection != null) {
            connection.close();
         }
      }

      assertNotNull(server.locateQueue(new SimpleString("trash")));
      assertNotNull(server.getAddressInfo(new SimpleString("trash")));
   }
}

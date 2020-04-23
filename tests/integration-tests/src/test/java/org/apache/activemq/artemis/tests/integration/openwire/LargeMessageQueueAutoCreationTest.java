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

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

//adapted from https://issues.apache.org/jira/browse/ARTEMIS-1416
@RunWith(Parameterized.class)
public class LargeMessageQueueAutoCreationTest extends BasicOpenWireTest {

   Queue queue1;
   Random random = new Random();
   ActiveMQConnection testConn;
   ClientSession clientSession;

   @Parameterized.Parameter
   public boolean usingCore;

   @Parameterized.Parameters(name = "isCore={0}")
   public static Collection<Object[]> params() {
      return Arrays.asList(new Object[][]{{true}, {false}});
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      String randomSuffix = new BigInteger(130, random).toString(32);
      testConn = (ActiveMQConnection)coreCf.createConnection();
      clientSession = testConn.getSessionFactory().createSession();
      queue1 = createCoreQueue("queue1_" + randomSuffix);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      testConn.close();
      super.tearDown();
   }

   @Override
   protected void extraServerConfig(Configuration serverConfig) {
      serverConfig.setJournalType(JournalType.NIO);
      Map<String, AddressSettings> map = serverConfig.getAddressesSettings();
      Map.Entry<String, AddressSettings> entry = map.entrySet().iterator().next();
      AddressSettings settings = entry.getValue();
      settings.setAutoCreateQueues(true);
   }


   protected Queue createCoreQueue(final String queueName) throws Exception {
      SimpleString address = SimpleString.toSimpleString(queueName);
      clientSession.createAddress(address, RoutingType.ANYCAST, false);
      return new ActiveMQQueue(queueName);
   }

   @Test(timeout = 30000)
   public void testSmallString() throws Exception {
      sendStringOfSize(1024);
   }

   @Test(timeout = 30000)
   public void testHugeString() throws Exception {
      sendStringOfSize(1024 * 1024);
   }

   private void sendStringOfSize(int msgSize) throws JMSException {

      ConnectionFactory factoryToUse = usingCore ? coreCf : factory;

      Connection conn = factoryToUse.createConnection();

      try {
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(queue1);

         TextMessage m = session.createTextMessage();

         m.setJMSDeliveryMode(DeliveryMode.PERSISTENT);

         StringBuffer buffer = new StringBuffer();
         while (buffer.length() < msgSize) {
            buffer.append(UUIDGenerator.getInstance().generateStringUUID());
         }

         final String originalString = buffer.toString();

         m.setText(originalString);

         prod.send(m);

         conn.close();

         conn = factoryToUse.createConnection();

         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(queue1);

         conn.start();

         TextMessage rm = (TextMessage) cons.receive(5000);
         Assert.assertNotNull(rm);

         String str = rm.getText();
         Assert.assertEquals(originalString, str);
      } finally {
         if (conn != null) {
            conn.close();
         }
      }
   }
}
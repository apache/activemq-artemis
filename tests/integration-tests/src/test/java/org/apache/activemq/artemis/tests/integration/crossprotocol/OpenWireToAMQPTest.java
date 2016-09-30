/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.crossprotocol;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.ArrayList;


public class OpenWireToAMQPTest extends ActiveMQTestBase {

   public static final String OWHOST = "localhost";
   public static final int OWPORT = 61616;
   protected static final String urlString = "tcp://" + OWHOST + ":" + OWPORT + "?wireFormat.cacheEnabled=true";

   private ActiveMQServer server;
   protected ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(urlString);
   protected ActiveMQXAConnectionFactory xaFactory = new ActiveMQXAConnectionFactory(urlString);
   private JmsConnectionFactory qpidfactory;
   protected String queueName = "amqTestQueue1";
   private SimpleString coreQueue;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(true, true);
      Configuration serverConfig = server.getConfiguration();
      serverConfig.getAddressesSettings().put("jms.queue.#", new AddressSettings().setAutoCreateJmsQueues(false).setDeadLetterAddress(new SimpleString("jms.queue.ActiveMQ.DLQ")));
      serverConfig.setSecurityEnabled(false);
      server.start();
      coreQueue = new SimpleString("jms.queue." + queueName);
      this.server.createQueue(coreQueue, coreQueue, null, false, false);
      qpidfactory = new JmsConnectionFactory("amqp://localhost:61616");
   }

   @Override
   @After
   public void tearDown() throws Exception {
      if (server != null) {
         server.stop();
         server = null;
      }
   }

   @Test
   public void testObjectMessage() throws Exception {
      Connection connection = null;
      try {
         connection = factory.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(queueName);
         MessageProducer producer = session.createProducer(queue);
         ArrayList list = new ArrayList();
         list.add("aString");
         ObjectMessage objectMessage = session.createObjectMessage(list);
         producer.send(objectMessage);
         connection.close();

         connection = qpidfactory.createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         queue = session.createQueue(coreQueue.toString());
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();
         ObjectMessage receive = (ObjectMessage) consumer.receive(5000);
         list = (ArrayList) receive.getObject();
         assertEquals(list.get(0), "aString");
         connection.close();
      }
      catch (Exception e) {
         e.printStackTrace();
      }
      finally {
         if (connection != null) {
            connection.close();
         }
      }
   }
}

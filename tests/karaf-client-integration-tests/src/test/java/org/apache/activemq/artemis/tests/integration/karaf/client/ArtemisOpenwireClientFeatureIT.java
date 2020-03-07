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
package org.apache.activemq.artemis.tests.integration.karaf.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

import static org.apache.activemq.artemis.tests.integration.karaf.client.PaxExamOptions.ARTEMIS_OPENWIRE_CLIENT;
import static org.apache.activemq.artemis.tests.integration.karaf.client.PaxExamOptions.ARTEMIS_TRANSACTION_MANAGER;
import static org.apache.activemq.artemis.tests.integration.karaf.client.PaxExamOptions.KARAF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.CoreOptions.when;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.debugConfiguration;


/**
 * Useful docs about this test: https://ops4j1.jira.com/wiki/display/paxexam/FAQ
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class ArtemisOpenwireClientFeatureIT {

   @Configuration
   public Option[] config() throws IOException {
      return options(
              KARAF.option(),
              ARTEMIS_OPENWIRE_CLIENT.option(),
              ARTEMIS_TRANSACTION_MANAGER.option(),
              when(false)
                      .useOptions(
                              debugConfiguration("5005", true))
      );
   }

   @Test
   public void testArtemisOpenwireClient() throws Exception {
      ConnectionFactory cf = new ActiveMQConnectionFactory();
      Connection connection = cf.createConnection();
      try {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue("artemisOpenwireClientFeatureIT");
         MessageProducer producer = session.createProducer(queue);

         String msg = "This is a text message";
         TextMessage message = session.createTextMessage(msg);

         producer.send(message);

         MessageConsumer messageConsumer = session.createConsumer(queue);

         connection.start();

         TextMessage messageReceived = (TextMessage) messageConsumer.receive(100);
         assertNull(messageReceived);
         session.commit();
         messageReceived = (TextMessage) messageConsumer.receive(100);
         assertEquals(msg, messageReceived.getText());
      } finally {
         connection.close();
      }
   }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.openwire;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Map;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.Assert;
import org.junit.Test;

public class OpenWireFlowControlFailTest extends OpenWireTestBase {

   public static final String OWHOST = "localhost";
   public static final int OWPORT = 61616;

   protected static final String urlString = "tcp://" + OWHOST + ":" + OWPORT + "?wireFormat.cacheEnabled=true";

   @Override
   protected void configureAddressSettings(Map<String, AddressSettings> addressSettingsMap) {
      addressSettingsMap.put("#", new AddressSettings().setAutoCreateQueues(false).setAutoCreateAddresses(false).
         setDeadLetterAddress(new SimpleString("ActiveMQ.DLQ")).setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL).setMaxSizeBytes(10000));
   }

   @Test(timeout = 60000)
   public void testMesagesNotSent() throws Exception {

      AddressInfo addressInfo = new AddressInfo(SimpleString.toSimpleString("Test"), RoutingType.ANYCAST);
      server.addAddressInfo(addressInfo);
      server.createQueue(addressInfo.getName(), RoutingType.ANYCAST, addressInfo.getName(), null, true, false);

      StringBuffer textBody = new StringBuffer();
      for (int i = 0; i < 10; i++) {
         textBody.append(" ");
      }
      ConnectionFactory factory = new ActiveMQConnectionFactory(urlString);
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(addressInfo.getName().toString());
         MessageProducer producer = session.createProducer(queue);
         int numberOfMessage = 0;
         boolean failed = false;
         try {
            for (int i = 0; i < 1000; i++) {
               producer.send(session.createTextMessage(textBody.toString()));
               numberOfMessage++;
            }
         } catch (Exception e) {
            e.printStackTrace(System.out);
            failed = true;
         }

         System.out.println("Message failed with " + numberOfMessage);

         Assert.assertTrue(failed);
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();
         for (int i = 0; i < numberOfMessage; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertEquals(textBody.toString(), message.getText());
         }

         Assert.assertNull(consumer.receiveNoWait());
      }
   }
}

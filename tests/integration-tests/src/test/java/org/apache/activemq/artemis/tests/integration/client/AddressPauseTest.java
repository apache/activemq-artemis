/*
 * Copyright 2019 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.client;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Test;

public class AddressPauseTest extends JMSTestBase {

   @Test
   public void testPauseAddress() throws Exception {
      try (Connection connection = cf.createConnection()) {
         connection.setClientID("myClientID");
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Topic topic = session.createTopic("jms.topic.MyTopic");
            TopicSubscriber subscriber1 = session.createDurableSubscriber(topic, "my-subscription1");
            AddressControl addressControl = (AddressControl) server.getManagementService().getResource(ResourceNames.ADDRESS + "jms.topic.MyTopic");
            MessageProducer producer = session.createProducer(topic);
            final int numMessages = 100;
            for (int i = 0; i < numMessages; i++) {
               TextMessage mess = session.createTextMessage("msg" + i);
               producer.send(mess);
            }
            for (int i = 0; i < numMessages; i++) {
               TextMessage m = (TextMessage) subscriber1.receive(5000);
               Assert.assertNotNull(m);
            }
            //Pausing the subscriptions
            addressControl.pause();
            Assert.assertTrue(addressControl.isPaused());
            //subscriber2 should be paused too
            TopicSubscriber subscriber2 = session.createDurableSubscriber(topic, "my-subscription2");
            for (int i = 0; i < numMessages; i++) {
               TextMessage mess = session.createTextMessage("msg" + i);
               producer.send(mess);
            }
            TextMessage message = (TextMessage) subscriber1.receive(5000);
            Assert.assertNull(message);
            message = (TextMessage) subscriber2.receive(5000);
            Assert.assertNull(message);
            //Resuming the subscriptions
            addressControl.resume();
            for (int i = 0; i < numMessages; i++) {
               TextMessage m = (TextMessage) subscriber1.receive(5000);
               Assert.assertNotNull(m);
            }
            for (int i = 0; i < numMessages; i++) {
               TextMessage m = (TextMessage) subscriber2.receive(5000);
               Assert.assertNotNull(m);
            }
         }
      }
   }
}

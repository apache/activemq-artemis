package journalcompatibility

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory
import org.apache.activemq.artemis.tests.compatibility.GroovyRun
import org.apache.activemq.artemis.api.core.management.AddressControl
import org.apache.activemq.artemis.api.core.management.ResourceNames
import org.apache.activemq.artemis.core.server.impl.AddressInfo

import javax.jms.*

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
ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");
try {
    cf.setEnable1xPrefixes(true);
} catch (Throwable totallyIgnored) {
    // older versions will not have this method, dont even bother about seeing the stack trace or exception
}
Connection connection = cf.createConnection();
Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
Topic topic = session.createTopic("jms.topic.MyTopic");
MessageProducer producer = session.createProducer(topic);
MessageConsumer messageConsumer1 = session.createConsumer(topic);
MessageConsumer messageConsumer2 = session.createConsumer(topic);
TextMessage message = session.createTextMessage("This is a text message");
System.out.println("Sent message: " + message.getText());
producer.send(message);
connection.start();
TextMessage messageReceived = (TextMessage) messageConsumer1.receive(5000);
GroovyRun.assertNotNull(messageReceived);
System.out.println("Consumer 1 Received message: " + messageReceived.getText());
messageReceived = (TextMessage) messageConsumer2.receive(5000);
GroovyRun.assertNotNull(messageReceived);
System.out.println("Consumer 2 Received message: " + messageReceived.getText());
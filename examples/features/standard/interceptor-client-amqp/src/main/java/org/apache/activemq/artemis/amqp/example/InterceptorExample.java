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
package org.apache.activemq.artemis.amqp.example;

import org.apache.qpid.jms.JmsConnectionFactory;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * A simple example that shows how to implement and use interceptors with ActiveMQ Artemis with the AMQP protocol.
 */
public class InterceptorExample {
   public static void main(final String[] args) throws Exception {
      JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:61616");

      try (Connection connection = factory.createConnection()) {

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue interceptorQueue = session.createQueue("interceptorQueue");

         MessageProducer producer = session.createProducer(interceptorQueue);

         TextMessage textMessage = session.createTextMessage("A text message");
         textMessage.setStringProperty("SimpleAmqpInterceptor", "SimpleAmqpInterceptorValue");
         producer.send(textMessage);
      }
   }
}

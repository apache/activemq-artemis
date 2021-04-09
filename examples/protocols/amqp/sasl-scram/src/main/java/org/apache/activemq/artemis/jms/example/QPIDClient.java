/*
 * <p>
 * All rights reserved. Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.JmsConnectionFactory;

public class QPIDClient {
   public static void main(String[] args) throws JMSException {
      sendReceive("SCRAM-SHA-256", "test", "test");
   }

   private static void sendReceive(String method, String username, String password) throws JMSException {
      ConnectionFactory connectionFactory =
               new JmsConnectionFactory("amqp://localhost:5672?amqp.saslMechanisms=" + method);
      try (Connection connection = connectionFactory.createConnection(username, password)) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue("exampleQueue");
         MessageProducer sender = session.createProducer(queue);
         sender.send(session.createTextMessage("Hello " + method));
         connection.start();
         MessageConsumer consumer = session.createConsumer(queue);
         TextMessage m = (TextMessage) consumer.receive(5000);
         System.out.println("message = " + m.getText());
      }
   }
}

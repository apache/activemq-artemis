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
package org.apache.activemq.artemis.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

/**
 * An example where browser-based clients can send JMS messages to a Topic.
 * The clients will be able to exchange messages with each other.
 */
public class StompWebSocketExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      InitialContext initialContext = null;
      try {
         initialContext = new InitialContext();
         Topic topic = (Topic) initialContext.lookup("topic/chat");
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(topic);

         System.out.println("Open up the chat/index.html file in a browser...press enter when finished");
         System.in.read();
         TextMessage message = session.createTextMessage("Server stopping!");
         producer.send(message);
      } finally {
         if (connection != null) {
            connection.close();
         }

         if (initialContext != null) {
            initialContext.close();
         }
      }
   }
}

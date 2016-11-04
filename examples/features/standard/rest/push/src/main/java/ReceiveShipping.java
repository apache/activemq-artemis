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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.activemq.artemis.rest.Jms;

public class ReceiveShipping {

   public static void main(String[] args) throws Exception {
      ConnectionFactory factory = new ActiveMQJMSConnectionFactory("tcp://localhost:61616");
      Destination destination = ActiveMQDestination.fromPrefixedName("queue://shipping");

      try (Connection conn = factory.createConnection()) {
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(destination);
         consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
               System.out.println("Received Message: ");
               Order order = Jms.getEntity(message, Order.class);
               System.out.println(order);
            }
         });
         conn.start();
         Thread.sleep(1000000);
      }
   }
}

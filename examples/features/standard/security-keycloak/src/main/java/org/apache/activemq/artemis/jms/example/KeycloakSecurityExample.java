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
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Queue;
import javax.naming.InitialContext;

import java.util.concurrent.TimeUnit;

public class KeycloakSecurityExample {

   public static void main(final String[] args) throws Exception {

      boolean result = true;
      Connection connection = null;

      InitialContext initialContext = null;

      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. perform lookup on the topics
         Queue genericTopic = (Queue) initialContext.lookup("queue/Info");

         // Step 3. perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");


         // Step 4. block till we make a connection as mdoe
         System.out.println("------------------------blocking on connection creation----------------");

         while (connection == null) {
            try {
               connection = createConnection("mdoe", "password", cf);
               connection.start();
            } catch (JMSException expectedTillInfraStarted) {
               System.out.println("---- expected error on connect till broker starts: " + expectedTillInfraStarted + ", retry in 10s");
               TimeUnit.SECONDS.sleep(10);
            }
         }

         // Step 5. block till we get a message
         System.out.println("------------------------blocking on message receipt from console----------------");
         System.out.println("------------------------send to address Info as user: jdoe password: password----------------");

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(genericTopic);

         Message receivedMsg = null;
         while (receivedMsg == null) {
            receivedMsg = consumer.receive(10000);
            if (receivedMsg != null) {
               String validatedUser = receivedMsg.getStringProperty(org.apache.activemq.artemis.api.core.Message.HDR_VALIDATED_USER.toString());
               System.out.println("---------------------from: " + validatedUser + " -------------------------");
               System.out.println("---------------------reveived: " + receivedMsg);
               System.out.println("---------------------all done!------------------------------------------");
            }
         }

         session.close();

         System.out.println("-------------------------------------------------------------------------------------");

      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   private static Connection createConnection(final String username,
                                              final String password,
                                              final ConnectionFactory cf) throws JMSException {
      return cf.createConnection(username, password);
   }
}

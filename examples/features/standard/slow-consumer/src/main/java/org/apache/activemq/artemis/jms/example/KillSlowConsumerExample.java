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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;

/**
 * A simple JMS example that demonstrates the KILL slow consumer policy:
 *
 * - sends messages to a queue "slow.consumer.kill".
 * - starts a consumer BUT does not consume any messages.
 * - waits for 10 seconds and tries to consume a message.
 * - receive an exception as the connection should already be closed.
 */
public class KillSlowConsumerExample {

   public static final int WAIT_TIME = 10;

   public static void main(final String[] args) throws Exception {

      // Step 1. Create an initial context to perform the JNDI lookup.
      InitialContext initialContext = new InitialContext();

      // Step 2. Perform a lookup on the queue
      Queue slowConsumerKillQueue = (Queue) initialContext.lookup("queue/slow.consumer.kill");

      // Step 3. Perform a lookup on the Connection Factory
      ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

      // Step 4.Create a JMS Connection
      try (Connection connection = connectionFactory.createConnection()) {

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(slowConsumerKillQueue);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         System.out.println("Sending messages to queue ... ");

         // Step 8. Send messages to the queue
         for (int i = 0; i < 50; i++) {
            producer.send(message);
         }

         // Step 9. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(slowConsumerKillQueue);

         // Step 10. Start the Connection
         connection.start();

         System.out.println("About to wait for " + WAIT_TIME + " seconds");

         // Step 11. Wait for slow consumer to be detected
         Thread.sleep(TimeUnit.SECONDS.toMillis(WAIT_TIME));

         try {
            //step 12. Try to us the connection - expect it to be closed already
            messageConsumer.receive(TimeUnit.SECONDS.toMillis(1));

            //messageConsumer.receive() should throw exception - we should not get to here.
            throw new RuntimeException("SlowConsumerExample.slowConsumerKill() FAILED - expected " + "connection to be shutdown by Slow Consumer policy");

         } catch (JMSException ex) {
            if (ex.getCause() instanceof ActiveMQObjectClosedException) {
               //received exception - as expected
               System.out.println("SUCCESS! Received EXPECTED exception: " + ex);
            } else {
               throw new RuntimeException("SlowConsumerExample.slowConsumerKill() FAILED - expected " + "ActiveMQObjectClosedException BUT got " + ex.getCause());
            }
         }
      }
   }
}




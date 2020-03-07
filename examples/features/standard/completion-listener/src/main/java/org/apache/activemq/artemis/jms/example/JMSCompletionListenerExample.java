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

import javax.jms.CompletionListener;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * A JMS Completion Listener Example.
 */
public class JMSCompletionListenerExample {

   public static void main(final String[] args) throws Exception {
      JMSContext jmsContext = null;
      try {
         // Step 2. Perfom a lookup on the queue
         Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616?confirmationWindowSize=10240");

         // Step 4.Create a JMS Context
         jmsContext = cf.createContext();

         // Step 5. Create a message producer.
         JMSProducer producer = jmsContext.createProducer();

         final CountDownLatch latch = new CountDownLatch(1);

         //Step 6. We want to send the message Asynchronously and be notified when the Broker receives it so we set a completion handler
         producer.setAsync(new CompletionListener() {
            @Override
            public void onCompletion(Message message) {
               System.out.println("message acknowledged by ActiveMQ");
               latch.countDown();
            }

            @Override
            public void onException(Message message, Exception e) {
               e.printStackTrace();
            }
         });

         //Step 6. Send the Message
         producer.send(queue, "this is a string");

         //Step 7. wait for the Completion handler
         if (!latch.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Completion listener not called as expected.");
         }
      } finally {
         if (jmsContext != null) {
            jmsContext.close();
         }
      }
   }
}

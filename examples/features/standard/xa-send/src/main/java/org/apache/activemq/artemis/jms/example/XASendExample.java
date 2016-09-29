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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.InitialContext;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.utils.UUIDGenerator;

/**
 * A simple JMS example showing the usage of XA support in JMS.
 */
public class XASendExample {

   public static void main(final String[] args) throws Exception {
      AtomicBoolean result = new AtomicBoolean(true);
      final ArrayList<String> receiveHolder = new ArrayList<>();
      XAConnection connection = null;
      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Lookup on the queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. Perform a lookup on the XA Connection Factory
         XAConnectionFactory cf = (XAConnectionFactory) initialContext.lookup("XAConnectionFactory");

         // Step 4.Create a JMS XAConnection
         connection = cf.createXAConnection();

         // Step 5. Start the connection
         connection.start();

         // Step 6. Create a JMS XASession
         XASession xaSession = connection.createXASession();

         // Step 7. Create a normal session
         Session normalSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 8. Create a normal Message Consumer
         MessageConsumer normalConsumer = normalSession.createConsumer(queue);
         normalConsumer.setMessageListener(new SimpleMessageListener(receiveHolder, result));

         // Step 9. Get the JMS Session
         Session session = xaSession.getSession();

         // Step 10. Create a message producer
         MessageProducer producer = session.createProducer(queue);

         // Step 11. Create two Text Messages
         TextMessage helloMessage = session.createTextMessage("hello");
         TextMessage worldMessage = session.createTextMessage("world");

         // Step 12. create a transaction
         Xid xid1 = new DummyXid("xa-example1".getBytes(StandardCharsets.UTF_8), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

         // Step 13. Get the JMS XAResource
         XAResource xaRes = xaSession.getXAResource();

         // Step 14. Begin the Transaction work
         xaRes.start(xid1, XAResource.TMNOFLAGS);

         // Step 15. do work, sending two messages.
         producer.send(helloMessage);
         producer.send(worldMessage);

         Thread.sleep(2000);

         // Step 16. Check the result, it should receive none!
         checkNoMessageReceived(receiveHolder);

         // Step 17. Stop the work
         xaRes.end(xid1, XAResource.TMSUCCESS);

         // Step 18. Prepare
         xaRes.prepare(xid1);

         // Step 19. Roll back the transaction
         xaRes.rollback(xid1);

         // Step 20. No messages should be received!
         checkNoMessageReceived(receiveHolder);

         // Step 21. Create another transaction
         Xid xid2 = new DummyXid("xa-example2".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

         // Step 22. Start the transaction
         xaRes.start(xid2, XAResource.TMNOFLAGS);

         // Step 23. Re-send those messages
         producer.send(helloMessage);
         producer.send(worldMessage);

         // Step 24. Stop the work
         xaRes.end(xid2, XAResource.TMSUCCESS);

         // Step 25. Prepare
         xaRes.prepare(xid2);

         // Step 26. No messages should be received at this moment
         checkNoMessageReceived(receiveHolder);

         // Step 27. Commit!
         xaRes.commit(xid2, false);

         Thread.sleep(2000);

         // Step 28. Check the result, all message received
         checkAllMessageReceived(receiveHolder);

         if (!result.get())
            throw new IllegalStateException();
      } finally {
         // Step 29. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }

   private static void checkAllMessageReceived(ArrayList<String> receiveHolder) {
      if (receiveHolder.size() != 2) {
         throw new IllegalStateException("Number of messages received not correct ! -- " + receiveHolder.size());
      }
      receiveHolder.clear();
   }

   private static void checkNoMessageReceived(ArrayList<String> receiveHolder) {
      if (receiveHolder.size() > 0) {
         throw new IllegalStateException("Message received, wrong!");
      }
      receiveHolder.clear();
   }
}

class SimpleMessageListener implements MessageListener {

   ArrayList<String> receiveHolder;
   AtomicBoolean result;

   SimpleMessageListener(ArrayList<String> receiveHolder, AtomicBoolean result) {
      this.receiveHolder = receiveHolder;
      this.result = result;
   }

   @Override
   public void onMessage(final Message message) {
      try {
         System.out.println("Message received: " + message);
         receiveHolder.add(((TextMessage) message).getText());
      } catch (JMSException e) {
         result.set(false);
         e.printStackTrace();
      }
   }
}

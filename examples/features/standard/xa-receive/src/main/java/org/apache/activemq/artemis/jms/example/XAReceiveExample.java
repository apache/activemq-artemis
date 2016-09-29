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

import javax.jms.MessageConsumer;
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

import org.apache.activemq.artemis.utils.UUIDGenerator;

/**
 * A simple JMS example showing the usage of XA support in JMS.
 */
public class XAReceiveExample {

   public static void main(final String[] args) throws Exception {
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

         // Step 8. Create a normal Message Producer
         MessageProducer normalProducer = normalSession.createProducer(queue);

         // Step 9. Get the JMS Session
         Session session = xaSession.getSession();

         // Step 10. Create a message consumer
         MessageConsumer xaConsumer = session.createConsumer(queue);

         // Step 11. Create two Text Messages
         TextMessage helloMessage = session.createTextMessage("hello");
         TextMessage worldMessage = session.createTextMessage("world");

         // Step 12. create a transaction
         Xid xid1 = new DummyXid("xa-example1".getBytes(StandardCharsets.US_ASCII), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

         // Step 13. Get the JMS XAResource
         XAResource xaRes = xaSession.getXAResource();

         // Step 14. Begin the Transaction work
         xaRes.start(xid1, XAResource.TMNOFLAGS);

         // Step 15. Send two messages.
         normalProducer.send(helloMessage);
         normalProducer.send(worldMessage);

         // Step 16. Receive the message
         TextMessage rm1 = (TextMessage) xaConsumer.receive();
         System.out.println("Message received: " + rm1.getText());
         TextMessage rm2 = (TextMessage) xaConsumer.receive();
         System.out.println("Message received: " + rm2.getText());

         // Step 17. Stop the work
         xaRes.end(xid1, XAResource.TMSUCCESS);

         // Step 18. Prepare
         xaRes.prepare(xid1);

         // Step 19. Roll back the transaction
         xaRes.rollback(xid1);

         // Step 20. Create another transaction
         Xid xid2 = new DummyXid("xa-example2".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

         // Step 21. Start the transaction
         xaRes.start(xid2, XAResource.TMNOFLAGS);

         // Step 22. receive those messages again
         rm1 = (TextMessage) xaConsumer.receive();
         System.out.println("Message received again: " + rm1.getText());
         rm2 = (TextMessage) xaConsumer.receive();
         System.out.println("Message received again: " + rm2.getText());

         // Step 23. Stop the work
         xaRes.end(xid2, XAResource.TMSUCCESS);

         // Step 24. Prepare
         xaRes.prepare(xid2);

         // Step 25. Commit!
         xaRes.commit(xid2, false);

         // Step 26. Check no more messages are received.
         TextMessage rm3 = (TextMessage) xaConsumer.receive(2000);
         if (rm3 == null) {
            System.out.println("No message received after commit.");
         } else {
            throw new IllegalStateException();
         }
      } finally {
         // Step 27. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}

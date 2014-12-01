/**
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
package org.apache.activemq.jms.example;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.InitialContext;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.arjuna.ats.jta.TransactionManager;

import org.apache.activemq.common.example.ActiveMQExample;

/**
 * A simple JMS example showing the ActiveMQ XA support with JTA.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class XAwithJTAExample extends ActiveMQExample
{
   private volatile boolean result = true;

   public static void main(final String[] args)
   {
      new XAwithJTAExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      XAConnection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the XA Connection Factory
         XAConnectionFactory cf = (XAConnectionFactory)initialContext.lookup("/XAConnectionFactory");

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

         // Step 12. Get the Transaction Manager
         javax.transaction.TransactionManager txMgr = TransactionManager.transactionManager();

         // Step 13. Start a transaction
         txMgr.begin();

         // Step 14. Get the JMS XAResource
         XAResource xaRes = xaSession.getXAResource();

         // Step 15. enlist the resource in the Transaction work
         Transaction transaction = txMgr.getTransaction();
         transaction.enlistResource(new DummyXAResource());
         transaction.enlistResource(xaRes);

         // Step 16. Send two messages.
         normalProducer.send(helloMessage);
         normalProducer.send(worldMessage);

         // Step 17. Receive the message
         TextMessage rm1 = (TextMessage)xaConsumer.receive();
         System.out.println("Message received: " + rm1.getText());
         TextMessage rm2 = (TextMessage)xaConsumer.receive();
         System.out.println("Message received: " + rm2.getText());

         // Step 18. Roll back the transaction
         txMgr.rollback();

         // Step 19. Create another transaction
         txMgr.begin();
         transaction = txMgr.getTransaction();

         // Step 20. Enlist the resources to start the transaction work
         transaction.enlistResource(new DummyXAResource());
         transaction.enlistResource(xaRes);

         // Step 21. receive those messages again
         rm1 = (TextMessage)xaConsumer.receive();
         System.out.println("Message received again: " + rm1.getText());
         rm2 = (TextMessage)xaConsumer.receive();
         System.out.println("Message received again: " + rm2.getText());

         // Step 22. Commit!
         txMgr.commit();

         // Step 23. Check no more messages are received.
         TextMessage rm3 = (TextMessage)xaConsumer.receive(2000);
         if (rm3 == null)
         {
            System.out.println("No message received after commit.");
         }
         else
         {
            result = false;
         }

         return result;
      }
      finally
      {
         // Step 24. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if (connection != null)
         {
            connection.close();
         }
      }
   }

   public static class DummyXAResource implements XAResource
   {

      public DummyXAResource()
      {
      }

      public void commit(final Xid xid, final boolean arg1) throws XAException
      {
         System.out.println("DummyXAResource commit() called, xid: " + xid);
      }

      public void end(final Xid xid, final int arg1) throws XAException
      {
         System.out.println("DummyXAResource end() called, xid: " + xid);
      }

      public void forget(final Xid xid) throws XAException
      {
         System.out.println("DummyXAResource forget() called, xid: " + xid);
      }

      public int getTransactionTimeout() throws XAException
      {
         return 0;
      }

      public boolean isSameRM(final XAResource arg0) throws XAException
      {
         return this == arg0;
      }

      public int prepare(final Xid xid) throws XAException
      {
         return XAResource.XA_OK;
      }

      public Xid[] recover(final int arg0) throws XAException
      {
         return null;
      }

      public void rollback(final Xid xid) throws XAException
      {
         System.out.println("DummyXAResource rollback() called, xid: " + xid);
      }

      public boolean setTransactionTimeout(final int arg0) throws XAException
      {
         return false;
      }

      public void start(final Xid xid, final int arg1) throws XAException
      {
         System.out.println("DummyXAResource start() called, Xid: " + xid);
      }

   }

}

/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.javaee.example.server;

import javax.ejb.Remote;
import javax.ejb.Stateless;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.InitialContext;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * An EJB which sends a JMS message in the transaction and "pauses" while the transaction
 * is prepared so that the server can be crashed in this state.
 * The JMS message will be recovered when the server is restarted.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
@Stateless
@Remote(XARecoveryExampleService.class)
@TransactionManagement(TransactionManagementType.BEAN)
public class XARecoveryExampleBean implements XARecoveryExampleService
{
   public void send(final String text) throws Exception
   {
      InitialContext ic = null;
      XAConnection xaConnection = null;
      try
      {
         // Step 1. Create the initial context
         ic = new InitialContext();

         // Step 2. Lookup the Transaction Manager
         TransactionManager tm = (TransactionManager)ic.lookup("java:/TransactionManager");

         // Step 3. Look up the XA Connection Factory
         XAConnectionFactory xacf = (XAConnectionFactory)ic.lookup("java:/JmsXA");

         // Step 4. Look up the Queue
         Queue queue = (Queue)ic.lookup("queue/testQueue");

         // Step 5. Create a XA connection, a XA session and a message producer for the queue
         xaConnection = xacf.createXAConnection();
         XASession session = xaConnection.createXASession();
         MessageProducer messageProducer = session.createProducer(queue);

         // Step 6. Create a "fake" XAResource which will crash the server in its commit phase
         XAResource failingXAResource = new FailingXAResource();

         // Step 7. Begin the transaction
         tm.begin();
         Transaction tx = tm.getTransaction();

         // Step 8. Enlist the failing XAResource
         tx.enlistResource(failingXAResource);

         // Step 9. Enlist the JMS XAResource
         tx.enlistResource(session.getXAResource());

         // Step 10. Send The Text Message
         TextMessage message = session.createTextMessage(text);
         messageProducer.send(message);
         System.out.format("Sent message: %s\n\t(JMS MessageID: %s)\n", message.getText(), message.getJMSMessageID());

         // Step 12. Delist the failing XAResource
         tx.delistResource(failingXAResource, XAResource.TMSUCCESS);

         // Step 13. Delist the JMS XAResource
         tx.delistResource(session.getXAResource(), XAResource.TMSUCCESS);

         // Step 14. Commit the transaction
         // Both XA Resources will be prepared.
         // then the failingXAResource will crash the server in its commit phase
         // and the commit method will never be called on the JMS XA Resource: it will
         // be in the prepared state when the server crashes
         System.out.println("committing the tx");
         tx.commit();
      }
      finally
      {
         // Step 15. Be sure to close all resources!
         if (ic != null)
         {
            ic.close();
         }
         if (xaConnection != null)
         {
            xaConnection.close();
         }
      }
   }

   /**
    * A XAResource which crashes the server in its commit phase
    */
   private static class FailingXAResource implements XAResource
   {

      public void commit(final Xid arg0, final boolean arg1) throws XAException
      {
         System.out.println("########################");
         System.out.println("# Crashing the server! #");
         System.out.println("########################");
         Runtime.getRuntime().halt(1);
      }

      public void end(final Xid arg0, final int arg1) throws XAException
      {
      }

      public void forget(final Xid arg0) throws XAException
      {
      }

      public int getTransactionTimeout() throws XAException
      {
         return 0;
      }

      public boolean isSameRM(final XAResource arg0) throws XAException
      {
         return false;
      }

      public int prepare(final Xid arg0) throws XAException
      {

         return XAResource.XA_OK;
      }

      public Xid[] recover(final int arg0) throws XAException
      {
         return null;
      }

      public void rollback(final Xid arg0) throws XAException
      {
      }

      public boolean setTransactionTimeout(final int arg0) throws XAException
      {
         return false;
      }

      public void start(final Xid arg0, final int arg1) throws XAException
      {
      }

   }
}

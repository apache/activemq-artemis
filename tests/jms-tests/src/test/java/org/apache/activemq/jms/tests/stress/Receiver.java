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
package org.apache.activemq.jms.tests.stress;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;

import org.apache.activemq.jms.tests.JmsTestLogger;

/**
 * Receives messages from a destination for stress testing
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class Receiver extends Runner implements MessageListener
{
   private static final JmsTestLogger log = JmsTestLogger.LOGGER;

   private static final long RECEIVE_TIMEOUT = 120000;

   protected MessageConsumer cons;

   protected int count;

   protected boolean isListener;

   protected Map<String, Object> counts = new HashMap<String, Object>();

   protected boolean isCC;

   protected Connection conn;

   protected ConnectionConsumer cc;

   private final Object lock1 = new Object();

   private final Object lock2 = new Object();

   private Message theMessage;

   private boolean finished;

   public Receiver(final Connection conn, final Session sess, final int numMessages, final Destination dest) throws Exception
   {
      super(sess, numMessages);

      isListener = true;

      isCC = true;

      sess.setMessageListener(this);

      cc = conn.createConnectionConsumer(dest, null, new MockServerSessionPool(sess), 10);

   }

   public Receiver(final Session sess, final MessageConsumer cons, final int numMessages, final boolean isListener) throws Exception
   {
      super(sess, numMessages);
      this.cons = cons;
      this.isListener = isListener;
      if (this.isListener)
      {
         cons.setMessageListener(this);
      }
   }

   private boolean done;

   public void onMessage(final Message m)
   {
      try
      {
         synchronized (lock1)
         {
            theMessage = m;

            lock1.notify();
         }

         // Wait for message to be processed
         synchronized (lock2)
         {
            while (!done && !finished)
            {
               lock2.wait();
            }
            done = false;
         }

      }
      catch (Exception e)
      {
         Receiver.log.error("Failed to put in channel", e);
         setFailed(true);
      }
   }

   protected void finished()
   {
      synchronized (lock2)
      {
         finished = true;
         lock2.notify();
      }
   }

   protected Message getMessage() throws Exception
   {
      Message m;

      if (isListener)
      {
         synchronized (lock1)
         {
            long start = System.currentTimeMillis();
            long waitTime = Receiver.RECEIVE_TIMEOUT;
            while (theMessage == null && waitTime >= 0)
            {
               lock1.wait(waitTime);

               waitTime = Receiver.RECEIVE_TIMEOUT - (System.currentTimeMillis() - start);
            }
            m = theMessage;
            theMessage = null;
         }
      }
      else
      {
         m = cons.receive(Receiver.RECEIVE_TIMEOUT);
      }

      return m;
   }

   protected void processingDone()
   {
      if (isListener)
      {
         synchronized (lock2)
         {
            done = true;
            lock2.notify();
         }
      }
   }

   @Override
   public void run()
   {

      // Small pause so as not to miss any messages in a topic
      try
      {
         Thread.sleep(1000);
      }
      catch (InterruptedException e)
      {
      }

      try
      {
         String prodName = null;
         Integer msgCount = null;

         while (count < numMessages)
         {
            Message m = getMessage();

            if (m == null)
            {
               Receiver.log.error("Message is null");
               setFailed(true);
               processingDone();
               return;
            }

            prodName = m.getStringProperty("PROD_NAME");
            msgCount = new Integer(m.getIntProperty("MSG_NUMBER"));

            // log.info(this + " Got message " + prodName + ":" + msgCount + "M: " + m.getJMSMessageID());

            Integer prevCount = (Integer)counts.get(prodName);
            if (prevCount == null)
            {
               if (msgCount.intValue() != 0)
               {
                  Receiver.log.error("First message received not zero");
                  setFailed(true);
                  processingDone();
                  return;
               }
            }
            else
            {
               if (prevCount.intValue() != msgCount.intValue() - 1)
               {
                  Receiver.log.error("Message out of sequence for " + prodName +
                                     ", expected:" +
                                     (prevCount.intValue() + 1) +
                                     " got " +
                                     msgCount);
                  setFailed(true);
                  processingDone();
                  return;
               }
            }
            counts.put(prodName, msgCount);

            count++;

            processingDone();
         }

      }
      catch (Exception e)
      {
         Receiver.log.error("Failed to receive message", e);
         setFailed(true);
      }
      finally
      {
         if (cc != null)
         {
            try
            {
               cc.close();
            }
            catch (JMSException e)
            {
               Receiver.log.error("Failed to close connection consumer", e);
            }
         }
      }
   }

   static final class MockServerSessionPool implements ServerSessionPool
   {
      private final ServerSession serverSession;

      MockServerSessionPool(final Session sess)
      {
         serverSession = new MockServerSession(sess);
      }

      public ServerSession getServerSession() throws JMSException
      {
         return serverSession;
      }
   }

   static final class MockServerSession implements ServerSession
   {
      Session session;

      MockServerSession(final Session sess)
      {
         session = sess;
      }

      public Session getSession() throws JMSException
      {
         return session;
      }

      public void start() throws JMSException
      {
         session.run();
      }

   }

}

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

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;

import org.apache.activemq.core.transaction.impl.XidImpl;
import org.apache.activemq.jms.tests.JmsTestLogger;
import org.apache.activemq.utils.UUIDGenerator;

/**
 *
 * A receiver that receives messages in a XA transaction
 *
 * Receives <commitSize> messages then prepares, commits, then
 * Receives <rollbackSize> messages then prepares, rollsback until
 * a total of <numMessages> messages have been received (committed)
 * <numMessages> must be a multiple of <commitSize>
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Transactional2PCReceiver extends Receiver
{
   private static final JmsTestLogger log = JmsTestLogger.LOGGER;

   protected int commitSize;

   protected int rollbackSize;

   protected XAResource xaResource;

   class Count
   {
      int lastCommitted;

      int lastReceived;
   }

   public Transactional2PCReceiver(final XASession sess,
                                   final MessageConsumer cons,
                                   final int numMessages,
                                   final int commitSize,
                                   final int rollbackSize,
                                   final boolean isListener) throws Exception
   {
      super(sess, cons, numMessages, isListener);
      this.commitSize = commitSize;
      this.rollbackSize = rollbackSize;
      xaResource = sess.getXAResource();
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
         int iterations = numMessages / commitSize;

         XidImpl xid = null;

         xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
         xaResource.start(xid, XAResource.TMNOFLAGS);

         for (int outerCount = 0; outerCount < iterations; outerCount++)
         {

            for (int innerCount = 0; innerCount < commitSize; innerCount++)
            {
               Message m = getMessage();

               if (m == null)
               {
                  Transactional2PCReceiver.log.error("Message is null");
                  setFailed(true);
                  return;
               }
               String prodName = m.getStringProperty("PROD_NAME");
               Integer msgCount = new Integer(m.getIntProperty("MSG_NUMBER"));

               Count count = (Count)counts.get(prodName);
               if (count == null)
               {
                  // First time
                  if (msgCount.intValue() != 0)
                  {
                     Transactional2PCReceiver.log.error("First message from " + prodName +
                                                        " is not 0, it is " +
                                                        msgCount);
                     setFailed(true);
                     return;
                  }
                  else
                  {
                     count = new Count();
                     counts.put(prodName, count);
                  }
               }
               else
               {
                  if (count.lastCommitted != msgCount.intValue() - 1)
                  {
                     Transactional2PCReceiver.log.error("Message out of sequence for " + prodName +
                                                        ", expected " +
                                                        (count.lastCommitted + 1) +
                                                        ", actual " +
                                                        msgCount);
                     setFailed(true);
                     return;
                  }
               }
               count.lastCommitted = msgCount.intValue();

               count.lastReceived = msgCount.intValue();

               if (innerCount == commitSize - 1)
               {
                  xaResource.end(xid, XAResource.TMSUCCESS);
                  xaResource.prepare(xid);
                  xaResource.commit(xid, false);

                  // Starting new tx
                  xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
                  xaResource.start(xid, XAResource.TMNOFLAGS);

               }

               processingDone();
            }

            if (outerCount == iterations - 1)
            {
               break;
            }

            for (int innerCount = 0; innerCount < rollbackSize; innerCount++)
            {
               Message m = getMessage();

               if (m == null)
               {
                  Transactional2PCReceiver.log.error("Message is null (rollback)");
                  setFailed(true);
                  return;
               }
               String prodName = m.getStringProperty("PROD_NAME");
               Integer msgCount = new Integer(m.getIntProperty("MSG_NUMBER"));

               Count count = (Count)counts.get(prodName);
               if (count == null)
               {
                  // First time
                  if (msgCount.intValue() != 0)
                  {
                     Transactional2PCReceiver.log.error("First message from " + prodName +
                                                        " is not 0, it is " +
                                                        msgCount);
                     setFailed(true);
                     return;
                  }
                  else
                  {
                     count = new Count();
                     count.lastCommitted = -1;
                     counts.put(prodName, count);
                  }
               }
               else
               {
                  if (count.lastReceived != msgCount.intValue() - 1)
                  {
                     Transactional2PCReceiver.log.error("Message out of sequence");
                     setFailed(true);
                     return;
                  }
               }
               count.lastReceived = msgCount.intValue();

               if (innerCount == rollbackSize - 1)
               {
                  xaResource.end(xid, XAResource.TMSUCCESS);
                  xaResource.prepare(xid);
                  xaResource.rollback(xid);

                  xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
                  xaResource.start(xid, XAResource.TMNOFLAGS);
               }
               processingDone();
            }
         }

         xaResource.end(xid, XAResource.TMSUCCESS);
         xaResource.prepare(xid);
         xaResource.commit(xid, false);

         finished();

      }
      catch (Exception e)
      {
         Transactional2PCReceiver.log.error("Failed to receive message", e);
         setFailed(true);
      }
   }
}

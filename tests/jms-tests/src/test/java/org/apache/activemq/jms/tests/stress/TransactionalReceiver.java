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
package org.apache.activemq.jms.tests.stress;

import org.apache.activemq.jms.tests.JmsTestLogger;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

/**
 *
 * A Receiver that receives messages from a destination in a JMS transaction
 *
 * Receives <commitSize> messages then commits, then
 * Receives <rollbackSize> messages then rollsback until
 * a total of <numMessages> messages have been received (committed)
 * <nuMessages> must be a multiple of <commitSize>
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TransactionalReceiver extends Receiver
{
   private static final JmsTestLogger log = JmsTestLogger.LOGGER;

   protected int commitSize;

   protected int rollbackSize;

   class Count
   {
      int lastCommitted;

      int lastReceived;
   }

   public TransactionalReceiver(final Session sess,
                                final MessageConsumer cons,
                                final int numMessages,
                                final int commitSize,
                                final int rollbackSize,
                                final boolean isListener) throws Exception
   {
      super(sess, cons, numMessages, isListener);
      this.commitSize = commitSize;
      this.rollbackSize = rollbackSize;
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

         for (int outerCount = 0; outerCount < iterations; outerCount++)
         {
            for (int innerCount = 0; innerCount < commitSize; innerCount++)
            {
               Message m = getMessage();

               if (m == null)
               {
                  TransactionalReceiver.log.error("Message is null");
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
                     TransactionalReceiver.log.error("First message from " + prodName + " is not 0, it is " + msgCount);
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
                     TransactionalReceiver.log.error("Message out of sequence for " + m.getJMSMessageID() +
                                                     " " +
                                                     prodName +
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
                  sess.commit();
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
                  TransactionalReceiver.log.error("Message is null");
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
                     TransactionalReceiver.log.error("First message from " + prodName + " is not 0, it is " + msgCount);
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
                     TransactionalReceiver.log.error("Message out of sequence");
                     setFailed(true);
                     return;
                  }
               }
               count.lastReceived = msgCount.intValue();

               if (innerCount == rollbackSize - 1 && outerCount != iterations - 1)
               {
                  // Don't roll back on the very last one
                  sess.rollback();
               }
               processingDone();
            }
         }
         finished();
      }
      catch (Exception e)
      {
         TransactionalReceiver.log.error("Failed to receive message", e);
         setFailed(true);
      }
   }
}
